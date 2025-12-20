#include <arpa/inet.h>
#include <errno.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "backend.h"
#include "gateway.h"
#include "mempool.h"
#include "utils.h"
#include "websocket.h"

ws_read_state_t *ws_read_states = NULL;
ws_send_state_t *ws_send_states = NULL;

void ws_init(void) {
  ws_read_states = calloc(MAX_CLIENTS, sizeof(ws_read_state_t));
  ws_send_states = calloc(MAX_CLIENTS, sizeof(ws_send_state_t));

  if (!ws_read_states || !ws_send_states) {
    fprintf(stderr, "[WebSocket] Failed to allocate state arrays\n");
    exit(1);
  }

  printf("[WebSocket] Initialized state arrays for %d clients\n", MAX_CLIENTS);
}

void ws_cleanup(void) {
  if (ws_read_states) {
    // Free any active contexts using unref
    for (int i = 0; i < MAX_CLIENTS; i++) {
      if (ws_read_states[i].ctx) {
        msg_context_unref(ws_read_states[i].ctx);
        ws_read_states[i].ctx = NULL;
      }
    }
    free(ws_read_states);
    ws_read_states = NULL;
  }
  if (ws_send_states) {
    free(ws_send_states);
    ws_send_states = NULL;
  }
}

char *base64_encode(const unsigned char *input, int length) {
  BIO *bio, *b64;
  BUF_MEM *buffer_ptr;

  b64 = BIO_new(BIO_f_base64());
  bio = BIO_new(BIO_s_mem());
  bio = BIO_push(b64, bio);

  BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
  BIO_write(bio, input, length);
  BIO_flush(bio);
  BIO_get_mem_ptr(bio, &buffer_ptr);

  char *result = malloc(buffer_ptr->length + 1);
  memcpy(result, buffer_ptr->data, buffer_ptr->length);
  result[buffer_ptr->length] = '\0';

  BIO_free_all(bio);
  return result;
}

int handle_ws_handshake(int fd) {
  char buffer[4096];
  ssize_t n = recv(fd, buffer, sizeof(buffer) - 1, 0);
  if (n <= 0)
    return -1;

  buffer[n] = '\0';

  char *key_start = strstr(buffer, "Sec-WebSocket-Key: ");
  if (!key_start)
    return -1;

  key_start += 19;
  char *key_end = strstr(key_start, "\r\n");
  if (!key_end)
    return -1;

  char key[256];
  int key_len = key_end - key_start;
  memcpy(key, key_start, key_len);
  key[key_len] = '\0';

  char concat[512];
  snprintf(concat, sizeof(concat), "%s258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
           key);

  unsigned char hash[SHA_DIGEST_LENGTH];
  SHA1((unsigned char *)concat, strlen(concat), hash);

  char *accept_key = base64_encode(hash, SHA_DIGEST_LENGTH);

  char response[1024];
  int len = snprintf(response, sizeof(response),
                     "HTTP/1.1 101 Switching Protocols\r\n"
                     "Upgrade: websocket\r\n"
                     "Connection: Upgrade\r\n"
                     "Sec-WebSocket-Accept: %s\r\n\r\n",
                     accept_key);

  free(accept_key);
  send(fd, response, len, 0);
  return 0;
}

chunk_t *parse_ws_chunk(int fd) {
  ws_read_state_t *st = &ws_read_states[fd % MAX_CLIENTS];

  // Try to receive data into buffer
  if (st->pos < sizeof(st->buffer)) {
    ssize_t n = recv(fd, st->buffer + st->pos, sizeof(st->buffer) - st->pos,
                     MSG_DONTWAIT);

    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // No data available
      } else {
        // Error - reset state
        st->pos = 0;
        st->header_parsed = 0;
        st->chunks_created = 0;
        if (st->ctx) {
          msg_context_unref(st->ctx);
          st->ctx = NULL;
        }
        return NULL;
      }
    } else if (n == 0) {
      // Connection closed
      st->pos = 0;
      st->header_parsed = 0;
      st->chunks_created = 0;
      if (st->ctx) {
        msg_context_unref(st->ctx);
        st->ctx = NULL;
      }
      return NULL;
    } else {
      st->pos += n;
    }
  }

  // Step 1: Parse WebSocket frame header
  if (!st->header_parsed) {
    if (st->pos < 2)
      return NULL;

    uint8_t fin_opcode = st->buffer[0];
    uint8_t mask_len = st->buffer[1];

    uint8_t opcode = fin_opcode & 0x0F;
    if (opcode == WS_OPCODE_CLOSE) {
      st->pos = 0;
      st->chunks_created = 0;
      if (st->ctx) {
        msg_context_unref(st->ctx);
        st->ctx = NULL;
      }
      return NULL;
    }

    if (!(fin_opcode & WS_FIN)) {
      // Fragmented frames not supported in this context
      st->pos = 0;
      st->chunks_created = 0;
      if (st->ctx) {
        msg_context_unref(st->ctx);
        st->ctx = NULL;
      }
      return NULL;
    }

    st->masked = mask_len & WS_MASK;
    st->payload_len = mask_len & 0x7F;
    st->header_size = 2;

    // Extended payload length
    if (st->payload_len == 126) {
      if (st->pos < 4)
        return NULL;
      st->payload_len = (st->buffer[2] << 8) | st->buffer[3];
      st->header_size = 4;
    } else if (st->payload_len == 127) {
      if (st->pos < 10)
        return NULL;
      st->payload_len = 0;
      for (int i = 0; i < 8; i++) {
        st->payload_len = (st->payload_len << 8) | st->buffer[2 + i];
      }
      st->header_size = 10;
    }

    if (st->masked) {
      if (st->pos < st->header_size + 4)
        return NULL;
      memcpy(st->mask, st->buffer + st->header_size, 4);
      st->header_size += 4;
    }

    st->header_parsed = 1;
    st->payload_read = 0;
    st->chunks_created = 0; // Reset chunk counter for new message

    // Payload format: [12-byte backend header][actual data]
    if (st->payload_len < BACKEND_HEADER_SIZE) {
      fprintf(stderr, "[WebSocket] Payload too small for backend header\n");
      st->pos = 0;
      st->header_parsed = 0;
      st->chunks_created = 0;
      if (st->ctx) {
        msg_context_unref(st->ctx);
        st->ctx = NULL;
      }
      return NULL;
    }

    // Check if we have backend header in buffer
    uint32_t available_payload = st->pos - st->header_size;
    if (available_payload < BACKEND_HEADER_SIZE) {
      // Need more data for backend header
      return NULL;
    }

    // Unmask and parse backend header
    uint8_t backend_header[BACKEND_HEADER_SIZE];
    for (int i = 0; i < BACKEND_HEADER_SIZE; i++) {
      uint8_t byte = st->buffer[st->header_size + i];
      if (st->masked) {
        byte ^= st->mask[i % 4];
      }
      backend_header[i] = byte;
    }

    uint32_t network_len, network_client_id, network_backend_id;
    memcpy(&network_len, backend_header, 4);
    memcpy(&network_client_id, backend_header + 4, 4);
    memcpy(&network_backend_id, backend_header + 8, 4);

    uint32_t backend_payload_len = ntohl(network_len);
    uint32_t client_id = ntohl(network_client_id);
    uint32_t backend_id = ntohl(network_backend_id);

    // Validate
    if (backend_payload_len != st->payload_len - BACKEND_HEADER_SIZE) {
      fprintf(stderr, "[WebSocket] Backend header length mismatch\n");
      st->pos = 0;
      st->header_parsed = 0;
      st->chunks_created = 0;
      if (st->ctx) {
        msg_context_unref(st->ctx);
        st->ctx = NULL;
      }
      return NULL;
    }

    // Create message context (ref_count starts at 1)
    st->ctx = msg_context_alloc();
    if (!st->ctx) {
      fprintf(stderr, "[WebSocket] Failed to allocate message context\n");
      st->pos = 0;
      st->header_parsed = 0;
      st->chunks_created = 0;
      return NULL;
    }

    st->ctx->client_id = client_id;
    st->ctx->backend_id = backend_id;
    st->ctx->total_len = backend_payload_len;
    st->ctx->bytes_sent = 0;
    st->ctx->complete = 0;
    st->ctx->timestamp_ns = get_time_ns();

    st->payload_read = BACKEND_HEADER_SIZE; // Mark backend header as read
  }

  // Step 2: Extract data chunks from buffer
  uint32_t remaining_in_message = (st->payload_len - st->payload_read);
  if (remaining_in_message == 0) {
    st->ctx->complete = 1;
    // Message complete - release read state's reference
    msg_context_unref(st->ctx);
    st->ctx = NULL;
    st->pos = 0;
    st->header_parsed = 0;
    st->chunks_created = 0;
    return NULL;
  }

  uint32_t available_in_buffer = st->pos - st->header_size - st->payload_read;
  if (available_in_buffer == 0) {
    // Need more data
    return NULL;
  }

  uint32_t chunk_data_size = (remaining_in_message > MAX_CHUNK_SIZE)
                                 ? MAX_CHUNK_SIZE
                                 : remaining_in_message;
  uint32_t to_read = (available_in_buffer > chunk_data_size)
                         ? chunk_data_size
                         : available_in_buffer;

  chunk_t *chunk = chunk_alloc(to_read);
  if (!chunk) {
    fprintf(stderr, "[WebSocket] Failed to allocate chunk\n");
    return NULL;
  }

  // Copy and unmask data
  uint32_t src_offset = st->header_size + st->payload_read;
  for (uint32_t i = 0; i < to_read; i++) {
    uint8_t byte = st->buffer[src_offset + i];
    if (st->masked) {
      byte ^= st->mask[(st->payload_read + i) % 4];
    }
    chunk->data[i] = byte;
  }

  chunk->ctx = st->ctx;
  msg_context_ref(st->ctx);
  chunk->chunk_index = st->chunks_created;
  st->chunks_created++; // Increment AFTER assignment
  chunk->len = to_read;
  chunk->is_last_chunk =
      (st->payload_read + to_read >= st->payload_len) ? 1 : 0;
  chunk->is_first_chunk = (chunk->chunk_index == 0) ? 1 : 0;
  st->payload_read += to_read;

  // Check if message is complete
  if (st->payload_read >= st->payload_len) {
    st->ctx->complete = 1;
    // Message complete - release read state's reference
    msg_context_unref(st->ctx);
    st->ctx = NULL;
    st->chunks_created = 0;

    // Handle remaining data in buffer (next frame)
    uint32_t frame_total = st->header_size + st->payload_len;
    if (st->pos > frame_total) {
      uint32_t remaining = st->pos - frame_total;
      memmove(st->buffer, st->buffer + frame_total, remaining);
      st->pos = remaining;
    } else {
      st->pos = 0;
    }
    st->header_parsed = 0;
  }

  return chunk;
}

int send_ws_chunk(int fd, chunk_t *chunk) {
  ws_send_state_t *ss = &ws_send_states[fd % MAX_CLIENTS];

  // If this is a different chunk, reset state
  if (ss->current_chunk != chunk) {
    ss->current_chunk = chunk;
    ss->header_sent = 0;
    ss->data_sent = 0;

    // If this is the first chunk, create WebSocket + backend headers
    if (chunk->is_first_chunk && chunk->ctx) {
      uint32_t total_payload = BACKEND_HEADER_SIZE + chunk->ctx->total_len;

      // Build WebSocket header
      ss->header[0] = WS_FIN | WS_OPCODE_BIN;

      if (total_payload < 126) {
        ss->header[1] = total_payload;
        ss->header_len = 2;
      } else if (total_payload < 65536) {
        ss->header[1] = 126;
        uint16_t len16 = htons((uint16_t)total_payload);
        memcpy(&ss->header[2], &len16, 2);
        ss->header_len = 4;
      } else {
        ss->header[1] = 127;
        uint64_t len64 = (uint64_t)total_payload;
        uint32_t high = htonl((uint32_t)(len64 >> 32));
        uint32_t low = htonl((uint32_t)(len64 & 0xFFFFFFFF));
        memcpy(&ss->header[2], &high, 4);
        memcpy(&ss->header[6], &low, 4);
        ss->header_len = 10;
      }

      // Append backend header
      uint32_t network_len = htonl(chunk->ctx->total_len);
      uint32_t network_client_id = htonl(chunk->ctx->client_id);
      uint32_t network_backend_id = htonl(chunk->ctx->backend_id);

      memcpy(&ss->header[ss->header_len], &network_len, 4);
      memcpy(&ss->header[ss->header_len + 4], &network_client_id, 4);
      memcpy(&ss->header[ss->header_len + 8], &network_backend_id, 4);

      ss->header_len += BACKEND_HEADER_SIZE;
    } else {
      // Not first chunk, skip header
      ss->header_sent = ss->header_len;
    }
  }

  // Step 1: Send headers if needed
  while (ss->header_sent < ss->header_len) {
    ssize_t sent =
        send(fd, ss->header + ss->header_sent, ss->header_len - ss->header_sent,
             MSG_DONTWAIT | MSG_NOSIGNAL);

    if (sent < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return 1; // Partial send
      }
      // Error
      ss->current_chunk = NULL;
      ss->header_sent = 0;
      ss->data_sent = 0;
      return -1;
    }

    ss->header_sent += sent;
  }

  // Step 2: Send chunk data
  while (ss->data_sent < chunk->len) {
    ssize_t sent =
        send(fd, chunk->data + ss->data_sent, chunk->len - ss->data_sent,
             MSG_DONTWAIT | MSG_NOSIGNAL);

    if (sent < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return 1; // Partial send
      }
      // Error
      ss->current_chunk = NULL;
      ss->header_sent = 0;
      ss->data_sent = 0;
      return -1;
    }

    ss->data_sent += sent;
  }

  // Complete send
  ss->current_chunk = NULL;
  ss->header_sent = 0;
  ss->data_sent = 0;
  ss->header_len = 0;

  return 0; // Success
}
