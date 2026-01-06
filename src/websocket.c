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
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

#include "backend.h"
#include "gateway.h"
#include "mempool.h"
#include "utils.h"
#include "websocket.h"

ws_state_t *ws_states = NULL;
ws_send_state_t *ws_send_states = NULL;

void ws_init(void) {
  ws_states = calloc(MAX_CLIENTS, sizeof(ws_state_t));
  ws_send_states = calloc(MAX_CLIENTS, sizeof(ws_send_state_t));

  if (!ws_states || !ws_send_states) {
    fprintf(stderr, "[WebSocket] Failed to allocate state arrays\n");
    exit(1);
  }

  printf("[WebSocket] Initialized state arrays for %d clients\n", MAX_CLIENTS);
}

void ws_cleanup(void) {
  if (ws_states) {
    free(ws_states);
    ws_states = NULL;
  }
  if (ws_send_states) {
    free(ws_send_states);
    ws_send_states = NULL;
  }
}

void cleanup_stale_ws_sends(void) {
  time_t now = time(NULL);

  for (int i = 0; i < MAX_CLIENTS; i++) {
    ws_send_state_t *ss = &ws_send_states[i];

    if ((ss->header_sent > 0 || ss->data_sent > 0) &&
        now - ss->start_time > WS_SEND_TIMEOUT) {
      fprintf(stderr, "[WS] Aborting stuck send on client slot %d (timeout)\n",
              i);

      // Reset send state
      ss->header_sent = 0;
      ss->data_sent = 0;
      ss->header_len = 0;
      ss->total_len = 0;
      ss->start_time = 0;
      ss->retry_count = 0;

      // Close the stuck connection
      if (clients[i].fd > 0) {
        close(clients[i].fd);
        clients[i].fd = -1;
      }
    }
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

message_t *parse_ws_backend_frame(int fd) {
  // Safety checks
  if (!ws_states || fd < 0 || fd >= MAX_CLIENTS) {
    errno = EINVAL;
    return NULL;
  }

  ws_state_t *st = &ws_states[fd];

  // 1. Try to read MORE data if we don't have enough for a frame
  // Loop recv until EAGAIN - Critical for EPOLLET
  while (1) {
    // Rate limit check
    if (!check_rate_limit(fd)) {
      errno = EAGAIN;
      break;
    }

    size_t to_read = MAX_MESSAGE_SIZE - st->pos;
    if (to_read == 0) {
      fprintf(stderr, "[WebSocket] Buffer full for client %d, resetting\n", fd);
      st->pos = 0;
      return NULL;
    }

    if (to_read > MAX_RECV_PER_CALL) {
      to_read = MAX_RECV_PER_CALL;
    }

    ssize_t n = recv(fd, st->buffer + st->pos, to_read, MSG_DONTWAIT);

    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        break; // Drained OS buffer
      }
      st->pos = 0;
      return NULL;
    }

    if (n == 0) {
      // Socket closed
      st->pos = 0;
      return NULL;
    }

    st->pos += n;
    clients[fd].bytes_recv_this_sec += n;

    // Safety: if we have enough for a header, we can check if we have enough for the frame
    // but continuing to read until EAGAIN is actually faster/better for ET.
  }

  // 2. Parse frame from current buffer
  if (st->pos < 2) {
    errno = EAGAIN;
    return NULL;
  }

  uint8_t fin_opcode = st->buffer[0];
  uint8_t mask_len = st->buffer[1];

  uint8_t opcode = fin_opcode & 0x0F;
  if (opcode == WS_OPCODE_CLOSE) {
    st->pos = 0;
    return NULL;
  }

  // We only support single-frame messages or final frames for now
  // In a real impl, we'd need to reassemble fragmented messages.
  // Given the backend protocol uses its own framing, we usually get one WS frame per backend message.

  int masked = mask_len & WS_MASK;
  uint64_t payload_len = mask_len & 0x7F;
  uint32_t header_size = 2;

  if (payload_len == 126) {
    if (st->pos < 4) { errno = EAGAIN; return NULL; }
    payload_len = (st->buffer[2] << 8) | st->buffer[3];
    header_size = 4;
  } else if (payload_len == 127) {
    if (st->pos < 10) { errno = EAGAIN; return NULL; }
    payload_len = 0;
    for (int i = 0; i < 8; i++) {
      payload_len = (payload_len << 8) | st->buffer[2 + i];
    }
    header_size = 10;
  }

  if (masked)
    header_size += 4;

  uint32_t frame_size = header_size + payload_len;

  if (frame_size > MAX_MESSAGE_SIZE) {
    fprintf(stderr, "[WebSocket] Frame too large: %u bytes\n", frame_size);
    st->pos = 0;
    return NULL;
  }

  if (st->pos < frame_size) {
    errno = EAGAIN;
    return NULL;
  }

  if (payload_len < BACKEND_HEADER_SIZE) {
    fprintf(stderr, "[WebSocket] Payload too small for backend header\n");
    // Clear the bad frame
    if (st->pos > frame_size) {
      memmove(st->buffer, st->buffer + frame_size, st->pos - frame_size);
      st->pos -= frame_size;
    } else {
      st->pos = 0;
    }
    return NULL;
  }

  // Unmask payload in-place
  uint8_t *unmasked_payload;
  if (masked) {
    uint8_t *mask = &st->buffer[header_size - 4];
    uint8_t *payload = &st->buffer[header_size];
    for (uint64_t i = 0; i < payload_len; i++) {
      payload[i] = payload[i] ^ mask[i % 4];
    }
    unmasked_payload = payload;
  } else {
    unmasked_payload = &st->buffer[header_size];
  }

  // Parse backend header
  uint32_t network_len, network_client_id, network_backend_id;
  memcpy(&network_len, unmasked_payload, 4);
  memcpy(&network_client_id, unmasked_payload + 4, 4);
  memcpy(&network_backend_id, unmasked_payload + 8, 4);

  uint32_t backend_payload_len = ntohl(network_len);
  uint32_t client_id = ntohl(network_client_id);
  uint32_t backend_id = ntohl(network_backend_id);

  if (backend_payload_len != payload_len - BACKEND_HEADER_SIZE) {
    fprintf(stderr, "[WebSocket] Backend header length mismatch: expect %u, got %lu\n",
            backend_payload_len, payload_len - BACKEND_HEADER_SIZE);
    // Clear bad data
    if (st->pos > frame_size) {
      memmove(st->buffer, st->buffer + frame_size, st->pos - frame_size);
      st->pos -= frame_size;
    } else {
      st->pos = 0;
    }
    return NULL;
  }

  // Allocate message
  message_t *msg = msg_alloc(backend_payload_len);
  if (!msg) {
    fprintf(stderr, "[WebSocket] Failed to allocate message\n");
    st->pos = 0;
    return NULL;
  }

  msg->client_id = client_id;
  msg->backend_id = backend_id;
  msg->len = backend_payload_len;
  msg->timestamp_ns = get_time_ns();

  memcpy(msg->data, unmasked_payload + BACKEND_HEADER_SIZE,
         backend_payload_len);

  // Handle remaining data
  if (st->pos > frame_size) {
    uint32_t remaining = st->pos - frame_size;
    memmove(st->buffer, st->buffer + frame_size, remaining);
    st->pos = remaining;
  } else {
    st->pos = 0;
  }

  return msg;
}

int send_ws_backend_frame(int fd, message_t *msg) {
  // Safety check
  if (!ws_send_states || fd < 0 || fd >= MAX_CLIENTS) {
    return -1;
  }

  ws_send_state_t *ss = &ws_send_states[fd];

  // First call - initialize
  if (ss->header_sent == 0 && ss->data_sent == 0) {
    ss->total_len = BACKEND_HEADER_SIZE + msg->len;
    ss->start_time = time(NULL);

    // Build WebSocket header
    ss->header[0] = WS_FIN | WS_OPCODE_BIN;

    if (ss->total_len < 126) {
      ss->header[1] = ss->total_len;
      ss->header_len = 2;
    } else if (ss->total_len < 65536) {
      ss->header[1] = 126;
      uint16_t len16 = htons((uint16_t)ss->total_len);
      memcpy(&ss->header[2], &len16, 2);
      ss->header_len = 4;
    } else {
      ss->header[1] = 127;
      uint64_t len64 = (uint64_t)ss->total_len;
      uint32_t high = htonl((uint32_t)(len64 >> 32));
      uint32_t low = htonl((uint32_t)(len64 & 0xFFFFFFFF));
      memcpy(&ss->header[2], &high, 4);
      memcpy(&ss->header[6], &low, 4);
      ss->header_len = 10;
    }

    // Append backend header
    uint32_t network_len = htonl(msg->len);
    uint32_t network_client_id = htonl(msg->client_id);
    uint32_t network_backend_id = htonl(msg->backend_id);

    memcpy(&ss->header[ss->header_len], &network_len, 4);
    memcpy(&ss->header[ss->header_len + 4], &network_client_id, 4);
    memcpy(&ss->header[ss->header_len + 8], &network_backend_id, 4);

    ss->header_len += BACKEND_HEADER_SIZE;
  }

  // Check timeout
  if (time(NULL) - ss->start_time > WS_SEND_TIMEOUT) {
    ss->retry_count++;
    if (ss->retry_count > 3) {
      ss->header_sent = 0;
      ss->data_sent = 0;
      ss->header_len = 0;
      ss->retry_count = 0;
      return -1;
    }
  }

  // Optimized Linux Send: Use writev to send header and data in one syscall if possible
  while (ss->header_sent < ss->header_len || ss->data_sent < msg->len) {
    struct iovec iov[2];
    int iovcnt = 0;

    if (ss->header_sent < ss->header_len) {
      iov[iovcnt].iov_base = ss->header + ss->header_sent;
      iov[iovcnt].iov_len = ss->header_len - ss->header_sent;
      iovcnt++;
    }

    if (ss->data_sent < msg->len) {
      iov[iovcnt].iov_base = msg->data + ss->data_sent;
      iov[iovcnt].iov_len = msg->len - ss->data_sent;
      iovcnt++;
    }

    if (iovcnt == 0) break;

    ssize_t sent = writev(fd, iov, iovcnt);

    if (sent < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return 1;
      }
      ss->header_sent = 0;
      ss->data_sent = 0;
      ss->header_len = 0;
      ss->retry_count = 0;
      return -1;
    }

    // Distribute 'sent' bytes between header and data
    size_t remaining = (size_t)sent;

    if (ss->header_sent < ss->header_len) {
      size_t header_to_ack = ss->header_len - ss->header_sent;
      if (remaining >= header_to_ack) {
        remaining -= header_to_ack;
        ss->header_sent = ss->header_len;
      } else {
        ss->header_sent += remaining;
        remaining = 0;
      }
    }

    if (remaining > 0 && ss->data_sent < msg->len) {
      ss->data_sent += remaining;
    }
  }

  // Complete - reset
  ss->header_sent = 0;
  ss->data_sent = 0;
  ss->header_len = 0;
  ss->total_len = 0;
  ss->start_time = 0;
  ss->retry_count = 0;

  return 0;
}
