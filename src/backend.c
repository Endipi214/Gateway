#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "backend.h"
#include "gateway.h"
#include "mempool.h"
#include "utils.h"

backend_read_state_t *backend_read_states = NULL;
backend_send_state_t *backend_send_states = NULL;

void backend_init(void) {
  backend_read_states = calloc(MAX_BACKENDS, sizeof(backend_read_state_t));
  backend_send_states = calloc(MAX_BACKENDS, sizeof(backend_send_state_t));

  if (!backend_read_states || !backend_send_states) {
    fprintf(stderr, "[Backend] Failed to allocate state arrays\n");
    exit(1);
  }

  printf("[Backend] Initialized state arrays for %d backends\n", MAX_BACKENDS);
}

void backend_cleanup(void) {
  if (backend_read_states) {
    // Free any active contexts using unref
    for (int i = 0; i < MAX_BACKENDS; i++) {
      if (backend_read_states[i].ctx) {
        msg_context_unref(backend_read_states[i].ctx);
        backend_read_states[i].ctx = NULL;
      }
    }
    free(backend_read_states);
    backend_read_states = NULL;
  }
  if (backend_send_states) {
    free(backend_send_states);
    backend_send_states = NULL;
  }
}

chunk_t *read_backend_chunk(int fd) {
  backend_read_state_t *st = &backend_read_states[fd % MAX_BACKENDS];

  // Step 1: Read header if not complete
  if (!st->header_complete) {
    while (st->header_pos < BACKEND_HEADER_SIZE) {
      ssize_t n = recv(fd, st->header_buf + st->header_pos,
                       BACKEND_HEADER_SIZE - st->header_pos, MSG_DONTWAIT);

      if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          return NULL; // Need more data
        }
        // Error - reset state
        st->header_pos = 0;
        st->header_complete = 0;
        st->chunks_created = 0;
        if (st->ctx) {
          msg_context_unref(st->ctx);
          st->ctx = NULL;
        }
        return NULL;
      }

      if (n == 0) {
        // Connection closed
        st->header_pos = 0;
        st->header_complete = 0;
        st->chunks_created = 0;
        if (st->ctx) {
          msg_context_unref(st->ctx);
          st->ctx = NULL;
        }
        return NULL;
      }

      st->header_pos += n;
    }

    // Parse header
    uint32_t network_len, network_client_id, network_backend_id;
    memcpy(&network_len, st->header_buf, 4);
    memcpy(&network_client_id, st->header_buf + 4, 4);
    memcpy(&network_backend_id, st->header_buf + 8, 4);

    st->expected_len = ntohl(network_len);
    st->client_id = ntohl(network_client_id);
    st->backend_id = ntohl(network_backend_id);
    st->header_complete = 1;

    // Create message context (ref_count starts at 1)
    st->ctx = msg_context_alloc();
    if (!st->ctx) {
      fprintf(stderr, "[Backend] Failed to allocate message context\n");
      st->header_pos = 0;
      st->header_complete = 0;
      st->chunks_created = 0;
      return NULL;
    }

    st->ctx->client_id = st->client_id;
    st->ctx->backend_id = st->backend_id;
    st->ctx->total_len = st->expected_len;
    st->ctx->bytes_sent = 0;
    st->ctx->complete = 0;
    st->ctx->timestamp_ns = get_time_ns();

    st->bytes_read = 0;
    st->chunks_created = 0; // Reset chunk counter for new message
  }

  // Step 2: Read payload in chunks
  uint32_t remaining = st->expected_len - st->bytes_read;
  if (remaining == 0) {
    st->ctx->complete = 1;
    // Message complete - release our reference and reset
    msg_context_unref(st->ctx);
    st->ctx = NULL;
    st->header_pos = 0;
    st->header_complete = 0;
    st->expected_len = 0;
    st->bytes_read = 0;
    st->chunks_created = 0;
    return NULL;
  }

  uint32_t chunk_size =
      (remaining > MAX_CHUNK_SIZE) ? MAX_CHUNK_SIZE : remaining;

  chunk_t *chunk = chunk_alloc(chunk_size);
  if (!chunk) {
    fprintf(stderr, "[Backend] Failed to allocate chunk\n");
    return NULL;
  }

  // Read data into chunk
  uint32_t to_read = chunk_size;
  uint32_t chunk_pos = 0;

  while (chunk_pos < to_read) {
    ssize_t n =
        recv(fd, chunk->data + chunk_pos, to_read - chunk_pos, MSG_DONTWAIT);

    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        if (chunk_pos > 0) {
          // Partial chunk received - save it
          chunk->ctx = st->ctx;
          msg_context_ref(st->ctx);
          chunk->chunk_index = st->chunks_created;
          st->chunks_created++; // Increment AFTER assignment
          chunk->len = chunk_pos;
          chunk->is_last_chunk = 0;
          chunk->is_first_chunk = (chunk->chunk_index == 0) ? 1 : 0;
          st->bytes_read += chunk_pos;
          return chunk;
        }
        // No data yet
        chunk_free(chunk);
        return NULL;
      }
      // Error
      chunk_free(chunk);
      st->header_pos = 0;
      st->header_complete = 0;
      st->chunks_created = 0;
      if (st->ctx) {
        msg_context_unref(st->ctx);
        st->ctx = NULL;
      }
      return NULL;
    }

    if (n == 0) {
      // Connection closed
      chunk_free(chunk);
      st->header_pos = 0;
      st->header_complete = 0;
      st->chunks_created = 0;
      if (st->ctx) {
        msg_context_unref(st->ctx);
        st->ctx = NULL;
      }
      return NULL;
    }

    chunk_pos += n;
  }

  // Complete chunk
  chunk->ctx = st->ctx;
  msg_context_ref(st->ctx);
  chunk->chunk_index = st->chunks_created;
  st->chunks_created++; // Increment AFTER assignment
  chunk->len = chunk_pos;
  chunk->is_last_chunk =
      (st->bytes_read + chunk_pos >= st->expected_len) ? 1 : 0;
  chunk->is_first_chunk = (chunk->chunk_index == 0) ? 1 : 0;
  st->bytes_read += chunk_pos;

  // Check if this is the last chunk
  if (st->bytes_read >= st->expected_len) {
    st->ctx->complete = 1;
    // Message reading complete - release read state's reference
    msg_context_unref(st->ctx);
    st->ctx = NULL;
    st->header_pos = 0;
    st->header_complete = 0;
    st->expected_len = 0;
    st->bytes_read = 0;
    st->chunks_created = 0;
  }

  return chunk;
}

int write_backend_chunk(int fd, chunk_t *chunk) {
  backend_send_state_t *ss = &backend_send_states[fd % MAX_BACKENDS];

  // If this is a different chunk, reset state
  if (ss->current_chunk != chunk) {
    ss->current_chunk = chunk;
    ss->header_sent = 0;
    ss->data_sent = 0;

    // If this is the first chunk of the message, prepare header
    if (chunk->is_first_chunk && chunk->ctx) {
      uint32_t network_len = htonl(chunk->ctx->total_len);
      uint32_t network_client_id = htonl(chunk->ctx->client_id);
      uint32_t network_backend_id = htonl(chunk->ctx->backend_id);

      memcpy(ss->header, &network_len, 4);
      memcpy(ss->header + 4, &network_client_id, 4);
      memcpy(ss->header + 8, &network_backend_id, 4);
    } else {
      // Not first chunk, skip header
      ss->header_sent = BACKEND_HEADER_SIZE;
    }
  }

  // Step 1: Send header if needed
  while (ss->header_sent < BACKEND_HEADER_SIZE) {
    ssize_t sent = send(fd, ss->header + ss->header_sent,
                        BACKEND_HEADER_SIZE - ss->header_sent,
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

  // Complete send - reset state
  ss->current_chunk = NULL;
  ss->header_sent = 0;
  ss->data_sent = 0;

  return 0; // Success
}

int connect_to_backend(const char *host, int port) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return -1;

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, host, &addr.sin_addr);

  if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    close(fd);
    return -1;
  }

  set_nonblocking(fd);
  set_tcp_nodelay(fd);

  return fd;
}
