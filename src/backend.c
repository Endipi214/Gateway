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

backend_state_t *backend_states = NULL;
backend_send_state_t *backend_send_states = NULL;

// Initialize backend state arrays
void backend_init(void) {
  backend_states = calloc(MAX_BACKENDS, sizeof(backend_state_t));
  backend_send_states = calloc(MAX_BACKENDS, sizeof(backend_send_state_t));

  if (!backend_states || !backend_send_states) {
    fprintf(stderr, "[Backend] Failed to allocate state arrays\n");
    exit(1);
  }

  printf("[Backend] Initialized state arrays for %d backends\n", MAX_BACKENDS);
}

void backend_cleanup(void) {
  if (backend_states) {
    free(backend_states);
    backend_states = NULL;
  }
  if (backend_send_states) {
    free(backend_send_states);
    backend_send_states = NULL;
  }
}

// Backend protocol API
message_t *read_backend_frame(int fd) {
  backend_state_t *st = &backend_states[fd % MAX_BACKENDS];

  // Try to receive data
  ssize_t n = recv(fd, st->buffer + st->pos, sizeof(st->buffer) - st->pos,
                   MSG_DONTWAIT);

  if (n < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return NULL; // No data available, try again later
    }
    // Real error
    return NULL;
  }

  if (n == 0) {
    // Connection closed
    return NULL;
  }

  st->pos += n;

  // Step 1: Read header (8 bytes: 4 len + 4 backend_id)
  if (!st->header_complete && st->pos >= BACKEND_HEADER_SIZE) {
    uint32_t network_len, network_id;
    memcpy(&network_len, st->buffer, 4);
    memcpy(&network_id, st->buffer + 4, 4);

    st->expected_len = ntohl(network_len);
    st->backend_id = ntohl(network_id);
    st->header_complete = 1;

    // Validate length
    if (st->expected_len > MAX_MESSAGE_SIZE) {
      // Invalid frame, reset state
      st->pos = 0;
      st->expected_len = 0;
      st->backend_id = 0;
      st->header_complete = 0;
      return NULL;
    }
  }

  // Step 2: Read payload
  if (st->header_complete &&
      st->pos >= BACKEND_HEADER_SIZE + st->expected_len) {
    // Complete frame received
    message_t *msg = msg_alloc(st->expected_len);
    if (!msg) {
      // Allocation failed, reset and drop frame
      st->pos = 0;
      st->expected_len = 0;
      st->backend_id = 0;
      st->header_complete = 0;
      return NULL;
    }

    msg->backend_fd = fd;
    msg->len = st->expected_len;
    msg->timestamp_ns = get_time_ns();

    // Copy payload data
    memcpy(msg->data, st->buffer + BACKEND_HEADER_SIZE, st->expected_len);

    // Handle remaining data in buffer (next frame)
    uint32_t frame_size = BACKEND_HEADER_SIZE + st->expected_len;
    if (st->pos > frame_size) {
      memmove(st->buffer, st->buffer + frame_size, st->pos - frame_size);
      st->pos -= frame_size;
    } else {
      st->pos = 0;
    }

    // Reset state for next frame
    st->expected_len = 0;
    st->backend_id = 0;
    st->header_complete = 0;

    return msg;
  }

  // Incomplete frame, need more data
  return NULL;
}

int write_backend_frame(int fd, message_t *msg, uint32_t backend_id) {
  backend_send_state_t *ss = &backend_send_states[fd % MAX_BACKENDS];

  // First call for this message - initialize send state
  if (ss->header_sent == 0 && ss->data_sent == 0) {
    ss->total_len = msg->len;

    // Prepare header: [4 bytes len][4 bytes backend_id]
    uint32_t network_len = htonl(msg->len);
    uint32_t network_id = htonl(backend_id);
    memcpy(ss->header, &network_len, 4);
    memcpy(ss->header + 4, &network_id, 4);
  }

  // Step 1: Send header if not complete
  while (ss->header_sent < BACKEND_HEADER_SIZE) {
    ssize_t sent = send(fd, ss->header + ss->header_sent,
                        BACKEND_HEADER_SIZE - ss->header_sent,
                        MSG_DONTWAIT | MSG_NOSIGNAL);

    if (sent < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return 1; // Partial send, call again later
      }
      // Error occurred
      ss->header_sent = 0;
      ss->data_sent = 0;
      return -1;
    }

    ss->header_sent += sent;
  }

  // Step 2: Send payload data
  while (ss->data_sent < ss->total_len) {
    ssize_t sent =
        send(fd, msg->data + ss->data_sent, ss->total_len - ss->data_sent,
             MSG_DONTWAIT | MSG_NOSIGNAL);

    if (sent < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return 1; // Partial send, call again later
      }
      // Error occurred
      ss->header_sent = 0;
      ss->data_sent = 0;
      return -1;
    }

    ss->data_sent += sent;
  }

  // Complete send - reset state
  ss->header_sent = 0;
  ss->data_sent = 0;
  ss->total_len = 0;

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
