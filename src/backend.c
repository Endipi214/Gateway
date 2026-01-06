#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <netdb.h>

#include "backend.h"
#include "gateway.h"
#include "mempool.h"
#include "utils.h"

backend_state_t *backend_states = NULL;
backend_send_state_t *backend_send_states = NULL;

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

void cleanup_stale_backend_sends(void) {
  time_t now = time(NULL);

  for (int i = 0; i < MAX_BACKENDS; i++) {
    backend_send_state_t *ss = &backend_send_states[i];

    if ((ss->header_sent > 0 || ss->data_sent > 0) &&
        now - ss->start_time > BACKEND_SEND_TIMEOUT) {
      fprintf(stderr,
              "[Backend] Aborting stuck send on backend slot %d (timeout)\n",
              i);

      ss->header_sent = 0;
      ss->data_sent = 0;
      ss->total_len = 0;
      ss->start_time = 0;
      ss->retry_count = 0;
    }
  }
}

int check_backend_rate_limit(int fd) {
  if (fd < 0)
    return 0;

  backend_state_t *st = &backend_states[fd % MAX_BACKENDS];

  if (st->max_bytes_per_sec == 0)
    return 1;

  time_t now = time(NULL);

  if (now != st->rate_limit_window) {
    st->rate_limit_window = now;
    st->bytes_recv_this_sec = 0;
  }

  if (st->bytes_recv_this_sec >= st->max_bytes_per_sec) {
    return 0;
  }

  return 1;
}

message_t *read_backend_frame(int fd) {
  backend_state_t *st = &backend_states[fd % MAX_BACKENDS];

  // Check rate limit
  if (!check_backend_rate_limit(fd)) {
    errno = EAGAIN;
    return NULL;
  }

  // Step 1: Try to read ALL available data (loop until EAGAIN)
  // This is CRITICAL for edge-triggered epoll (EPOLLET)
  int total_read_this_call = 0;

  while (1) {
    size_t space_available = sizeof(st->buffer) - st->pos;

    if (space_available == 0) {
      fprintf(stderr,
              "[Backend] Buffer full on fd %d (pos=%u), likely invalid frame - "
              "resetting\n",
              fd, st->pos);
      st->pos = 0;
      st->expected_len = 0;
      st->client_id = 0;
      st->backend_id = 0;
      st->header_complete = 0;
      return NULL;
    }

    size_t to_read = (space_available < MAX_RECV_PER_CALL) ? space_available
                                                           : MAX_RECV_PER_CALL;

    ssize_t n = recv(fd, st->buffer + st->pos, to_read, MSG_DONTWAIT);

    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // No more data available right now - this is normal!
        // If we have partial frame, just wait for next epoll event
        // Don't change errno - it's already EAGAIN

        // If we read some data this call, continue to parsing
        if (total_read_this_call > 0) {
          break; // Exit read loop, try to parse what we have
        }
        return NULL; // No new data at all
      }
      // Real error - reset state
      fprintf(stderr, "[Backend] recv error on fd %d: %s\n", fd,
              strerror(errno));
      st->pos = 0;
      st->expected_len = 0;
      st->client_id = 0;
      st->backend_id = 0;
      st->header_complete = 0;
      // errno is already set by recv(), keep it
      return NULL;
    }

    if (n == 0) {
      // Connection closed by peer
      fprintf(stderr, "[Backend] Connection closed by peer on fd %d\n", fd);
      st->pos = 0;
      st->expected_len = 0;
      st->client_id = 0;
      st->backend_id = 0;
      st->header_complete = 0;
      return NULL;
    }

    st->pos += n;
    st->bytes_recv_this_sec += n;
    total_read_this_call += n;

    // If we know the frame size and have it all, stop reading
    if (st->header_complete) {
      uint32_t total_needed = BACKEND_HEADER_SIZE + st->expected_len;
      if (st->pos >= total_needed) {
        break; // We have complete frame, stop reading
      }
    }

    // Continue loop to read more data
  }

  // Step 2: Parse header if not complete
  if (!st->header_complete) {
    if (st->pos < BACKEND_HEADER_SIZE) {
      // Need more data for header
      errno = EAGAIN; // CRITICAL: Not an error, just incomplete
      return NULL;
    }

    uint32_t network_len, network_client_id, network_backend_id;
    memcpy(&network_len, st->buffer, 4);
    memcpy(&network_client_id, st->buffer + 4, 4);
    memcpy(&network_backend_id, st->buffer + 8, 4);

    st->expected_len = ntohl(network_len);
    st->client_id = ntohl(network_client_id);
    st->backend_id = ntohl(network_backend_id);
    st->header_complete = 1;

    // Validate payload length
    if (st->expected_len > MAX_MESSAGE_SIZE) {
      fprintf(stderr, "[Backend] Invalid frame length: %u (max: %u) on fd %d\n",
              st->expected_len, MAX_MESSAGE_SIZE, fd);
      st->pos = 0;
      st->expected_len = 0;
      st->client_id = 0;
      st->backend_id = 0;
      st->header_complete = 0;
      return NULL;
    }

    // printf("[Backend] Frame header parsed: len=%u, client_id=%u, backend_id=%u (fd=%d)\n", st->expected_len, st->client_id, st->backend_id, fd);
  }

  // Step 3: Check if we have complete frame
  uint32_t total_frame_size = BACKEND_HEADER_SIZE + st->expected_len;

  if (st->pos < total_frame_size) {
    // Need more data - calculate how much we're missing
    uint32_t missing = total_frame_size - st->pos;

    // Only log large frames to avoid spam
    // Only log large frames to avoid spam
    /*
    if (st->expected_len > 65536) {
      printf("[Backend] Large frame in progress: %u/%u bytes received (%.1f%%) "
             "on fd %d\n",
             st->pos, total_frame_size, (float)st->pos / total_frame_size * 100,
             fd);
    }
    */

    // CRITICAL: Set errno so caller knows this is NOT an error
    errno = EAGAIN;
    return NULL; // Wait for more data
  }

  // Step 4: We have complete frame - allocate and copy
  message_t *msg = msg_alloc(st->expected_len);
  if (!msg) {
    fprintf(
        stderr,
        "[Backend] Failed to allocate message for %u byte payload on fd %d\n",
        st->expected_len, fd);
    st->pos = 0;
    st->expected_len = 0;
    st->client_id = 0;
    st->backend_id = 0;
    st->header_complete = 0;
    return NULL;
  }

  msg->client_id = st->client_id;
  msg->backend_id = st->backend_id;
  msg->len = st->expected_len;
  msg->timestamp_ns = get_time_ns();

  memcpy(msg->data, st->buffer + BACKEND_HEADER_SIZE, st->expected_len);

  // --- PRIORITY CLASSIFICATION ---
  // Updated for Flow Control: Check first byte for Traffic Class
  if (msg->len > 0) {
      uint8_t traffic_class = ((uint8_t*)msg->data)[0];

      if (traffic_class == TRAFFIC_CONTROL) {
          msg->priority = MSG_PRIORITY_HIGH;

          // DIAGNOSTIC (Optional): Log Critical Messages
          /*
          if (msg->len > 1 && msg->len < 200) {
              struct timeval tv;
              gettimeofday(&tv, NULL);
              long long ms = tv.tv_sec*1000LL + tv.tv_usec/1000;
              // Skip 0x01 prefix for printing
              printf("[GW-BackEnd-HighPrio] %.*s at %lld\n", msg->len-1, (char*)msg->data+1, ms);
          }
          */
      } else {
          msg->priority = MSG_PRIORITY_NORMAL;
      }
  }

  // printf("[Backend] Complete frame received: %u bytes on fd %d\n", st->expected_len, fd);

  // Step 5: Handle remaining data in buffer (next frame if any)
  if (st->pos > total_frame_size) {
    uint32_t remaining = st->pos - total_frame_size;
    memmove(st->buffer, st->buffer + total_frame_size, remaining);
    st->pos = remaining;
    // printf("[Backend] %u bytes remaining in buffer (next frame?) on fd %d\n", remaining, fd);
  } else {
    st->pos = 0;
  }

  // Reset state for next frame
  st->expected_len = 0;
  st->client_id = 0;
  st->backend_id = 0;
  st->header_complete = 0;

  return msg;
}

int write_backend_frame(int fd, message_t *msg, uint32_t client_id,
                        uint32_t backend_id) {
  backend_send_state_t *ss = &backend_send_states[fd % MAX_BACKENDS];

  // First call - initialize
  if (ss->header_sent == 0 && ss->data_sent == 0) {
    ss->total_len = msg->len;
    ss->start_time = time(NULL);

    uint32_t network_len = htonl(msg->len);
    uint32_t network_client_id = htonl(client_id);
    uint32_t network_backend_id = htonl(backend_id);
    memcpy(ss->header, &network_len, 4);
    memcpy(ss->header + 4, &network_client_id, 4);
    memcpy(ss->header + 8, &network_backend_id, 4);
  }

  // Check timeout
  if (time(NULL) - ss->start_time > BACKEND_SEND_TIMEOUT) {
    ss->retry_count++;
    if (ss->retry_count > 3) {
      fprintf(stderr, "[Backend] Send timeout on fd %d after %u retries\n", fd,
              ss->retry_count);
      ss->header_sent = 0;
      ss->data_sent = 0;
      ss->retry_count = 0;
      return -1;
    }
  }

  // Send header
  while (ss->header_sent < BACKEND_HEADER_SIZE) {
    ssize_t sent = send(fd, ss->header + ss->header_sent,
                        BACKEND_HEADER_SIZE - ss->header_sent,
                        MSG_DONTWAIT | MSG_NOSIGNAL);

    if (sent < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return 1;
      }
      ss->header_sent = 0;
      ss->data_sent = 0;
      ss->retry_count = 0;
      return -1;
    }

    ss->header_sent += sent;
  }

  // Send payload
  while (ss->data_sent < ss->total_len) {
    ssize_t sent =
        send(fd, msg->data + ss->data_sent, ss->total_len - ss->data_sent,
             MSG_DONTWAIT | MSG_NOSIGNAL);

    if (sent < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return 1;
      }
      ss->header_sent = 0;
      ss->data_sent = 0;
      ss->retry_count = 0;
      return -1;
    }

    ss->data_sent += sent;
  }

  // Complete - reset
  ss->header_sent = 0;
  ss->data_sent = 0;
  ss->total_len = 0;
  ss->start_time = 0;
  ss->retry_count = 0;

  return 0;
}

int connect_to_backend(const char *host, int port) {
  struct addrinfo hints, *res, *p;
  char port_str[16];
  int fd = -1;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  snprintf(port_str, sizeof(port_str), "%d", port);

  if (getaddrinfo(host, port_str, &hints, &res) != 0) {
    return -1;
  }

  for (p = res; p != NULL; p = p->ai_next) {
    if ((fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) < 0) {
      continue;
    }

    if (connect(fd, p->ai_addr, p->ai_addrlen) == -1) {
      close(fd);
      fd = -1;
      continue;
    }
    break;
  }

  freeaddrinfo(res);

  if (fd < 0) return -1;

  set_nonblocking(fd);
  set_tcp_nodelay(fd);

  return fd;
}
