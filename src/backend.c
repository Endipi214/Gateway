#include <arpa/inet.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include "backend.h"
#include "gateway.h"
#include "mempool.h"
#include "utils.h"

backend_state_t backend_states[MAX_BACKENDS];

// Backend protocol API
message_t *read_backend_frame(int fd) {
  backend_state_t *st = &backend_states[fd % MAX_BACKENDS];

  ssize_t n = recv(fd, st->buffer + st->pos, sizeof(st->buffer) - st->pos,
                   MSG_DONTWAIT);

  if (n <= 0)
    return NULL;

  st->pos += n;

  if (st->expected == 0 && st->pos >= 4) {
    memcpy(&st->expected, st->buffer, 4);
    if (st->expected > MAX_MESSAGE_SIZE) {
      st->pos = 0;
      st->expected = 0;
      return NULL;
    }
  }

  if (st->expected > 0 && st->pos >= st->expected + 4) {
    message_t *msg = msg_alloc();
    if (!msg) {
      st->pos = 0;
      st->expected = 0;
      return NULL;
    }

    msg->backend_fd = fd;
    msg->len = st->expected;
    msg->timestamp_ns = get_time_ns();
    memcpy(msg->data, st->buffer + 4, st->expected);

    if (st->pos > st->expected + 4) {
      memmove(st->buffer, st->buffer + st->expected + 4,
              st->pos - st->expected - 4);
      st->pos -= (st->expected + 4);
    } else {
      st->pos = 0;
    }
    st->expected = 0;

    return msg;
  }

  return NULL;
}

int write_backend_frame(int fd, message_t *msg) {
  uint32_t len = msg->len;

  struct iovec iov[2];
  iov[0].iov_base = &len;
  iov[0].iov_len = sizeof(len);
  iov[1].iov_base = msg->data;
  iov[1].iov_len = msg->len;

  ssize_t sent = writev(fd, iov, 2);
  return (sent == (ssize_t)(sizeof(len) + msg->len)) ? 0 : -1;
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
