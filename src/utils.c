#include <bits/time.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "utils.h"

uint64_t get_time_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

int set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags == -1)
    return -1;
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int set_tcp_nodelay(int fd) {
  int flag = 1;
  return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
}

void signal_eventfd(int efd) {
  uint64_t val = 1;
  if (write(efd, &val, sizeof(val)) < 0) {
      // safe to ignore in this context or log if critical
  }
}

void drain_eventfd(int efd) {
  uint64_t val;
  if (read(efd, &val, sizeof(val)) < 0) {
      // safe to ignore
  }
}
