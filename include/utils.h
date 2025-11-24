#ifndef __UTILS_H__
#define __UTILS_H__

#include <stdint.h>

// Returns current time in nanoseconds since an arbitrary epoch (monotonic)
uint64_t get_time_ns(void);

// Set a file descriptor to non-blocking mode
// Returns 0 on success, -1 on failure
int set_nonblocking(int fd);

// Disable Nagle's algorithm on a TCP socket (improves latency for small
// packets) Returns 0 on success, -1 on failure
int set_tcp_nodelay(int fd);

// Signal an eventfd (increments its counter)
// Used for inter-thread notifications
void signal_eventfd(int efd);

// Drain an eventfd (read and clear the counter)
// Typically used after receiving an event notification
void drain_eventfd(int efd);

#endif // __UTILS_H__
