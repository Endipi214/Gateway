#ifndef __MESSAGE_H__
#define __MESSAGE_H__

#include <stdint.h>

// --------- Message Limits ---------
#define MAX_MESSAGE_SIZE (64 * 1024)

// --------- Message Structure ---------
typedef struct {
  uint32_t client_fd;
  uint32_t backend_fd;
  uint32_t len;
  uint64_t timestamp_ns;
  uint8_t data[MAX_MESSAGE_SIZE];
} message_t;

#endif // __MESSAGE_H__
