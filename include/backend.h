#ifndef __BACKEND_H__
#define __BACKEND_H__

#include "gateway.h"
#include "mempool.h"
#include <stdint.h>

// ---------- Backend struct ---------
typedef struct {
  uint8_t buffer[MAX_MESSAGE_SIZE + 4];
  uint32_t pos;
  uint32_t expected;
} backend_state_t;

// Global backend struct
extern backend_state_t backend_states[MAX_BACKENDS];

// Backend protocol API
message_t *read_backend_frame(int fd);
int write_backend_frame(int fd, message_t *msg);

// Connection management
int connect_to_backend(const char *host, int port);

#endif // __BACKEND_H__
