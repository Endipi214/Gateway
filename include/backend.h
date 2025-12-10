#ifndef __BACKEND_H__
#define __BACKEND_H__

#include "mempool.h"
#include <stdint.h>

// ---------- Backend Protocol ----------
// Frame format:
// [4 bytes: payload length (network order)]
// [4 bytes: backend_id (network order)]
// [N bytes: payload data]
#define BACKEND_HEADER_SIZE 8

// ---------- Backend struct ---------
typedef struct {
  uint8_t buffer[MAX_MESSAGE_SIZE + BACKEND_HEADER_SIZE];
  uint32_t pos;            // Current position in buffer
  uint32_t expected_len;   // Expected payload length
  uint32_t backend_id;     // Backend ID from header
  uint8_t header_complete; // Have we read the full header?
} backend_state_t;

// ---------- Backend Send State ---------
typedef struct {
  uint8_t header[BACKEND_HEADER_SIZE];
  uint32_t header_sent; // Bytes of header sent
  uint32_t data_sent;   // Bytes of data sent
  uint32_t total_len;   // Total payload length
} backend_send_state_t;

// Global backend structs
extern backend_state_t *backend_states;
extern backend_send_state_t *backend_send_states;

// Backend protocol API
// Returns: message if complete frame received, NULL if incomplete or error
message_t *read_backend_frame(int fd);

// Returns: 0 if complete, 1 if partial (call again), -1 on error
int write_backend_frame(int fd, message_t *msg, uint32_t backend_id);

// Connection management
int connect_to_backend(const char *host, int port);

// Initialize backend state arrays (call once at startup)
void backend_init(void);
void backend_cleanup(void);

#endif // __BACKEND_H__
