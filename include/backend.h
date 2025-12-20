#ifndef __BACKEND_H__
#define __BACKEND_H__

#include "mempool.h"
#include <stdint.h>

// ---------- Backend Protocol ----------
// Frame format:
// [4 bytes: payload length (network order)]
// [4 bytes: client_id (network order)]
// [4 bytes: backend_id (network order)]
// [N bytes: payload data]
//
// Special IDs:
// client_id = 0: broadcast to all clients
// backend_id = 0: broadcast to all backends
#define BACKEND_HEADER_SIZE 12

// ---------- Backend Read State ---------
typedef struct {
  uint8_t header_buf[BACKEND_HEADER_SIZE];
  uint32_t header_pos;     // Current position in header buffer
  uint32_t expected_len;   // Expected payload length
  uint32_t client_id;      // Client ID from header
  uint32_t backend_id;     // Backend ID from header
  uint8_t header_complete; // Have we read the full header?
  msg_context_t *ctx;      // Current message context
  uint32_t bytes_read;     // Bytes read of current message
  uint32_t chunks_created; // Number of chunks created for current message
} backend_read_state_t;

// ---------- Backend Send State ---------
typedef struct {
  uint8_t header[BACKEND_HEADER_SIZE];
  uint32_t header_sent;   // Bytes of header sent
  uint32_t data_sent;     // Bytes of data sent (within current chunk)
  chunk_t *current_chunk; // Chunk being sent
} backend_send_state_t;

// Global backend structs
extern backend_read_state_t *backend_read_states;
extern backend_send_state_t *backend_send_states;

// Backend protocol API
// Returns: chunk if chunk is complete, NULL if incomplete or error
chunk_t *read_backend_chunk(int fd);

// Returns: 0 if complete, 1 if partial (call again), -1 on error
int write_backend_chunk(int fd, chunk_t *chunk);

// Connection management
int connect_to_backend(const char *host, int port);

// Initialize backend state arrays (call once at startup)
void backend_init(void);
void backend_cleanup(void);

#endif // __BACKEND_H__
