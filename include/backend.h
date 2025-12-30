#ifndef __BACKEND_H__
#define __BACKEND_H__

#include "mempool.h"
#include <stdint.h>
#include <time.h>

// ---------- Backend Protocol ----------
// Frame format:
// [4 bytes: payload length (network order)]
// [4 bytes: client_id (network order)] - sender if from backend, dest if from
// client [4 bytes: backend_id (network order)] - sender if from client, dest if
// from backend [N bytes: payload data]
//
// Special IDs:
// client_id = 0: broadcast to all clients
// backend_id = 0: broadcast to all backends
#define BACKEND_HEADER_SIZE 12
#define BACKEND_SEND_TIMEOUT 5 // 5 seconds

// --------- Backend State (Enhanced) ---------
typedef struct {
  uint8_t buffer[MAX_MESSAGE_SIZE + BACKEND_HEADER_SIZE];
  uint32_t pos;
  uint32_t expected_len;
  uint32_t client_id;
  uint32_t backend_id;
  uint8_t header_complete;

  // Rate limiting
  uint32_t bytes_recv_this_sec;
  time_t rate_limit_window;
  uint32_t max_bytes_per_sec; // 0 = unlimited
} backend_state_t;

// --------- Backend Send State (Enhanced with timeout) ---------
typedef struct {
  uint8_t header[BACKEND_HEADER_SIZE];
  uint32_t header_sent;
  uint32_t data_sent;
  uint32_t total_len;

  // Timeout tracking
  time_t start_time;
  uint32_t retry_count;
} backend_send_state_t;

extern backend_state_t *backend_states;
extern backend_send_state_t *backend_send_states;

// Backend protocol API
message_t *read_backend_frame(int fd);
int write_backend_frame(int fd, message_t *msg, uint32_t client_id,
                        uint32_t backend_id);
int connect_to_backend(const char *host, int port);

void backend_init(void);
void backend_cleanup(void);
void cleanup_stale_backend_sends(void);
int check_backend_rate_limit(int fd);

#endif // __BACKEND_H__
