#ifndef __WEBSOCKET_H__
#define __WEBSOCKET_H__

#include "mempool.h"
#include <time.h>

#define WS_SEND_TIMEOUT 5 // 5 seconds

// --------- WebSocket State ---------
typedef struct {
  uint8_t buffer[MAX_MESSAGE_SIZE];
  uint32_t pos;
} ws_state_t;

// --------- WebSocket Send State (Enhanced with timeout) ---------
typedef struct {
  uint8_t header[26];
  uint32_t header_len;
  uint32_t header_sent;
  uint32_t data_sent;
  uint32_t total_len;

  // Timeout tracking
  time_t start_time;
  uint32_t retry_count;
} ws_send_state_t;

#define WS_FIN 0x80
#define WS_OPCODE_TEXT 0x01
#define WS_OPCODE_BIN 0x02
#define WS_OPCODE_CLOSE 0x08
#define WS_MASK 0x80

extern ws_state_t *ws_states;
extern ws_send_state_t *ws_send_states;

// API
char *base64_encode(const unsigned char *input, int length);
int handle_ws_handshake(int fd);

void ws_init(void);
void ws_cleanup(void);

message_t *parse_ws_backend_frame(int fd);
int send_ws_backend_frame(int fd, message_t *msg);
void cleanup_stale_ws_sends(void);

#endif // __WEBSOCKET_H__
