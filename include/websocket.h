#ifndef __WEBSOCKET_H__
#define __WEBSOCKET_H__

#include "mempool.h"

// ---------- WebSocket state ----------
typedef struct {
  uint8_t buffer[MAX_MESSAGE_SIZE];
  uint32_t pos;
} ws_state_t;

// ---------- WebSocket Send State ----------
typedef struct {
  uint8_t header[14]; // Max header size: 2 + 8 (extended length) + 4 (mask)
  uint32_t header_len;
  uint32_t header_sent;
  uint32_t data_sent;
  uint32_t total_len;
} ws_send_state_t;

// WebSocket frame flags / opcodes
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

// Initialize WebSocket state arrays (call once at startup)
void ws_init(void);
void ws_cleanup(void);

// Returns: message if complete frame received, NULL if incomplete or error
message_t *parse_ws_backend_frame(int fd);

// Returns: 0 if complete, 1 if partial (call again), -1 on error
int send_ws_backend_frame(int fd, message_t *msg);

#endif // __WEBSOCKET_H__
