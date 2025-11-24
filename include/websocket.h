#ifndef __WEBSOCKET_H__
#define __WEBSOCKET_H__

#include "gateway.h"
#include "message.h"

// ---------- WebSocket state ----------
typedef struct {
  uint8_t buffer[MAX_MESSAGE_SIZE];
  uint32_t pos;
} ws_state_t;

// WebSocket frame flags / opcodes
#define WS_FIN 0x80
#define WS_OPCODE_TEXT 0x01
#define WS_OPCODE_BIN 0x02
#define WS_OPCODE_CLOSE 0x08
#define WS_MASK 0x80

extern ws_state_t ws_states[MAX_CLIENTS];

// API
char *base64_encode(const unsigned char *input, int length);
int handle_ws_handshake(int fd);
message_t *parse_ws_frame(int fd);
int send_ws_frame(int fd, message_t *msg);

#endif // __WEBSOCKET_H__
