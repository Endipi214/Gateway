#ifndef __WEBSOCKET_H__
#define __WEBSOCKET_H__

#include "mempool.h"

// ---------- WebSocket Read State ----------
typedef struct {
  uint8_t buffer[16384]; // Buffer for WebSocket frame header + partial data
  uint32_t pos;
  uint8_t header_parsed;
  uint64_t payload_len;
  uint32_t header_size;
  uint8_t masked;
  uint8_t mask[4];
  uint32_t payload_read;   // Bytes of payload read so far
  msg_context_t *ctx;      // Current message context
  uint32_t chunks_created; // Number of chunks created for current message
} ws_read_state_t;

// ---------- WebSocket Send State ----------
typedef struct {
  uint8_t header[26]; // WS header + backend header
  uint32_t header_len;
  uint32_t header_sent;
  uint32_t data_sent; // Bytes sent from current chunk
  chunk_t *current_chunk;
} ws_send_state_t;

// WebSocket frame flags / opcodes
#define WS_FIN 0x80
#define WS_OPCODE_TEXT 0x01
#define WS_OPCODE_BIN 0x02
#define WS_OPCODE_CLOSE 0x08
#define WS_MASK 0x80

extern ws_read_state_t *ws_read_states;
extern ws_send_state_t *ws_send_states;

// API
char *base64_encode(const unsigned char *input, int length);
int handle_ws_handshake(int fd);

// Initialize WebSocket state arrays (call once at startup)
void ws_init(void);
void ws_cleanup(void);

// Returns: chunk if chunk received, NULL if incomplete or error
chunk_t *parse_ws_chunk(int fd);

// Returns: 0 if complete, 1 if partial (call again), -1 on error
int send_ws_chunk(int fd, chunk_t *chunk);

#endif // __WEBSOCKET_H__
