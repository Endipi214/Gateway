#include <errno.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include "mempool.h"
#include "utils.h"
#include "websocket.h"

ws_state_t ws_states[MAX_CLIENTS];

// API
// Encoding for WebSocket
char *base64_encode(const unsigned char *input, int length) {
  BIO *bio, *b64;
  BUF_MEM *buffer_ptr;

  b64 = BIO_new(BIO_f_base64());
  bio = BIO_new(BIO_s_mem());
  bio = BIO_push(b64, bio);

  BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
  BIO_write(bio, input, length);
  BIO_flush(bio);
  BIO_get_mem_ptr(bio, &buffer_ptr);

  char *result = malloc(buffer_ptr->length + 1);
  memcpy(result, buffer_ptr->data, buffer_ptr->length);
  result[buffer_ptr->length] = '\0';

  BIO_free_all(bio);
  return result;
}

// WebSocket handskaking
int handle_ws_handshake(int fd) {
  char buffer[4096];
  ssize_t n = recv(fd, buffer, sizeof(buffer) - 1, 0);
  if (n <= 0)
    return -1;

  buffer[n] = '\0';

  // Extract WebSocket key
  char *key_start = strstr(buffer, "Sec-WebSocket-Key: ");
  if (!key_start)
    return -1;

  key_start += 19;
  char *key_end = strstr(key_start, "\r\n");
  if (!key_end)
    return -1;

  char key[256];
  int key_len = key_end - key_start;
  memcpy(key, key_start, key_len);
  key[key_len] = '\0';

  // Generate accept key
  char concat[512];
  snprintf(concat, sizeof(concat), "%s258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
           key);

  unsigned char hash[SHA_DIGEST_LENGTH];
  SHA1((unsigned char *)concat, strlen(concat), hash);

  char *accept_key = base64_encode(hash, SHA_DIGEST_LENGTH);

  // Send handshake response
  char response[1024];
  int len = snprintf(response, sizeof(response),
                     "HTTP/1.1 101 Switching Protocols\r\n"
                     "Upgrade: websocket\r\n"
                     "Connection: Upgrade\r\n"
                     "Sec-WebSocket-Accept: %s\r\n\r\n",
                     accept_key);

  free(accept_key);

  send(fd, response, len, 0);
  return 0;
}

message_t *parse_ws_frame(int fd) {
  ws_state_t *st = &ws_states[fd % MAX_CLIENTS];

  ssize_t n =
      recv(fd, st->buffer + st->pos, MAX_MESSAGE_SIZE - st->pos, MSG_DONTWAIT);

  if (n <= 0) {
    if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
      return NULL;
    }
    return NULL;
  }

  st->pos += n;

  if (st->pos < 2)
    return NULL;

  uint8_t fin_opcode = st->buffer[0];
  uint8_t mask_len = st->buffer[1];

  uint8_t opcode = fin_opcode & 0x0F;
  if (opcode == WS_OPCODE_CLOSE) {
    return NULL;
  }

  if (!(fin_opcode & WS_FIN))
    return NULL;

  int masked = mask_len & WS_MASK;
  uint64_t payload_len = mask_len & 0x7F;
  uint32_t header_size = 2;

  if (payload_len == 126) {
    if (st->pos < 4)
      return NULL;
    payload_len = (st->buffer[2] << 8) | st->buffer[3];
    header_size = 4;
  } else if (payload_len == 127) {
    if (st->pos < 10)
      return NULL;
    payload_len = 0;
    for (int i = 0; i < 8; i++) {
      payload_len = (payload_len << 8) | st->buffer[2 + i];
    }
    header_size = 10;
  }

  if (masked)
    header_size += 4;

  uint32_t frame_size = header_size + payload_len;
  if (st->pos < frame_size)
    return NULL;

  message_t *msg = msg_alloc();
  if (!msg) {
    st->pos = 0;
    return NULL;
  }

  msg->client_fd = fd;
  msg->len = payload_len;
  msg->timestamp_ns = get_time_ns();

  if (masked) {
    uint8_t *mask = &st->buffer[header_size - 4];
    uint8_t *payload = &st->buffer[header_size];
    for (uint64_t i = 0; i < payload_len; i++) {
      msg->data[i] = payload[i] ^ mask[i % 4];
    }
  } else {
    memcpy(msg->data, &st->buffer[header_size], payload_len);
  }

  // Shift remaining data
  if (st->pos > frame_size) {
    memmove(st->buffer, st->buffer + frame_size, st->pos - frame_size);
    st->pos -= frame_size;
  } else {
    st->pos = 0;
  }

  return msg;
}

int send_ws_frame(int fd, message_t *msg) {
  uint8_t header[10];
  uint32_t header_len = 2;

  header[0] = WS_FIN | WS_OPCODE_BIN;

  if (msg->len < 126) {
    header[1] = msg->len;
  } else if (msg->len < 65536) {
    header[1] = 126;
    header[2] = (msg->len >> 8) & 0xFF;
    header[3] = msg->len & 0xFF;
    header_len = 4;
  } else {
    header[1] = 127;
    for (int i = 0; i < 8; i++) {
      header[2 + i] = (msg->len >> (56 - i * 8)) & 0xFF;
    }
    header_len = 10;
  }

  struct iovec iov[2];
  iov[0].iov_base = header;
  iov[0].iov_len = header_len;
  iov[1].iov_base = msg->data;
  iov[1].iov_len = msg->len;

  ssize_t sent = write(fd, iov, 2);
  return (sent == (ssize_t)(header_len + msg->len)) ? 0 : -1;
}
