#ifndef __GATEWAY_H__
#define __GATEWAY_H__

#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <time.h>

// --------- Global Limits ---------
#define QUEUE_SIZE 4096
#define POOL_SIZE 8192
#define MAX_CLIENTS 1024
#define MAX_BACKENDS 16
#define EPOLL_EVENTS 64
#define LISTEN_BACKLOG 128
#define BACKEND_POOL_SIZE 10
#define MAX_BACKEND_SERVERS 16
#define RECONNECT_INTERVAL 5

// --------- Declare globals ---------
extern int eventfd_ws;
extern int eventfd_backend;
extern atomic_bool running;
extern pthread_mutex_t ws_send_lock;

// --------- Client Connection ---------
typedef struct {
  int fd;
  uint32_t state;
  time_t last_activity;
} client_conn_t;

#define CLIENT_STATE_HANDSHAKE 0
#define CLIENT_STATE_ACTIVE 1

// --------- Backend Connection ---------
typedef struct {
  int fd;
  atomic_bool connected;
  time_t last_attempt;
  uint32_t reconnect_count;
} backend_conn_t;

typedef struct {
  char host[256];
  int port;
} backend_server_t;

// --------- Global Arrays ---------
extern client_conn_t clients[MAX_CLIENTS];
extern backend_conn_t backends[MAX_BACKEND_SERVERS];

// --------- Configuration ---------
extern int ws_port;
extern int backend_server_count;
extern backend_server_t backend_servers[MAX_BACKEND_SERVERS];

#endif // __GATEWAY_H__
