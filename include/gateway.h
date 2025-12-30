#ifndef __GATEWAY_H__
#define __GATEWAY_H__

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

// Connection health thresholds
#define IDLE_TIMEOUT_SEC 60
#define MAX_CONSECUTIVE_FAILURES 10
#define CIRCUIT_BREAKER_TIMEOUT 30
#define MAX_RECV_PER_CALL 65536

// --------- Declare globals ---------
extern int eventfd_ws;
extern int eventfd_backend;
extern atomic_bool running;

// --------- Client Connection (Enhanced) ---------
typedef struct {
  int fd;
  uint32_t state;
  time_t last_activity;

  // Health metrics
  uint32_t messages_recv;
  uint32_t messages_sent;
  uint32_t errors;
  uint32_t consecutive_send_failures;
  time_t connected_at;

  // Rate limiting
  uint32_t bytes_recv_this_sec;
  time_t rate_limit_window;
  uint32_t max_bytes_per_sec; // 0 = unlimited
} client_conn_t;

#define CLIENT_STATE_HANDSHAKE 0
#define CLIENT_STATE_ACTIVE 1
#define CLIENT_STATE_THROTTLED 2

// --------- Backend Connection (Enhanced with Circuit Breaker) ---------
typedef enum { CB_CLOSED, CB_OPEN, CB_HALF_OPEN } circuit_breaker_state_t;

typedef struct {
  int fd;
  atomic_bool connected;
  time_t last_attempt;
  uint32_t reconnect_count;

  // Circuit breaker
  uint32_t consecutive_failures;
  time_t circuit_open_until;
  circuit_breaker_state_t circuit_state;

  // Health
  time_t last_successful_send;
  uint32_t messages_sent;
  uint32_t messages_failed;
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
extern int use_discovery;

// --------- Circuit Breaker API ---------
int can_send_to_backend(int slot);
void record_backend_failure(int slot);
void record_backend_success(int slot);

// --------- Health Check API ---------
void check_connection_health(void);
int check_rate_limit(int client_fd);

#endif // __GATEWAY_H__
