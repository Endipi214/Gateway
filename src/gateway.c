#include "gateway.h"
#include <stdatomic.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

// Circuit breaker implementation
int can_send_to_backend(int slot) {
  if (slot < 0 || slot >= MAX_BACKEND_SERVERS)
    return 0;

  time_t now = time(NULL);
  backend_conn_t *b = &backends[slot];

  switch (b->circuit_state) {
  case CB_OPEN:
    // Circuit is open - don't try to send
    if (now >= b->circuit_open_until) {
      b->circuit_state = CB_HALF_OPEN;
      printf("[Circuit] Backend %d: OPEN -> HALF_OPEN (testing recovery)\n",
             slot);
      return 1; // Allow one test request
    }
    return 0;

  case CB_HALF_OPEN:
    return 1; // Allow requests to test if backend recovered

  case CB_CLOSED:
    return 1;
  }

  return 0;
}

void record_backend_failure(int slot) {
  if (slot < 0 || slot >= MAX_BACKEND_SERVERS)
    return;

  backend_conn_t *b = &backends[slot];
  b->consecutive_failures++;
  b->messages_failed++;

  if (b->consecutive_failures >= 5) {
    b->circuit_state = CB_OPEN;
    b->circuit_open_until = time(NULL) + CIRCUIT_BREAKER_TIMEOUT;
    printf("[Circuit] Backend %d: CLOSED -> OPEN (failures: %u)\n", slot,
           b->consecutive_failures);
  }
}

void record_backend_success(int slot) {
  if (slot < 0 || slot >= MAX_BACKEND_SERVERS)
    return;

  backend_conn_t *b = &backends[slot];
  b->consecutive_failures = 0;
  b->messages_sent++;
  b->last_successful_send = time(NULL);

  if (b->circuit_state == CB_HALF_OPEN) {
    b->circuit_state = CB_CLOSED;
    printf("[Circuit] Backend %d: HALF_OPEN -> CLOSED (recovered!)\n", slot);
  }
}

// Connection health monitoring - Periodic Status (every 10s)
void check_connection_health(void) {
  time_t now = time(NULL);

  // Count active frontends
  int frontend_count = 0;
  for (int i = 0; i < MAX_CLIENTS; i++) {
    if (clients[i].fd > 0) {
      frontend_count++;

      // Check idle timeout
      if (now - clients[i].last_activity > IDLE_TIMEOUT_SEC) {
        printf("[STATUS] Frontend #%d: TIMEOUT (idle %lds), disconnecting\n",
               i, now - clients[i].last_activity);
        close(clients[i].fd);
        clients[i].fd = -1;
        frontend_count--;
        continue;
      }

      // Check too many consecutive failures
      if (clients[i].consecutive_send_failures >= MAX_CONSECUTIVE_FAILURES) {
        printf("[STATUS] Frontend #%d: TOO MANY FAILURES (%u), disconnecting\n",
               i, clients[i].consecutive_send_failures);
        close(clients[i].fd);
        clients[i].fd = -1;
        frontend_count--;
        continue;
      }
    }
  }

  // Count active backends
  int backend_count = 0;
  for (int i = 0; i < MAX_BACKENDS; i++) {
    if (atomic_load(&backends[i].connected)) {
      backend_count++;
    }
  }

  // Print summary status
  if (frontend_count > 0 || backend_count > 0) {
    printf("\n[Gateway Status] %lds uptime | 🌐 %d Frontends | 💻 %d Backends\n",
           now - (time_t)0, frontend_count, backend_count);
  } else {
    printf("[Gateway Status] Idle (No connections)\n");
  }
}

// Rate limiting check
int check_rate_limit(int client_fd) {
  if (client_fd < 0 || client_fd >= MAX_CLIENTS)
    return 0;

  client_conn_t *c = &clients[client_fd];

  // If no rate limit set, allow
  if (c->max_bytes_per_sec == 0)
    return 1;

  time_t now = time(NULL);

  // Reset counter if new second
  if (now != c->rate_limit_window) {
    c->rate_limit_window = now;
    c->bytes_recv_this_sec = 0;
  }

  // Check if over limit
  if (c->bytes_recv_this_sec >= c->max_bytes_per_sec) {
    return 0; // Rate limited
  }

  return 1;
}
