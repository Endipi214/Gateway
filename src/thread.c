#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "backend.h"
#include "discovery.h"
#include "gateway.h"
#include "mempool.h"
#include "metrics.h"
#include "queue.h"
#include "thread.h"
#include "utils.h"
#include "websocket.h"

pthread_mutex_t clients_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
  message_t *msg;
  int in_progress;
} ws_pending_send_t;

typedef struct {
  message_t *msg;
  uint32_t client_id;
  uint32_t backend_id;
  int in_progress;
} backend_pending_send_t;

static ws_pending_send_t ws_pending_sends[MAX_CLIENTS] = {0};
static backend_pending_send_t backend_pending_sends[MAX_BACKENDS] = {0};

void *ws_thread_fn(void *arg) {
  int listen_fd = *(int *)arg;
  int epoll_fd = epoll_create1(0);
  if (epoll_fd < 0) {
    perror("epoll_create1");
    return NULL;
  }

  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = listen_fd;
  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev);

  ev.events = EPOLLIN;
  ev.data.fd = eventfd_ws;
  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, eventfd_ws, &ev);

  printf("[WS Thread] Started with backpressure control\n");

  struct epoll_event events[EPOLL_EVENTS];
  time_t last_cleanup = time(NULL);
  time_t last_health_check = time(NULL);

  while (atomic_load_explicit(&running, memory_order_acquire)) {
    time_t now = time(NULL);

    // Periodic cleanup (every 5 seconds)
    if (now - last_cleanup >= 5) {
      cleanup_stale_ws_sends();
      last_cleanup = now;
    }

    // Health check (every 10 seconds)
    if (now - last_health_check >= 10) {
      check_connection_health();
      last_health_check = now;
    }

    // Process outgoing messages with backpressure awareness
    message_t *msg;
    int messages_processed = 0;
    int should_throttle = 0;

    while ((msg = queue_pop(&q_backend_to_ws)) != NULL &&
           messages_processed < 100) {
      messages_processed++;
      uint32_t target_client_id = msg->client_id;

      // Broadcast to all clients
      if (target_client_id == 0) {
        int broadcast_count = 0;
        pthread_mutex_lock(&clients_lock);

        for (int i = 1; i < MAX_CLIENTS; i++) {
          if (clients[i].fd > 0 && clients[i].state == CLIENT_STATE_ACTIVE) {
            // Skip throttled clients
            if (clients[i].state == CLIENT_STATE_THROTTLED) {
              continue;
            }

            message_t *broadcast_msg = msg_alloc(msg->len);
            if (broadcast_msg) {
              broadcast_msg->client_id = clients[i].fd;
              broadcast_msg->backend_id = msg->backend_id;
              broadcast_msg->len = msg->len;
              broadcast_msg->timestamp_ns = msg->timestamp_ns;
              memcpy(broadcast_msg->data, msg->data, msg->len);

              int client_fd = clients[i].fd;
              int client_idx = client_fd % MAX_CLIENTS;

              if (ws_pending_sends[client_idx].in_progress) {
                msg_free(broadcast_msg);
                continue;
              }

              ws_pending_sends[client_idx].msg = broadcast_msg;
              ws_pending_sends[client_idx].in_progress = 1;

              int result = send_ws_backend_frame(client_fd, broadcast_msg);

              if (result == 0) {
                uint64_t latency = get_time_ns() - broadcast_msg->timestamp_ns;
                atomic_fetch_add(&metrics_ws.messages_sent, 1);
                atomic_fetch_add(&metrics_ws.bytes_sent,
                                 BACKEND_HEADER_SIZE + broadcast_msg->len);
                atomic_fetch_add(&metrics_ws.latency_sum_ns, latency);
                atomic_fetch_add(&metrics_ws.latency_count, 1);

                clients[i].messages_sent++;
                clients[i].consecutive_send_failures = 0;

                msg_free(broadcast_msg);
                ws_pending_sends[client_idx].in_progress = 0;
                ws_pending_sends[client_idx].msg = NULL;
                broadcast_count++;
              } else if (result == 1) {
                ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
                ev.data.fd = client_fd;
                epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client_fd, &ev);
                broadcast_count++;
              } else {
                clients[i].consecutive_send_failures++;
                msg_free(broadcast_msg);
                ws_pending_sends[client_idx].in_progress = 0;
                ws_pending_sends[client_idx].msg = NULL;
              }
            }
          }
        }
        pthread_mutex_unlock(&clients_lock);

        msg_free(msg);
        continue;
      }

      // Unicast to specific client
      int client_fd = target_client_id;
      int client_idx = client_fd % MAX_CLIENTS;

      if (client_fd <= 0 || client_fd >= MAX_CLIENTS) {
        msg_free(msg);
        continue;
      }

      pthread_mutex_lock(&clients_lock);
      int client_valid = (clients[client_fd].fd == client_fd &&
                          clients[client_fd].state == CLIENT_STATE_ACTIVE);
      pthread_mutex_unlock(&clients_lock);

      if (!client_valid) {
        msg_free(msg);
        continue;
      }

      if (ws_pending_sends[client_idx].in_progress) {
        msg_free(msg);
        continue;
      }

      ws_pending_sends[client_idx].msg = msg;
      ws_pending_sends[client_idx].in_progress = 1;

      int result = send_ws_backend_frame(client_fd, msg);

      if (result == 0) {
        uint64_t latency = get_time_ns() - msg->timestamp_ns;
        atomic_fetch_add(&metrics_ws.messages_sent, 1);
        atomic_fetch_add(&metrics_ws.bytes_sent,
                         BACKEND_HEADER_SIZE + msg->len);
        atomic_fetch_add(&metrics_ws.latency_sum_ns, latency);
        atomic_fetch_add(&metrics_ws.latency_count, 1);

        clients[client_fd].messages_sent++;
        clients[client_fd].consecutive_send_failures = 0;

        msg_free(msg);
        ws_pending_sends[client_idx].in_progress = 0;
        ws_pending_sends[client_idx].msg = NULL;
      } else if (result == 1) {
        ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
        ev.data.fd = client_fd;
        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client_fd, &ev);
      } else {
        clients[client_fd].consecutive_send_failures++;
        msg_free(msg);
        ws_pending_sends[client_idx].in_progress = 0;
        ws_pending_sends[client_idx].msg = NULL;
      }
    }

    int n = epoll_wait(epoll_fd, events, EPOLL_EVENTS, 100);

    for (int i = 0; i < n; i++) {
      int fd = events[i].data.fd;

      if (fd == eventfd_ws) {
        drain_eventfd(eventfd_ws);
        continue;
      }

      if (fd == listen_fd) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept(fd, (struct sockaddr *)&client_addr, &addr_len);

        if (client_fd >= 0) {
          set_nonblocking(client_fd);
          set_tcp_nodelay(client_fd);

          pthread_mutex_lock(&clients_lock);
          if (client_fd < MAX_CLIENTS && client_fd > 0) {
            clients[client_fd].fd = client_fd;
            clients[client_fd].state = CLIENT_STATE_HANDSHAKE;
            clients[client_fd].last_activity = time(NULL);
            clients[client_fd].connected_at = time(NULL);
            clients[client_fd].messages_recv = 0;
            clients[client_fd].messages_sent = 0;
            clients[client_fd].errors = 0;
            clients[client_fd].consecutive_send_failures = 0;
            clients[client_fd].bytes_recv_this_sec = 0;
            clients[client_fd].rate_limit_window = time(NULL);
            clients[client_fd].max_bytes_per_sec = 0; // No limit by default

            ws_pending_sends[client_fd % MAX_CLIENTS].in_progress = 0;
            ws_pending_sends[client_fd % MAX_CLIENTS].msg = NULL;

            printf("[WS] New connection: client_id=%d\n", client_fd);
          }
          pthread_mutex_unlock(&clients_lock);

          ev.events = EPOLLIN | EPOLLET;
          ev.data.fd = client_fd;
          epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);

          atomic_fetch_add(&metrics_ws.connections, 1);
        }
        continue;
      }

      // Handle EPOLLOUT
      if (events[i].events & EPOLLOUT) {
        int client_idx = fd % MAX_CLIENTS;
        if (ws_pending_sends[client_idx].in_progress) {
          message_t *pending_msg = ws_pending_sends[client_idx].msg;

          int result = send_ws_backend_frame(fd, pending_msg);

          if (result == 0) {
            uint64_t latency = get_time_ns() - pending_msg->timestamp_ns;
            atomic_fetch_add(&metrics_ws.messages_sent, 1);
            atomic_fetch_add(&metrics_ws.bytes_sent,
                             BACKEND_HEADER_SIZE + pending_msg->len);
            atomic_fetch_add(&metrics_ws.latency_sum_ns, latency);
            atomic_fetch_add(&metrics_ws.latency_count, 1);

            if (fd < MAX_CLIENTS) {
              clients[fd].messages_sent++;
              clients[fd].consecutive_send_failures = 0;
            }

            msg_free(pending_msg);
            ws_pending_sends[client_idx].in_progress = 0;
            ws_pending_sends[client_idx].msg = NULL;

            ev.events = EPOLLIN | EPOLLET;
            ev.data.fd = fd;
            epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
          } else if (result == -1) {
            if (fd < MAX_CLIENTS) {
              clients[fd].consecutive_send_failures++;
            }
            msg_free(pending_msg);
            ws_pending_sends[client_idx].in_progress = 0;
            ws_pending_sends[client_idx].msg = NULL;
          }
        }
      }

      if (!(events[i].events & EPOLLIN)) {
        continue;
      }

      pthread_mutex_lock(&clients_lock);
      if (fd >= MAX_CLIENTS || fd <= 0 || clients[fd].fd != fd) {
        pthread_mutex_unlock(&clients_lock);
        continue;
      }

      if (clients[fd].state == CLIENT_STATE_HANDSHAKE) {
        if (handle_ws_handshake(fd) == 0) {
          clients[fd].state = CLIENT_STATE_ACTIVE;
          printf("[WS] Handshake complete for client_id=%d\n", fd);
        } else {
          printf("[WS] Handshake failed for client_id=%d\n", fd);
          epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
          close(fd);
          clients[fd].fd = -1;
          atomic_fetch_add(&metrics_ws.disconnections, 1);
        }
        pthread_mutex_unlock(&clients_lock);
        continue;
      }
      pthread_mutex_unlock(&clients_lock);

      message_t *msg = parse_ws_backend_frame(fd);
      if (!msg) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
          printf("[WS] Client client_id=%d disconnected\n", fd);
          epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
          close(fd);
          pthread_mutex_lock(&clients_lock);
          if (fd < MAX_CLIENTS && fd > 0) {
            clients[fd].fd = -1;
          }
          pthread_mutex_unlock(&clients_lock);
          atomic_fetch_add(&metrics_ws.disconnections, 1);
        }
        continue;
      }

      // Override client_id with actual fd
      msg->client_id = fd;

      atomic_fetch_add(&metrics_ws.messages_recv, 1);
      atomic_fetch_add(&metrics_ws.bytes_recv, BACKEND_HEADER_SIZE + msg->len);

      if (fd < MAX_CLIENTS) {
        clients[fd].messages_recv++;
        clients[fd].last_activity = time(NULL);
      }

      // Route to backend(s) with backpressure check
      int push_result = queue_push(&q_ws_to_backend, msg, &should_throttle);

      if (push_result == 1) {
        signal_eventfd(eventfd_backend);

        // Handle backpressure
        if (should_throttle && fd < MAX_CLIENTS) {
          clients[fd].state = CLIENT_STATE_THROTTLED;
          printf("[WS] Client %d throttled due to backpressure\n", fd);
        }
      } else if (push_result == 0) {
        // Message dropped
        fprintf(stderr, "[WS] Queue full, message dropped\n");
        msg_free(msg);
      } else {
        // Queue full, cannot drop - apply backpressure
        fprintf(stderr, "[WS] Queue full, cannot accept more messages\n");
        msg_free(msg);

        if (fd < MAX_CLIENTS) {
          clients[fd].state = CLIENT_STATE_THROTTLED;
        }
      }
    }
  }

  close(epoll_fd);
  printf("[WS Thread] Stopped\n");
  return NULL;
}

void *backend_thread_fn(void *arg) {
  int use_disc = (arg != NULL) ? *((int *)arg) : 0;

  int epoll_fd = epoll_create1(0);
  if (epoll_fd < 0) {
    perror("epoll_create1");
    return NULL;
  }

  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = eventfd_backend;
  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, eventfd_backend, &ev);

  if (use_disc) {
    printf("[Backend Thread] Started with service discovery\n");
  } else {
    printf("[Backend Thread] Started with %d backend servers\n",
           backend_server_count);
  }

  struct epoll_event events[EPOLL_EVENTS];
  time_t last_reconnect = 0;
  time_t last_discovery_check = 0;
  time_t last_cleanup = time(NULL);

  while (atomic_load_explicit(&running, memory_order_acquire)) {
    time_t now = time(NULL);

    // Periodic cleanup
    if (now - last_cleanup >= 5) {
      cleanup_stale_backend_sends();
      last_cleanup = now;
    }

    // Update backends from discovery
    if (use_disc && (now - last_discovery_check >= 2)) {
      discovered_backend_t discovered[MAX_DISCOVERED_BACKENDS];
      int count = get_discovered_backends(discovered, MAX_DISCOVERED_BACKENDS);

      if (count > 0) {
        backend_server_count = 0;
        for (int i = 0; i < count && i < MAX_BACKEND_SERVERS; i++) {
          strncpy(backend_servers[i].host, discovered[i].host,
                  sizeof(backend_servers[i].host) - 1);
          backend_servers[i].port = discovered[i].port;
          backend_server_count++;
        }
      }

      last_discovery_check = now;
    }

    // Reconnect backends
    if (now - last_reconnect >= RECONNECT_INTERVAL) {
      for (int i = 0; i < backend_server_count; i++) {
        // Check circuit breaker
        if (!can_send_to_backend(i)) {
          continue;
        }

        if (!atomic_load_explicit(&backends[i].connected,
                                  memory_order_acquire)) {
          int fd = connect_to_backend(backend_servers[i].host,
                                      backend_servers[i].port);

          if (fd >= 0) {
            backends[i].fd = fd;
            atomic_store_explicit(&backends[i].connected, 1,
                                  memory_order_release);
            backends[i].reconnect_count++;
            backends[i].circuit_state = CB_CLOSED;
            backends[i].consecutive_failures = 0;

            backend_pending_sends[fd % MAX_BACKENDS].in_progress = 0;
            backend_pending_sends[fd % MAX_BACKENDS].msg = NULL;

            ev.events = EPOLLIN | EPOLLET;
            ev.data.fd = fd;
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev);

            atomic_fetch_add_explicit(&metrics_backend.connections, 1,
                                      memory_order_relaxed);
            printf("[Backend] Connected to %s:%d (fd=%d, backend_id=%d)\n",
                   backend_servers[i].host, backend_servers[i].port, fd, i + 1);

            record_backend_success(i);
          } else {
            record_backend_failure(i);
            if (!use_disc) {
              fprintf(stderr,
                      "[Backend] Failed to connect to %s:%d (backend_id=%d)\n",
                      backend_servers[i].host, backend_servers[i].port, i + 1);
            }
          }
        }
      }
      last_reconnect = now;
    }

    // Process outgoing messages
    message_t *msg;
    int messages_processed = 0;
    int should_throttle = 0;

    while ((msg = queue_pop(&q_ws_to_backend)) != NULL &&
           messages_processed < 100) {
      messages_processed++;
      int backend_slot = msg->backend_id - 1;

      if (backend_slot < 0 || backend_slot >= backend_server_count) {
        fprintf(stderr, "[Backend] Invalid backend_id=%u, dropping message\n",
                msg->backend_id);
        msg_free(msg);
        continue;
      }

      // Check circuit breaker
      if (!can_send_to_backend(backend_slot)) {
        fprintf(stderr,
                "[Backend] Circuit breaker OPEN for backend_id=%u, dropping "
                "message\n",
                msg->backend_id);
        msg_free(msg);
        continue;
      }

      if (!atomic_load_explicit(&backends[backend_slot].connected,
                                memory_order_acquire)) {
        fprintf(stderr,
                "[Backend] Backend_id=%u not connected, dropping message\n",
                msg->backend_id);
        msg_free(msg);
        continue;
      }

      int backend_fd = backends[backend_slot].fd;
      int backend_idx = backend_fd % MAX_BACKENDS;

      if (backend_pending_sends[backend_idx].in_progress) {
        fprintf(stderr,
                "[Backend] Backend_id=%u has pending send, dropping message\n",
                msg->backend_id);
        msg_free(msg);
        continue;
      }

      backend_pending_sends[backend_idx].msg = msg;
      backend_pending_sends[backend_idx].client_id = msg->client_id;
      backend_pending_sends[backend_idx].backend_id = msg->backend_id;
      backend_pending_sends[backend_idx].in_progress = 1;

      int result =
          write_backend_frame(backend_fd, msg, msg->client_id, msg->backend_id);

      if (result == 0) {
        atomic_fetch_add_explicit(&metrics_backend.messages_sent, 1,
                                  memory_order_relaxed);
        atomic_fetch_add_explicit(&metrics_backend.bytes_sent, msg->len,
                                  memory_order_relaxed);

        record_backend_success(backend_slot);

        msg_free(msg);
        backend_pending_sends[backend_idx].in_progress = 0;
        backend_pending_sends[backend_idx].msg = NULL;
      } else if (result == 1) {
        ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
        ev.data.fd = backend_fd;
        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, backend_fd, &ev);
      } else {
        record_backend_failure(backend_slot);
        msg_free(msg);
        backend_pending_sends[backend_idx].in_progress = 0;
        backend_pending_sends[backend_idx].msg = NULL;
      }
    }

    int n = epoll_wait(epoll_fd, events, EPOLL_EVENTS, 100);

    for (int i = 0; i < n; i++) {
      int fd = events[i].data.fd;

      if (fd == eventfd_backend) {
        drain_eventfd(eventfd_backend);
        continue;
      }

      // Handle EPOLLOUT
      if (events[i].events & EPOLLOUT) {
        int backend_idx = fd % MAX_BACKENDS;
        if (backend_pending_sends[backend_idx].in_progress) {
          message_t *pending_msg = backend_pending_sends[backend_idx].msg;
          uint32_t client_id = backend_pending_sends[backend_idx].client_id;
          uint32_t backend_id = backend_pending_sends[backend_idx].backend_id;

          int result =
              write_backend_frame(fd, pending_msg, client_id, backend_id);

          if (result == 0) {
            atomic_fetch_add_explicit(&metrics_backend.messages_sent, 1,
                                      memory_order_relaxed);
            atomic_fetch_add_explicit(&metrics_backend.bytes_sent,
                                      pending_msg->len, memory_order_relaxed);

            record_backend_success(backend_id - 1);

            msg_free(pending_msg);
            backend_pending_sends[backend_idx].in_progress = 0;
            backend_pending_sends[backend_idx].msg = NULL;

            ev.events = EPOLLIN | EPOLLET;
            ev.data.fd = fd;
            epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
          } else if (result == -1) {
            record_backend_failure(backend_id - 1);
            msg_free(pending_msg);
            backend_pending_sends[backend_idx].in_progress = 0;
            backend_pending_sends[backend_idx].msg = NULL;
          }
        }
      }

      if (!(events[i].events & EPOLLIN)) {
        continue;
      }

      message_t *msg = read_backend_frame(fd);
      if (!msg) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
          for (int j = 0; j < backend_server_count; j++) {
            if (backends[j].fd == fd) {
              epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
              close(fd);
              backends[j].fd = -1;
              atomic_store_explicit(&backends[j].connected, 0,
                                    memory_order_release);
              atomic_fetch_add_explicit(&metrics_backend.disconnections, 1,
                                        memory_order_relaxed);
              printf(
                  "[Backend] Disconnected from %s:%d (fd=%d, backend_id=%d)\n",
                  backend_servers[j].host, backend_servers[j].port, fd, j + 1);

              record_backend_failure(j);

              int backend_idx = fd % MAX_BACKENDS;
              if (backend_pending_sends[backend_idx].in_progress) {
                msg_free(backend_pending_sends[backend_idx].msg);
                backend_pending_sends[backend_idx].in_progress = 0;
                backend_pending_sends[backend_idx].msg = NULL;
              }
              break;
            }
          }
        }
        continue;
      }

      int backend_slot = -1;
      for (int j = 0; j < backend_server_count; j++) {
        if (backends[j].fd == fd) {
          backend_slot = j;
          break;
        }
      }

      if (backend_slot == -1) {
        fprintf(stderr,
                "[Backend] Received message from unknown fd=%d, dropping\n",
                fd);
        msg_free(msg);
        continue;
      }

      msg->backend_id = backend_slot + 1;

      atomic_fetch_add_explicit(&metrics_backend.messages_recv, 1,
                                memory_order_relaxed);
      atomic_fetch_add_explicit(&metrics_backend.bytes_recv, msg->len,
                                memory_order_relaxed);

      int push_result = queue_push(&q_backend_to_ws, msg, &should_throttle);

      if (push_result == 1) {
        signal_eventfd(eventfd_ws);
      } else {
        fprintf(stderr,
                "[Backend] Queue full, dropping message from backend_id=%u\n",
                msg->backend_id);
        msg_free(msg);
      }
    }
  }

  // Cleanup
  for (int i = 0; i < backend_server_count; i++) {
    if (backends[i].fd >= 0) {
      close(backends[i].fd);
    }
  }

  close(epoll_fd);
  printf("[Backend Thread] Stopped\n");
  return NULL;
}

void *monitor_thread_fn(void *arg) {
  printf("\n[Monitor] Started - reporting every 5 seconds\n\n");

  while (atomic_load_explicit(&running, memory_order_acquire)) {
    sleep(5);

    printf("═══════════════════════════════════════════════════════════════\n");
    printf("Tiered Memory Pool:\n");

    uint64_t total_allocs = atomic_load(&g_tiered_pool.total_allocs);
    uint64_t total_frees = atomic_load(&g_tiered_pool.total_frees);
    uint64_t in_use = total_allocs - total_frees;

    printf("   Total Allocations:  %10lu\n", total_allocs);
    printf("   Total Frees:        %10lu\n", total_frees);
    printf("   Messages In-Use:    %10lu\n", in_use);

    uint32_t total_slots_free = 0;
    uint32_t total_slots = 0;

    for (int i = 0; i < TIER_COUNT; i++) {
      uint32_t free, total, failures;
      uint64_t allocs;
      pool_get_tier_stats(i, &free, &total, &allocs, &failures);
      total_slots_free += free;
      total_slots += total;

      if (failures > 0) {
        printf("   [WARN] Tier %d: %u allocation failures\n", i, failures);
      }
    }

    uint32_t total_used = total_slots - total_slots_free;
    float overall_usage =
        (total_slots > 0) ? (100.0f * total_used / total_slots) : 0.0f;
    printf("   TOTAL: %4u/%4u slots used (%.1f%% utilization)\n", total_used,
           total_slots, overall_usage);

    printf("WebSocket Thread:\n");
    uint64_t ws_sent = atomic_load(&metrics_ws.messages_sent);
    uint64_t ws_recv = atomic_load(&metrics_ws.messages_recv);
    uint64_t ws_bytes_s = atomic_load(&metrics_ws.bytes_sent);
    uint64_t ws_bytes_r = atomic_load(&metrics_ws.bytes_recv);
    uint32_t ws_conn = atomic_load(&metrics_ws.connections);
    uint32_t ws_disc = atomic_load(&metrics_ws.disconnections);

    printf("   Sent:       %10lu msgs  |  %10lu bytes\n", ws_sent, ws_bytes_s);
    printf("   Received:   %10lu msgs  |  %10lu bytes\n", ws_recv, ws_bytes_r);
    printf("   Connections:  %6u total  |  %6u disconnections\n", ws_conn,
           ws_disc);

    uint32_t lat_count = atomic_load(&metrics_ws.latency_count);
    if (lat_count > 0) {
      uint64_t lat_sum = atomic_load(&metrics_ws.latency_sum_ns);
      uint64_t avg_lat = lat_sum / lat_count;
      printf("   Avg Latency: %8lu ns  |  %6.2f µs\n", avg_lat,
             avg_lat / 1000.0);
    }

    printf("Backend Thread:\n");
    uint64_t be_sent = atomic_load(&metrics_backend.messages_sent);
    uint64_t be_recv = atomic_load(&metrics_backend.messages_recv);
    uint64_t be_bytes_s = atomic_load(&metrics_backend.bytes_sent);
    uint64_t be_bytes_r = atomic_load(&metrics_backend.bytes_recv);
    uint32_t be_conn = atomic_load(&metrics_backend.connections);
    uint32_t be_disc = atomic_load(&metrics_backend.disconnections);

    printf("   Sent:       %10lu msgs  |  %10lu bytes\n", be_sent, be_bytes_s);
    printf("   Received:   %10lu msgs  |  %10lu bytes\n", be_recv, be_bytes_r);
    printf("   Connections:  %6u total  |  %6u disconnections\n", be_conn,
           be_disc);

    printf("Queue Statistics:\n");
    uint32_t q1_depth = queue_depth(&q_ws_to_backend);
    uint32_t q1_drops = atomic_load(&q_ws_to_backend.drops);
    uint32_t q1_hw = atomic_load(&q_ws_to_backend.high_water);
    uint32_t q1_bp = atomic_load(&q_ws_to_backend.backpressure_events);
    float q1_util = queue_utilization(&q_ws_to_backend);
    printf("   WS→Backend:  depth=%4u | drops=%6u | HW=%4u | BP=%4u | "
           "util=%.1f%%\n",
           q1_depth, q1_drops, q1_hw, q1_bp, q1_util * 100);

    uint32_t q2_depth = queue_depth(&q_backend_to_ws);
    uint32_t q2_drops = atomic_load(&q_backend_to_ws.drops);
    uint32_t q2_hw = atomic_load(&q_backend_to_ws.high_water);
    uint32_t q2_bp = atomic_load(&q_backend_to_ws.backpressure_events);
    float q2_util = queue_utilization(&q_backend_to_ws);
    printf("   Backend→WS:  depth=%4u | drops=%6u | HW=%4u | BP=%4u | "
           "util=%.1f%%\n",
           q2_depth, q2_drops, q2_hw, q2_bp, q2_util * 100);

    printf("Circuit Breaker Status:\n");
    for (int i = 0; i < backend_server_count && i < 8; i++) {
      const char *state_str = "UNKNOWN";
      switch (backends[i].circuit_state) {
      case CB_CLOSED:
        state_str = "CLOSED";
        break;
      case CB_OPEN:
        state_str = "OPEN";
        break;
      case CB_HALF_OPEN:
        state_str = "HALF_OPEN";
        break;
      }

      printf("   [%d] %s:%-5d  [%s]  fails=%u  sent=%u  failed=%u\n", i,
             backend_servers[i].host, backend_servers[i].port, state_str,
             backends[i].consecutive_failures, backends[i].messages_sent,
             backends[i].messages_failed);
    }

    printf(
        "═══════════════════════════════════════════════════════════════\n\n");
  }

  printf("[Monitor] Stopped\n");
  return NULL;
}
