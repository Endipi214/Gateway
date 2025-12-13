#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "backend.h"
#include "gateway.h"
#include "mempool.h"
#include "metrics.h"
#include "queue.h"
#include "thread.h"
#include "utils.h"
#include "websocket.h"

pthread_mutex_t clients_lock = PTHREAD_MUTEX_INITIALIZER;

// Helper structure to track pending sends for WebSocket
typedef struct {
  message_t *msg;
  int in_progress;
} ws_pending_send_t;

// Helper structure to track pending sends for Backend
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

  printf("[WS Thread] Started - forwarding full backend frames\n");

  struct epoll_event events[EPOLL_EVENTS];

  while (atomic_load_explicit(&running, memory_order_acquire)) {
    // Process outgoing messages from queue
    message_t *msg;
    while ((msg = queue_pop(&q_backend_to_ws)) != NULL) {
      uint32_t target_client_id = msg->client_id;

      // Broadcast to all clients
      if (target_client_id == 0) {
        int broadcast_count = 0;
        pthread_mutex_lock(&clients_lock);

        // DEBUG: Count total clients
        int total_clients = 0;
        int active_clients = 0;
        for (int i = 1; i < MAX_CLIENTS; i++) {
          if (clients[i].fd > 0) {
            total_clients++;
            if (clients[i].state == CLIENT_STATE_ACTIVE) {
              active_clients++;
            }
          }
        }
        printf("[WS] DEBUG: Found %d total clients, %d active clients\n",
               total_clients, active_clients);

        for (int i = 1; i < MAX_CLIENTS; i++) {
          if (clients[i].fd > 0 && clients[i].state == CLIENT_STATE_ACTIVE) {
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
                fprintf(stderr, "[WS] Client fd=%d busy, dropping\n",
                        client_fd);
                msg_free(broadcast_msg);
                continue;
              }

              ws_pending_sends[client_idx].msg = broadcast_msg;
              ws_pending_sends[client_idx].in_progress = 1;

              // USE NEW FUNCTION - sends backend frame format
              int result = send_ws_backend_frame(client_fd, broadcast_msg);

              if (result == 0) {
                uint64_t latency = get_time_ns() - broadcast_msg->timestamp_ns;
                atomic_fetch_add(&metrics_ws.messages_sent, 1);
                atomic_fetch_add(&metrics_ws.bytes_sent,
                                 BACKEND_HEADER_SIZE + broadcast_msg->len);
                atomic_fetch_add(&metrics_ws.latency_sum_ns, latency);
                atomic_fetch_add(&metrics_ws.latency_count, 1);
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
                msg_free(broadcast_msg);
                ws_pending_sends[client_idx].in_progress = 0;
                ws_pending_sends[client_idx].msg = NULL;
              }
            }
          }
        }
        pthread_mutex_unlock(&clients_lock);

        printf("[WS] Broadcast backend_id=%u → %d clients (%u+12 bytes)\n",
               msg->backend_id, broadcast_count, msg->len);
        msg_free(msg);
        continue;
      }

      // Unicast to specific client
      int client_fd = target_client_id;
      int client_idx = client_fd % MAX_CLIENTS;

      if (client_fd <= 0 || client_fd >= MAX_CLIENTS) {
        fprintf(stderr, "[WS] Invalid client_id=%u\n", target_client_id);
        msg_free(msg);
        continue;
      }

      pthread_mutex_lock(&clients_lock);
      int client_valid = (clients[client_fd].fd == client_fd &&
                          clients[client_fd].state == CLIENT_STATE_ACTIVE);
      pthread_mutex_unlock(&clients_lock);

      if (!client_valid) {
        fprintf(stderr, "[WS] Client id=%u not active\n", target_client_id);
        msg_free(msg);
        continue;
      }

      if (ws_pending_sends[client_idx].in_progress) {
        fprintf(stderr, "[WS] Client id=%u busy\n", target_client_id);
        msg_free(msg);
        continue;
      }

      ws_pending_sends[client_idx].msg = msg;
      ws_pending_sends[client_idx].in_progress = 1;

      // USE NEW FUNCTION - sends backend frame format
      int result = send_ws_backend_frame(client_fd, msg);

      if (result == 0) {
        uint64_t latency = get_time_ns() - msg->timestamp_ns;
        atomic_fetch_add(&metrics_ws.messages_sent, 1);
        atomic_fetch_add(&metrics_ws.bytes_sent,
                         BACKEND_HEADER_SIZE + msg->len);
        atomic_fetch_add(&metrics_ws.latency_sum_ns, latency);
        atomic_fetch_add(&metrics_ws.latency_count, 1);
        printf("[WS] Sent backend_id=%u → client_id=%u (%u+12 bytes)\n",
               msg->backend_id, target_client_id, msg->len);
        msg_free(msg);
        ws_pending_sends[client_idx].in_progress = 0;
        ws_pending_sends[client_idx].msg = NULL;
      } else if (result == 1) {
        printf("[WS] Partial send to client_id=%u\n", target_client_id);
        ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
        ev.data.fd = client_fd;
        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client_fd, &ev);
      } else {
        fprintf(stderr, "[WS] Error sending to client_id=%u\n",
                target_client_id);
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

          // USE NEW FUNCTION
          int result = send_ws_backend_frame(fd, pending_msg);

          if (result == 0) {
            uint64_t latency = get_time_ns() - pending_msg->timestamp_ns;
            atomic_fetch_add(&metrics_ws.messages_sent, 1);
            atomic_fetch_add(&metrics_ws.bytes_sent,
                             BACKEND_HEADER_SIZE + pending_msg->len);
            atomic_fetch_add(&metrics_ws.latency_sum_ns, latency);
            atomic_fetch_add(&metrics_ws.latency_count, 1);
            printf("[WS] Completed partial send to client_id=%d\n", fd);
            msg_free(pending_msg);
            ws_pending_sends[client_idx].in_progress = 0;
            ws_pending_sends[client_idx].msg = NULL;

            ev.events = EPOLLIN | EPOLLET;
            ev.data.fd = fd;
            epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
          } else if (result == -1) {
            fprintf(stderr, "[WS] Error completing send to client_id=%d\n", fd);
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

          // Send the client its assigned ID
          // Message format: client_id=<assigned_id>, backend_id=0 (from
          // gateway), payload="WELCOME"
          message_t *welcome_msg = msg_alloc(0);
          if (welcome_msg) {
            welcome_msg->client_id = fd;
            welcome_msg->backend_id = 0; // 0 means from gateway itself
            welcome_msg->len = 0; // Empty payload or you could send "WELCOME"
            welcome_msg->timestamp_ns = get_time_ns();

            int client_idx = fd % MAX_CLIENTS;
            if (!ws_pending_sends[client_idx].in_progress) {
              ws_pending_sends[client_idx].msg = welcome_msg;
              ws_pending_sends[client_idx].in_progress = 1;

              int result = send_ws_backend_frame(fd, welcome_msg);
              if (result == 0) {
                printf("[WS] Sent ID assignment to client_id=%d\n", fd);
                msg_free(welcome_msg);
                ws_pending_sends[client_idx].in_progress = 0;
                ws_pending_sends[client_idx].msg = NULL;
              } else if (result == 1) {
                // Partial send - will complete with EPOLLOUT
                ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
                ev.data.fd = fd;
                epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
              } else {
                msg_free(welcome_msg);
                ws_pending_sends[client_idx].in_progress = 0;
                ws_pending_sends[client_idx].msg = NULL;
              }
            } else {
              msg_free(welcome_msg);
            }
          }
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

      // USE NEW FUNCTION - parses backend frame format
      message_t *msg = parse_ws_backend_frame(fd);
      if (!msg) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
          printf("[WS] Client client_id=%d disconnected\n", fd);
          epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
          close(fd);
          pthread_mutex_lock(&clients_lock);
          if (fd < MAX_CLIENTS && fd > 0)
            clients[fd].fd = -1;
          pthread_mutex_unlock(&clients_lock);
          atomic_fetch_add(&metrics_ws.disconnections, 1);
        }
        continue;
      }

      // IMPORTANT: Override client_id with the actual sender's fd
      // The client cannot choose its own ID - the gateway assigns it
      msg->client_id = fd;

      // Client now sends messages in backend format!
      // msg->backend_id is from the client's message (tells us where to route)

      atomic_fetch_add(&metrics_ws.messages_recv, 1);
      atomic_fetch_add(&metrics_ws.bytes_recv, BACKEND_HEADER_SIZE + msg->len);

      printf("[WS] Received client_id=%u → backend_id=%u (%u+12 bytes)\n",
             msg->client_id, msg->backend_id, msg->len);

      // Route to backend(s)
      if (msg->backend_id == 0) {
        // Broadcast to all backends
        int backend_broadcast_count = 0;
        for (int j = 0; j < backend_server_count; j++) {
          if (atomic_load(&backends[j].connected)) {
            message_t *broadcast_msg = msg_alloc(msg->len);
            if (broadcast_msg) {
              broadcast_msg->client_id = msg->client_id;
              broadcast_msg->backend_id = j + 1;
              broadcast_msg->len = msg->len;
              broadcast_msg->timestamp_ns = msg->timestamp_ns;
              memcpy(broadcast_msg->data, msg->data, msg->len);

              if (queue_push(&q_ws_to_backend, broadcast_msg)) {
                backend_broadcast_count++;
              } else {
                msg_free(broadcast_msg);
              }
            }
          }
        }

        if (backend_broadcast_count > 0) {
          signal_eventfd(eventfd_backend);
          printf("[WS] Broadcast client_id=%u → %d backends\n", msg->client_id,
                 backend_broadcast_count);
        }

        // NEW: Also broadcast to all clients
        int client_broadcast_count = 0;
        pthread_mutex_lock(&clients_lock);
        for (int j = 1; j < MAX_CLIENTS; j++) {
          if (clients[j].fd > 0 && clients[j].state == CLIENT_STATE_ACTIVE) {
            message_t *client_msg = msg_alloc(msg->len);
            if (client_msg) {
              client_msg->client_id = clients[j].fd;
              client_msg->backend_id = msg->backend_id;
              client_msg->len = msg->len;
              client_msg->timestamp_ns = msg->timestamp_ns;
              memcpy(client_msg->data, msg->data, msg->len);

              int client_fd = clients[j].fd;
              int client_idx = client_fd % MAX_CLIENTS;
              if (!ws_pending_sends[client_idx].in_progress) {
                ws_pending_sends[client_idx].msg = client_msg;
                ws_pending_sends[client_idx].in_progress = 1;

                int result = send_ws_backend_frame(client_fd, client_msg);
                if (result == 0) {
                  atomic_fetch_add(&metrics_ws.messages_sent, 1);
                  atomic_fetch_add(&metrics_ws.bytes_sent,
                                   BACKEND_HEADER_SIZE + client_msg->len);
                  msg_free(client_msg);
                  ws_pending_sends[client_idx].in_progress = 0;
                  ws_pending_sends[client_idx].msg = NULL;
                  client_broadcast_count++;
                } else if (result == 1) {
                  ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
                  ev.data.fd = client_fd;
                  epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client_fd, &ev);
                  client_broadcast_count++;
                } else {
                  msg_free(client_msg);
                  ws_pending_sends[client_idx].in_progress = 0;
                  ws_pending_sends[client_idx].msg = NULL;
                }
              } else {
                msg_free(client_msg);
              }
            }
          }
        }
        pthread_mutex_unlock(&clients_lock);

        if (client_broadcast_count > 0) {
          printf("[WS] Also broadcast to %d clients\n", client_broadcast_count);
        }

        msg_free(msg);
        continue;
      } else {
        // Unicast to specific backend
        int backend_slot = msg->backend_id - 1;

        if (backend_slot >= 0 && backend_slot < backend_server_count &&
            atomic_load(&backends[backend_slot].connected)) {

          if (queue_push(&q_ws_to_backend, msg)) {
            signal_eventfd(eventfd_backend);
            continue;
          } else {
            fprintf(stderr, "[WS] Queue full, dropping message\n");
          }
        } else {
          fprintf(stderr, "[WS] Backend_id=%u not connected\n",
                  msg->backend_id);
        }
      }

      msg_free(msg);
    }
  }

  close(epoll_fd);
  printf("[WS Thread] Stopped\n");
  return NULL;
}

void *backend_thread_fn(void *arg) {
  int epoll_fd = epoll_create1(0);
  if (epoll_fd < 0) {
    perror("epoll_create1");
    return NULL;
  }

  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = eventfd_backend;
  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, eventfd_backend, &ev);

  printf("[Backend Thread] Started with DIRECT FORWARDING optimization\n");
  printf("[Backend Thread] Unicast messages bypass queue for lower latency\n");

  struct epoll_event events[EPOLL_EVENTS];
  time_t last_reconnect = 0;

  // Statistics
  uint64_t direct_sends = 0;
  uint64_t queued_sends = 0;

  while (atomic_load_explicit(&running, memory_order_acquire)) {
    // Attempt to connect/reconnect backends
    time_t now = time(NULL);
    if (now - last_reconnect >= RECONNECT_INTERVAL) {
      for (int i = 0; i < backend_server_count; i++) {
        if (!atomic_load_explicit(&backends[i].connected,
                                  memory_order_acquire)) {
          int fd = connect_to_backend(backend_servers[i].host,
                                      backend_servers[i].port);

          if (fd >= 0) {
            backends[i].fd = fd;
            atomic_store_explicit(&backends[i].connected, 1,
                                  memory_order_release);
            backends[i].reconnect_count++;

            backend_pending_sends[fd % MAX_BACKENDS].in_progress = 0;
            backend_pending_sends[fd % MAX_BACKENDS].msg = NULL;

            ev.events = EPOLLIN | EPOLLET;
            ev.data.fd = fd;
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev);

            atomic_fetch_add_explicit(&metrics_backend.connections, 1,
                                      memory_order_relaxed);
            printf("[Backend] Connected to %s:%d (fd=%d, backend_id=%d)\n",
                   backend_servers[i].host, backend_servers[i].port, fd, i + 1);
          }
        }
      }
      last_reconnect = now;
    }

    // Process outgoing messages from queue (WS → Backend)
    message_t *msg;
    while ((msg = queue_pop(&q_ws_to_backend)) != NULL) {
      int backend_slot = msg->backend_id - 1;

      if (backend_slot < 0 || backend_slot >= backend_server_count) {
        fprintf(stderr, "[Backend] Invalid backend_id=%u, dropping message\n",
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
        msg_free(msg);
        backend_pending_sends[backend_idx].in_progress = 0;
        backend_pending_sends[backend_idx].msg = NULL;
      } else if (result == 1) {
        ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
        ev.data.fd = backend_fd;
        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, backend_fd, &ev);
      } else {
        fprintf(stderr, "[Backend] Error sending to backend_id=%u\n",
                msg->backend_id);
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
            msg_free(pending_msg);
            backend_pending_sends[backend_idx].in_progress = 0;
            backend_pending_sends[backend_idx].msg = NULL;

            ev.events = EPOLLIN | EPOLLET;
            ev.data.fd = fd;
            epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
          } else if (result == -1) {
            fprintf(stderr, "[Backend] Error completing send to fd=%d\n", fd);
            msg_free(pending_msg);
            backend_pending_sends[backend_idx].in_progress = 0;
            backend_pending_sends[backend_idx].msg = NULL;
          }
        }
      }

      if (!(events[i].events & EPOLLIN)) {
        continue;
      }

      // Read backend frame
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

      // Find which backend this came from
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

      // OPTIMIZATION: Direct forwarding for unicast
      if (msg->client_id != 0) {
        // UNICAST: Send directly without queue
        int client_fd = msg->client_id;

        pthread_mutex_lock(&clients_lock);
        int client_valid = (client_fd > 0 && client_fd < MAX_CLIENTS &&
                            clients[client_fd].fd == client_fd &&
                            clients[client_fd].state == CLIENT_STATE_ACTIVE);
        pthread_mutex_unlock(&clients_lock);

        if (!client_valid) {
          fprintf(stderr,
                  "[Backend] DIRECT: Client id=%u not active, dropping\n",
                  msg->client_id);
          msg_free(msg);
          continue;
        }

        // Try direct send (protected by mutex since WS thread might also send)
        pthread_mutex_lock(&ws_send_lock);
        int result = send_ws_backend_frame(client_fd, msg);
        pthread_mutex_unlock(&ws_send_lock);

        if (result == 0) {
          // Success
          uint64_t latency = get_time_ns() - msg->timestamp_ns;
          atomic_fetch_add(&metrics_ws.messages_sent, 1);
          atomic_fetch_add(&metrics_ws.bytes_sent,
                           BACKEND_HEADER_SIZE + msg->len);
          atomic_fetch_add(&metrics_ws.latency_sum_ns, latency);
          atomic_fetch_add(&metrics_ws.latency_count, 1);

          direct_sends++;
          if (direct_sends % 100 == 0) {
            printf(
                "[Backend] DIRECT: Sent %lu messages (avg latency: %.2f µs)\n",
                direct_sends, latency / 1000.0);
          }

          msg_free(msg);
        } else {
          // Partial send or error - fall back to queue
          if (!queue_push(&q_backend_to_ws, msg)) {
            fprintf(stderr, "[Backend] DIRECT failed, queue full, dropping\n");
            msg_free(msg);
          } else {
            signal_eventfd(eventfd_ws);
            queued_sends++;
          }
        }

      } else {
        // BROADCAST: Use queue as before
        if (!queue_push(&q_backend_to_ws, msg)) {
          fprintf(
              stderr,
              "[Backend] Queue full, dropping broadcast from backend_id=%u\n",
              msg->backend_id);
          msg_free(msg);
        } else {
          signal_eventfd(eventfd_ws);
          queued_sends++;
        }
      }
    }
  }

  // Cleanup
  printf("[Backend Thread] Statistics:\n");
  printf("  Direct sends: %lu (bypassed queue)\n", direct_sends);
  printf("  Queued sends: %lu (broadcasts or partial sends)\n", queued_sends);

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

    // Global pool statistics
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
    }
    // Overall pool utilization
    uint32_t total_used = total_slots - total_slots_free;
    float overall_usage =
        (total_slots > 0) ? (100.0f * total_used / total_slots) : 0.0f;
    printf("   TOTAL: %4u/%4u slots used (%.1f%% utilization)\n", total_used,
           total_slots, overall_usage);

    // Memory usage estimation
    uint64_t tier_sizes[] = {512, 4096, 32768, 262144, 1048576, 8388608};
    uint64_t memory_in_use = 0;
    for (int i = 0; i < TIER_COUNT; i++) {
      uint32_t free, total, failures;
      uint64_t allocs;
      pool_get_tier_stats(i, &free, &total, &allocs, &failures);
      memory_in_use += (total - free) * (tier_sizes[i] + sizeof(message_t));
    }
    printf("   Estimated Memory In-Use: %.2f MB\n",
           memory_in_use / (1024.0 * 1024.0));

    // WebSocket Thread
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

    // Broadcast Statistics
    printf("Broadcast Statistics:\n");

    // Count active backends
    int active_backends = 0;
    for (int i = 0; i < backend_server_count; i++) {
      if (atomic_load(&backends[i].connected))
        active_backends++;
    }

    // Calculate broadcast multiplier
    float broadcast_ratio =
        (ws_recv > 0) ? (float)atomic_load(&metrics_backend.messages_sent) /
                            (float)ws_recv
                      : 0.0f;

    printf("   Active Backends:    %2d / %-2d\n", active_backends,
           backend_server_count);
    printf("   Broadcast Ratio:    1 msg → %.1f backends avg\n",
           broadcast_ratio);
    printf("   Total Broadcasted:  %10lu messages\n",
           atomic_load(&metrics_backend.messages_sent));

    // Show individual backend status
    if (backend_server_count > 0) {
      printf("Backend Connections:\n");
      for (int i = 0; i < backend_server_count && i < 8; i++) {
        int connected = atomic_load(&backends[i].connected);
        uint32_t reconnects = backends[i].reconnect_count;

        char status_icon = connected ? '+' : 'x';
        printf("   [%d] %-15s:%-5d  [%c]  reconnects: %3u\n", i,
               backend_servers[i].host, backend_servers[i].port, status_icon,
               reconnects);
      }

      if (backend_server_count > 8) {
        printf("   ... and %d more backends\n", backend_server_count - 8);
      }
    }

    // Backend Thread
    printf("Backend Thread:\n");
    uint64_t be_sent = atomic_load(&metrics_backend.messages_sent);
    uint64_t be_recv = atomic_load(&metrics_backend.messages_recv);
    uint64_t be_bytes_s = atomic_load(&metrics_backend.bytes_sent);
    uint64_t be_bytes_r = atomic_load(&metrics_backend.bytes_recv);
    uint32_t be_conn = atomic_load(&metrics_backend.connections);
    uint32_t be_disc = atomic_load(&metrics_backend.disconnections);

    printf("   Sent: %10lu msgs  |  %10lu bytes\n", be_sent, be_bytes_s);
    printf("   Received: %10lu msgs  |  %10lu bytes\n", be_recv, be_bytes_r);
    printf("   Connections: %6u total  |  %6u disconnections\n", be_conn,
           be_disc);

    // Queue Statistics
    printf("Queue Statistics:\n");
    uint32_t q1_depth = queue_depth(&q_ws_to_backend);
    uint32_t q1_drops = atomic_load(&q_ws_to_backend.drops);
    uint32_t q1_hw = atomic_load(&q_ws_to_backend.high_water);
    printf("   WS→Backend:  depth=%4u | drops=%6u | HW=%4u\n", q1_depth,
           q1_drops, q1_hw);

    uint32_t q2_depth = queue_depth(&q_backend_to_ws);
    uint32_t q2_drops = atomic_load(&q_backend_to_ws.drops);
    uint32_t q2_hw = atomic_load(&q_backend_to_ws.high_water);
    printf("   Backend→WS:  depth=%4u | drops=%6u | HW=%4u\n", q2_depth,
           q2_drops, q2_hw);
    printf(
        "═══════════════════════════════════════════════════════════════\n\n");
  }

  printf("[Monitor] Stopped\n");
  return NULL;
}
