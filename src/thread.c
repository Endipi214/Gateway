#include <errno.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
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

// --- Priority Queue Helpers ---
static void client_q_init(client_conn_t *c) {
    c->high_prio_q.head = c->high_prio_q.tail = c->high_prio_q.count = 0;
    c->low_prio_q.head = c->low_prio_q.tail = c->low_prio_q.count = 0;
}

static void client_q_push(client_conn_t *c, message_t *msg) {
    // Heuristic: Binary Video (0x01/0x02) = Low Priority. Anything else = High.
    // If msg->len is huge (>1024), likely video too.
    int is_video = 0;
    if (msg->len > 0) {
        uint8_t type = ((uint8_t*)msg->data)[0];
        if (type == TRAFFIC_VIDEO) is_video = 1;
    }

    // Explicit override: If "STATUS" or "INFO" in text, it is high
    // But simple heuristic is fine for now.

    int is_high = !is_video;

    client_queue_t *q = is_high ? &c->high_prio_q : &c->low_prio_q;

    // === AGGRESSIVE VIDEO THROTTLING ===
    // For video: Keep ONLY 3 frames max to prevent buffer bloat
    // This ensures <100ms latency (3 frames / 30fps = 100ms)
    if (is_video && q->count >= 3) {
        // Drop OLDEST frames to show newest
        while (q->count >= 3) {
            int head_idx = q->head;
            message_t *dropped = (message_t*)q->buffer[head_idx];
            msg_free(dropped);
            q->head = (q->head + 1) % CLIENT_QUEUE_SIZE;
            q->count--;
        }
    }

    if (q->count >= CLIENT_QUEUE_SIZE) {
        if (is_high) {
             // High Priority: Drop Oldest (Head) to make room for new status
             // This ensures we don't get stuck with old status if stuck
             int head_idx = q->head;
             message_t *dropped = (message_t*)q->buffer[head_idx];
             msg_free(dropped);
             q->head = (q->head + 1) % CLIENT_QUEUE_SIZE;
             q->count--;
             // printf("[WS] WARN: High Prio Queue Full! Dropped oldest msg.\n");
        } else {
             // Low Priority (Video): Drop Oldest (Head) = "Frame Skipping"
             // This is excellent for Video.
             int head_idx = q->head;
             message_t *dropped = (message_t*)q->buffer[head_idx];
             msg_free(dropped);
             q->head = (q->head + 1) % CLIENT_QUEUE_SIZE;
             q->count--;
        }
    }

    q->buffer[q->tail] = msg;
    q->tail = (q->tail + 1) % CLIENT_QUEUE_SIZE;
    q->count++;
}

static message_t* client_q_pop(client_conn_t *c) {
    // 1. Drain High Priority FIRST
    if (c->high_prio_q.count > 0) {
        message_t *msg = (message_t*)c->high_prio_q.buffer[c->high_prio_q.head];
        c->high_prio_q.head = (c->high_prio_q.head + 1) % CLIENT_QUEUE_SIZE;
        c->high_prio_q.count--;
        return msg;
    }
    // 2. Drain Low Priority
    if (c->low_prio_q.count > 0) {
        message_t *msg = (message_t*)c->low_prio_q.buffer[c->low_prio_q.head];
        c->low_prio_q.head = (c->low_prio_q.head + 1) % CLIENT_QUEUE_SIZE;
        c->low_prio_q.count--;
        return msg;
    }
    return NULL;
}
// ------------------------------


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

    // Periodic cleanup (every 1 second for faster recovery)
    if (now - last_cleanup >= 1) {
      cleanup_stale_ws_sends();
      last_cleanup = now;

      // ACK Flow Control Watchdog: Reset ready_for_video if stuck
      // This prevents deadlock if ACK packet is lost
      pthread_mutex_lock(&clients_lock);
      for (int i = 1; i < MAX_CLIENTS; i++) {
        if (clients[i].fd > 0 && !clients[i].ready_for_video) {
          if (clients[i].last_video_sent > 0 &&
              (now - clients[i].last_video_sent) >= VIDEO_ACK_TIMEOUT_SEC) {
            // printf("[GW] Watchdog: Resetting ready_for_video for client %d\n", i);
            clients[i].ready_for_video = 1;
            clients[i].last_video_sent = 0; // Reset last sent time
          }
        }
      }
      pthread_mutex_unlock(&clients_lock);
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
      // printf("[WS] Processing Msg: ClID=%u BackID=%u Len=%u\n", target_client_id, msg->backend_id, msg->len);

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
              if (client_fd < 0 || client_fd >= MAX_CLIENTS) {
                msg_free(broadcast_msg);
                continue;
              }
              int client_idx = client_fd;

              // NEW PRIORITY QUEUE LOGIC
              client_q_push(&clients[i], broadcast_msg);

              // If socket is idle, try to start sending immediately to minimize latency
              if (!ws_pending_sends[client_idx].in_progress) {
                   // This mimics the EPOLLOUT logic: try to pop and send
                   message_t *next = client_q_pop(&clients[i]);
                   if (next) { // Should be broadcast_msg or higher prio
                       ws_pending_sends[client_idx].msg = next;
                       ws_pending_sends[client_idx].in_progress = 1;

                       int res = send_ws_backend_frame(client_fd, next);
                       if (res == 0) {
                           // Sent immediately! Record metrics
                           uint64_t latency = get_time_ns() - next->timestamp_ns;
                           atomic_fetch_add(&metrics_ws.messages_sent, 1);
                           atomic_fetch_add(&metrics_ws.bytes_sent, BACKEND_HEADER_SIZE + next->len);
                           atomic_fetch_add(&metrics_ws.latency_sum_ns, latency);
                           atomic_fetch_add(&metrics_ws.latency_count, 1);
                           clients[i].messages_sent++;
                           clients[i].consecutive_send_failures = 0;

                           msg_free(next);
                           ws_pending_sends[client_idx].in_progress = 0;
                           ws_pending_sends[client_idx].msg = NULL;

                           // If queue not empty, trigger EPOLLOUT to drain rest?
                           // Or loop? For simplicity, we trigger EPOLLOUT if more provided.
                           if (clients[i].high_prio_q.count > 0 || clients[i].low_prio_q.count > 0) {
                               ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
                               ev.data.fd = client_fd;
                               epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client_fd, &ev);
                           }

                       } else if (res == 1) {
                           // Blocked
                           ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
                           ev.data.fd = client_fd;
                           epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client_fd, &ev);
                       } else {
                           // Error
                           clients[i].consecutive_send_failures++;
                           msg_free(next); // Failed msg
                           ws_pending_sends[client_idx].in_progress = 0;
                           ws_pending_sends[client_idx].msg = NULL;
                       }
                   }
              } else {
                  // Already busy, ensure EPOLLOUT is set so we drain this new msg eventually
                  // Usually EPOLLOUT is already set if busy?
                  // Yes, if res==1 we set it. But maybe it was cleared?
                  // To be safe, we can optimize: don't syscall if known set.
                  // But for reliability, we can ensure it.
                  // For now, assume logic holds.
                  broadcast_count++;
              }
              broadcast_count++; // Just to match old logic var usage
            }
          }
        }
        pthread_mutex_unlock(&clients_lock);

        msg_free(msg);
        continue;
      }

      // Unicast to specific client
      int client_fd = target_client_id;
      int client_idx = client_fd;  // Already bounds-checked below

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

      // NEW PRIORITY QUEUE LOGIC (Unicast)
      client_q_push(&clients[client_fd], msg);

      // Try send immediately if idle
      if (!ws_pending_sends[client_idx].in_progress) {
           message_t *next = client_q_pop(&clients[client_fd]);
           if (next) {
               ws_pending_sends[client_idx].msg = next;
               ws_pending_sends[client_idx].in_progress = 1;

               int result = send_ws_backend_frame(client_fd, next);

               if (result == 0) {
                 uint64_t latency = get_time_ns() - next->timestamp_ns;
                 atomic_fetch_add(&metrics_ws.messages_sent, 1);
                 atomic_fetch_add(&metrics_ws.bytes_sent,
                                  BACKEND_HEADER_SIZE + next->len);
                 atomic_fetch_add(&metrics_ws.latency_sum_ns, latency);
                 atomic_fetch_add(&metrics_ws.latency_count, 1);

                 clients[client_fd].messages_sent++;
                 clients[client_fd].consecutive_send_failures = 0;

                 msg_free(next);
                 ws_pending_sends[client_idx].in_progress = 0;
                 ws_pending_sends[client_idx].msg = NULL;

                  // If queue not empty, trigger EPOLLOUT
                 if (clients[client_fd].high_prio_q.count > 0 || clients[client_fd].low_prio_q.count > 0) {
                    ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
                    ev.data.fd = client_fd;
                    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client_fd, &ev);
                 }

               } else if (result == 1) {
                 ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
                 ev.data.fd = client_fd;
                 epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client_fd, &ev);
               } else {
                 clients[client_fd].consecutive_send_failures++;
                 msg_free(next);
                 ws_pending_sends[client_idx].in_progress = 0;
                 ws_pending_sends[client_idx].msg = NULL;
               }
           }
      }
    }

    // === OPTIMIZED QUEUE DRAIN ===
    // Instead of iterating ALL 1024 clients, we trigger drain INLINE during
    // message distribution above. This code is a safety fallback that only
    // runs occasionally to catch any stuck queues.
    static int drain_counter = 0;
    if (++drain_counter >= 10) { // Only run every 10th cycle (~100ms)
        drain_counter = 0;
        pthread_mutex_lock(&clients_lock);
        for (int i = 1; i < MAX_CLIENTS; i++) {
            if (clients[i].fd > 0 && clients[i].state == CLIENT_STATE_ACTIVE) {
                // Only check if queue has pending items
                if (clients[i].high_prio_q.count > 0 || clients[i].low_prio_q.count > 0) {
                    int client_fd = clients[i].fd;
                    int client_idx = client_fd;

                    if (ws_pending_sends[client_idx].in_progress) continue;

                    message_t *next = client_q_pop(&clients[i]);
                    if (next) {
                        ws_pending_sends[client_idx].msg = next;
                        ws_pending_sends[client_idx].in_progress = 1;

                        int result = send_ws_backend_frame(client_fd, next);
                        if (result == 0) {
                            atomic_fetch_add(&metrics_ws.messages_sent, 1);
                            msg_free(next);
                            ws_pending_sends[client_idx].in_progress = 0;
                            ws_pending_sends[client_idx].msg = NULL;
                        } else if (result == 1) {
                            ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
                            ev.data.fd = client_fd;
                            epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client_fd, &ev);
                        } else {
                            msg_free(next);
                            ws_pending_sends[client_idx].in_progress = 0;
                            ws_pending_sends[client_idx].msg = NULL;
                        }
                    }
                }
            }
        }
        pthread_mutex_unlock(&clients_lock);
    }

    int n = epoll_wait(epoll_fd, events, EPOLL_EVENTS, 10); // Reduced from 100ms for faster command response

    // Heartbeat disabled for production (was causing log spam)
    // static long long last_hb_ws = 0;
    // ...

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

            ws_pending_sends[client_fd].in_progress = 0;
            ws_pending_sends[client_fd].msg = NULL;

            // FIX: Reset send state to prevent corrupt partial sends on FD reuse
            client_q_init(&clients[client_fd]); // <--- INIT QUEUES HERE
            // Revert: memset(&ws_send_states[client_fd], 0, sizeof(ws_send_state_t));

            // ACK Flow Control: Initialize video ready state
            clients[client_fd].ready_for_video = 1;
            clients[client_fd].last_video_sent = 0;

            // printf("[WS] New connection: client_id=%d\n", client_fd);
          } else {
            // FD too high, reject connection
            printf("[WS] Rejected connection: fd=%d exceeds MAX_CLIENTS=%d\n", client_fd, MAX_CLIENTS);
            close(client_fd);
            pthread_mutex_unlock(&clients_lock);
            continue;
          }
          pthread_mutex_unlock(&clients_lock);

          ev.events = EPOLLIN | EPOLLET;
          ev.data.fd = client_fd;
          epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);

          atomic_fetch_add(&metrics_ws.connections, 1);
        }
        continue;
      }

      // Handle EPOLLOUT (New Drain Logic)
      if (events[i].events & EPOLLOUT) {
        if (fd < 0 || fd >= MAX_CLIENTS) continue;

        int client_idx = fd;
        int max_loop = 10; // Prevent starvation of other events
        int loop_count = 0;

        while (loop_count < max_loop) {
             loop_count++;

             // 1. Check if we have a pending partial message
             if (ws_pending_sends[client_idx].in_progress) {
                  message_t *pending_msg = ws_pending_sends[client_idx].msg;
                  int result = send_ws_backend_frame(fd, pending_msg);

                  if (result == 0) {
                      // Success
                      uint64_t latency = get_time_ns() - pending_msg->timestamp_ns;
                      atomic_fetch_add(&metrics_ws.messages_sent, 1);
                      atomic_fetch_add(&metrics_ws.bytes_sent, BACKEND_HEADER_SIZE + pending_msg->len);
                      atomic_fetch_add(&metrics_ws.latency_sum_ns, latency);
                      atomic_fetch_add(&metrics_ws.latency_count, 1);
                      if (fd < MAX_CLIENTS) clients[fd].messages_sent++;

                      msg_free(pending_msg);
                      ws_pending_sends[client_idx].in_progress = 0;
                      ws_pending_sends[client_idx].msg = NULL;

                      // Continue loop to send next msg
                  } else if (result == 1) {
                      // Blocked again
                      // NOTE: We don't need to mod EPOLL, it's already on (Level/Edge?)
                      // We use EPOLLET. We MUST return and wait for next Edge.
                      // Wait, if it returned 1 (AGAIN), we are blocked.
                      // Break loop.
                      break;
                  } else {
                      // Error
                      if (fd < MAX_CLIENTS) clients[fd].consecutive_send_failures++;
                      msg_free(pending_msg);
                      ws_pending_sends[client_idx].in_progress = 0;
                      ws_pending_sends[client_idx].msg = NULL;
                      break;
                  }
             }

             // 2. If no pending, pop next from PRIORITY QUEUES
             if (!ws_pending_sends[client_idx].in_progress) {
                 message_t *next = client_q_pop(&clients[fd]);
                 if (!next) {
                     // Queue empty
                     break;
                 }

                 // Install as pending
                 ws_pending_sends[client_idx].msg = next;
                 ws_pending_sends[client_idx].in_progress = 1;

                 // Loop continues to try sending this new 'pending' msg
             }
        }

        // If we broke because blocked, we are good (waiting for event).
        // If we broke because empty, we are good.
        // Re-arm EPOLLOUT?
        // With EPOLLET, we only get event on change.
        // If we wrote until EAGAIN, we will get next event.
        // If we stopped because Empty, we won't get next event until we Write again?
        // Actually, if we stopped because Empty, we don't need EPOLLOUT.
        // But we don't explicitly disable it.
        // It's acceptable.
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

          // FIX: Send Welcome Frame so Frontend knows its ID and starts Discovery
          message_t *welcome = msg_alloc(7); // "Welcome"
          if (welcome) {
              welcome->client_id = fd;
              welcome->backend_id = 0;
              welcome->len = 7;
              welcome->timestamp_ns = get_time_ns();
              memcpy(welcome->data, "Welcome", 7);

              send_ws_backend_frame(fd, welcome);
              msg_free(welcome);
          } else {
             printf("[WS] Failed to allocate Welcome message for client_id=%d\n", fd);
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

      // Process all available frames from this client - Critical for EPOLLET
      while (1) {
        message_t *msg = parse_ws_backend_frame(fd);
        if (!msg) {
          if (errno != EAGAIN && errno != EWOULDBLOCK) {
            printf("[WS] Client client_id=%d disconnected or error\n", fd);
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
            close(fd);
            pthread_mutex_lock(&clients_lock);
            if (fd < MAX_CLIENTS && fd > 0) {
              clients[fd].fd = -1;
            }
            pthread_mutex_unlock(&clients_lock);
            atomic_fetch_add(&metrics_ws.disconnections, 1);
          }
          break; // Exit loop when no more frames or error
        }

        // Override client_id with actual fd
        msg->client_id = fd;

        atomic_fetch_add(&metrics_ws.messages_recv, 1);
        atomic_fetch_add(&metrics_ws.bytes_recv, BACKEND_HEADER_SIZE + msg->len);

        if (fd < MAX_CLIENTS) {
          clients[fd].messages_recv++;
          clients[fd].last_activity = time(NULL);
        }

        // ACK Flow Control: Intercept TRAFFIC_ACK (0x03) from Frontend
        if (msg->len == 1 && ((uint8_t*)msg->data)[0] == TRAFFIC_ACK) {
          if (fd < MAX_CLIENTS) {
            clients[fd].ready_for_video = 1;
          }
          msg_free(msg);
          continue; // Try next frame
        }

        // Route to backend(s) with backpressure check
        int should_throttle = 0;
        uint32_t depth_before = queue_depth(&q_ws_to_backend);
        int push_result = queue_push(&q_ws_to_backend, msg, &should_throttle);
        uint32_t depth_after = queue_depth(&q_ws_to_backend);

        if (push_result == 1) {
          signal_eventfd(eventfd_backend);

          // LOGGING: Track command latency
          if (msg->len < 100) {
              struct timeval tv;
              gettimeofday(&tv, NULL);
              long long now_ms = tv.tv_sec*1000LL + tv.tv_usec/1000;
              // printf("[GW-RECV-TRACE] Cmd=%.20s at %lld (QDepth %u->%u)\n", (char*)msg->data, now_ms, depth_before, depth_after);
          }

          if (should_throttle && fd < MAX_CLIENTS) {
            clients[fd].state = CLIENT_STATE_THROTTLED;
          }
        } else {
          // Message dropped or Queue full
          msg_free(msg);
          if (fd < MAX_CLIENTS) {
            clients[fd].state = CLIENT_STATE_THROTTLED;
          }
        }
      }
    }
  }

  close(epoll_fd);
  printf("[WS Thread] Stopped\n");
  return NULL;
}

void *backend_thread_fn(void *arg) {
  // Native Mode: Connect to Backends (Manual or Discovered)
  // No more listening on AGENT_PORT to avoid conflicts

  int epoll_fd = epoll_create1(0);
  if (epoll_fd < 0) {
    perror("epoll_create1");
    return NULL;
  }

  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = eventfd_backend;
  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, eventfd_backend, &ev);

  struct epoll_event events[EPOLL_EVENTS];
  time_t last_cleanup = time(NULL);
  time_t last_conn_check = 0;

  // Discovery buffer
  discovered_backend_t discovered_list[MAX_DISCOVERED_BACKENDS];

  printf("[Backend Thread] Started (Native Discovery & Connection Manager)\n");

  while (atomic_load_explicit(&running, memory_order_acquire)) {
    time_t now = time(NULL);

    // Periodic Cleanup
    if (now - last_cleanup >= 5) {
      cleanup_stale_backend_sends();
      last_cleanup = now;
    }

    // Connection Manager (Every 1 second)
    if (now - last_conn_check >= 1) {
      last_conn_check = now;

      // 1. Process Native Discovery
      if (use_discovery) {
          int count = get_discovered_backends(discovered_list, MAX_DISCOVERED_BACKENDS);
          for (int i = 0; i < count; i++) {
              if (discovered_list[i].active) {
                  // Check if already in our backend_servers list (regardless of connection status)
                  int is_already_known = 0;
                  for (int j = 0; j < MAX_BACKEND_SERVERS; j++) {
                      if (backend_servers[j].port != 0 &&
                          strcmp(backend_servers[j].host, discovered_list[i].host) == 0 &&
                          backend_servers[j].port == discovered_list[i].port) {
                          is_already_known = 1; // Already known
                          break;
                      }
                  }

                  if (!is_already_known) {
                      printf("[Backend] Found new backend: %s:%d\n", discovered_list[i].host, discovered_list[i].port);

                      int control_fd = connect_to_backend(discovered_list[i].host, discovered_list[i].port);
                      if (control_fd >= 0) {
                          // Connect to DATA port
                          int data_fd = connect_to_backend(discovered_list[i].host, discovered_list[i].port + 1);
                          if (data_fd < 0) {
                              close(control_fd);
                              continue;
                          }

                          // Find free slot
                          int slot = -1;
                          for (int j = 0; j < MAX_BACKENDS; j++) {
                              if (!atomic_load(&backends[j].connected) && backend_servers[j].port == 0) {
                                  slot = j;
                                  break;
                              }
                          }

                          if (slot >= 0) {
                              backends[slot].control_fd = control_fd;
                              backends[slot].data_fd = data_fd;

                              // Reset state
                              backends[slot].consecutive_failures = 0;
                              backends[slot].circuit_state = CB_CLOSED;
                              backends[slot].last_successful_send = time(NULL);

                              strncpy(backend_servers[slot].host, discovered_list[i].host, 255);
                              backend_servers[slot].port = discovered_list[i].port;

                              atomic_store_explicit(&backends[slot].connected, 1, memory_order_release);

                              struct epoll_event ev_conn;
                              ev_conn.events = EPOLLIN | EPOLLET;
                              ev_conn.data.fd = control_fd;
                              epoll_ctl(epoll_fd, EPOLL_CTL_ADD, control_fd, &ev_conn);

                              struct epoll_event ev_data;
                              ev_data.events = EPOLLIN | EPOLLET;
                              ev_data.data.fd = data_fd;
                              epoll_ctl(epoll_fd, EPOLL_CTL_ADD, data_fd, &ev_data);

                              atomic_fetch_add_explicit(&metrics_backend.connections, 1, memory_order_relaxed);

                              if (slot >= backend_server_count) backend_server_count = slot + 1;
                              printf("[Backend] TCP connected to discovered backend %s:%d/%d (slot %d) - Handshake in progress...\n",
                                     discovered_list[i].host, discovered_list[i].port, discovered_list[i].port + 1, slot);

                              // FIX: Send initial ping to trigger INFO response
                              message_t *ping_msg = msg_alloc(4);
                              if (ping_msg) {
                                  ping_msg->client_id = 0;
                                  ping_msg->backend_id = slot + 1;
                                  ping_msg->len = 4;
                                  ping_msg->timestamp_ns = get_time_ns();
                                  memcpy(ping_msg->data, "ping", 4);
                                  int ping_res = write_backend_frame(control_fd, ping_msg, 0, slot + 1);
                                  if (ping_res == 0) {
                                      printf("[Backend] Sent initial ping to slot %d\n", slot);
                                  }
                                  msg_free(ping_msg);
                              }
                          } else {
                              printf("[Backend] No free slots for %s:%d\n", discovered_list[i].host, discovered_list[i].port);
                              close(control_fd);
                              close(data_fd);
                          }
                      }
                  }
              }
          }
      }

      // 2. Unified Connection Manager (DUAL CHANNEL)
      for (int i = 0; i < backend_server_count; i++) {
          if (!atomic_load(&backends[i].connected) && backend_servers[i].port != 0) {
              if (now - backends[i].last_attempt >= RECONNECT_INTERVAL) {
                  backends[i].last_attempt = now;

                  // Connect to CONTROL port first
                  int control_fd = connect_to_backend(backend_servers[i].host, backend_servers[i].port);
                  if (control_fd < 0) continue;

                  // Connect to DATA port (control_port + 1)
                  int data_fd = connect_to_backend(backend_servers[i].host, backend_servers[i].port + 1);
                  if (data_fd < 0) {
                      close(control_fd);
                      continue;
                  }

                  backends[i].control_fd = control_fd;
                  backends[i].data_fd = data_fd;
                  atomic_store_explicit(&backends[i].connected, 1, memory_order_release);
                  atomic_store_explicit(&backends[i].is_verified, 0, memory_order_relaxed); // Init as not verified
                  backends[i].consecutive_failures = 0;
                  backends[i].circuit_state = CB_CLOSED;
                  backends[i].last_successful_send = time(NULL);
                  backends[i].reconnect_count++;

                  // Register CONTROL fd for recv (commands/status come back here)
                  struct epoll_event ev_control;
                  ev_control.events = EPOLLIN | EPOLLET;
                  ev_control.data.fd = control_fd;
                  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, control_fd, &ev_control);

                  // Register DATA fd for recv (video/data comes back here)
                  struct epoll_event ev_data;
                  ev_data.events = EPOLLIN | EPOLLET;
                  ev_data.data.fd = data_fd;
                  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, data_fd, &ev_data);

                  atomic_fetch_add_explicit(&metrics_backend.connections, 1, memory_order_relaxed);
                  printf("[Backend] Dual channel TCP connected to %s:%d/%d (slot %d) - Handshake in progress...\n",
                         backend_servers[i].host, backend_servers[i].port, backend_servers[i].port + 1, i);

                  // FIX: Send initial ping to trigger INFO response
                  message_t *ping_msg = msg_alloc(4);
                  if (ping_msg) {
                      ping_msg->client_id = 0;
                      ping_msg->backend_id = i + 1;
                      ping_msg->len = 4;
                      ping_msg->timestamp_ns = get_time_ns();
                      memcpy(ping_msg->data, "ping", 4);
                      int ping_res = write_backend_frame(control_fd, ping_msg, 0, i + 1);
                      if (ping_res == 0) {
                          printf("[Backend] Sent initial ping to slot %d\n", i);
                      }
                      msg_free(ping_msg);
                  }
              }
          }
      }
    }

    // Process outgoing messages
    message_t *msg;
    int messages_processed = 0;
    int should_throttle = 0;

    while ((msg = queue_pop(&q_ws_to_backend)) != NULL &&
           messages_processed < 100) {
          messages_processed++;
          // Broadcast Logic (backend_id == 0)
          if (msg->backend_id == 0) {
              int sent_count = 0;
              for (int i = 0; i < MAX_BACKENDS; i++) {
                   if (atomic_load(&backends[i].connected)) {
                        message_t *out_msg = msg;

                        // For subsequent backends, we must clone the message
                        if (sent_count > 0) {
                             out_msg = msg_alloc(msg->len);
                             if (!out_msg) continue;
                             out_msg->client_id = msg->client_id;
                             out_msg->backend_id = msg->backend_id;
                             out_msg->len = msg->len;
                             out_msg->timestamp_ns = msg->timestamp_ns;
                             memcpy(out_msg->data, msg->data, msg->len);
                        }

                         // Send to Backend Slot 'i' via CONTROL channel (commands)
                        int backend_fd = backends[i].control_fd;
                        int backend_idx = i; // Use slot index directly

                         struct timeval tv;
                         gettimeofday(&tv, NULL);
                         long long milliseconds = tv.tv_sec*1000LL + tv.tv_usec/1000;
                         // LOGGING: Safe length-limited print
                         /*
                         printf("[GW] WS->Backend: %.*s at %lld\n",
                                (int)(out_msg->len < 32 ? out_msg->len : 32),
                                (char*)out_msg->data, milliseconds);
                         */

                         if (backend_pending_sends[backend_idx].in_progress) {
                             // FIX: Convert to UNICAST properly before Re-Queueing
                             // to prevent Infinite Broadcast Loop!
                             out_msg->backend_id = i + 1; // Convert to Unicast ID (1-based)
                             out_msg->priority = MSG_PRIORITY_HIGH;

                             if (queue_push(&q_ws_to_backend, out_msg, NULL) <= 0) {
                                 if (out_msg != msg) msg_free(out_msg);
                             }
                             continue;
                         }

                        backend_pending_sends[backend_idx].msg = out_msg;
                        backend_pending_sends[backend_idx].client_id = out_msg->client_id;
                        backend_pending_sends[backend_idx].backend_id = out_msg->backend_id;
                        backend_pending_sends[backend_idx].in_progress = 1;

                        int result = write_backend_frame(backend_fd, out_msg, out_msg->client_id, out_msg->backend_id);

                        if (result == 0) {
                            atomic_fetch_add_explicit(&metrics_backend.messages_sent, 1, memory_order_relaxed);
                            atomic_fetch_add_explicit(&metrics_backend.bytes_sent, out_msg->len, memory_order_relaxed);
                            record_backend_success(i);
                            msg_free(out_msg);
                            backend_pending_sends[backend_idx].in_progress = 0;
                            backend_pending_sends[backend_idx].msg = NULL;
                        } else if (result == 1) {
                             struct epoll_event ev_out;
                             ev_out.events = EPOLLIN | EPOLLOUT | EPOLLET;
                             ev_out.data.fd = backend_fd;
                             epoll_ctl(epoll_fd, EPOLL_CTL_MOD, backend_fd, &ev_out);
                        } else {
                             record_backend_failure(i);
                             msg_free(out_msg);
                             backend_pending_sends[backend_idx].in_progress = 0;
                             backend_pending_sends[backend_idx].msg = NULL;
                        }

                        sent_count++;
                   }
              }

              if (sent_count == 0) msg_free(msg);
              continue;
          }

          // Unicast Logic
          int backend_slot = msg->backend_id - 1;

          if (backend_slot < 0 || backend_slot >= MAX_BACKENDS) {
             msg_free(msg);
             continue;
          }

          if (!atomic_load(&backends[backend_slot].connected)) {
              msg_free(msg);
              continue; // Drop if not connected
          }

          int backend_fd = backends[backend_slot].control_fd;
          int backend_idx = backend_slot; // Correct: Use slot index directly

          struct timeval tv;
          gettimeofday(&tv, NULL);
          long long milliseconds = tv.tv_sec*1000LL + tv.tv_usec/1000;
          // LOGGING: Safe length-limited print
          printf("[GW] WS->Backend (Unicast): %.*s at %lld\n",
                 (int)(msg->len < 32 ? msg->len : 32),
                 (char*)msg->data, milliseconds);

          if (backend_pending_sends[backend_idx].in_progress) {
              // FIX BUG #2: Re-queue instead of drop
              msg->priority = MSG_PRIORITY_HIGH;
              if (queue_push(&q_ws_to_backend, msg, NULL) <= 0) {
                  msg_free(msg);
              }
              continue;
          }

          backend_pending_sends[backend_idx].msg = msg;
          backend_pending_sends[backend_idx].client_id = msg->client_id;
          backend_pending_sends[backend_idx].backend_id = msg->backend_id;
          backend_pending_sends[backend_idx].in_progress = 1;

          int result = write_backend_frame(backend_fd, msg, msg->client_id, msg->backend_id);

          if (result == 0) {
              atomic_fetch_add_explicit(&metrics_backend.messages_sent, 1, memory_order_relaxed);
              atomic_fetch_add_explicit(&metrics_backend.bytes_sent, msg->len, memory_order_relaxed);
              record_backend_success(backend_slot);
              msg_free(msg);
              backend_pending_sends[backend_idx].in_progress = 0;
              backend_pending_sends[backend_idx].msg = NULL;
          } else if (result == 1) {
               struct epoll_event ev_out;
               ev_out.events = EPOLLIN | EPOLLOUT | EPOLLET;
               ev_out.data.fd = backend_fd;
               epoll_ctl(epoll_fd, EPOLL_CTL_MOD, backend_fd, &ev_out);
          } else {
               record_backend_failure(backend_slot);
               msg_free(msg);
               backend_pending_sends[backend_idx].in_progress = 0;
               backend_pending_sends[backend_idx].msg = NULL;
          }

    }

    int n = epoll_wait(epoll_fd, events, EPOLL_EVENTS, 10); // Reduced from 100ms for faster command response

    for (int i = 0; i < n; i++) {
      int fd = events[i].data.fd;

      if (fd == eventfd_backend) {
        drain_eventfd(eventfd_backend);
        continue;
      }

      // Handle EPOLLOUT (Pending Sends)
      if (events[i].events & EPOLLOUT) {
        int backend_idx = -1;
        // Find backend slot by FD
        for(int j=0; j<MAX_BACKENDS; j++) {
            if(atomic_load(&backends[j].connected) && backends[j].control_fd == fd) {
                backend_idx = j;
                break;
            }
        }

        if (backend_idx != -1 && backend_pending_sends[backend_idx].in_progress) {
          message_t *pending_msg = backend_pending_sends[backend_idx].msg;
          uint32_t client_id = backend_pending_sends[backend_idx].client_id;
          uint32_t backend_id = backend_pending_sends[backend_idx].backend_id;
          int result = write_backend_frame(fd, pending_msg, client_id, backend_id);

          if (result == 0) {
             atomic_fetch_add_explicit(&metrics_backend.messages_sent, 1, memory_order_relaxed);
             record_backend_success(backend_id - 1); // Approx slot
             msg_free(pending_msg);
             backend_pending_sends[backend_idx].in_progress = 0;
             backend_pending_sends[backend_idx].msg = NULL;

             ev.events = EPOLLIN | EPOLLET;
             ev.data.fd = fd;
             epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
          } else if (result == -1) {
             msg_free(pending_msg);
             backend_pending_sends[backend_idx].in_progress = 0;
             backend_pending_sends[backend_idx].msg = NULL;
          }
        }
      }

      // Handle EPOLLIN (Incoming Backend Data)
      if (events[i].events & EPOLLIN) {
          message_t *msg = read_backend_frame(fd);
          if (!msg) {
             if (errno != EAGAIN && errno != EWOULDBLOCK) {
                 // Disconnection
                 for (int j = 0; j < MAX_BACKENDS; j++) {
                     if (backends[j].control_fd == fd || backends[j].data_fd == fd) {
                         printf("[Backend] Disconnected: %s:%d (slot %d)\n", backend_servers[j].host, backend_servers[j].port, j);

                         if (backends[j].control_fd >= 0) {
                             epoll_ctl(epoll_fd, EPOLL_CTL_DEL, backends[j].control_fd, NULL);
                             close(backends[j].control_fd);
                             backends[j].control_fd = -1;
                         }
                         if (backends[j].data_fd >= 0) {
                             epoll_ctl(epoll_fd, EPOLL_CTL_DEL, backends[j].data_fd, NULL);
                             close(backends[j].data_fd);
                             backends[j].data_fd = -1;
                         }

                         atomic_store_explicit(&backends[j].connected, 0, memory_order_release);
                         atomic_store_explicit(&backends[j].is_verified, 0, memory_order_relaxed);
                         atomic_fetch_add_explicit(&metrics_backend.disconnections, 1, memory_order_relaxed);

                         // NOTIFY FRONTEND: Backend is offline
                         message_t *disc_notif = msg_alloc(64);
                         if (disc_notif) {
                             disc_notif->client_id = 0; // Broadcast
                             disc_notif->backend_id = 0; // From Gateway
                             disc_notif->len = snprintf((char*)disc_notif->data, 64, "GATEWAY:BACKEND_DISCONNECTED:%d", j + 1);
                             disc_notif->timestamp_ns = get_time_ns();
                             if (queue_push(&q_backend_to_ws, disc_notif, &should_throttle) > 0) {
                                 signal_eventfd(eventfd_ws);
                             } else {
                                 msg_free(disc_notif);
                             }
                         }

                          // Fixed indexing
                          if (backend_pending_sends[j].in_progress) { msg_free(backend_pending_sends[j].msg); backend_pending_sends[j].in_progress = 0; backend_pending_sends[j].msg = NULL; }
                          continue;
                         break;
                     }
                 }
             }
             continue;
          }

          // Map to slot
          int backend_slot = -1;
           for (int j = 0; j < MAX_BACKENDS; j++) {
            if (backends[j].control_fd == fd || backends[j].data_fd == fd) {
              backend_slot = j;
              break;
            }
          }

          if (backend_slot != -1) {
              msg->backend_id = backend_slot + 1;
              atomic_fetch_add_explicit(&metrics_backend.messages_recv, 1, memory_order_relaxed);
              atomic_fetch_add_explicit(&metrics_backend.bytes_recv, msg->len, memory_order_relaxed);

              // VERIFICATION LOGIC: If this is the first message after TCP connect, verify and notify FE
              if (!atomic_load_explicit(&backends[backend_slot].is_verified, memory_order_acquire)) {
                  atomic_store_explicit(&backends[backend_slot].is_verified, 1, memory_order_release);
                  printf("[Backend] Verified connection for slot %d (Received first message)\n", backend_slot);

                  // NOTIFY FRONTEND: Backend is now online and responding
                  message_t *conn_notif = msg_alloc(64);
                  if (conn_notif) {
                      conn_notif->client_id = 0; // Broadcast
                      conn_notif->backend_id = 0; // From Gateway
                      conn_notif->len = snprintf((char*)conn_notif->data, 64, "GATEWAY:BACKEND_CONNECTED:%d", backend_slot + 1);
                      conn_notif->timestamp_ns = get_time_ns();
                      if (queue_push(&q_backend_to_ws, conn_notif, NULL) > 0) {
                          signal_eventfd(eventfd_ws);
                      } else {
                          msg_free(conn_notif);
                      }
                  }
              }

              // === TRAFFIC CLASS HANDLING ===
              // Set drop_if_full and priority based on traffic type
              // This prevents video from blocking control messages when queue fills
              if (msg->len > 0) {
                  uint8_t traffic_type = ((uint8_t*)msg->data)[0];
                  if (traffic_type == TRAFFIC_VIDEO) {
                      // Video: Drop when queue is full (acceptable for real-time)
                      msg->drop_if_full = 1;
                      msg->priority = MSG_PRIORITY_LOW;
                  } else if (traffic_type == TRAFFIC_CONTROL || traffic_type == TRAFFIC_FILE) {
                      // Control or File Chunks: Never drop, high priority
                      msg->drop_if_full = 0;
                      msg->priority = MSG_PRIORITY_HIGH;
                  }
              }

              int push_res = queue_push(&q_backend_to_ws, msg, &should_throttle);

              if (strncmp((char*)msg->data, "STATUS:", 7) == 0 || strncmp((char*)msg->data, "DATA:FILES", 10) == 0) {
                 struct timeval tv;
                 gettimeofday(&tv, NULL);
                 long long milliseconds = tv.tv_sec*1000LL + tv.tv_usec/1000;
                 printf("[GW] Backend->WS: Slot=%d, TargetCID=%u, Data=%.100s at %lld\n", backend_slot, msg->client_id, (char*)msg->data, milliseconds);
              }

              if (push_res == 1) {
                  // printf("[DEBUG] Pushed msg from Backend %d to WS Queue (Len=%d)\n", backend_slot, msg->len);
                  signal_eventfd(eventfd_ws);
              } else {
                  // printf("[WS] Failed to push msg from Backend %d (Queue Full/Error)\n", backend_slot);
                  msg_free(msg);
              }
          } else {
              // printf("[Backend] ERROR: Received message from FD=%d but not found in backend slots! Dropping.\n", fd);
              msg_free(msg);
          }
      }
    }
  }

  // Cleanup - close both channels
  for (int i = 0; i < MAX_BACKENDS; i++) {
    if (backends[i].control_fd >= 0) close(backends[i].control_fd);
    if (backends[i].data_fd >= 0) close(backends[i].data_fd);
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
