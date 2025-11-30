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

void *ws_thread_fn(void *arg) {
  int listen_fd = *(int *)arg;
  int epoll_fd = epoll_create1(0);
  if (epoll_fd < 0) {
    perror("epoll_create1");
    return NULL;
  }

  // Add listen socket
  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = listen_fd;
  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev);

  // Add eventfd
  ev.events = EPOLLIN;
  ev.data.fd = eventfd_ws;
  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, eventfd_ws, &ev);

  printf("[WS Thread] Started on port %d (BROADCAST MODE)\n", ws_port);

  struct epoll_event events[EPOLL_EVENTS];

  while (atomic_load_explicit(&running, memory_order_acquire)) {
    // Process outgoing messages
    message_t *msg;
    while ((msg = queue_pop(&q_backend_to_ws)) != NULL) {
      if (send_ws_frame(msg->client_fd, msg) == 0) {
        uint64_t latency = get_time_ns() - msg->timestamp_ns;
        atomic_fetch_add_explicit(&metrics_ws.messages_sent, 1,
                                  memory_order_relaxed);
        atomic_fetch_add_explicit(&metrics_ws.bytes_sent, msg->len,
                                  memory_order_relaxed);
        atomic_fetch_add_explicit(&metrics_ws.latency_sum_ns, latency,
                                  memory_order_relaxed);
        atomic_fetch_add_explicit(&metrics_ws.latency_count, 1,
                                  memory_order_relaxed);
      }
      msg_free(msg);
    }

    int n = epoll_wait(epoll_fd, events, EPOLL_EVENTS, 100);

    for (int i = 0; i < n; i++) {
      int fd = events[i].data.fd;

      if (fd == eventfd_ws) {
        drain_eventfd(eventfd_ws);
        continue;
      }

      if (fd == listen_fd) {
        // Accept new connection
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept(fd, (struct sockaddr *)&client_addr, &addr_len);

        if (client_fd >= 0) {
          set_nonblocking(client_fd);
          set_tcp_nodelay(client_fd);

          pthread_mutex_lock(&clients_lock);
          if (client_fd < MAX_CLIENTS) {
            clients[client_fd].fd = client_fd;
            clients[client_fd].state = CLIENT_STATE_HANDSHAKE;
            clients[client_fd].last_activity = time(NULL);
          }
          pthread_mutex_unlock(&clients_lock);

          ev.events = EPOLLIN | EPOLLET;
          ev.data.fd = client_fd;
          epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);

          atomic_fetch_add_explicit(&metrics_ws.connections, 1,
                                    memory_order_relaxed);
        }
        continue;
      }

      // Handle client data
      pthread_mutex_lock(&clients_lock);
      if (fd >= MAX_CLIENTS || clients[fd].fd != fd) {
        pthread_mutex_unlock(&clients_lock);
        continue;
      }

      if (clients[fd].state == CLIENT_STATE_HANDSHAKE) {
        if (handle_ws_handshake(fd) == 0) {
          clients[fd].state = CLIENT_STATE_ACTIVE;
        } else {
          epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
          close(fd);
          clients[fd].fd = -1;
          atomic_fetch_add_explicit(&metrics_ws.disconnections, 1,
                                    memory_order_relaxed);
        }
        pthread_mutex_unlock(&clients_lock);
        continue;
      }
      pthread_mutex_unlock(&clients_lock);

      // Parse WebSocket frame
      message_t *msg = parse_ws_frame(fd);
      if (!msg) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
          epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
          close(fd);
          pthread_mutex_lock(&clients_lock);
          if (fd < MAX_CLIENTS)
            clients[fd].fd = -1;
          pthread_mutex_unlock(&clients_lock);
          atomic_fetch_add_explicit(&metrics_ws.disconnections, 1,
                                    memory_order_relaxed);
        }
        continue;
      }

      atomic_fetch_add_explicit(&metrics_ws.messages_recv, 1,
                                memory_order_relaxed);
      atomic_fetch_add_explicit(&metrics_ws.bytes_recv, msg->len,
                                memory_order_relaxed);

      // Broadcast To All Backends
      int broadcast_count = 0;

      for (int i = 0; i < backend_server_count; i++) {
        if (atomic_load_explicit(&backends[i].connected,
                                 memory_order_acquire)) {
          // Create a separate message copy for each backend
          message_t *broadcast_msg = msg_alloc(msg->len);
          if (broadcast_msg) {
            // Copy the original message data
            broadcast_msg->client_fd = msg->client_fd;
            broadcast_msg->backend_fd = backends[i].fd;
            broadcast_msg->len = msg->len;
            broadcast_msg->timestamp_ns = msg->timestamp_ns;
            memcpy(broadcast_msg->data, msg->data, msg->len);

            // Send to this backend
            if (queue_push(&q_ws_to_backend, broadcast_msg)) {
              broadcast_count++;
            } else {
              msg_free(broadcast_msg);
            }
          }
        }
      }

      // Signal backend thread if we queued any messages
      if (broadcast_count > 0) {
        signal_eventfd(eventfd_backend);
      }

      // Free the original message
      msg_free(msg);
      // ===============================================
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

  // Add eventfd
  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = eventfd_backend;
  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, eventfd_backend, &ev);

  printf("[Backend Thread] Started, connecting to %d backend servers\n",
         backend_server_count);

  struct epoll_event events[EPOLL_EVENTS];
  time_t last_reconnect = 0;

  while (atomic_load_explicit(&running, memory_order_acquire)) {
    // Attempt to connect each slot to its corresponding backend server
    time_t now = time(NULL);
    if (now - last_reconnect >= RECONNECT_INTERVAL) {
      for (int i = 0; i < backend_server_count; i++) {
        if (!atomic_load_explicit(&backends[i].connected,
                                  memory_order_acquire)) {
          // Connect slot i to backend server i
          int fd = connect_to_backend(backend_servers[i].host,
                                      backend_servers[i].port);

          if (fd >= 0) {
            backends[i].fd = fd;
            atomic_store_explicit(&backends[i].connected, 1,
                                  memory_order_release);
            backends[i].reconnect_count++;

            ev.events = EPOLLIN | EPOLLET;
            ev.data.fd = fd;
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev);

            atomic_fetch_add_explicit(&metrics_backend.connections, 1,
                                      memory_order_relaxed);
            printf("[Backend] Connected to %s:%d (fd=%d, slot=%d)\n",
                   backend_servers[i].host, backend_servers[i].port, fd, i);
          } else {
            printf("[Backend] Failed to connect to %s:%d (slot=%d)\n",
                   backend_servers[i].host, backend_servers[i].port, i);
          }
        }
      }
      last_reconnect = now;
    }

    // Process outgoing messages
    message_t *msg;
    while ((msg = queue_pop(&q_ws_to_backend)) != NULL) {
      if (write_backend_frame(msg->backend_fd, msg) == 0) {
        atomic_fetch_add_explicit(&metrics_backend.messages_sent, 1,
                                  memory_order_relaxed);
        atomic_fetch_add_explicit(&metrics_backend.bytes_sent, msg->len,
                                  memory_order_relaxed);
      }
      msg_free(msg);
    }

    int n = epoll_wait(epoll_fd, events, EPOLL_EVENTS, 100);

    for (int i = 0; i < n; i++) {
      int fd = events[i].data.fd;

      if (fd == eventfd_backend) {
        drain_eventfd(eventfd_backend);
        continue;
      }

      // Read backend frame
      message_t *msg = read_backend_frame(fd);
      if (!msg) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
          // Find which backend slot disconnected
          for (int j = 0; j < backend_server_count; j++) {
            if (backends[j].fd == fd) {
              epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
              close(fd);
              backends[j].fd = -1;
              atomic_store_explicit(&backends[j].connected, 0,
                                    memory_order_release);
              atomic_fetch_add_explicit(&metrics_backend.disconnections, 1,
                                        memory_order_relaxed);
              printf("[Backend] Disconnected from %s:%d (fd=%d, slot=%d)\n",
                     backend_servers[j].host, backend_servers[j].port, fd, j);
              break;
            }
          }
        }
        continue;
      }

      atomic_fetch_add_explicit(&metrics_backend.messages_recv, 1,
                                memory_order_relaxed);
      atomic_fetch_add_explicit(&metrics_backend.bytes_recv, msg->len,
                                memory_order_relaxed);

      if (!queue_push(&q_backend_to_ws, msg)) {
        msg_free(msg);
      } else {
        signal_eventfd(eventfd_ws);
      }
    }
  }

  // Cleanup backend connections
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

    printf("\n╔════════════════════════════════════════════════════════════════"
           "╗\n");
    printf(
        "║                     GATEWAY METRICS                            ║\n");
    printf(
        "╠════════════════════════════════════════════════════════════════╣\n");

    printf(
        "║ Tiered Memory Pool:                                            ║\n");

    // Global pool statistics
    uint64_t total_allocs = atomic_load(&g_tiered_pool.total_allocs);
    uint64_t total_frees = atomic_load(&g_tiered_pool.total_frees);
    uint64_t in_use = total_allocs - total_frees;

    printf("║   Total Allocations:  %10lu                               ║\n",
           total_allocs);
    printf("║   Total Frees:        %10lu                               ║\n",
           total_frees);
    printf("║   Messages In-Use:    %10lu                               ║\n",
           in_use);

    printf(
        "╟────────────────────────────────────────────────────────────────╢\n");
    printf(
        "║ Per-Tier Statistics:                                           ║\n");
    printf(
        "║   Tier | Size    | Free/Total | Usage %% | Allocs    | Fails  ║\n");
    printf(
        "║   ─────┼─────────┼────────────┼─────────┼───────────┼────────║\n");

    const char *tier_names[] = {"512B  ", "4KB   ", "32KB  ",
                                "256KB ", "1MB   ", "8MB   "};

    uint32_t total_slots_free = 0;
    uint32_t total_slots = 0;

    for (int i = 0; i < TIER_COUNT; i++) {
      uint32_t free, total, failures;
      uint64_t allocs;
      pool_get_tier_stats(i, &free, &total, &allocs, &failures);

      total_slots_free += free;
      total_slots += total;

      uint32_t used = total - free;
      float usage_pct = (total > 0) ? (100.0f * used / total) : 0.0f;

      printf("║   %d    | %s | %4u/%4u  | %5.1f%%  | %9lu | %6u ║\n", i,
             tier_names[i], free, total, usage_pct, allocs, failures);
    }

    printf(
        "║   ─────┴─────────┴────────────┴─────────┴───────────┴────────║\n");

    // Overall pool utilization
    uint32_t total_used = total_slots - total_slots_free;
    float overall_usage =
        (total_slots > 0) ? (100.0f * total_used / total_slots) : 0.0f;
    printf("║   TOTAL: %4u/%4u slots used (%.1f%% utilization)            ║\n",
           total_used, total_slots, overall_usage);

    // Memory usage estimation
    uint64_t tier_sizes[] = {512, 4096, 32768, 262144, 1048576, 8388608};
    uint64_t memory_in_use = 0;
    for (int i = 0; i < TIER_COUNT; i++) {
      uint32_t free, total, failures;
      uint64_t allocs;
      pool_get_tier_stats(i, &free, &total, &allocs, &failures);
      memory_in_use += (total - free) * (tier_sizes[i] + sizeof(message_t));
    }
    printf(
        "║   Estimated Memory In-Use: %.2f MB                            ║\n",
        memory_in_use / (1024.0 * 1024.0));

    // WebSocket Thread
    printf(
        "╟────────────────────────────────────────────────────────────────╢\n");
    printf(
        "║ WebSocket Thread:                                              ║\n");
    uint64_t ws_sent = atomic_load(&metrics_ws.messages_sent);
    uint64_t ws_recv = atomic_load(&metrics_ws.messages_recv);
    uint64_t ws_bytes_s = atomic_load(&metrics_ws.bytes_sent);
    uint64_t ws_bytes_r = atomic_load(&metrics_ws.bytes_recv);
    uint32_t ws_conn = atomic_load(&metrics_ws.connections);
    uint32_t ws_disc = atomic_load(&metrics_ws.disconnections);

    printf("║   Sent:       %10lu msgs  |  %10lu bytes           ║\n", ws_sent,
           ws_bytes_s);
    printf("║   Received:   %10lu msgs  |  %10lu bytes           ║\n", ws_recv,
           ws_bytes_r);
    printf("║   Connections:  %6u total  |  %6u disconnections     ║\n",
           ws_conn, ws_disc);

    uint32_t lat_count = atomic_load(&metrics_ws.latency_count);
    if (lat_count > 0) {
      uint64_t lat_sum = atomic_load(&metrics_ws.latency_sum_ns);
      uint64_t avg_lat = lat_sum / lat_count;
      printf("║   Avg Latency: %8lu ns  |  %6.2f µs                    ║\n",
             avg_lat, avg_lat / 1000.0);
    }

    // Broadcast Statistics
    printf(
        "╟────────────────────────────────────────────────────────────────╢\n");
    printf(
        "║ Broadcast Statistics:                                          ║\n");

    // Count active backends
    int active_backends = 0;
    for (int i = 0; i < backend_server_count; i++) {
      if (atomic_load(&backends[i].connected))
        active_backends++;
    }

    // Calculate broadcast multiplier (how many copies per message)
    float broadcast_ratio =
        (ws_recv > 0) ? (float)atomic_load(&metrics_backend.messages_sent) /
                            (float)ws_recv
                      : 0.0f;

    printf(
        "║   Active Backends:    %2d / %-2d                               ║\n",
        active_backends, backend_server_count);
    printf("║   Broadcast Ratio:    1 msg → %.1f backends avg              ║\n",
           broadcast_ratio);
    printf("║   Total Broadcasted:  %10lu messages                    ║\n",
           atomic_load(&metrics_backend.messages_sent));

    // Show individual backend status
    if (backend_server_count > 0) {
      printf("╟────────────────────────────────────────────────────────────────"
             "╢\n");
      printf("║ Backend Connections:                                           "
             "║\n");

      for (int i = 0; i < backend_server_count && i < 8;
           i++) { // Limit to 8 for display
        int connected = atomic_load(&backends[i].connected);
        uint32_t reconnects = backends[i].reconnect_count;

        // Format: [slot] host:port status reconnects
        char status_icon = connected ? '+' : 'x';
        printf("║   [%d] %-15s:%-5d  [%c]  reconnects: %3u        ║\n", i,
               backend_servers[i].host, backend_servers[i].port, status_icon,
               reconnects);
      }

      if (backend_server_count > 8) {
        printf("║   ... and %d more backends                                   "
               " ║\n",
               backend_server_count - 8);
      }
    }

    // Backend Thread
    printf(
        "╟────────────────────────────────────────────────────────────────╢\n");
    printf(
        "║ Backend Thread:                                                ║\n");
    uint64_t be_sent = atomic_load(&metrics_backend.messages_sent);
    uint64_t be_recv = atomic_load(&metrics_backend.messages_recv);
    uint64_t be_bytes_s = atomic_load(&metrics_backend.bytes_sent);
    uint64_t be_bytes_r = atomic_load(&metrics_backend.bytes_recv);
    uint32_t be_conn = atomic_load(&metrics_backend.connections);
    uint32_t be_disc = atomic_load(&metrics_backend.disconnections);

    printf("║   Sent:       %10lu msgs  |  %10lu bytes           ║\n", be_sent,
           be_bytes_s);
    printf("║   Received:   %10lu msgs  |  %10lu bytes           ║\n", be_recv,
           be_bytes_r);
    printf("║   Connections:  %6u total  |  %6u disconnections     ║\n",
           be_conn, be_disc);

    // Queue Statistics
    printf(
        "╟────────────────────────────────────────────────────────────────╢\n");
    printf(
        "║ Queue Statistics:                                              ║\n");
    uint32_t q1_depth = queue_depth(&q_ws_to_backend);
    uint32_t q1_drops = atomic_load(&q_ws_to_backend.drops);
    uint32_t q1_hw = atomic_load(&q_ws_to_backend.high_water);
    printf("║   WS→Backend:  depth=%4u | drops=%6u | HW=%4u          ║\n",
           q1_depth, q1_drops, q1_hw);

    uint32_t q2_depth = queue_depth(&q_backend_to_ws);
    uint32_t q2_drops = atomic_load(&q_backend_to_ws.drops);
    uint32_t q2_hw = atomic_load(&q_backend_to_ws.high_water);
    printf("║   Backend→WS:  depth=%4u | drops=%6u | HW=%4u          ║\n",
           q2_depth, q2_drops, q2_hw);

    printf("╚════════════════════════════════════════════════════════════════╝"
           "\n\n");
  }

  printf("[Monitor] Stopped\n");
  return NULL;
}
