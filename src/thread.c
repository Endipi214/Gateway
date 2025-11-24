#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "backend.h"
#include "gateway.h"
#include "mempool.h"
#include "message.h"
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

  printf("[WS Thread] Started on port %d\n", ws_port);

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

      // Route to available backend
      msg->backend_fd = get_available_backend();

      if (msg->backend_fd < 0 || !queue_push(&q_ws_to_backend, msg)) {
        msg_free(msg);
      } else {
        signal_eventfd(eventfd_backend);
      }
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

  // Initialize backend connections
  for (int i = 0; i < BACKEND_POOL_SIZE; i++) {
    backends[i].fd = -1;
    atomic_store_explicit(&backends[i].connected, 0, memory_order_release);
    backends[i].last_attempt = 0;
    backends[i].reconnect_count = 0;
  }

  printf("[Backend Thread] Started, connecting to %s:%d\n", backend_host,
         backend_port);

  struct epoll_event events[EPOLL_EVENTS];
  time_t last_reconnect = 0;

  while (atomic_load_explicit(&running, memory_order_acquire)) {
    // Attempt reconnections
    time_t now = time(NULL);
    if (now - last_reconnect >= RECONNECT_INTERVAL) {
      for (int i = 0; i < BACKEND_POOL_SIZE; i++) {
        if (!atomic_load_explicit(&backends[i].connected,
                                  memory_order_acquire)) {
          int fd = connect_to_backend(backend_host, backend_port);
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
                   backend_host, backend_port, fd, i);
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
        // Check for disconnection
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
          for (int j = 0; j < BACKEND_POOL_SIZE; j++) {
            if (backends[j].fd == fd) {
              epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
              close(fd);
              backends[j].fd = -1;
              atomic_store_explicit(&backends[j].connected, 0,
                                    memory_order_release);
              atomic_fetch_add_explicit(&metrics_backend.disconnections, 1,
                                        memory_order_relaxed);
              printf("[Backend] Disconnected from %s:%d (fd=%d, slot=%d)\n",
                     backend_host, backend_port, fd, j);
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
  for (int i = 0; i < BACKEND_POOL_SIZE; i++) {
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

    // Memory Pool
    uint32_t free_count = atomic_load(&g_pool.free_count);
    uint32_t alloc_fail = atomic_load(&g_pool.alloc_failures);
    printf("║ Memory Pool:  %4u / %4u free  |  %6u alloc failures     ║\n",
           free_count, POOL_SIZE, alloc_fail);

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

    // Backend pool status
    int active_backends = 0;
    for (int i = 0; i < BACKEND_POOL_SIZE; i++) {
      if (atomic_load(&backends[i].connected))
        active_backends++;
    }
    printf(
        "║   Active backends: %d / %d                                     ║\n",
        active_backends, BACKEND_POOL_SIZE);

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
