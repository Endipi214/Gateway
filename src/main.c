#include <asm-generic/socket.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
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

int eventfd_ws;
int eventfd_backend;
atomic_bool running;

client_conn_t clients[MAX_CLIENTS];

atomic_bool running = ATOMIC_VAR_INIT(1);

metrics_t metrics_ws;
metrics_t metrics_backend;

int ws_port;
backend_server_t backend_servers[MAX_BACKEND_SERVERS];
int backend_server_count = 0;
backend_conn_t backends[MAX_BACKEND_SERVERS];
int use_discovery = 0; // NEW: Discovery mode flag

// Signal handling
void signal_handler(int sig) {
  printf("\n[Main] Received signal %d, shutting down...\n", sig);
  atomic_store_explicit(&running, 0, memory_order_release);
  signal_eventfd(eventfd_ws);
  signal_eventfd(eventfd_backend);
}

int main(int argc, char **argv) {
  if (argc < 2) {
    fprintf(
        stderr,
        "Usage: %s <ws_port> [backend1_host:port] [backend2_host:port] ...\n",
        argv[0]);
    fprintf(stderr, "       %s <ws_port> --discover (use UDP discovery)\n",
            argv[0]);
    fprintf(stderr, "Example: %s 8080 127.0.0.1:9090 127.0.0.1:9091\n",
            argv[0]);
    fprintf(stderr, "         %s 8080 --discover\n", argv[0]);
    return 1;
  }

  ws_port = atoi(argv[1]);

  // Check for discovery mode
  if (argc >= 3 && strcmp(argv[2], "--discover") == 0) {
    use_discovery = 1;
    printf("[Main] Using UDP service discovery mode\n");
  } else {
    // Parse manual backend list
    for (int i = 2; i < argc && backend_server_count < MAX_BACKEND_SERVERS;
         i++) {
      char *colon = strchr(argv[i], ':');
      if (!colon) {
        fprintf(stderr, "Invalid backend format: %s (expected host:port)\n",
                argv[i]);
        continue;
      }

      int host_len = colon - argv[i];
      strncpy(backend_servers[backend_server_count].host, argv[i], host_len);
      backend_servers[backend_server_count].host[host_len] = '\0';
      backend_servers[backend_server_count].port = atoi(colon + 1);
      backend_server_count++;
    }

    if (backend_server_count == 0 && !use_discovery) {
      fprintf(stderr, "Error: No valid backend servers specified\n");
      return 1;
    }
  }

  // Banner
  printf("╔═══════════════════════════════════════════════════════════════╗\n");
  printf("║       High-Performance WebSocket Gateway (Production)         ║\n");
  printf("╠═══════════════════════════════════════════════════════════════╣\n");
  printf("║  WebSocket Port:    %5d                                      ║\n",
         ws_port);

  if (use_discovery) {
    printf("║  Discovery Mode:    ENABLED (UDP port %d)                   ║\n",
           DISCOVERY_PORT);
    printf(
        "║  Backend Servers:   Auto-discovery active                     ║\n");
  } else {
    printf(
        "║  Backend Servers:   %d configured                             ║\n",
        backend_server_count);
    for (int i = 0; i < backend_server_count; i++) {
      printf("║    [%d] %s:%-5d                                           ║\n",
             i, backend_servers[i].host, backend_servers[i].port);
    }
  }

  printf("║  Memory Pool:       %d messages                              ║\n",
         POOL_SIZE);
  printf("║  Queue Size:        %d slots each                            ║\n",
         QUEUE_SIZE);
  printf(
      "╚═══════════════════════════════════════════════════════════════╝\n\n");

  // Setup signal handlers
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
  signal(SIGPIPE, SIG_IGN);

  // Initialize subsystems
  printf("[Main] Initializing subsystems...\n");
  pool_init();
  queue_init(&q_ws_to_backend);
  queue_init(&q_backend_to_ws);
  ws_init();
  backend_init();

  // Initialize discovery if enabled
  pthread_t t_discovery;
  if (use_discovery) {
    if (discovery_init() < 0) {
      fprintf(stderr, "Failed to initialize discovery\n");
      return 1;
    }
    pthread_create(&t_discovery, NULL, discovery_thread_fn, NULL);
    printf("[Main] Discovery thread started\n");
  }

  // Initialize client array
  for (int i = 0; i < MAX_CLIENTS; i++) {
    clients[i].fd = -1;
  }

  // Initialize backend array
  for (int i = 0; i < MAX_BACKEND_SERVERS; i++) {
    backends[i].fd = -1;
    atomic_store(&backends[i].connected, 0);
    backends[i].last_attempt = 0;
    backends[i].reconnect_count = 0;
  }

  // Create eventfds
  eventfd_ws = eventfd(0, EFD_NONBLOCK);
  eventfd_backend = eventfd(0, EFD_NONBLOCK);

  if (eventfd_ws < 0 || eventfd_backend < 0) {
    perror("eventfd");
    return 1;
  }

  // Create WebSocket listen socket
  int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd < 0) {
    perror("socket");
    return 1;
  }

  int opt = 1;
  setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(ws_port);

  if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    perror("bind");
    close(listen_fd);
    return 1;
  }

  if (listen(listen_fd, LISTEN_BACKLOG) < 0) {
    perror("listen");
    close(listen_fd);
    return 1;
  }

  set_nonblocking(listen_fd);

  printf("[Main] Listening on port %d\n", ws_port);
  printf("[Main] Starting threads...\n\n");

  // Start threads
  pthread_t t_ws, t_backend, t_monitor;

  pthread_create(&t_ws, NULL, ws_thread_fn, &listen_fd);
  pthread_create(&t_backend, NULL, backend_thread_fn,
                 &use_discovery); // Pass discovery flag
  pthread_create(&t_monitor, NULL, monitor_thread_fn, NULL);

  printf("[Main] All threads started. Press Ctrl+C to stop.\n");

  // Wait for threads
  pthread_join(t_ws, NULL);
  pthread_join(t_backend, NULL);
  pthread_join(t_monitor, NULL);

  // Wait for discovery thread if running
  if (use_discovery) {
    pthread_join(t_discovery, NULL);
  }

  // Cleanup
  close(listen_fd);
  close(eventfd_ws);
  close(eventfd_backend);
  pool_cleanup();
  ws_cleanup();
  backend_cleanup();

  if (use_discovery) {
    discovery_cleanup();
  }

  printf("\n[Main] Gateway stopped cleanly.\n");
  return 0;
}
