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

#include "gateway.h"
#include "mempool.h"
#include "metrics.h"
#include "queue.h"
#include "thread.h"
#include "utils.h"

int eventfd_ws;
int eventfd_backend;
atomic_bool running;

client_conn_t clients[MAX_CLIENTS];
backend_conn_t backends[BACKEND_POOL_SIZE];

metrics_t metrics_ws;
metrics_t metrics_backend;

int ws_port;
int backend_port;
char backend_host[256];

// Signal handling
void signal_handler(int sig) {
  printf("\n[Main] Received signal %d, shutting down...\n", sig);
  atomic_store_explicit(&running, 0, memory_order_release);
  signal_eventfd(eventfd_ws);
  signal_eventfd(eventfd_backend);
}

int main(int argc, char **argv) {
  if (argc != 4) {
    fprintf(stderr, "Usage: %s <ws_port> <backend_host> <backend_port>\n",
            argv[0]);
    fprintf(stderr, "Example: %s 8080 127.0.0.1 9090\n", argv[0]);
    return 1;
  }

  ws_port = atoi(argv[1]);
  strncpy(backend_host, argv[2], sizeof(backend_host) - 1);
  backend_port = atoi(argv[3]);

  printf(
      "╔════════════════════════════════════════════════════════════════╗\n");
  printf("║       High-Performance WebSocket Gateway (Production)         ║\n");
  printf(
      "╠════════════════════════════════════════════════════════════════╣\n");
  printf("║  WebSocket Port:    %5d                                      ║\n",
         ws_port);
  printf("║  Backend:           %s:%-5d                              ║\n",
         backend_host, backend_port);
  printf("║  Memory Pool:       %d messages                              ║\n",
         POOL_SIZE);
  printf("║  Queue Size:        %d slots each                            ║\n",
         QUEUE_SIZE);
  printf("║  Backend Pool:      %d connections                           ║\n",
         BACKEND_POOL_SIZE);
  printf(
      "╚════════════════════════════════════════════════════════════════╝\n\n");

  // Setup signal handlers
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
  signal(SIGPIPE, SIG_IGN);

  // Initialize subsystems
  pool_init();
  queue_init(&q_ws_to_backend);
  queue_init(&q_backend_to_ws);

  // Initialize client array
  for (int i = 0; i < MAX_CLIENTS; i++) {
    clients[i].fd = -1;
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
  pthread_create(&t_backend, NULL, backend_thread_fn, NULL);
  pthread_create(&t_monitor, NULL, monitor_thread_fn, NULL);

  printf("[Main] All threads started. Press Ctrl+C to stop.\n");

  // Wait for threads
  pthread_join(t_ws, NULL);
  pthread_join(t_backend, NULL);
  pthread_join(t_monitor, NULL);

  // Cleanup
  close(listen_fd);
  close(eventfd_ws);
  close(eventfd_backend);
  free(g_pool.pool);

  printf("\n[Main] Gateway stopped cleanly.\n");
  return 0;
}
