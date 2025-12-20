#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
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

// Message queue entry
typedef struct message_entry {
  uint32_t ctx_id;
  chunk_t **chunks;
  uint32_t chunk_count;
  uint32_t total_chunks;
  uint32_t next_send_index;
  int complete;
  struct message_entry *next;
} message_entry_t;

// Per-connection message buffer with queue support
typedef struct {
  message_entry_t *head;    // First message in queue
  message_entry_t *tail;    // Last message in queue
  message_entry_t *sending; // Currently sending message
  uint32_t queue_size;      // Number of messages queued
  uint32_t max_queue_size;  // Maximum queue size
} message_buffer_t;

static message_buffer_t *ws_msg_buffers = NULL;
static message_buffer_t *backend_msg_buffers = NULL;

// Initialize message buffer
static void init_message_buffer(message_buffer_t *buf, uint32_t max_queue) {
  buf->head = NULL;
  buf->tail = NULL;
  buf->sending = NULL;
  buf->queue_size = 0;
  buf->max_queue_size = max_queue;
}

// Free a message entry
static void free_message_entry(message_entry_t *entry) {
  if (!entry)
    return;

  if (entry->chunks) {
    for (uint32_t i = 0; i < entry->chunk_count; i++) {
      if (entry->chunks[i]) {
        msg_context_unref(entry->chunks[i]->ctx);
        chunk_free(entry->chunks[i]);
      }
    }
    free(entry->chunks);
  }
  free(entry);
}

// Free message buffer
static void free_message_buffer(message_buffer_t *buf) {
  message_entry_t *entry = buf->head;
  while (entry) {
    message_entry_t *next = entry->next;
    free_message_entry(entry);
    entry = next;
  }

  if (buf->sending && buf->sending != buf->head) {
    free_message_entry(buf->sending);
  }

  buf->head = NULL;
  buf->tail = NULL;
  buf->sending = NULL;
  buf->queue_size = 0;
}

// Find or create message entry for a chunk
static message_entry_t *find_or_create_message(message_buffer_t *buf,
                                               chunk_t *chunk) {
  // Check if message already exists in queue
  message_entry_t *entry = buf->head;
  while (entry) {
    if (entry->ctx_id == chunk->ctx->ctx_id) {
      return entry;
    }
    entry = entry->next;
  }

  // Check queue size limit
  if (buf->queue_size >= buf->max_queue_size) {
    fprintf(stderr, "[Buffer] Queue full, cannot create new message\n");
    return NULL;
  }

  // Create new entry
  entry = calloc(1, sizeof(message_entry_t));
  if (!entry)
    return NULL;

  entry->ctx_id = chunk->ctx->ctx_id;
  entry->total_chunks = chunk->ctx->total_chunks;
  entry->chunks = calloc(entry->total_chunks, sizeof(chunk_t *));
  if (!entry->chunks) {
    free(entry);
    return NULL;
  }
  entry->chunk_count = 0;
  entry->next_send_index = 0;
  entry->complete = 0;
  entry->next = NULL;

  // Add to queue
  if (buf->tail) {
    buf->tail->next = entry;
    buf->tail = entry;
  } else {
    buf->head = buf->tail = entry;
  }
  buf->queue_size++;

  return entry;
}

// Add chunk to buffer (returns 1 on success, 0 if full)
static int buffer_add_chunk(message_buffer_t *buf, chunk_t *chunk) {
  message_entry_t *entry = find_or_create_message(buf, chunk);
  if (!entry)
    return 0;

  // Check if chunk already exists (duplicate)
  if (chunk->chunk_index < entry->total_chunks &&
      entry->chunks[chunk->chunk_index] != NULL) {
    return 1; // Already have this chunk, silently ignore
  }

  // Store chunk in its position
  if (chunk->chunk_index < entry->total_chunks) {
    entry->chunks[chunk->chunk_index] = chunk;
    entry->chunk_count++;

    // Check if message is complete
    if (entry->chunk_count >= entry->total_chunks) {
      entry->complete = 1;
    }
  }

  return 1;
}

// Get next chunk to send (in order)
static chunk_t *buffer_get_next_chunk(message_buffer_t *buf) {
  if (!buf->sending) {
    // Start sending first complete message
    message_entry_t *entry = buf->head;
    while (entry) {
      if (entry->complete) {
        buf->sending = entry;
        break;
      }
      entry = entry->next;
    }

    if (!buf->sending)
      return NULL; // No complete messages
  }

  message_entry_t *sending = buf->sending;

  // Get next chunk in sequence
  if (sending->next_send_index >= sending->total_chunks) {
    // Message complete, remove from queue
    if (buf->head == sending) {
      buf->head = sending->next;
      if (!buf->head)
        buf->tail = NULL;
    } else {
      // Find previous entry
      message_entry_t *prev = buf->head;
      while (prev && prev->next != sending) {
        prev = prev->next;
      }
      if (prev) {
        prev->next = sending->next;
        if (buf->tail == sending)
          buf->tail = prev;
      }
    }

    buf->queue_size--;
    free_message_entry(sending);
    buf->sending = NULL;

    // Try next message
    return buffer_get_next_chunk(buf);
  }

  chunk_t *chunk = sending->chunks[sending->next_send_index];
  if (!chunk) {
    // Chunk missing? Should not happen if complete
    fprintf(stderr, "[Buffer] Missing chunk %u in complete message\n",
            sending->next_send_index);
    return NULL;
  }

  sending->chunks[sending->next_send_index] = NULL; // Remove reference
  sending->next_send_index++;

  return chunk;
}

// Try to send chunks from buffer
static void try_send_buffered_chunks(int fd, int epoll_fd,
                                     message_buffer_t *buf,
                                     int (*send_func)(int, chunk_t *),
                                     metrics_t *metrics, int is_ws) {
  struct epoll_event ev;

  // For WebSocket: Only send complete messages
  if (is_ws) {
    // Check if we have at least one complete message
    int has_complete = 0;
    message_entry_t *entry = buf->head;
    while (entry) {
      if (entry->complete) {
        has_complete = 1;
        break;
      }
      entry = entry->next;
    }
    if (!has_complete)
      return;
  }

  // Keep sending chunks while available
  while (1) {
    chunk_t *chunk = buffer_get_next_chunk(buf);
    if (!chunk)
      break;

    int result = send_func(fd, chunk);

    if (result == 0) {
      // Complete send
      atomic_fetch_add(&metrics->messages_sent, 1);
      atomic_fetch_add(&metrics->bytes_sent, chunk->len);

      if (is_ws) {
        printf("[WS] Sent chunk %u/%u → client_id=%u (%u bytes)\n",
               chunk->chunk_index + 1, chunk->ctx->total_chunks,
               chunk->ctx->client_id, chunk->len);
      } else {
        printf("[Backend] Sent chunk %u/%u → backend_id=%u (%u bytes)\n",
               chunk->chunk_index + 1, chunk->ctx->total_chunks,
               chunk->ctx->backend_id, chunk->len);
      }

      msg_context_unref(chunk->ctx);
      chunk_free(chunk);

    } else if (result == 1) {
      // Partial send - need to retry this chunk
      // Put chunk back at current position
      if (buf->sending) {
        if (buf->sending->next_send_index > 0) {
          buf->sending->next_send_index--;
          buf->sending->chunks[buf->sending->next_send_index] = chunk;
        }
      }

      // Enable EPOLLOUT
      ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
      ev.data.fd = fd;
      epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
      break;

    } else {
      // Error
      fprintf(stderr, "[Send] Error sending chunk to fd=%d\n", fd);
      msg_context_unref(chunk->ctx);
      chunk_free(chunk);
      break;
    }
  }
}

static void drain_backend_to_ws_queue(int epoll_fd) {
  chunk_t *chunk;

  while ((chunk = queue_pop(&q_backend_to_ws)) != NULL) {
    if (!chunk->ctx) {
      fprintf(stderr, "[WS] Chunk missing context, dropping\n");
      chunk_free(chunk);
      continue;
    }

    uint32_t target_client_id = chunk->ctx->client_id;

    // Broadcast to all clients
    if (target_client_id == 0) {
      int broadcast_count = 0;
      pthread_mutex_lock(&clients_lock);

      for (int i = 1; i < MAX_CLIENTS; i++) {
        if (clients[i].fd > 0 && clients[i].state == CLIENT_STATE_ACTIVE) {
          chunk_t *bc = chunk_alloc(chunk->len);
          if (bc) {
            bc->ctx = chunk->ctx;
            msg_context_ref(chunk->ctx);
            bc->chunk_index = chunk->chunk_index;
            bc->len = chunk->len;
            bc->is_first_chunk = chunk->is_first_chunk;
            memcpy(bc->data, chunk->data, chunk->len);

            int client_fd = clients[i].fd;
            message_buffer_t *buf = &ws_msg_buffers[client_fd % MAX_CLIENTS];

            if (buffer_add_chunk(buf, bc)) {
              try_send_buffered_chunks(client_fd, epoll_fd, buf, send_ws_chunk,
                                       &metrics_ws, 1);
              broadcast_count++;
            } else {
              msg_context_unref(bc->ctx);
              chunk_free(bc);
            }
          }
        }
      }
      pthread_mutex_unlock(&clients_lock);

      printf("[WS] Broadcast chunk %u/%u → %d clients (%u bytes)\n",
             chunk->chunk_index + 1, chunk->ctx->total_chunks, broadcast_count,
             chunk->len);
      msg_context_unref(chunk->ctx);
      chunk_free(chunk);
      continue;
    }

    // Unicast to specific client
    int client_fd = target_client_id;

    if (client_fd <= 0 || client_fd >= MAX_CLIENTS) {
      fprintf(stderr, "[WS] Invalid client_id=%u\n", target_client_id);
      msg_context_unref(chunk->ctx);
      chunk_free(chunk);
      continue;
    }

    pthread_mutex_lock(&clients_lock);
    int client_valid = (clients[client_fd].fd == client_fd &&
                        clients[client_fd].state == CLIENT_STATE_ACTIVE);
    pthread_mutex_unlock(&clients_lock);

    if (!client_valid) {
      fprintf(stderr, "[WS] Client id=%u not active\n", target_client_id);
      msg_context_unref(chunk->ctx);
      chunk_free(chunk);
      continue;
    }

    message_buffer_t *buf = &ws_msg_buffers[client_fd % MAX_CLIENTS];

    if (buffer_add_chunk(buf, chunk)) {
      try_send_buffered_chunks(client_fd, epoll_fd, buf, send_ws_chunk,
                               &metrics_ws, 1);
    } else {
      fprintf(stderr, "[WS] Buffer full for client_id=%u, dropping chunk\n",
              target_client_id);
      msg_context_unref(chunk->ctx);
      chunk_free(chunk);
    }
  }
}

void *ws_thread_fn(void *arg) {
  int listen_fd = *(int *)arg;
  int epoll_fd = epoll_create1(0);
  if (epoll_fd < 0) {
    perror("epoll_create1");
    return NULL;
  }

  // Initialize message buffers
  ws_msg_buffers = calloc(MAX_CLIENTS, sizeof(message_buffer_t));
  for (int i = 0; i < MAX_CLIENTS; i++) {
    init_message_buffer(&ws_msg_buffers[i],
                        32); // Queue up to 32 concurrent messages per client
  }

  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = listen_fd;
  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev);

  ev.events = EPOLLIN;
  ev.data.fd = eventfd_ws;
  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, eventfd_ws, &ev);

  printf(
      "[WS Thread] Started - chunked operation mode with message buffering\n");

  struct epoll_event events[EPOLL_EVENTS];

  while (atomic_load_explicit(&running, memory_order_acquire)) {
    drain_backend_to_ws_queue(epoll_fd);

    int n = epoll_wait(epoll_fd, events, EPOLL_EVENTS, 100);

    for (int i = 0; i < n; i++) {
      int fd = events[i].data.fd;

      if (fd == eventfd_ws) {
        drain_eventfd(eventfd_ws);
        drain_backend_to_ws_queue(epoll_fd);
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
        message_buffer_t *buf = &ws_msg_buffers[fd % MAX_CLIENTS];
        try_send_buffered_chunks(fd, epoll_fd, buf, send_ws_chunk, &metrics_ws,
                                 1);
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

          msg_context_t *ctx = msg_context_alloc();
          if (ctx) {
            ctx->client_id = fd;
            ctx->backend_id = 0;
            ctx->total_len = 0;
            ctx->total_chunks = 1;
            ctx->timestamp_ns = get_time_ns();

            chunk_t *welcome = chunk_alloc(0);
            if (welcome) {
              welcome->ctx = ctx;
              welcome->chunk_index = 0;
              welcome->len = 0;
              welcome->is_first_chunk = 1;

              message_buffer_t *buf = &ws_msg_buffers[fd % MAX_CLIENTS];
              if (buffer_add_chunk(buf, welcome)) {
                try_send_buffered_chunks(fd, epoll_fd, buf, send_ws_chunk,
                                         &metrics_ws, 1);
              } else {
                msg_context_unref(ctx);
                chunk_free(welcome);
              }
            } else {
              msg_context_unref(ctx);
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

      // Read chunks from client
      chunk_t *chunk;
      while ((chunk = parse_ws_chunk(fd)) != NULL) {
        if (!chunk->ctx) {
          chunk_free(chunk);
          continue;
        }

        chunk->ctx->client_id =
            fd; // IMPORTANT: Set actual client FD as client_id

        atomic_fetch_add(&metrics_ws.messages_recv, 1);
        atomic_fetch_add(&metrics_ws.bytes_recv, chunk->len);

        printf("[WS] Received chunk %u/%u from client_id=%u → backend_id=%u "
               "(%u bytes)\n",
               chunk->chunk_index + 1, chunk->ctx->total_chunks,
               chunk->ctx->client_id, chunk->ctx->backend_id, chunk->len);

        // Broadcast to all backends
        if (chunk->ctx->backend_id == 0) {
          int backend_broadcast_count = 0;
          for (int j = 0; j < backend_server_count; j++) {
            if (atomic_load(&backends[j].connected)) {
              chunk_t *bc = chunk_alloc(chunk->len);
              if (bc) {
                bc->ctx = chunk->ctx;
                msg_context_ref(chunk->ctx);
                bc->chunk_index = chunk->chunk_index;
                bc->len = chunk->len;
                bc->is_first_chunk = chunk->is_first_chunk;
                memcpy(bc->data, chunk->data, chunk->len);

                if (queue_push(&q_ws_to_backend, bc)) {
                  backend_broadcast_count++;
                } else {
                  msg_context_unref(bc->ctx);
                  chunk_free(bc);
                }
              }
            }
          }

          if (backend_broadcast_count > 0) {
            signal_eventfd(eventfd_backend);
          }

          // Broadcast to other clients
          int client_broadcast_count = 0;
          pthread_mutex_lock(&clients_lock);
          for (int j = 1; j < MAX_CLIENTS; j++) {
            if (clients[j].fd > 0 && clients[j].fd != fd &&
                clients[j].state == CLIENT_STATE_ACTIVE) {
              chunk_t *cc = chunk_alloc(chunk->len);
              if (cc) {
                cc->ctx = chunk->ctx;
                msg_context_ref(chunk->ctx);
                cc->chunk_index = chunk->chunk_index;
                cc->len = chunk->len;
                cc->is_first_chunk = chunk->is_first_chunk;
                memcpy(cc->data, chunk->data, chunk->len);

                int client_fd = clients[j].fd;
                message_buffer_t *buf =
                    &ws_msg_buffers[client_fd % MAX_CLIENTS];

                if (buffer_add_chunk(buf, cc)) {
                  try_send_buffered_chunks(client_fd, epoll_fd, buf,
                                           send_ws_chunk, &metrics_ws, 1);
                  client_broadcast_count++;
                } else {
                  msg_context_unref(cc->ctx);
                  chunk_free(cc);
                }
              }
            }
          }
          pthread_mutex_unlock(&clients_lock);

          msg_context_unref(chunk->ctx);
          chunk_free(chunk);
          continue;
        }

        // Send to specific backend
        int backend_slot = chunk->ctx->backend_id - 1;
        if (backend_slot >= 0 && backend_slot < backend_server_count &&
            atomic_load(&backends[backend_slot].connected)) {
          if (queue_push(&q_ws_to_backend, chunk)) {
            signal_eventfd(eventfd_backend);
            continue;
          }
        }

        msg_context_unref(chunk->ctx);
        chunk_free(chunk);
      }

      if (errno != EAGAIN && errno != EWOULDBLOCK) {
        printf("[WS] Client client_id=%d disconnected\n", fd);
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
        close(fd);
        pthread_mutex_lock(&clients_lock);
        if (fd < MAX_CLIENTS && fd > 0) {
          clients[fd].fd = -1;
          free_message_buffer(&ws_msg_buffers[fd % MAX_CLIENTS]);
        }
        pthread_mutex_unlock(&clients_lock);
        atomic_fetch_add(&metrics_ws.disconnections, 1);
      }
    }
  }

  // Cleanup
  for (int i = 0; i < MAX_CLIENTS; i++) {
    free_message_buffer(&ws_msg_buffers[i]);
  }
  free(ws_msg_buffers);

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

  // Initialize message buffers
  backend_msg_buffers = calloc(MAX_BACKENDS, sizeof(message_buffer_t));
  for (int i = 0; i < MAX_BACKENDS; i++) {
    init_message_buffer(&backend_msg_buffers[i], 32);
  }

  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = eventfd_backend;
  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, eventfd_backend, &ev);

  printf("[Backend Thread] Started - chunked operation mode with message "
         "buffering\n");

  struct epoll_event events[EPOLL_EVENTS];
  time_t last_reconnect = 0;

  while (atomic_load_explicit(&running, memory_order_acquire)) {
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

    // Process outgoing chunks
    chunk_t *chunk;
    while ((chunk = queue_pop(&q_ws_to_backend)) != NULL) {
      if (!chunk->ctx) {
        chunk_free(chunk);
        continue;
      }

      int backend_slot = chunk->ctx->backend_id - 1;

      if (backend_slot < 0 || backend_slot >= backend_server_count) {
        fprintf(stderr, "[Backend] Invalid backend_id=%u\n",
                chunk->ctx->backend_id);
        msg_context_unref(chunk->ctx);
        chunk_free(chunk);
        continue;
      }

      if (!atomic_load_explicit(&backends[backend_slot].connected,
                                memory_order_acquire)) {
        fprintf(stderr, "[Backend] Backend_id=%u not connected\n",
                chunk->ctx->backend_id);
        msg_context_unref(chunk->ctx);
        chunk_free(chunk);
        continue;
      }

      int backend_fd = backends[backend_slot].fd;
      message_buffer_t *buf = &backend_msg_buffers[backend_fd % MAX_BACKENDS];

      if (buffer_add_chunk(buf, chunk)) {
        try_send_buffered_chunks(backend_fd, epoll_fd, buf, write_backend_chunk,
                                 &metrics_backend, 0);
      } else {
        fprintf(stderr, "[Backend] Buffer full, dropping chunk\n");
        msg_context_unref(chunk->ctx);
        chunk_free(chunk);
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
        message_buffer_t *buf = &backend_msg_buffers[fd % MAX_BACKENDS];
        try_send_buffered_chunks(fd, epoll_fd, buf, write_backend_chunk,
                                 &metrics_backend, 0);
      }

      if (!(events[i].events & EPOLLIN)) {
        continue;
      }

      // Read chunks from backend
      chunk_t *chunk;
      while ((chunk = read_backend_chunk(fd)) != NULL) {
        if (!chunk->ctx) {
          chunk_free(chunk);
          continue;
        }

        // Find which backend slot this is (for logging only, don't modify
        // chunk->ctx->backend_id!)
        int backend_slot = -1;
        for (int j = 0; j < backend_server_count; j++) {
          if (backends[j].fd == fd) {
            backend_slot = j;
            break;
          }
        }

        atomic_fetch_add_explicit(&metrics_backend.messages_recv, 1,
                                  memory_order_relaxed);
        atomic_fetch_add_explicit(&metrics_backend.bytes_recv, chunk->len,
                                  memory_order_relaxed);

        printf("[Backend] Received chunk %u/%u from backend slot %d (fd=%d): "
               "%u bytes → client_id=%u\n",
               chunk->chunk_index + 1, chunk->ctx->total_chunks, backend_slot,
               fd, chunk->len, chunk->ctx->client_id);

        // IMPORTANT: Do NOT modify chunk->ctx->backend_id here
        // The backend_id in the chunk was set by the backend server itself
        // The client_id tells us where to route this response

        if (!queue_push(&q_backend_to_ws, chunk)) {
          fprintf(stderr, "[Backend] Queue full, dropping chunk\n");
          msg_context_unref(chunk->ctx);
          chunk_free(chunk);
        } else {
          signal_eventfd(eventfd_ws);
        }
      }

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
            printf("[Backend] Disconnected from %s:%d (fd=%d, backend_id=%d)\n",
                   backend_servers[j].host, backend_servers[j].port, fd, j + 1);

            free_message_buffer(&backend_msg_buffers[fd % MAX_BACKENDS]);
            break;
          }
        }
      }
    }
  }

  // Cleanup
  for (int i = 0; i < backend_server_count; i++) {
    if (backends[i].fd >= 0) {
      close(backends[i].fd);
    }
  }

  for (int i = 0; i < MAX_BACKENDS; i++) {
    free_message_buffer(&backend_msg_buffers[i]);
  }
  free(backend_msg_buffers);

  close(epoll_fd);
  printf("[Backend Thread] Stopped\n");
  return NULL;
}

void *monitor_thread_fn(void *arg) {
  printf("\n[Monitor] Started - reporting every 5 seconds\n\n");

  while (atomic_load_explicit(&running, memory_order_acquire)) {
    sleep(5);

    printf("═══════════════════════════════════════════════════════════\n");
    printf("Tiered Memory Pool (Chunked):\n");

    uint64_t total_allocs = atomic_load(&g_tiered_pool.total_allocs);
    uint64_t total_frees = atomic_load(&g_tiered_pool.total_frees);
    uint64_t in_use = total_allocs - total_frees;

    printf("   Total Chunk Allocations: %10lu\n", total_allocs);
    printf("   Total Frees:             %10lu\n", total_frees);
    printf("   Chunks In-Use:           %10lu\n", in_use);

    for (int i = 0; i < TIER_COUNT; i++) {
      uint32_t free, total, failures;
      uint64_t allocs;
      pool_get_tier_stats(i, &free, &total, &allocs, &failures);

      uint32_t used = total - free;
      float usage = (total > 0) ? (100.0f * used / total) : 0.0f;
      printf(
          "   Tier %d: %4u/%4u slots (%.1f%%) | allocs: %lu | failures: %u\n",
          i, used, total, usage, allocs, failures);
    }

    uint32_t ctx_free, ctx_total;
    pool_get_ctx_stats(&ctx_free, &ctx_total);
    uint32_t ctx_used = ctx_total - ctx_free;
    float ctx_usage = (ctx_total > 0) ? (100.0f * ctx_used / ctx_total) : 0.0f;
    printf("   Message Contexts: %4u/%4u (%.1f%% used)\n", ctx_used, ctx_total,
           ctx_usage);

    printf("WebSocket Thread:\n");
    uint64_t ws_sent = atomic_load(&metrics_ws.messages_sent);
    uint64_t ws_recv = atomic_load(&metrics_ws.messages_recv);
    uint64_t ws_bytes_s = atomic_load(&metrics_ws.bytes_sent);
    uint64_t ws_bytes_r = atomic_load(&metrics_ws.bytes_recv);

    printf("   Chunks Sent:    %10lu  |  %10lu bytes\n", ws_sent, ws_bytes_s);
    printf("   Chunks Received:%10lu  |  %10lu bytes\n", ws_recv, ws_bytes_r);

    printf("Backend Thread:\n");
    uint64_t be_sent = atomic_load(&metrics_backend.messages_sent);
    uint64_t be_recv = atomic_load(&metrics_backend.messages_recv);
    uint64_t be_bytes_s = atomic_load(&metrics_backend.bytes_sent);
    uint64_t be_bytes_r = atomic_load(&metrics_backend.bytes_recv);

    printf("   Chunks Sent:    %10lu  |  %10lu bytes\n", be_sent, be_bytes_s);
    printf("   Chunks Received:%10lu  |  %10lu bytes\n", be_recv, be_bytes_r);

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

    printf("Backend Connections:\n");
    for (int i = 0; i < backend_server_count && i < 8; i++) {
      int connected = atomic_load(&backends[i].connected);
      char status = connected ? '+' : 'x';
      printf("   [%d] %s:%d [%c]\n", i, backend_servers[i].host,
             backend_servers[i].port, status);
    }

    printf(
        "═══════════════════════════════════════════════════════════════\n\n");
  }

  printf("[Monitor] Stopped\n");
  return NULL;
}
