#include "queue.h"

// --------- Global SPSC Queue ---------
spsc_queue_t q_ws_to_backend;
spsc_queue_t q_backend_to_ws;

// --------- Queue API ---------
// Initialize the queue
void queue_init(spsc_queue_t *q) {
  atomic_store_explicit(&q->head, 0, memory_order_relaxed);
  atomic_store_explicit(&q->tail, 0, memory_order_relaxed);
  atomic_store_explicit(&q->drops, 0, memory_order_relaxed);
  atomic_store_explicit(&q->high_water, 0, memory_order_relaxed);
}

// Push a message into the queue, returns 0 if success, -1 if full
int queue_push(spsc_queue_t *q, message_t *msg) {
  uint32_t head = atomic_load_explicit(&q->head, memory_order_relaxed);
  uint32_t next = (head + 1) % QUEUE_SIZE;
  uint32_t tail = atomic_load_explicit(&q->tail, memory_order_acquire);

  if (next == tail) {
    atomic_fetch_add_explicit(&q->drops, 1, memory_order_relaxed);
    return 0;
  }

  q->buffer[head] = msg;
  atomic_store_explicit(&q->head, next, memory_order_release);

  uint32_t depth = (next >= tail) ? (next - tail) : (QUEUE_SIZE - tail + next);
  uint32_t hw = atomic_load_explicit(&q->high_water, memory_order_relaxed);
  if (depth > hw) {
    atomic_store_explicit(&q->high_water, depth, memory_order_relaxed);
  }

  return 1;
}

// Pop a message from the queue, returns NULL if empty
message_t *queue_pop(spsc_queue_t *q) {
  uint32_t tail = atomic_load_explicit(&q->tail, memory_order_relaxed);
  uint32_t head = atomic_load_explicit(&q->head, memory_order_acquire);

  if (tail == head)
    return NULL;

  message_t *msg = q->buffer[tail];
  atomic_store_explicit(&q->tail, (tail + 1) % QUEUE_SIZE,
                        memory_order_release);
  return msg;
}

// Returns the current number of messages in the queue
uint32_t queue_depth(spsc_queue_t *q) {
  uint32_t head = atomic_load_explicit(&q->head, memory_order_acquire);
  uint32_t tail = atomic_load_explicit(&q->tail, memory_order_acquire);
  return (head >= tail) ? (head - tail) : (QUEUE_SIZE - tail + head);
}
