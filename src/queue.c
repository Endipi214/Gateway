#include "queue.h"
#include <stdio.h>

priority_spsc_queue_t q_ws_to_backend;
priority_spsc_queue_t q_backend_to_ws;

void queue_init(priority_spsc_queue_t *q) {
  atomic_store_explicit(&q->head, 0, memory_order_relaxed);
  atomic_store_explicit(&q->tail, 0, memory_order_relaxed);
  atomic_store_explicit(&q->drops, 0, memory_order_relaxed);
  atomic_store_explicit(&q->high_water, 0, memory_order_relaxed);

  atomic_store_explicit(&q->priority_head, 0, memory_order_relaxed);
  atomic_store_explicit(&q->priority_tail, 0, memory_order_relaxed);
  atomic_store_explicit(&q->priority_drops, 0, memory_order_relaxed);

  atomic_store_explicit(&q->backpressure_events, 0, memory_order_relaxed);
}

int queue_push(priority_spsc_queue_t *q, message_t *msg, int *should_throttle) {
  if (should_throttle) {
    *should_throttle = 0;
  }

  // High priority messages go to priority queue
  if (msg->priority >= MSG_PRIORITY_HIGH) {
    uint32_t phead =
        atomic_load_explicit(&q->priority_head, memory_order_relaxed);
    uint32_t pnext = (phead + 1) % (QUEUE_SIZE / 4);
    uint32_t ptail =
        atomic_load_explicit(&q->priority_tail, memory_order_acquire);

    if (pnext == ptail) {
      // Priority queue full - this is serious
      atomic_fetch_add_explicit(&q->priority_drops, 1, memory_order_relaxed);
      fprintf(
          stderr,
          "[Queue] WARNING: Priority queue full! Dropping priority message\n");
      return 0;
    }

    q->priority_buffer[phead] = msg;
    atomic_store_explicit(&q->priority_head, pnext, memory_order_release);
    return 1;
  }

  // Normal priority messages
  uint32_t head = atomic_load_explicit(&q->head, memory_order_relaxed);
  uint32_t next = (head + 1) % QUEUE_SIZE;
  uint32_t tail = atomic_load_explicit(&q->tail, memory_order_acquire);

  uint32_t depth = (next >= tail) ? (next - tail) : (QUEUE_SIZE - tail + next);

  // Check for backpressure condition
  if (should_throttle && depth > (QUEUE_SIZE * QUEUE_HIGH_WATER_THRESHOLD)) {
    *should_throttle = 1;
    atomic_fetch_add_explicit(&q->backpressure_events, 1, memory_order_relaxed);
  }

  if (next == tail) {
    // Queue full
    if (msg->drop_if_full) {
      atomic_fetch_add_explicit(&q->drops, 1, memory_order_relaxed);
      return 0; // Message dropped
    }
    return -1; // Queue full, cannot drop - caller should retry
  }

  q->buffer[head] = msg;
  atomic_store_explicit(&q->head, next, memory_order_release);

  // Update high water mark
  uint32_t hw = atomic_load_explicit(&q->high_water, memory_order_relaxed);
  if (depth > hw) {
    atomic_store_explicit(&q->high_water, depth, memory_order_relaxed);
  }

  return 1;
}

message_t *queue_pop(priority_spsc_queue_t *q) {
  // Always check priority queue first
  uint32_t ptail =
      atomic_load_explicit(&q->priority_tail, memory_order_relaxed);
  uint32_t phead =
      atomic_load_explicit(&q->priority_head, memory_order_acquire);

  if (ptail != phead) {
    message_t *msg = q->priority_buffer[ptail];
    atomic_store_explicit(&q->priority_tail, (ptail + 1) % (QUEUE_SIZE / 4),
                          memory_order_release);
    return msg;
  }

  // Then check normal queue
  uint32_t tail = atomic_load_explicit(&q->tail, memory_order_relaxed);
  uint32_t head = atomic_load_explicit(&q->head, memory_order_acquire);

  if (tail == head)
    return NULL;

  message_t *msg = q->buffer[tail];
  atomic_store_explicit(&q->tail, (tail + 1) % QUEUE_SIZE,
                        memory_order_release);
  return msg;
}

uint32_t queue_depth(priority_spsc_queue_t *q) {
  uint32_t head = atomic_load_explicit(&q->head, memory_order_acquire);
  uint32_t tail = atomic_load_explicit(&q->tail, memory_order_acquire);
  uint32_t normal_depth =
      (head >= tail) ? (head - tail) : (QUEUE_SIZE - tail + head);

  uint32_t phead =
      atomic_load_explicit(&q->priority_head, memory_order_acquire);
  uint32_t ptail =
      atomic_load_explicit(&q->priority_tail, memory_order_acquire);
  uint32_t priority_depth =
      (phead >= ptail) ? (phead - ptail) : ((QUEUE_SIZE / 4) - ptail + phead);

  return normal_depth + priority_depth;
}

float queue_utilization(priority_spsc_queue_t *q) {
  uint32_t depth = queue_depth(q);
  return (float)depth / (float)QUEUE_SIZE;
}
