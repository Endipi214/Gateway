#ifndef __QUEUE_H__
#define __QUEUE_H__

#include "gateway.h"
#include "mempool.h"

#define QUEUE_HIGH_WATER_THRESHOLD 0.8 // 80% full triggers backpressure

// --------- Priority SPSC Queue ---------
typedef struct {
  message_t *buffer[QUEUE_SIZE];
  atomic_uint head;
  atomic_uint tail;
  atomic_uint drops;
  atomic_uint high_water;

  // Priority lane (25% of main queue size)
  message_t *priority_buffer[QUEUE_SIZE / 4];
  atomic_uint priority_head;
  atomic_uint priority_tail;
  atomic_uint priority_drops;

  // Backpressure tracking
  atomic_uint backpressure_events;
} priority_spsc_queue_t;

// --------- Global Queues ---------
extern priority_spsc_queue_t q_ws_to_backend;
extern priority_spsc_queue_t q_backend_to_ws;

// --------- Queue API ---------
void queue_init(priority_spsc_queue_t *q);
int queue_push(priority_spsc_queue_t *q, message_t *msg, int *should_throttle);
message_t *queue_pop(priority_spsc_queue_t *q);
uint32_t queue_depth(priority_spsc_queue_t *q);
float queue_utilization(priority_spsc_queue_t *q);

#endif // __QUEUE_H__
