#ifndef __QUEUE_H__
#define __QUEUE_H__

#include "gateway.h"
#include "message.h"

// --------- Single-Producer Single-Consumer Queue ---------
// This is a lock-free SPSC queue for passing messages between
// threads or event loops. Designed for high-performance, non-blocking use.
typedef struct {
  message_t *buffer[QUEUE_SIZE];
  atomic_uint head;
  atomic_uint tail;
  atomic_uint drops;
  atomic_uint high_water;
} spsc_queue_t;

// --------- Global SPSC Queue ---------
extern spsc_queue_t q_ws_to_backend;
extern spsc_queue_t q_backend_to_ws;

// --------- Queue API ---------
// Initialize the queue
void queue_init(spsc_queue_t *q);
// Push a message into the queue, returns 0 if success, -1 if full
int queue_push(spsc_queue_t *q, message_t *msg);
// Pop a message from the queue, returns NULL if empty
message_t *queue_pop(spsc_queue_t *q);
// Returns the current number of messages in the queue
uint32_t queue_depth(spsc_queue_t *q);

#endif // __QUEUE_H__
