#ifndef __QUEUE_H__
#define __QUEUE_H__

#include "gateway.h"
#include "mempool.h"

// --------- Single-Producer Single-Consumer Queue ---------
// Now works with chunks instead of full messages
typedef struct {
  chunk_t *buffer[QUEUE_SIZE];
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
// Push a chunk into the queue, returns 1 if success, 0 if full
int queue_push(spsc_queue_t *q, chunk_t *chunk);
// Pop a chunk from the queue, returns NULL if empty
chunk_t *queue_pop(spsc_queue_t *q);
// Returns the current number of chunks in the queue
uint32_t queue_depth(spsc_queue_t *q);

#endif // __QUEUE_H__
