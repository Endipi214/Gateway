#ifndef __MEMPOOL_H__
#define __MEMPOOL_H__

#include "gateway.h"
#include "message.h"

// --------- Memory Pool Structure ---------
// This structure manages a fixed-size pool of pre-allocated messages.
// It uses a stack of free indices and atomic counters for thread-safe
// allocation.
typedef struct {
  message_t *pool;
  atomic_uint free_stack[POOL_SIZE];
  atomic_uint free_count;
  atomic_uint alloc_failures;
} mem_pool_t;

// --------- Global Memory Pool ---------
extern mem_pool_t g_pool; // Single global memory pool instance

// --------- Memory Pool API ---------
void pool_init(void);
message_t *msg_alloc(void);
void msg_free(message_t *msg);

#endif // __MEMPOOL_H__
