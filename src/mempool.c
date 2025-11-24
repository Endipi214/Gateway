#include "mempool.h"
#include <stdio.h>
#include <stdlib.h>

mem_pool_t g_pool;

void pool_init(void) {
  g_pool.pool = aligned_alloc(64, POOL_SIZE * sizeof(message_t));
  if (!g_pool.pool) {
    perror("aligned_alloc");
    exit(1);
  }

  for (uint32_t i = 0; i < POOL_SIZE; i++) {
    atomic_store_explicit(&g_pool.free_stack[i], i, memory_order_relaxed);
  }
  atomic_store_explicit(&g_pool.free_count, POOL_SIZE, memory_order_release);
  atomic_store_explicit(&g_pool.alloc_failures, 0, memory_order_relaxed);
}

message_t *msg_alloc(void) {
  uint32_t count =
      atomic_load_explicit(&g_pool.free_count, memory_order_acquire);

  while (count > 0) {
    if (atomic_compare_exchange_weak_explicit(&g_pool.free_count, &count,
                                              count - 1, memory_order_acquire,
                                              memory_order_relaxed)) {

      uint32_t idx = atomic_load_explicit(&g_pool.free_stack[count - 1],
                                          memory_order_relaxed);
      return &g_pool.pool[idx];
    }
  }

  atomic_fetch_add_explicit(&g_pool.alloc_failures, 1, memory_order_relaxed);
  return NULL;
}

void msg_free(message_t *msg) {
  if (!msg)
    return;

  uint32_t idx = msg - g_pool.pool;
  uint32_t count =
      atomic_load_explicit(&g_pool.free_count, memory_order_acquire);

  atomic_store_explicit(&g_pool.free_stack[count], idx, memory_order_relaxed);
  atomic_fetch_add_explicit(&g_pool.free_count, 1, memory_order_release);
}
