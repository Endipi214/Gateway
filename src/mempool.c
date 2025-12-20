#include "mempool.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

tiered_mem_pool_t g_tiered_pool;
msg_context_pool_t g_ctx_pool;

// Tier configuration table
static const struct {
  uint32_t data_size;
  uint32_t slot_count;
} tier_config[TIER_COUNT] = {
    {TIER_0_SIZE, TIER_0_SLOTS},
    {TIER_1_SIZE, TIER_1_SLOTS},
    {TIER_2_SIZE, TIER_2_SLOTS},
    {TIER_3_SIZE, TIER_3_SLOTS},
};

// Helper: Calculate which tier is needed for a given size
static inline uint8_t get_tier_for_size(uint32_t size) {
  if (size <= TIER_0_SIZE)
    return 0;
  if (size <= TIER_1_SIZE)
    return 1;
  if (size <= TIER_2_SIZE)
    return 2;
  if (size <= TIER_3_SIZE)
    return 3;
  return TIER_COUNT; // Size too large
}

// Initialize a single tier
static int init_tier(tier_pool_t *tier, uint32_t data_size,
                     uint32_t slot_count) {
  tier->data_size = data_size;
  tier->slot_count = slot_count;
  tier->slot_size = sizeof(chunk_t) + data_size;

  // Allocate memory pool (aligned for performance)
  tier->pool = aligned_alloc(64, tier->slot_size * slot_count);
  if (!tier->pool) {
    fprintf(stderr,
            "[MemPool] Failed to allocate tier (data_size=%u, slot_count=%u)\n",
            data_size, slot_count);
    return -1;
  }

  // Allocate free stack
  tier->free_stack = malloc(slot_count * sizeof(atomic_uint));
  if (!tier->free_stack) {
    fprintf(stderr, "[MemPool] Failed to allocate free stack\n");
    free(tier->pool);
    return -1;
  }

  // Initialize free stack with all indices
  for (uint32_t i = 0; i < slot_count; i++) {
    atomic_store_explicit(&tier->free_stack[i], i, memory_order_relaxed);
  }

  atomic_store_explicit(&tier->free_count, slot_count, memory_order_release);
  atomic_store_explicit(&tier->alloc_failures, 0, memory_order_relaxed);

  return 0;
}

// Initialize message context pool
static int init_context_pool(void) {
  g_ctx_pool.contexts = calloc(MSG_CTX_POOL_SIZE, sizeof(msg_context_t));
  if (!g_ctx_pool.contexts) {
    fprintf(stderr, "[MemPool] Failed to allocate context pool\n");
    return -1;
  }

  g_ctx_pool.free_stack = malloc(MSG_CTX_POOL_SIZE * sizeof(atomic_uint));
  if (!g_ctx_pool.free_stack) {
    fprintf(stderr, "[MemPool] Failed to allocate context free stack\n");
    free(g_ctx_pool.contexts);
    return -1;
  }

  // Initialize free stack
  for (uint32_t i = 0; i < MSG_CTX_POOL_SIZE; i++) {
    atomic_store_explicit(&g_ctx_pool.free_stack[i], i, memory_order_relaxed);
  }

  atomic_store_explicit(&g_ctx_pool.free_count, MSG_CTX_POOL_SIZE,
                        memory_order_release);
  atomic_store_explicit(&g_ctx_pool.next_ctx_id, 1, memory_order_relaxed);

  return 0;
}

void pool_init(void) {
  printf("[MemPool] Initializing tiered memory pool (chunked)...\n");

  // Initialize all tiers
  for (int i = 0; i < TIER_COUNT; i++) {
    if (init_tier(&g_tiered_pool.tiers[i], tier_config[i].data_size,
                  tier_config[i].slot_count) < 0) {
      fprintf(stderr, "[MemPool] Failed to initialize tier %d\n", i);
      exit(1);
    }

    uint64_t tier_memory =
        (uint64_t)g_tiered_pool.tiers[i].slot_size * tier_config[i].slot_count;
    printf("[MemPool]   Tier %d: %6u bytes x %4u slots = %7.2f MB\n", i,
           tier_config[i].data_size, tier_config[i].slot_count,
           tier_memory / (1024.0 * 1024.0));

    atomic_store_explicit(&g_tiered_pool.tier_allocs[i], 0,
                          memory_order_relaxed);
  }

  // Initialize context pool
  if (init_context_pool() < 0) {
    fprintf(stderr, "[MemPool] Failed to initialize context pool\n");
    exit(1);
  }
  printf("[MemPool]   Context Pool: %u contexts\n", MSG_CTX_POOL_SIZE);

  atomic_store_explicit(&g_tiered_pool.total_allocs, 0, memory_order_relaxed);
  atomic_store_explicit(&g_tiered_pool.total_frees, 0, memory_order_relaxed);

  // Calculate total memory usage
  uint64_t total_memory = 0;
  for (int i = 0; i < TIER_COUNT; i++) {
    total_memory +=
        (uint64_t)g_tiered_pool.tiers[i].slot_size * tier_config[i].slot_count;
  }
  total_memory += (uint64_t)sizeof(msg_context_t) * MSG_CTX_POOL_SIZE;

  printf("[MemPool] Total memory allocated: %.2f MB\n",
         total_memory / (1024.0 * 1024.0));
}

chunk_t *chunk_alloc(uint32_t size) {
  if (size > MAX_CHUNK_SIZE) {
    fprintf(stderr, "[MemPool] Requested size %u exceeds maximum %u\n", size,
            MAX_CHUNK_SIZE);
    return NULL;
  }

  // Determine which tier to use
  uint8_t tier_idx = get_tier_for_size(size);
  if (tier_idx >= TIER_COUNT) {
    fprintf(stderr, "[MemPool] Size %u too large for any tier\n", size);
    return NULL;
  }

  // Try to allocate from the selected tier
  tier_pool_t *tier = &g_tiered_pool.tiers[tier_idx];
  uint32_t count =
      atomic_load_explicit(&tier->free_count, memory_order_acquire);

  while (count > 0) {
    if (atomic_compare_exchange_weak_explicit(&tier->free_count, &count,
                                              count - 1, memory_order_acquire,
                                              memory_order_relaxed)) {
      // Successfully reserved a slot
      uint32_t idx = atomic_load_explicit(&tier->free_stack[count - 1],
                                          memory_order_relaxed);

      // Calculate pointer to this slot
      chunk_t *chunk =
          (chunk_t *)((uint8_t *)tier->pool + idx * tier->slot_size);

      // Initialize chunk metadata
      chunk->ctx = NULL;
      chunk->chunk_index = 0;
      chunk->len = 0;
      chunk->capacity = tier->data_size;
      chunk->tier = tier_idx;
      chunk->is_first_chunk = 0;

      // Update statistics
      atomic_fetch_add_explicit(&g_tiered_pool.total_allocs, 1,
                                memory_order_relaxed);
      atomic_fetch_add_explicit(&g_tiered_pool.tier_allocs[tier_idx], 1,
                                memory_order_relaxed);

      return chunk;
    }
  }

  // Tier exhausted - try next tier up (if available)
  if (tier_idx < TIER_COUNT - 1) {
    atomic_fetch_add_explicit(&tier->alloc_failures, 1, memory_order_relaxed);
    return chunk_alloc(size); // Recursively try next tier
  }

  // All tiers exhausted
  atomic_fetch_add_explicit(&tier->alloc_failures, 1, memory_order_relaxed);
  fprintf(stderr,
          "[MemPool] Allocation failed for size %u (tier %u exhausted)\n", size,
          tier_idx);
  return NULL;
}

void chunk_free(chunk_t *chunk) {
  if (!chunk)
    return;

  uint8_t tier_idx = chunk->tier;
  if (tier_idx >= TIER_COUNT) {
    fprintf(stderr, "[MemPool] Invalid tier index %u in chunk\n", tier_idx);
    return;
  }

  tier_pool_t *tier = &g_tiered_pool.tiers[tier_idx];

  // Calculate index of this chunk in the pool
  uint32_t idx = ((uint8_t *)chunk - (uint8_t *)tier->pool) / tier->slot_size;

  if (idx >= tier->slot_count) {
    fprintf(stderr,
            "[MemPool] Invalid chunk pointer (calculated index %u >= %u)\n",
            idx, tier->slot_count);
    return;
  }

  // Return slot to free stack
  uint32_t count =
      atomic_load_explicit(&tier->free_count, memory_order_acquire);

  if (count >= tier->slot_count) {
    fprintf(stderr, "[MemPool] Double free detected (tier %u already full)\n",
            tier_idx);
    return;
  }

  atomic_store_explicit(&tier->free_stack[count], idx, memory_order_relaxed);
  atomic_fetch_add_explicit(&tier->free_count, 1, memory_order_release);
  atomic_fetch_add_explicit(&g_tiered_pool.total_frees, 1,
                            memory_order_relaxed);
}

msg_context_t *msg_context_alloc(void) {
  uint32_t count =
      atomic_load_explicit(&g_ctx_pool.free_count, memory_order_acquire);

  while (count > 0) {
    if (atomic_compare_exchange_weak_explicit(&g_ctx_pool.free_count, &count,
                                              count - 1, memory_order_acquire,
                                              memory_order_relaxed)) {
      // Successfully reserved a context
      uint32_t idx = atomic_load_explicit(&g_ctx_pool.free_stack[count - 1],
                                          memory_order_relaxed);

      msg_context_t *ctx = &g_ctx_pool.contexts[idx];

      // Initialize context
      memset(ctx, 0, sizeof(msg_context_t));
      ctx->in_use = 1;
      ctx->ctx_id = atomic_fetch_add_explicit(&g_ctx_pool.next_ctx_id, 1,
                                              memory_order_relaxed);
      atomic_store_explicit(&ctx->ref_count, 1, memory_order_relaxed);

      return ctx;
    }
  }

  fprintf(stderr, "[MemPool] Context pool exhausted\n");
  return NULL;
}

void msg_context_free(msg_context_t *ctx) {
  // Direct free (for backward compatibility in cleanup code)
  // Normally use msg_context_unref instead
  if (!ctx || !ctx->in_use)
    return;

  ctx->in_use = 0;

  uint32_t idx = ctx - g_ctx_pool.contexts;
  if (idx >= MSG_CTX_POOL_SIZE) {
    fprintf(stderr, "[MemPool] Invalid context pointer\n");
    return;
  }

  uint32_t count =
      atomic_load_explicit(&g_ctx_pool.free_count, memory_order_acquire);

  if (count >= MSG_CTX_POOL_SIZE) {
    fprintf(stderr, "[MemPool] Context pool already full\n");
    return;
  }

  atomic_store_explicit(&g_ctx_pool.free_stack[count], idx,
                        memory_order_relaxed);
  atomic_fetch_add_explicit(&g_ctx_pool.free_count, 1, memory_order_release);
}

void msg_context_ref(msg_context_t *ctx) {
  if (!ctx || !ctx->in_use)
    return;
  atomic_fetch_add_explicit(&ctx->ref_count, 1, memory_order_relaxed);
}

void msg_context_unref(msg_context_t *ctx) {
  if (!ctx || !ctx->in_use)
    return;

  uint32_t old_count =
      atomic_fetch_sub_explicit(&ctx->ref_count, 1, memory_order_release);

  if (old_count == 1) {
    // Last reference, actually free it
    ctx->in_use = 0;

    // Calculate index
    uint32_t idx = ctx - g_ctx_pool.contexts;
    if (idx >= MSG_CTX_POOL_SIZE) {
      fprintf(stderr, "[MemPool] Invalid context pointer\n");
      return;
    }

    // Return to free stack
    uint32_t count =
        atomic_load_explicit(&g_ctx_pool.free_count, memory_order_acquire);

    if (count >= MSG_CTX_POOL_SIZE) {
      fprintf(stderr, "[MemPool] Context pool already full (double free?)\n");
      return;
    }

    atomic_store_explicit(&g_ctx_pool.free_stack[count], idx,
                          memory_order_relaxed);
    atomic_fetch_add_explicit(&g_ctx_pool.free_count, 1, memory_order_release);
  }
}

void pool_get_tier_stats(uint8_t tier, uint32_t *free, uint32_t *total,
                         uint64_t *allocs, uint32_t *failures) {
  if (tier >= TIER_COUNT)
    return;

  tier_pool_t *t = &g_tiered_pool.tiers[tier];
  *free = atomic_load_explicit(&t->free_count, memory_order_acquire);
  *total = t->slot_count;
  *allocs = atomic_load_explicit(&g_tiered_pool.tier_allocs[tier],
                                 memory_order_relaxed);
  *failures = atomic_load_explicit(&t->alloc_failures, memory_order_relaxed);
}

void pool_get_ctx_stats(uint32_t *free, uint32_t *total) {
  *free = atomic_load_explicit(&g_ctx_pool.free_count, memory_order_acquire);
  *total = MSG_CTX_POOL_SIZE;
}

void pool_cleanup(void) {
  printf("[MemPool] Cleaning up tiered memory pool...\n");

  for (int i = 0; i < TIER_COUNT; i++) {
    tier_pool_t *tier = &g_tiered_pool.tiers[i];
    if (tier->pool) {
      free(tier->pool);
      tier->pool = NULL;
    }
    if (tier->free_stack) {
      free(tier->free_stack);
      tier->free_stack = NULL;
    }
  }

  if (g_ctx_pool.contexts) {
    free(g_ctx_pool.contexts);
    g_ctx_pool.contexts = NULL;
  }
  if (g_ctx_pool.free_stack) {
    free(g_ctx_pool.free_stack);
    g_ctx_pool.free_stack = NULL;
  }

  printf("[MemPool] Cleanup complete\n");
}
