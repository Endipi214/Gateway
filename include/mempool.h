#ifndef __MEMPOOL_H__
#define __MEMPOOL_H__

#include <stdatomic.h>
#include <stdint.h>

// --------- Tiered Memory Pool Config ---------
#define TIER_COUNT 6

// Tiered sized in bytes
#define TIER_0_SIZE 512
#define TIER_1_SIZE (4 * 1024)        // 4KB
#define TIER_2_SIZE (32 * 1024)       // 32KB
#define TIER_3_SIZE (256 * 1024)      // 256KB
#define TIER_4_SIZE (1024 * 1024)     // 1MB
#define TIER_5_SIZE (8 * 1024 * 1024) // 8MB

// Slots per tier
#define TIER_0_SLOTS 2048
#define TIER_1_SLOTS 1024
#define TIER_2_SLOTS 512
#define TIER_3_SLOTS 256
#define TIER_4_SLOTS 256
#define TIER_5_SLOTS 128

// Maximum message size supported
#define MAX_MESSAGE_SIZE TIER_5_SIZE

// Message priority levels
#define MSG_PRIORITY_LOW 0
#define MSG_PRIORITY_NORMAL 1
#define MSG_PRIORITY_HIGH 2
#define MSG_PRIORITY_CRITICAL 3

// --------- Message Structure ---------
typedef struct {
  uint32_t client_id;
  uint32_t backend_id;
  uint32_t len;
  uint32_t capacity;
  uint8_t tier;
  uint8_t priority;
  uint8_t drop_if_full;
  uint8_t retries;
  uint64_t timestamp_ns;
  uint8_t data[];
} message_t;

// --------- Tier Structure ---------
typedef struct {
  void *pool;
  atomic_uint *free_stack;
  atomic_uint free_count;
  atomic_uint alloc_failures;
  uint32_t slot_size;
  uint32_t slot_count;
  uint32_t data_size;
} tier_pool_t;

// --------- Global Tiered Memory Pool ---------
typedef struct {
  tier_pool_t tiers[TIER_COUNT];
  atomic_uint_least64_t total_allocs;
  atomic_uint_least64_t total_frees;
  atomic_uint_least64_t tier_allocs[TIER_COUNT];
} tiered_mem_pool_t;

extern tiered_mem_pool_t g_tiered_pool;

// --------- Memory Pool API ---------
void pool_init(void);
message_t *msg_alloc(uint32_t size);
message_t *msg_alloc_priority(uint32_t size, uint8_t priority,
                              uint8_t drop_if_full);
void msg_free(message_t *msg);
void pool_get_tier_stats(uint8_t tier, uint32_t *free, uint32_t *total,
                         uint64_t *allocs, uint32_t *failures);
void pool_cleanup(void);

#endif // __MEMPOOL_H__
