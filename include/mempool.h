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

// --------- Message Structure ---------
// Variable-size message structure
typedef struct {
  uint32_t client_fd;
  uint32_t backend_fd;
  uint32_t len;      // Actual data length
  uint32_t capacity; // Allocated capacity (tier size)
  uint8_t tier;      // Which tier this came from
  uint64_t timestamp_ns;
  uint8_t data[];
} message_t;

// --------- Tier Structure ---------
typedef struct {
  void *pool;                 // Base pointer to memory block
  atomic_uint *free_stack;    // Stack of free indices
  atomic_uint free_count;     // Number of free slots
  atomic_uint alloc_failures; // Failed allocations
  uint32_t slot_size;         // Size of each slot (including header)
  uint32_t slot_count;        // Total number of slots
  uint32_t data_size;         // Data capacity per slot
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
// Initialize the tiered memory pool
void pool_init(void);

// Allocate a message with at least 'size' bytes of data capacity
// Returns NULL if allocation fails
message_t *msg_alloc(uint32_t size);

// Free a message back to its tier
void msg_free(message_t *msg);

// Get statistics for a specific tier
void pool_get_tier_stats(uint8_t tier, uint32_t *free, uint32_t *total,
                         uint64_t *allocs, uint32_t *failures);

// Cleanup
void pool_cleanup(void);

#endif // __MEMPOOL_H__
