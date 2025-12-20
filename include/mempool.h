#ifndef __MEMPOOL_H__
#define __MEMPOOL_H__

#include <stdatomic.h>
#include <stdint.h>

// --------- Tiered Memory Pool Config ---------
#define TIER_COUNT 4

// Reduced tier sizes - max 64KB chunks
#define TIER_0_SIZE 512
#define TIER_1_SIZE (4 * 1024)  // 4KB
#define TIER_2_SIZE (16 * 1024) // 16KB
#define TIER_3_SIZE (64 * 1024) // 64KB

// Increased slots for smaller tiers
#define TIER_0_SLOTS 1024
#define TIER_1_SLOTS 1024
#define TIER_2_SLOTS 1024
#define TIER_3_SLOTS (16 * 1024)

// Maximum chunk size
#define MAX_CHUNK_SIZE TIER_3_SIZE

// Message context pool
#define MSG_CTX_POOL_SIZE 2048

// --------- Message Context ---------
// Stores metadata about a message being transmitted
typedef struct {
  uint32_t client_id;    // Client ID (sender or destination)
  uint32_t backend_id;   // Backend ID (sender or destination)
  uint32_t total_len;    // Total message length
  uint32_t bytes_sent;   // Number of bytes sent/received so far
  uint8_t complete;      // 1 if message is complete (all bytes received/sent)
  uint8_t in_use;        // 1 if this context is active
  uint64_t timestamp_ns; // Message creation timestamp
  uint32_t ctx_id;       // Unique context ID
  atomic_uint ref_count; // Reference count for shared contexts
} msg_context_t;

// --------- Chunk Structure ---------
// A chunk of data belonging to a message
typedef struct {
  msg_context_t *ctx;     // Pointer to message context
  uint32_t chunk_index;   // Index of this chunk in the message
  uint32_t len;           // Actual data length in this chunk
  uint32_t capacity;      // Allocated capacity (tier size)
  uint8_t tier;           // Which tier this came from
  uint8_t is_last_chunk;  // 1 if this is the last chunk
  uint8_t is_first_chunk; // 1 if this is the first chunk (needs header)
  uint8_t data[];         // Flexible array for data
} chunk_t;

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

// --------- Message Context Pool ---------
typedef struct {
  msg_context_t *contexts;
  atomic_uint *free_stack;
  atomic_uint free_count;
  atomic_uint next_ctx_id;
} msg_context_pool_t;

extern tiered_mem_pool_t g_tiered_pool;
extern msg_context_pool_t g_ctx_pool;

// --------- Memory Pool API ---------
// Initialize the tiered memory pool and context pool
void pool_init(void);

// Allocate a chunk with at least 'size' bytes of data capacity
// Returns NULL if allocation fails
chunk_t *chunk_alloc(uint32_t size);

// Free a chunk back to its tier
void chunk_free(chunk_t *chunk);

// Message Context API
msg_context_t *msg_context_alloc(void);
void msg_context_free(msg_context_t *ctx);
void msg_context_ref(msg_context_t *ctx);   // Increment reference count
void msg_context_unref(msg_context_t *ctx); // Decrement and free if zero

// Get statistics for a specific tier
void pool_get_tier_stats(uint8_t tier, uint32_t *free, uint32_t *total,
                         uint64_t *allocs, uint32_t *failures);

// Get context pool stats
void pool_get_ctx_stats(uint32_t *free, uint32_t *total);

// Cleanup
void pool_cleanup(void);

#endif // __MEMPOOL_H__
