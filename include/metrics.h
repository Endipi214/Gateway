#ifndef __METRIC_H__
#define __METRIC_H__

#include <stdatomic.h>

// Metrics structure for tracking messages, bytes, connections, and latency
// All counters are atomic to allow safe updates from multiple threads
typedef struct {
  atomic_uint_least64_t messages_sent;
  atomic_uint_least64_t messages_recv;
  atomic_uint_least64_t bytes_sent;
  atomic_uint_least64_t bytes_recv;
  atomic_uint_least64_t latency_sum_ns;
  atomic_uint latency_count;
  atomic_uint connections;
  atomic_uint disconnections;
} metrics_t;

// Global metrics instances
extern metrics_t metrics_ws;      // Metrics for WebSocket thread
extern metrics_t metrics_backend; // Metrics for backend thread

#endif // __METRIC_H__
