#ifndef __DISCOVERY_H__
#define __DISCOVERY_H__

#include <stdint.h>
#include <time.h>

#define DISCOVERY_PORT 9999
#define DISCOVERY_MAGIC 0x47415445 // "GATE" in hex
#define MAX_DISCOVERED_BACKENDS 16
#define DISCOVERY_INTERVAL 5
#define BACKEND_TIMEOUT 15

// Discovery packet format
typedef struct {
  uint32_t magic;
  uint32_t version;
  uint32_t service_port;
  char service_name[64];
  uint32_t capabilities;
} __attribute__((packed)) discovery_packet_t;

// Discovered backend info
typedef struct {
  char host[256];
  int port;
  time_t last_seen;
  char service_name[64];
  int active;
} discovered_backend_t;

// Initialize discovery system
int discovery_init(void);

// Cleanup discovery system
void discovery_cleanup(void);

// Discovery thread - listens for backend announcements
void *discovery_thread_fn(void *arg);

// Get list of discovered backends
int get_discovered_backends(discovered_backend_t *backends, int max_count);

#endif // __DISCOVERY_H__
