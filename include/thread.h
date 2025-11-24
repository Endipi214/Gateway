#ifndef __THREADS_H__
#define __THREADS_H__

#include <pthread.h>

// Mutex protecting access to global clients array
extern pthread_mutex_t clients_lock;

// Thread entry functions
// WebSocket listener/handler thread
void *ws_thread_fn(void *arg);
// Backend communication thread
void *backend_thread_fn(void *arg);
// Monitoring/logging thread
void *monitor_thread_fn(void *arg);

#endif // __THREADS_H__
