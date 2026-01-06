#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "discovery.h"
#include "gateway.h"

static int discovery_socket = -1;
static discovered_backend_t discovered_backends[MAX_DISCOVERED_BACKENDS];
static pthread_mutex_t discovery_lock = PTHREAD_MUTEX_INITIALIZER;

int discovery_init(void) {
  discovery_socket = socket(AF_INET, SOCK_DGRAM, 0);
  if (discovery_socket < 0) {
    perror("discovery socket");
    return -1;
  }

  int broadcast_enable = 1;
  if (setsockopt(discovery_socket, SOL_SOCKET, SO_BROADCAST, &broadcast_enable,
                 sizeof(broadcast_enable)) < 0) {
    perror("setsockopt SO_BROADCAST");
    close(discovery_socket);
    return -1;
  }

  int reuse = 1;
  setsockopt(discovery_socket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(DISCOVERY_PORT);

  if (bind(discovery_socket, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    perror("bind discovery socket");
    close(discovery_socket);
    return -1;
  }

  pthread_mutex_lock(&discovery_lock);
  for (int i = 0; i < MAX_DISCOVERED_BACKENDS; i++) {
    discovered_backends[i].active = 0;
    discovered_backends[i].last_seen = 0;
  }
  pthread_mutex_unlock(&discovery_lock);

  printf("[Discovery] Listening on UDP port %d for backend announcements\n",
         DISCOVERY_PORT);

  return 0;
}

void discovery_cleanup(void) {
  if (discovery_socket >= 0) {
    close(discovery_socket);
    discovery_socket = -1;
  }
}

void *discovery_thread_fn(void *arg) {
  discovery_packet_t packet;
  struct sockaddr_in sender_addr;
  socklen_t addr_len = sizeof(sender_addr);

  printf("[Discovery] Thread started\n");

  while (atomic_load_explicit(&running, memory_order_acquire)) {
    ssize_t recv_len = recvfrom(discovery_socket, &packet, sizeof(packet), 0,
                                (struct sockaddr *)&sender_addr, &addr_len);

    if (recv_len < 0) {
      if (errno == EINTR)
        continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        sleep(1);
        continue;
      }
      perror("recvfrom discovery");
      sleep(1);
      continue;
    }

    if (recv_len < sizeof(discovery_packet_t)) {
      continue;
    }

    if (ntohl(packet.magic) != DISCOVERY_MAGIC) {
      continue;
    }

    char host[256]; // Increased from INET_ADDRSTRLEN
    inet_ntop(AF_INET, &sender_addr.sin_addr, host, sizeof(host));

    // Check for advertised hostname (Docker NAT fix)
    packet.advertised_hostname[63] = '\0';
    if (packet.advertised_hostname[0] != '\0') {
         // Only use if it looks valid (simple check)
         strncpy(host, packet.advertised_hostname, sizeof(host) - 1);
         host[sizeof(host)-1] = '\0';
    }

    int port = ntohs(packet.service_port);
    // packet.service_name removed from protocol

    time_t now = time(NULL);

    pthread_mutex_lock(&discovery_lock);

    int found = -1;
    for (int i = 0; i < MAX_DISCOVERED_BACKENDS; i++) {
      if (discovered_backends[i].active &&
          strcmp(discovered_backends[i].host, host) == 0 &&
          discovered_backends[i].port == port) {
        found = i;
        break;
      }
    }

    if (found >= 0) {
      discovered_backends[found].last_seen = now;
    } else {
      for (int i = 0; i < MAX_DISCOVERED_BACKENDS; i++) {
        if (!discovered_backends[i].active) {
          strncpy(discovered_backends[i].host, host,
                  sizeof(discovered_backends[i].host) - 1);
          discovered_backends[i].port = port;
          discovered_backends[i].last_seen = now;
          strncpy(discovered_backends[i].service_name, "Universal Agent",
                  sizeof(discovered_backends[i].service_name) - 1);
          discovered_backends[i].active = 1;

          printf("[Discovery] New backend discovered: %s:%d\n", host, port);
          break;
        }
      }
    }

    for (int i = 0; i < MAX_DISCOVERED_BACKENDS; i++) {
      if (discovered_backends[i].active &&
          (now - discovered_backends[i].last_seen) > BACKEND_TIMEOUT) {
        printf("[Discovery] Backend timeout: %s:%d\n",
               discovered_backends[i].host, discovered_backends[i].port);
        discovered_backends[i].active = 0;
      }
    }

    pthread_mutex_unlock(&discovery_lock);
  }

  printf("[Discovery] Thread stopped\n");
  return NULL;
}

int get_discovered_backends(discovered_backend_t *backends, int max_count) {
  pthread_mutex_lock(&discovery_lock);

  int count = 0;
  for (int i = 0; i < MAX_DISCOVERED_BACKENDS && count < max_count; i++) {
    if (discovered_backends[i].active) {
      memcpy(&backends[count], &discovered_backends[i],
             sizeof(discovered_backend_t));
      count++;
    }
  }

  pthread_mutex_unlock(&discovery_lock);
  return count;
}
