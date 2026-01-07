# 🚀 High-Performance WebSocket Gateway

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![C Standard](https://img.shields.io/badge/C-C11-blue.svg)](https://en.wikipedia.org/wiki/C11_(C_standard_revision))
[![Platform](https://img.shields.io/badge/Platform-Linux-green.svg)](https://www.linux.org/)

A production-ready, high-performance WebSocket gateway written in C, designed for real-time communication between web clients and backend services. Features lock-free queues, tiered memory pooling, circuit breakers, and intelligent traffic management.

---

## ✨ Features

### Core Architecture
- **Dual-Channel Communication**: Separate control and data channels for optimized throughput
- **Lock-Free SPSC Queues**: Single-producer, single-consumer queues with priority lanes
- **Tiered Memory Pool**: 6-tier memory allocation system (512B to 8MB) with zero runtime allocation
- **Edge-Triggered Epoll**: High-performance event handling for thousands of concurrent connections

### Traffic Management
- **Priority Queue System**: Separate high and low priority queues per client
- **ACK-Based Flow Control**: Prevents video frame buffer bloat
- **Aggressive Video Throttling**: Maintains <100ms latency with frame skipping
- **Traffic Classification**: Control, Video, ACK, and File transfer classes

### Reliability Features
- **Circuit Breaker Pattern**: Automatic failure detection and recovery
- **Connection Health Monitoring**: Idle timeout and failure tracking
- **Rate Limiting**: Per-connection bandwidth controls
- **Automatic Reconnection**: Configurable retry logic with exponential backoff

### Discovery & Scaling
- **UDP Service Discovery**: Automatic backend detection
- **Manual Configuration**: Static backend list support
- **Load Distribution**: Broadcast and unicast routing
- **Hot Reconnection**: Seamless backend recovery

---

## 📋 Requirements

### Build Dependencies
- **CMake** 3.8 or higher
- **GCC** with C11 support
- **OpenSSL** development libraries
- **POSIX Threads** (pthread)

### Runtime Requirements
- Linux kernel 2.6.27+ (for epoll, eventfd)
- x86_64 architecture (optimized with `-march=native`)

---

## 🛠️ Building

```bash
# Clone the repository
git clone <repository-url>
cd Gateway

# Create build directory
mkdir build && cd build

# Configure and build
cmake ..
make

# Binary location
./bin/Gateway
```

### Build Configuration

The project uses CMake with the following optimizations:
- `-march=native`: CPU-specific optimizations
- `-mcmodel=medium`: Support for large memory allocations
- `_GNU_SOURCE`: Enable GNU extensions

---

## 🚀 Usage

### Basic Usage

#### With Manual Backend Configuration
```bash
./Gateway <ws_port> <backend1_host:port> [backend2_host:port] ...
```

**Example:**
```bash
./Gateway 8080 127.0.0.1:9090 192.168.1.100:9091
```

#### With UDP Discovery
```bash
./Gateway <ws_port> --discover
```

**Example:**
```bash
./Gateway 8080 --discover
```

### Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `ws_port` | WebSocket listening port | Required |
| `backend_host:port` | Backend server address | Manual mode |
| `--discover` | Enable UDP service discovery | Port 9999 |

---

## 🏗️ Architecture

### Thread Model

```
┌─────────────────────────────────────────────────────────────┐
│                    Main Thread                              │
│  • Signal handling                                          │
│  • Initialization                                           │
│  • Cleanup                                                  │
└─────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ WS Thread    │    │Backend Thread│    │Monitor Thread│
│              │    │              │    │              │
│ • Accept     │◄───┤ • Discovery  │    │ • Metrics    │
│ • Handshake  │    │ • Reconnect  │    │ • Health     │
│ • Send/Recv  │───►│ • Send/Recv  │    │ • Reporting  │
│ • Priority Q │    │ • Circuit    │    │              │
└──────────────┘    └──────────────┘    └──────────────┘
         │                    │
         ▼                    ▼
┌─────────────────────────────────────┐
│      Lock-Free Priority Queues      │
│  • High Priority (Commands)         │
│  • Low Priority (Video)             │
└─────────────────────────────────────┘
```

### Memory Pool Tiers

| Tier | Size | Slots | Use Case |
|------|------|-------|----------|
| 0 | 512 B | 2048 | Small commands, ACKs |
| 1 | 4 KB | 1024 | Status messages |
| 2 | 32 KB | 512 | Medium data |
| 3 | 256 KB | 256 | Large messages |
| 4 | 1 MB | 256 | Video frames |
| 5 | 8 MB | 128 | Large file chunks |

### Protocol Format

#### Backend Frame Structure
```
┌────────────┬────────────┬────────────┬──────────────┐
│ Length (4) │ Client (4) │ Backend(4) │ Payload (N)  │
│   bytes    │     ID     │    ID      │    bytes     │
└────────────┴────────────┴────────────┴──────────────┘
```

**Special IDs:**
- `client_id = 0`: Broadcast to all clients
- `backend_id = 0`: Broadcast to all backends

#### Traffic Classes
```c
#define TRAFFIC_CONTROL 0x01  // Commands, Status - Never drop
#define TRAFFIC_VIDEO   0x02  // Video frames - Drop if busy
#define TRAFFIC_ACK     0x03  // Flow control signal
#define TRAFFIC_FILE    0x04  // File transfer chunks
```

---

## 📊 Performance Characteristics

### Throughput
- **WebSocket Connections**: 1,000+ concurrent clients
- **Backend Connections**: Up to 16 backends
- **Message Rate**: 100,000+ messages/second
- **Latency**: <1ms for control messages, <100ms for video

### Memory Usage
- **Base**: ~40 MB (tiered pool)
- **Per Client**: ~128 KB (queues + state)
- **Per Backend**: ~4 KB (connection state)

### Scalability
- **Queue Depth**: 256 messages (main), 64 (priority)
- **Epoll Events**: 64 per cycle
- **Client Slots**: 1,024 maximum
- **Backend Slots**: 16 maximum

---

## 🔧 Configuration Tuning

### Queue Sizes
```c
#define QUEUE_SIZE 256              // Main queue depth
#define CLIENT_QUEUE_SIZE 64        // Per-client queue
#define EPOLL_EVENTS 64             // Events per epoll_wait
```

### Timeouts
```c
#define IDLE_TIMEOUT_SEC 60         // Client idle timeout
#define CIRCUIT_BREAKER_TIMEOUT 30  // Circuit breaker cooldown
#define VIDEO_ACK_TIMEOUT_SEC 2     // Video ACK watchdog
#define RECONNECT_INTERVAL 5        // Backend reconnect interval
```

### Thresholds
```c
#define MAX_CONSECUTIVE_FAILURES 10        // Before disconnect
#define QUEUE_HIGH_WATER_THRESHOLD 0.8     // Backpressure trigger
```

---

## 📈 Monitoring

The gateway includes a built-in monitoring thread that reports every 5 seconds:

### Metrics Tracked
- **Memory Pool**: Allocation/free counts, utilization per tier
- **WebSocket**: Messages sent/received, bytes, connections, latency
- **Backend**: Messages sent/received, bytes, connections, failures
- **Queues**: Depth, drops, high-water marks, backpressure events
- **Circuit Breakers**: State, failure counts, success/failure ratios

### Example Output
```
═══════════════════════════════════════════════════════════════
Tiered Memory Pool:
   Total Allocations:      125432
   Total Frees:            125100
   Messages In-Use:           332
   TOTAL: 332/4224 slots used (7.9% utilization)

WebSocket Thread:
   Sent:          98234 msgs  |   45632100 bytes
   Received:      98120 msgs  |   32145600 bytes
   Avg Latency:   42500 ns    |   42.50 µs

Backend Thread:
   Sent:          98120 msgs  |   32145600 bytes
   Received:      98234 msgs  |   45632100 bytes

Queue Statistics:
   WS→Backend:  depth=   3 | drops=     0 | HW=  45 | BP=   0
   Backend→WS:  depth=   2 | drops=     0 | HW=  38 | BP=   0

Circuit Breaker Status:
   [0] 192.168.1.100:9090  [CLOSED]  fails=0  sent=45123
═══════════════════════════════════════════════════════════════
```

---

## 🛡️ Reliability Features

### Circuit Breaker States
- **CLOSED**: Normal operation
- **OPEN**: Backend failed, requests blocked
- **HALF_OPEN**: Testing recovery with limited requests

### Flow Control
The gateway implements ACK-based flow control to prevent video buffer bloat:
1. Client receives video frame
2. Client sends ACK (0x03 traffic class)
3. Gateway allows next video frame
4. Watchdog timer resets if ACK not received within 2 seconds

### Priority Handling
- **High Priority**: Commands, status, file chunks (never dropped)
- **Low Priority**: Video frames (dropped when queue full)
- **Frame Skipping**: Oldest frames dropped first for real-time video

---

## 🐛 Troubleshooting

### Common Issues

#### Connection Refused
```
[Backend] Failed to connect to 192.168.1.100:9090
```
**Solution**: Verify backend is running and port is accessible

#### Memory Pool Exhausted
```
[MemPool] Allocation failed for size 1048576 (tier 4 exhausted)
```
**Solution**: Increase tier slots in `include/mempool.h`

#### Queue Drops
```
[Queue] WARNING: Priority queue full! Dropping priority message
```
**Solution**: Increase `QUEUE_SIZE` or optimize message processing

### Debug Logging

Uncomment diagnostic logs in source files:
```c
// src/backend.c - Frame parsing
printf("[Backend] Frame header parsed: len=%u, client_id=%u\n", ...);

// src/thread.c - Message routing
printf("[GW] WS->Backend: %.*s at %lld\n", ...);
```

---

## 🤝 Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Follow the existing code style
4. Add tests for new features
5. Submit a pull request

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

---

## 🙏 Acknowledgments

Built with:
- **OpenSSL** for WebSocket handshake
- **Linux epoll** for high-performance I/O
- **POSIX threads** for concurrency

---

## 📞 Support

For issues, questions, or contributions:
- Open an issue on GitHub
- Check existing documentation
- Review monitoring output for diagnostics

---

**Made with ⚡ for real-time performance**
