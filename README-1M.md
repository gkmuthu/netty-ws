# 1M WebSocket test

This is the container test plan for one server process and enough load-client processes to open 1,000,000 sockets. It is not a capacity promise. Measure at 10k, then 100k, then raise the target. Stop if RSS, direct memory, or `ws_connections_rejected_total` climbs without `ws_connections` climbing.

The server has one design. Container environment or args set the load limits. There is no feature-flag pipeline.

## What the process is

- One acceptor thread and `WS_WORKERS` NIO loops. No thread per socket.
- Echo only. No compression, no message aggregation, no session map, no per-connection queue.
- A socket that cannot be written is closed. That is the overload policy.
- `GET /metrics` is Prometheus text. Counters are process-wide. Do not expect one series per connection.
- `GET /health` returns `ok`.

Args override environment. Environment overrides `.env`.

```sh
java -jar target/netty-ws.jar --max-connections=1000000 --workers=6 --idle-seconds=3600
java -jar target/netty-ws.jar --mode=client --target=ws://127.0.0.1:8080/ws --connections=1000
```

The Java `--mode=client` path stays for integration tests and as an example other services can copy. Docker client services do not use it. They run `rust-client/`, a small Rust process with the same `TARGET`, `CONNECTIONS`, ramp, hold, ping, message, local-address, and `/metrics` settings. The client image compiles the binary in a Rust build stage and copies only that binary into the runtime stage. There is no JDK in the client container.

To send application text later, set `MESSAGE_TEXT` (or `--message-text`). That is sent as a text frame every `MESSAGE_INTERVAL_SECONDS`. `MESSAGE_BYTES` still sends that many zero bytes when `MESSAGE_TEXT` is empty, matching the Java client.

Heap and direct memory are server settings. Set them on the server container with `JAVA_TOOL_OPTIONS`. The server image does not bake in `-Xmx`. Client RSS is the Rust process plus kernel socket buffers, not a JVM heap.

## Metrics to keep

Server, `http://<host>:8080/metrics`:

| Name | Read it as |
| --- | --- |
| `ws_connections` | Sockets currently admitted. This is the scale number. |
| `ws_connections_accepted_total` | Admitted since start. |
| `ws_connections_rejected_total` | Hit `WS_MAX_CONNECTIONS`. |
| `ws_handshakes_completed_total` | Upgrades finished. Should track accepted after the ramp. |
| `ws_handshakes_expired_total` | Accept ran faster than handshake. Lower `RAMP_PER_SECOND`. |
| `ws_idle_closed_total` | Client sent nothing for `WS_IDLE_SECONDS`. |
| `ws_slow_consumer_closed_total` | A reader was too slow. |
| `jvm_heap_used_bytes` | Channel objects. 1M idle sockets need multi-GiB heap. |
| `jvm_direct_used_bytes` | Pooled buffers. Idle soak should stay far under `-XX:MaxDirectMemorySize`. |
| `process_rss_bytes` | Linux/WSL only. Includes heap, direct, and thread stacks. |
| `process_cpu_load` | 0 to 1. Idle soak should fall after the ramp. |

Client, `http://<host>:<METRICS_PORT>/metrics`, and one JSON line in the container log every `REPORT_SECONDS`:

| Name | Read it as |
| --- | --- |
| `ws_client_connected` | Open sockets in that client process. |
| `ws_client_failed_total` | Connect or handshake failures. Retried until the target or hold time. |
| `ws_client_messages_received_total` | Echoes. Stays 0 when `MESSAGE_BYTES=0`. |

Snapshot:

```sh
sh scripts/collect-metrics.sh \
  http://127.0.0.1:8080 \
  http://127.0.0.1:9101 http://127.0.0.1:9102 \
  http://127.0.0.1:9103 http://127.0.0.1:9104
```

## Why one client cannot open 1M

A TCP connection is `(client IP, client port, server IP, server port)`. One client IP has about 60k ephemeral ports to one server address. 1M needs at least 18 source addresses, practically 16 addresses split across 4 client processes in `docker-compose.1m.yml` (4 addresses x 62,500 ports each).

Run this inside WSL2. Docker Desktop on macOS does not give the same host network.

## Host preparation (WSL2 / Linux)

Apply kernel limits for 1M concurrent connections:

```sh
# File descriptors
sudo sysctl -w fs.file-max=2097152
sudo sysctl -w fs.nr_open=2097152
ulimit -n 1048576

# TCP listen backlog & SYN queue
sudo sysctl -w net.core.somaxconn=65536
sudo sysctl -w net.ipv4.tcp_max_syn_backlog=65536
sudo sysctl -w net.core.netdev_max_backlog=65536

# Ephemeral ports & TIME_WAIT recycling
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"
sudo sysctl -w net.ipv4.tcp_timestamps=1
sudo sysctl -w net.ipv4.tcp_tw_reuse=1
sudo sysctl -w net.ipv4.tcp_fin_timeout=15

# Connection tracking (CRITICAL: prevents timeout drops at ~200k connections)
# If Docker / iptables is loaded, conntrack defaults to ~200k-262k max entries.
# Option A: Disable conntrack on loopback (recommended for local benchmark):
sudo iptables -t raw -I PREROUTING -i lo -j NOTRACK
sudo iptables -t raw -I OUTPUT -o lo -j NOTRACK

# Option B: Or expand the conntrack table size:
sudo sysctl -w net.netfilter.nf_conntrack_max=2097152
sudo sysctl -w net.netfilter.nf_conntrack_buckets=524288
```

Add source addresses once per boot (32 source IPs for 1M connections, 31.25k ports per IP):

```sh
for i in $(seq 2 33); do
  sudo ip addr add 127.0.0.$i/8 dev lo
done
```

`/etc/wsl.conf` or the Windows `.wslconfig` must allow the distro about 50 GiB. The JVM flags below assume that.

Kernel socket buffers dominate. `WS_SO_RCVBUF=4096` and `WS_SO_SNDBUF=4096` are set in the 1M compose file. Linux may double the setsockopt value. Budget about 16–24 GiB of kernel memory for the server side and about the same again for the clients. OS defaults of 64–128 KiB per direction will not fit.

## Configs

Smoke, bridge network, 100 connections:

```sh
docker compose up --build
```

1M, host network, after the sysctls and loopback addresses:

```sh
docker compose -f docker-compose.1m.yml up --build
```

Server environment in that file:

| Variable | 1M value | Why |
| --- | --- | --- |
| `WS_HOST` | `0.0.0.0` | Container accepts on the host network. |
| `WS_PORT` | `8080` | Metrics and WebSocket on the same port. |
| `WS_TRANSPORT` | `epoll` | Transport mode: `epoll` (Linux native), `nio` (JDK NIO), or `auto` (auto-detect). |
| `WS_ACCEPTORS` | `2` | Number of acceptor threads handling new incoming connections. |
| `WS_SO_REUSEPORT` | `true` | Enables Linux `SO_REUSEPORT` for kernel-level connection distribution. |
| `WS_WORKERS` | `6` | Match the 6 cores. More loops do not raise the socket ceiling. |
| `WS_MAX_CONNECTIONS` | `1000000` | Admission cap. Connection 1000001 is rejected and counted. |
| `WS_IDLE_SECONDS` | `3600` | Soak without a ping storm. 60s idle is about 17k timer fires/second at 1M and drops quiet clients. |
| `WS_BACKLOG` | `8192` | Accept queue. Still raise ramp rate only if `ws_handshakes_expired_total` stays near 0. |
| `WS_SO_RCVBUF` / `WS_SO_SNDBUF` | `4096` | Required. `0` means the OS default. |
| `WS_MAX_FRAME_BYTES` | `4096` | Idle test does not need a 64 KiB ceiling. The write watermark grows with this. |
| `JAVA_TOOL_OPTIONS` | `-Xms12g -Xmx16g -XX:MaxDirectMemorySize=1g -Xss256k` | Heap holds channel objects. `-Xmx256m` cannot. |

Client environment:

| Variable | 1M value | Why |
| --- | --- | --- |
| `TARGET` | `ws://127.0.0.1:8080/ws` | One server port. Spread load with source addresses, not more server ports. |
| `CONNECTIONS` | `250000` per client | Four clients reach 1M. |
| `RAMP_PER_SECOND` | `2000` | 1M at this rate is about 8 minutes. Lower it if handshakes expire. |
| `HOLD_SECONDS` | `900` | Must be longer than the ramp. The process exits and closes sockets when this ends. |
| `PING_SECONDS` | `0` | No protocol pings during the idle soak. |
| `MESSAGE_BYTES` | `0` | Connect test, not a throughput test. Set `8` or more only after the idle set is stable. |
| `LOCAL_ADDRESSES` | four `127.0.0.x` each | Stays under the per-IP port cap. |
| `METRICS_PORT` | `9101`–`9104` | Host network, so ports must not collide. |
| `MESSAGE_TEXT` | empty | Optional application payload. When set, this text is sent instead of `MESSAGE_BYTES` zeros. |

Same knobs as args: `--max-connections=1000000`, `--target=ws://127.0.0.1:8080/ws`, `--connections=250000`, `--local-addresses=127.0.0.2,127.0.0.3`.

## Pass / fail

Pass for the connect soak:

- `ws_connections` reaches 1000000 and stays there for several minutes.
- `ws_connections_rejected_total` stays 0 until the cap, then only counts connections over the cap.
- `ws_handshakes_expired_total` and client `ws_client_failed_total` stay near 0. A rising handshake expiry means the ramp is too fast, not that the cap is too low.
- `jvm_direct_used_bytes` stays under 1 GiB during the idle hold.
- `process_cpu_load` drops after the ramp. A 6-core box holding idle sockets should not sit at 6.0 load.

Do not turn the idle soak into 1M messages/second. That is a different test. After the hold is stable, set `MESSAGE_BYTES=8` and `MESSAGE_INTERVAL_SECONDS=10` on one client only and watch `ws_messages_received_total`, CPU, and `ws_slow_consumer_closed_total`.

Horizontal scale is more server processes behind a WebSocket-aware load balancer. This repository does not fan out messages between processes.

## Engineering Concepts: Epoll vs Java NIO & 1M Concurrency

### 1. JDK NIO Selector Overhead
Standard Java NIO (`NioEventLoopGroup` + `NioServerSocketChannel`) uses OpenJDK's `Selector` implementation (`sun.nio.ch.EPollSelectorImpl` on Linux).
- **Selector Lock Contention**: The JDK `Selector` maintains synchronized collections (`selectedKeys`, `keys`, `fdToKey`). In a multi-worker setup registering thousands of channels concurrently, thread synchronization overhead degrades performance.
- **Level-Triggered Event Wakeups**: Standard NIO polls repeatedly on readiness. At hundreds of thousands of idle descriptors, wakeups and collection scanning generate garbage and CPU overhead.

### 2. Netty Native Epoll Transport
Netty's native transport (`EpollEventLoopGroup` + `EpollServerSocketChannel`) interfaces directly with Linux system calls via JNI (`io.netty:netty-transport-native-epoll`):
- **Edge-Triggered (`EPOLLET`)**: Netty native epoll uses edge-triggered mode. The Linux kernel notifies the event loop only when the descriptor state transitions (e.g., from unreadable to readable), eliminating redundant notifications for idle connections.
- **Zero Lock Contention**: Bypasses the JDK `Selector` and its synchronized data structures entirely. Socket events are polled directly into pre-allocated off-heap memory buffers (`epoll_event[]`).
- **`SO_REUSEPORT` Support**: Netty's `EpollChannelOption.SO_REUSEPORT` enables multiple acceptor threads to bind to the same listening port. The Linux kernel distributes incoming SYN packets across acceptor sockets using a 4-tuple hash in kernel space, eliminating the single-acceptor bottleneck during aggressive connection ramps.

### 3. Why Connections Timed Out at ~200k (2 Lakh)
- **Linux Netfilter Connection Tracking (`nf_conntrack`)**: Docker loads `iptables`/netfilter. By default, Linux tracks stateful connections on all interfaces, including loopback with `network_mode: host`. When the table hits `nf_conntrack_max` (typically 200,000–262,144 entries by default), the kernel **silently drops new TCP SYN packets**. The Rust client waits 5 seconds and reports `connect failed: connect timeout`.
- **SYN Queue (`tcp_max_syn_backlog`)**: Even with `somaxconn` raised, if `net.ipv4.tcp_max_syn_backlog` remains at the OS default (typically 1024–2048), rapid connection bursts overflow the half-open SYN queue, causing SYN drops.
- **Remedy**: Either bypass loopback tracking (`iptables -t raw -I PREROUTING -i lo -j NOTRACK`) or raise `nf_conntrack_max` to `2097152`, and set `tcp_max_syn_backlog=65536`.

