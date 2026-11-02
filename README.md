# Lean Netty WebSocket server

Java 17+ / Maven executable web application using Netty 4.1.139.Final.
No Spring, servlet container, JSON library, or runtime logging dependency.
Only the required Netty HTTP/handler modules and their transitive dependencies
are packaged, rather than `netty-all`.

## Build and run

```sh
./mvnw verify
java -Xms64m -Xmx256m -XX:MaxDirectMemorySize=128m -jar target/netty-ws.jar
```

On Windows use `mvnw.cmd`. The Maven wrapper downloads Maven on first use.
An installed Maven also works: `mvn verify`. VS Code includes a `Maven: verify`
build task.

```sh
curl --fail http://127.0.0.1:8080/health
```

Connect to `ws://127.0.0.1:8080/ws`. This is a **text/binary echo server**:
it returns frames to their sender, not a broadcast/chat implementation.
For the included browser client, start the server and open
**http://127.0.0.1:8080/sample-connector.html**. It connects automatically;
enter a message and click **Send Message** to see the echo. It sends a
heartbeat every 20 seconds for the default 60-second idle timeout (increase
the idle timeout if browser background-tab timer throttling is a concern).
The sample is packaged from `src/main/resources/sample-connector.html`.

Do not double-click the HTML file or open it with `file://`: local files send
`Origin: null`, which the server intentionally rejects. If using a different
HTTP host/port, add that exact page origin to `WS_ALLOWED_ORIGINS`; the sample
uses its serving host/port for `/ws`. A changed server port also needs a
matching origin allowlist setting.

Example from a browser page whose origin is `http://localhost:8080`:

```js
const ws = new WebSocket("ws://localhost:8080/ws");
ws.onopen = () => ws.send("hello");
ws.onmessage = event => console.log(event.data);
```

`/sample-connector.html` serves the demo and `/health` returns `200 ok`;
other HTTP paths return 404.
The WebSocket path is exact: query strings and subpaths are not accepted.
Ping/pong, UTF-8 validation, masked-client-frame validation, and close
handshakes are handled by Netty.

## Configuration

Capacity is set by container environment or command-line args. Args override the
environment. The environment overrides an optional `.env` file. Invalid values
fail startup. Heap is not a code setting: set `JAVA_TOOL_OPTIONS` on the container.

```sh
java -jar target/netty-ws.jar --max-connections=100000 --workers=6 --idle-seconds=3600
```

The 1M Docker layout, client args, and metrics are in [README-1M.md](README-1M.md).
A small compose smoke test is `docker compose up --build`. That client is the
Rust image in `rust-client/`. It is built and packed inside its own Docker
build. The Java load client remains in this jar for tests and as an example
other services can call:

```sh
java -jar target/netty-ws.jar --mode=client --target=ws://127.0.0.1:8080/ws --connections=100
```

| Variable | Arg | Default | Meaning |
| --- | --- | --- | --- |
| `WS_HOST` | `--host` | `127.0.0.1` | Bind address; use `0.0.0.0` in a container |
| `WS_PORT` | `--port` | `8080` | TCP port; 0 selects an ephemeral port |
| `WS_WORKERS` | `--workers` | min(8, available processors) | I/O event-loop threads, plus one acceptor |
| `WS_MAX_CONNECTIONS` | `--max-connections` | `10000` | Admitted TCP connections, including HTTP |
| `WS_MAX_FRAME_BYTES` | `--max-frame-bytes` | `16384` | Maximum payload per inbound frame |
| `WS_MAX_MESSAGE_BYTES` | `--max-message-bytes` | `65536` | Cumulative payload across fragments |
| `WS_IDLE_SECONDS` | `--idle-seconds` | `60` | Close a connection with no inbound read |
| `WS_BACKLOG` | `--backlog` | `1024` | Listen backlog |
| `WS_SO_RCVBUF` | `--so-rcvbuf` | `0` | Socket receive buffer; `0` keeps the OS default |
| `WS_SO_SNDBUF` | `--so-sndbuf` | `0` | Socket send buffer; `0` keeps the OS default |
| `WS_ALLOWED_ORIGINS` | `--allowed-origins` | localhost and 127.0.0.1 on the port | Exact, comma-separated browser origins |

`GET /metrics` is Prometheus text. `GET /health` returns `ok`.

Origin-bearing requests must match the allowlist. An empty allowlist rejects
all Origin-bearing upgrades. Native clients without Origin are allowed.
Origin filtering is **not authentication**. Changing the port does not change
the origin allowlist automatically.

Each connection has a fixed five-second deadline from admission to completed
WebSocket handshake, including HTTP header receipt. Idle connections close;
native clients should send ping frames more often than the idle timeout.
Browser clients cannot send protocol pings and should send application
heartbeat messages, which this echo server returns.

## Memory and scalability design

- Java NIO event loops multiplex connections; no thread per socket.
- Reference-counted payload buffers are reused for echo writes without a
  payload copy. Writes flush once per read batch.
- No WebSocket message aggregation: fragmented frames are echoed as they
  arrive. A per-connection counter bounds total message bytes. Earlier
  fragments can already have been echoed when a later fragment exceeds the
  limit; the connection then closes with code 1009.
- HTTP aggregation is limited to 8 KiB, headers to 8 KiB, and request lines to
  4 KiB. HTTP aggregation/routing is removed after successful upgrade.
- The pooled direct allocator uses at most four arenas, 512 KiB chunks, no
  heap arenas, and no thread-local small/normal buffer caches. This favors
  a smaller retained footprint over maximum allocator throughput.
- Receive allocations use 4 KiB buffers. Outbound watermarks are 8/32 KiB by
  default; the high watermark grows to frame limit + 1 KiB if necessary.
  A connection becoming unwritable is disconnected, **not** given an
  unbounded application queue. Watermarks are thresholds, not exact byte
  caps: the triggering write and protocol overhead can exceed them.
- No compression/extensions, session map, business executor, or message queue.
- Shutdown stops accepts, closes tracked clients, then terminates event loops.
  This is bounded shutdown, not delivery-guaranteed application draining.

The default 10,000-connection limit is an admission setting, **not a measured
capacity promise**. Heap, direct buffers, thread stacks, JVM metadata, and
kernel socket buffers all contribute to memory use. `-Xmx` only limits heap;
the example JVM limits are starting points, not a guarantee that 10,000
connections fit. Pooled chunks can remain retained after a traffic burst.

Before raising limits, load-test on the actual target hardware with realistic
message sizes, rates, slow readers, and idle connections. Measure RSS,
heap/direct-buffer use, CPU, p95/p99 latency, rejected connections, and
disconnect rate. Increase OS file-descriptor limits and review listen backlog,
socket memory, load-balancer timeouts, and load-generator port limits.
Do not run a blocking database call or other blocking business logic on an
I/O event loop; use a bounded executor with an explicit overload policy if
adding such work.

For horizontal scale, place independent instances behind a WebSocket-aware
load balancer. Each upgraded connection remains on one instance. Cross-node
fan-out requires a separate broker and bounded per-client delivery policy;
neither is part of this echo server.

## Production deployment

Keep the default loopback bind behind a reverse proxy, or restrict a network
bind with firewall policy. Terminate TLS at the ingress and expose `wss://`.
Configure trusted origins, authentication/authorization before upgrade,
connection/rate limits per caller, and ingress handshake timeouts before
exposing the service publicly. Do not log credentials or message payloads.
No authentication or TLS is built into this starter.

Connection-limit, timeout, slow-consumer, and protocol/I/O diagnostics use
`java.util.logging` at `FINE` to avoid per-disconnect production log storms.
Enable it when troubleshooting, for example with an external logging
configuration passed using `-Djava.util.logging.config.file=...`.
Startup and fatal startup errors remain visible by default.

## Tests

`./mvnw verify` runs embedded-channel and real TCP/WebSocket tests covering
configuration validation, buffer ownership, fragmented-message limits, HTTP
routing, origin rejection, connection admission/release, slow-consumer
disconnects, incomplete handshake expiration, concurrent clients, text/binary
echo, ping/pong, oversized frames, read-idle expiry, and shutdown.

The concurrent-client test is a functional test, not a capacity benchmark.
The project targets Java 17 bytecode; newer JDKs may print Netty warnings
about deprecated `sun.misc.Unsafe` APIs.
