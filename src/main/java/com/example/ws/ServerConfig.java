package com.example.ws;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Capacity of one server process. Set these with container environment variables or args.
 * They do not change the pipeline. Compression, aggregation, and a per-session map are not
 * part of this server: each one retains memory on every socket and fights a 1M idle soak.
 */
public record ServerConfig(String host, int port, int workers, int maxConnections,
                           int maxFrameBytes, int maxMessageBytes, int idleSeconds,
                           int backlog, int socketReceiveBuffer, int socketSendBuffer,
                           int handshakeTimeoutSeconds, Set<String> allowedOrigins,
                           String transport, int acceptors, boolean reusePort) {
    public ServerConfig {
        if (host == null || host.isBlank() || port < 0 || port > 65535 || workers < 1
                || maxConnections < 1 || maxFrameBytes < 1 || maxFrameBytes > Integer.MAX_VALUE - 1024
                || maxMessageBytes < maxFrameBytes || idleSeconds < 1 || backlog < 1
                || socketReceiveBuffer < 0 || socketSendBuffer < 0 || handshakeTimeoutSeconds < 1
                || allowedOrigins == null || acceptors < 1) {
            throw new IllegalArgumentException("Invalid server configuration");
        }
        if (transport == null || (!transport.equalsIgnoreCase("auto")
                && !transport.equalsIgnoreCase("epoll")
                && !transport.equalsIgnoreCase("nio"))) {
            throw new IllegalArgumentException("WS_TRANSPORT must be one of: auto, epoll, nio");
        }
        transport = transport.trim().toLowerCase();
        allowedOrigins = Set.copyOf(allowedOrigins);
    }

    public ServerConfig(String host, int port, int workers, int maxConnections,
                        int maxFrameBytes, int maxMessageBytes, int idleSeconds,
                        int backlog, int socketReceiveBuffer, int socketSendBuffer,
                        int handshakeTimeoutSeconds, Set<String> allowedOrigins) {
        this(host, port, workers, maxConnections, maxFrameBytes, maxMessageBytes,
                idleSeconds, backlog, socketReceiveBuffer, socketSendBuffer,
                handshakeTimeoutSeconds, allowedOrigins, "auto", 1, true);
    }

    public ServerConfig(String host, int port, int workers, int maxConnections,
                        int maxFrameBytes, int maxMessageBytes, int idleSeconds,
                        int backlog, int socketReceiveBuffer, int socketSendBuffer,
                        Set<String> allowedOrigins) {
        this(host, port, workers, maxConnections, maxFrameBytes, maxMessageBytes,
                idleSeconds, backlog, socketReceiveBuffer, socketSendBuffer, 30, allowedOrigins);
    }

    public static ServerConfig from(Map<String, String> env) {
        int port = Settings.number(env, "WS_PORT", 8080);
        return new ServerConfig(
                Settings.text(env, "WS_HOST", "127.0.0.1"),
                port,
                Settings.number(env, "WS_WORKERS", Math.min(8, Runtime.getRuntime().availableProcessors())),
                Settings.number(env, "WS_MAX_CONNECTIONS", 10000),
                Settings.number(env, "WS_MAX_FRAME_BYTES", 16384),
                Settings.number(env, "WS_MAX_MESSAGE_BYTES", 65536),
                Settings.number(env, "WS_IDLE_SECONDS", 60),
                Settings.number(env, "WS_BACKLOG", 1024),
                Settings.number(env, "WS_SO_RCVBUF", 0),
                Settings.number(env, "WS_SO_SNDBUF", 0),
                Settings.number(env, "WS_HANDSHAKE_TIMEOUT_SECONDS", 30),
                Arrays.stream(Settings.text(env, "WS_ALLOWED_ORIGINS",
                                "http://localhost:" + port + ",http://127.0.0.1:" + port).split(","))
                        .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet()),
//                1M load changes
                Settings.text(env, "WS_TRANSPORT", "auto"),
                Settings.number(env, "WS_ACCEPTORS", 1),
                Settings.bool(env, "WS_SO_REUSEPORT", true));
    }

    public String summary() {
        return "host=" + host + " port=" + port + " transport=" + transport + " acceptors=" + acceptors
                + " reusePort=" + reusePort + " workers=" + workers
                + " maxConnections=" + maxConnections + " idleSeconds=" + idleSeconds
                + " backlog=" + backlog + " soRcvBuf=" + socketReceiveBuffer
                + " soSndBuf=" + socketSendBuffer + " maxFrameBytes=" + maxFrameBytes
                + " handshakeTimeout=" + handshakeTimeoutSeconds;
    }
}

