package com.example.ws.load;

import com.example.ws.Settings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Load applied by one client process. Set with the same container args or environment as the server.
 * One client IP can open about 60k connections to one server IP:port. Use {@code LOCAL_ADDRESSES}
 * so this process spreads sockets across source addresses.
 */
public record ClientConfig(String target, int connections, int rampPerSecond, int holdSeconds,
                           int pingSeconds, int messageBytes, int messageIntervalSeconds,
                           int workers, int metricsPort, int reportSeconds,
                           int socketReceiveBuffer, int socketSendBuffer,
                           List<String> localAddresses) {
    public ClientConfig {
        if (target == null || target.isBlank() || connections < 1 || rampPerSecond < 1
                || holdSeconds < 1 || pingSeconds < 0 || messageBytes < 0 || messageIntervalSeconds < 1
                || workers < 1 || metricsPort < 0 || metricsPort > 65535 || reportSeconds < 1
                || socketReceiveBuffer < 0 || socketSendBuffer < 0 || localAddresses == null) {
            throw new IllegalArgumentException("Invalid client configuration");
        }
        localAddresses = List.copyOf(localAddresses);
    }

    public static ClientConfig from(Map<String, String> env) {
        return new ClientConfig(
                Settings.text(env, "TARGET", ""),
                Settings.number(env, "CONNECTIONS", 1),
                Settings.number(env, "RAMP_PER_SECOND", 200),
                Settings.number(env, "HOLD_SECONDS", 60),
                Settings.number(env, "PING_SECONDS", 0),
                Settings.number(env, "MESSAGE_BYTES", 0),
                Settings.number(env, "MESSAGE_INTERVAL_SECONDS", 10),
                Settings.number(env, "WS_WORKERS", Math.min(8, Runtime.getRuntime().availableProcessors())),
                Settings.number(env, "METRICS_PORT", 0),
                Settings.number(env, "REPORT_SECONDS", 5),
                Settings.number(env, "WS_SO_RCVBUF", 4096),
                Settings.number(env, "WS_SO_SNDBUF", 4096),
                Arrays.stream(Settings.text(env, "LOCAL_ADDRESSES", "").split(","))
                        .map(String::trim).filter(s -> !s.isEmpty()).toList());
    }

    public String summary() {
        return "target=" + target + " connections=" + connections + " rampPerSecond=" + rampPerSecond
                + " holdSeconds=" + holdSeconds + " localAddresses=" + localAddresses.size()
                + " messageBytes=" + messageBytes + " metricsPort=" + metricsPort;
    }
}

