package com.example.ws;

import io.github.cdimascio.dotenv.Dotenv;
import io.github.cdimascio.dotenv.DotenvEntry;

import java.util.HashMap;
import java.util.Map;

/**
 * Resolves process settings. Precedence is defaults, then {@code .env}, then the container
 * environment, then command-line args. Docker should set the environment; args override it
 * for a one-off run without rebuilding the image.
 */
public final class Settings {
    private static final Map<String, String> ARG_KEYS = Map.ofEntries(
            Map.entry("host", "WS_HOST"),
            Map.entry("port", "WS_PORT"),
            Map.entry("workers", "WS_WORKERS"),
            Map.entry("max-connections", "WS_MAX_CONNECTIONS"),
            Map.entry("max-frame-bytes", "WS_MAX_FRAME_BYTES"),
            Map.entry("max-message-bytes", "WS_MAX_MESSAGE_BYTES"),
            Map.entry("idle-seconds", "WS_IDLE_SECONDS"),
            Map.entry("backlog", "WS_BACKLOG"),
            Map.entry("so-rcvbuf", "WS_SO_RCVBUF"),
            Map.entry("so-sndbuf", "WS_SO_SNDBUF"),
            Map.entry("allowed-origins", "WS_ALLOWED_ORIGINS"),
            Map.entry("transport", "WS_TRANSPORT"),
            Map.entry("acceptors", "WS_ACCEPTORS"),
            Map.entry("so-reuseport", "WS_SO_REUSEPORT"),
            Map.entry("reuse-port", "WS_SO_REUSEPORT"),
            Map.entry("mode", "MODE"),
            Map.entry("target", "TARGET"),
            Map.entry("connections", "CONNECTIONS"),
            Map.entry("ramp-per-second", "RAMP_PER_SECOND"),
            Map.entry("hold-seconds", "HOLD_SECONDS"),
            Map.entry("ping-seconds", "PING_SECONDS"),
            Map.entry("message-bytes", "MESSAGE_BYTES"),
            Map.entry("message-interval-seconds", "MESSAGE_INTERVAL_SECONDS"),
            Map.entry("local-addresses", "LOCAL_ADDRESSES"),
            Map.entry("metrics-port", "METRICS_PORT"),
            Map.entry("report-seconds", "REPORT_SECONDS"));

    private Settings() {
    }

    public static Map<String, String> resolve(String[] args) {
        Map<String, String> values = new HashMap<>();
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        for (DotenvEntry entry : dotenv.entries()) {
            values.put(entry.getKey(), entry.getValue());
        }
        values.putAll(System.getenv());
        applyArgs(values, args == null ? new String[0] : args);
        return values;
    }

    public static boolean clientMode(String[] args, Map<String, String> values) {
        if ("client".equalsIgnoreCase(values.get("MODE"))) {
            return true;
        }
        return args != null && args.length > 0 && "client".equalsIgnoreCase(args[0]);
    }

    public static int number(Map<String, String> values, String name, int fallback) {
        String value = values.get(name);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer", e);
        }
    }

    public static boolean bool(Map<String, String> values, String name, boolean fallback) {
        String value = values.get(name);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return Boolean.parseBoolean(value.trim());
    }

    public static String text(Map<String, String> values, String name, String fallback) {
        String value = values.get(name);
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    private static void applyArgs(Map<String, String> values, String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                continue;
            }
            String body = arg.substring(2);
            String key;
            String value;
            int eq = body.indexOf('=');
            if (eq >= 0) {
                key = body.substring(0, eq);
                value = body.substring(eq + 1);
            } else if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                key = body;
                value = args[++i];
            } else {
                key = body;
                value = "true";
            }
            values.put(ARG_KEYS.getOrDefault(key, key), value);
        }
    }
}

