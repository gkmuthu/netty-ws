package com.example.ws;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Process-wide counters. Updating a counter is O(1) and allocates nothing, so 1M sockets do not
 * create 1M metric series. Scrape {@code GET /metrics} from another connection; do not log these
 * per event or the log stream becomes the bottleneck.
 */
public final class ServerMetrics {
    private final long startedAt = System.nanoTime();
    private final AtomicInteger connections;
    public final AtomicInteger activeWebSockets = new AtomicInteger();
    public final LongAdder accepted = new LongAdder();
    public final LongAdder rejected = new LongAdder();
    public final LongAdder handshakes = new LongAdder();
    public final LongAdder handshakeExpired = new LongAdder();
    public final LongAdder idleClosed = new LongAdder();
    public final LongAdder slowConsumers = new LongAdder();
    public final LongAdder protocolErrors = new LongAdder();
    public final LongAdder messagesIn = new LongAdder();
    public final LongAdder messagesOut = new LongAdder();
    public final LongAdder bytesIn = new LongAdder();
    public final LongAdder bytesOut = new LongAdder();
    private volatile boolean epollActive;

    public ServerMetrics(AtomicInteger connections) {
        this.connections = connections;
    }

    public void setEpollActive(boolean epollActive) {
        this.epollActive = epollActive;
    }

    public boolean isEpollActive() {
        return epollActive;
    }

    public String scrape() {
        var body = new StringBuilder(2048);
        gauge(body, "ws_transport_epoll", "1 if Linux native epoll transport is active, 0 for Java NIO", epollActive ? 1 : 0);
        gauge(body, "ws_active_connections", "Active established WebSocket connections", activeWebSockets.get());
        gauge(body, "ws_connections", "Admitted TCP connections, including handshakes", connections.get());
        counter(body, "ws_connections_accepted_total", "TCP connections admitted", accepted.sum());
        counter(body, "ws_connections_rejected_total", "TCP connections rejected at the cap", rejected.sum());
        counter(body, "ws_handshakes_completed_total", "WebSocket handshakes completed", handshakes.sum());
        counter(body, "ws_handshakes_expired_total", "Connections closed before handshake", handshakeExpired.sum());
        counter(body, "ws_idle_closed_total", "Connections closed for read idle", idleClosed.sum());
        counter(body, "ws_slow_consumer_closed_total", "Connections closed at the write watermark", slowConsumers.sum());
        counter(body, "ws_protocol_errors_total", "Connections closed after a protocol or I/O error", protocolErrors.sum());
        counter(body, "ws_messages_received_total", "WebSocket data frames received", messagesIn.sum());
        counter(body, "ws_messages_sent_total", "WebSocket data frames sent", messagesOut.sum());
        counter(body, "ws_bytes_received_total", "WebSocket payload bytes received", bytesIn.sum());
        counter(body, "ws_bytes_sent_total", "WebSocket payload bytes sent", bytesOut.sum());
        gauge(body, "ws_uptime_seconds", "Process uptime", (System.nanoTime() - startedAt) / 1_000_000_000L);
        var heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        gauge(body, "jvm_heap_used_bytes", "Heap used", heap.getUsed());
        gauge(body, "jvm_heap_committed_bytes", "Heap committed", heap.getCommitted());
        for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
            if ("direct".equals(pool.getName())) {
                gauge(body, "jvm_direct_used_bytes", "Direct buffer pool used", pool.getMemoryUsed());
                gauge(body, "jvm_direct_count", "Direct buffer pool count", pool.getCount());
            }
        }
        long rss = rssBytes();
        if (rss >= 0) {
            gauge(body, "process_rss_bytes", "Resident set size from /proc/self/status", rss);
        }
        cpu(body);
        return body.toString();
    }

    public String scrapeToJson() {
        var body = new StringBuilder(2048);

        body.append("{\n").append( "  \"ws_active_connections\": ").append(activeWebSockets.get()).append(",\n");
        body.append( "  \"ws_connections\": ").append(connections.get()).append(",\n");
        body.append("  \"ws_connections_accepted_total\": ").append(accepted.sum()).append(",\n");
        body.append("  \"ws_connections_rejected_total\": ").append(rejected.sum()).append(",\n");
        body.append("  \"ws_handshakes_completed_total\":  ").append(handshakes.sum()).append(",\n");
        body.append("  \"ws_handshakes_expired_total\": ").append(handshakeExpired.sum()).append(",\n");
        body.append("  \"ws_idle_closed_total\": ").append(idleClosed.sum()).append(",\n");
        body.append("  \"ws_slow_consumer_closed_total\": ").append(slowConsumers.sum()).append(",\n");
        body.append("  \"ws_protocol_errors_total\": ").append(protocolErrors.sum()).append(",\n");
        body.append("  \"ws_messages_received_total\": ").append(messagesIn.sum()).append(",\n");
        body.append("  \"ws_messages_sent_total\": ").append(messagesOut.sum()).append(",\n");
        body.append("  \"ws_bytes_received_total\": ").append(bytesIn.sum()).append(",\n");
        body.append("  \"ws_bytes_sent_total\": ").append(bytesOut.sum()).append(",\n");
        body.append("  \"ws_uptime_seconds\": ").append((System.nanoTime() - startedAt) / 1_000_000_000L).append(",\n");
        var heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        body.append("  \"jvm_heap_used_bytes\": ").append(heap.getUsed()).append(",\n");
        body.append("  \"jvm_heap_committed_bytes\": ").append(heap.getCommitted()).append(",\n");
        for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
            if ("direct".equals(pool.getName())) {
                body.append("  \"jvm_direct_used_bytes\": ").append(pool.getMemoryUsed()).append(",\n");
                body.append("  \"jvm_direct_count\": ").append(pool.getCount()).append(",\n");
            }
        }
        long rss = rssBytes();
        if (rss >= 0) {
            body.append("  \"process_rss_bytes\": ").append(rss).append(",\n");
        }
        body.append("  \"epoll\": ").append(epollActive);
        cpuJson(body);
        body.append("\n}\n");
        return body.toString();

    }

    private static long rssBytes() {
        try {
            for (String line : Files.readAllLines(Path.of("/proc/self/status"))) {
                if (line.startsWith("VmRSS:")) {
                    String[] parts = line.trim().split("\\s+");
                    return Long.parseLong(parts[1]) * 1024L;
                }
            }
        } catch (Exception ignored) {
            return -1;
        }
        return -1;
    }

    private static void cpuJson(StringBuilder body) {
        var os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof com.sun.management.OperatingSystemMXBean hotSpot) {
            double process = hotSpot.getProcessCpuLoad();
            double system = hotSpot.getCpuLoad();
            if (process >= 0) {
                body.append(",\n  \"process_cpu_load\": ").append(process);
            }
            if (system >= 0) {
                body.append(",\n  \"system_cpu_load\": ").append(system);
            }
        }
    }

    private static void cpu(StringBuilder body) {
        var os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof com.sun.management.OperatingSystemMXBean hotSpot) {
            double process = hotSpot.getProcessCpuLoad();
            double system = hotSpot.getCpuLoad();
            if (process >= 0) {
                gauge(body, "process_cpu_load", "Process CPU load, 0 to 1", process);
            }
            if (system >= 0) {
                gauge(body, "system_cpu_load", "System CPU load, 0 to 1", system);
            }
        }
    }

    private static void counter(StringBuilder body, String name, String help, long value) {
        body.append("# HELP ").append(name).append(' ').append(help).append('\n');
        body.append("# TYPE ").append(name).append(" counter\n");
        body.append(name).append(' ').append(value).append('\n');
    }

    private static void gauge(StringBuilder body, String name, String help, long value) {
        body.append("# HELP ").append(name).append(' ').append(help).append('\n');
        body.append("# TYPE ").append(name).append(" gauge\n");
        body.append(name).append(' ').append(value).append('\n');
    }

    private static void gauge(StringBuilder body, String name, String help, double value) {
        body.append("# HELP ").append(name).append(' ').append(help).append('\n');
        body.append("# TYPE ").append(name).append(" gauge\n");
        body.append(name).append(' ').append(value).append('\n');
    }
}

