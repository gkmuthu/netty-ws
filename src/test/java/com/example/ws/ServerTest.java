package com.example.ws;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.net.http.WebSocketHandshakeException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ServerTest {
    @Test
    void validatesConfiguration() {
        assertEquals(10000, ServerConfig.from(Map.of()).maxConnections());
        assertThrows(IllegalArgumentException.class,
                () -> ServerConfig.from(Map.of("WS_PORT", "not-a-port")));
        assertThrows(IllegalArgumentException.class,
                () -> ServerConfig.from(Map.of("WS_WORKERS", "0")));
        assertThrows(IllegalArgumentException.class,
                () -> ServerConfig.from(Map.of("WS_MAX_MESSAGE_BYTES", "10")));
        assertThrows(IllegalArgumentException.class,
                () -> ServerConfig.from(Map.of("WS_TRANSPORT", "kqueue")));
        assertThrows(IllegalArgumentException.class,
                () -> ServerConfig.from(Map.of("WS_ACCEPTORS", "0")));
        assertEquals(4096, ServerConfig.from(Map.of("WS_SO_RCVBUF", "4096")).socketReceiveBuffer());
        assertEquals("auto", ServerConfig.from(Map.of()).transport());
        assertEquals(1, ServerConfig.from(Map.of()).acceptors());
        assertTrue(ServerConfig.from(Map.of()).reusePort());
        assertEquals("nio", ServerConfig.from(Map.of("WS_TRANSPORT", "nio")).transport());
        assertEquals(2, ServerConfig.from(Map.of("WS_ACCEPTORS", "2")).acceptors());
        assertFalse(ServerConfig.from(Map.of("WS_SO_REUSEPORT", "false")).reusePort());
        assertEquals(250000, Settings.number(Settings.resolve(new String[]{"--connections=250000"}), "CONNECTIONS", 1));
        assertEquals("nio", Settings.text(Settings.resolve(new String[]{"--transport=nio"}), "WS_TRANSPORT", "auto"));
        assertEquals(2, Settings.number(Settings.resolve(new String[]{"--acceptors=2"}), "WS_ACCEPTORS", 1));
        assertFalse(Settings.bool(Settings.resolve(new String[]{"--so-reuseport=false"}), "WS_SO_REUSEPORT", true));
    }

    @Test
    void supportsNioTransportExplicitly() throws Exception {
        var config = new ServerConfig("127.0.0.1", 0, 2, 100,
                16384, 65536, 60, 128, 0, 0, 30, Set.of("https://trusted.example"), "nio", 1, false);
        try (var server = new WebSocketServer(config)) {
            assertFalse(server.isEpollActive());
            int port = server.start();
            assertTrue(port > 0);
        }
    }

    @Test
    void echoesWithoutCopyAndReleasesReferences() {
        var channel = new EmbeddedChannel(new EchoHandler(32));
        var input = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));
        try {
            channel.writeInbound(input);
            WebSocketFrame output = channel.readOutbound();
            assertSame(input, output);
            assertEquals(1, output.refCnt());
            output.release();
            assertEquals(0, input.refCnt());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void boundsFragmentedMessagesAndResetsAfterFinalFragment() {
        var channel = new EmbeddedChannel(new EchoHandler(5));
        try {
            channel.writeInbound(new TextWebSocketFrame(false, 0, "abc"));
            WebSocketFrame first = channel.readOutbound();
            assertFalse(first.isFinalFragment());
            first.release();
            channel.writeInbound(new ContinuationWebSocketFrame(true, 0, "de"));
            ((WebSocketFrame) channel.readOutbound()).release();
            channel.writeInbound(new TextWebSocketFrame(false, 0, "1234"));
            ((WebSocketFrame) channel.readOutbound()).release();
            var rejected = new ContinuationWebSocketFrame(true, 0, "56");
            channel.writeInbound(rejected);
            CloseWebSocketFrame close = channel.readOutbound();
            assertEquals(1009, close.statusCode());
            close.release();
            assertEquals(0, rejected.refCnt());
            assertFalse(channel.isActive());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void rejectsDisallowedOriginsAndUnknownRoutes() {
        assertHttpStatus("/ws", "https://untrusted.example", 403);
        assertHttpStatus("/ws", "null", 403);
        assertHttpStatus("/missing", null, 404);
        assertHttpStatus("/health", null, 200);
        assertHttpStatus("/ws", null, 426);
    }

    private static void assertHttpStatus(String path, String origin, int expected) {
        var channel = new EmbeddedChannel(new HttpRouter(Set.of("https://trusted.example")));
        try {
            var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path);
            if (origin != null) {
                request.headers().set(HttpHeaderNames.ORIGIN, origin);
            }
            channel.writeInbound(request);
            FullHttpResponse response = channel.readOutbound();
            assertEquals(expected, response.status().code());
            response.release();
            assertEquals(0, request.refCnt());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void capsConnectionsAndClosesUnwritableClients() {
        var count = new AtomicInteger();
        var group = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        var first = new EmbeddedChannel(new ConnectionHandler(count, 1, group));
        var second = new EmbeddedChannel(new ConnectionHandler(count, 1, group));
        try {
            assertTrue(first.isActive());
            assertFalse(second.isActive());
            assertEquals(1, count.get());
            first.unsafe().outboundBuffer().setUserDefinedWritability(1, false);
            first.runPendingTasks();
            assertFalse(first.isActive());
            assertEquals(0, count.get());
        } finally {
            first.finishAndReleaseAll();
            second.finishAndReleaseAll();
        }
    }

    @Test
    void incompleteHandshakesExpireAndReleaseConnectionSlots() {
        var count = new AtomicInteger();
        var channel = new EmbeddedChannel(new ConnectionHandler(count, 1,
                new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)));
        try {
            assertEquals(1, count.get());
            channel.advanceTimeBy(6, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertFalse(channel.isActive());
            assertEquals(0, count.get());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void completedHandshakeCancelsDeadline() {
        var count = new AtomicInteger();
        var channel = new EmbeddedChannel(new ConnectionHandler(count, 1,
                new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)));
        try {
            channel.pipeline().fireUserEventTriggered(
                    new WebSocketServerProtocolHandler.HandshakeComplete(
                            "/ws", new DefaultHttpHeaders(), null));
            channel.advanceTimeBy(6, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertTrue(channel.isActive());
            assertEquals(1, count.get());
        } finally {
            channel.finishAndReleaseAll();
        }
        assertEquals(0, count.get());
    }

    @Test
    void realNetworkSupportsHealthConcurrentClientsBinaryPingAndClose() throws Exception {
        try (var server = server(16384, 65536, 60)) {
            int port = server.start();
            var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
            var health = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/health"))
                    .timeout(Duration.ofSeconds(3)).build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(200, health.statusCode());
            assertEquals("ok\n", health.body());
            var page = client.send(HttpRequest.newBuilder(
                            URI.create("http://127.0.0.1:" + port + "/sample-connector.html")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, page.statusCode());
            assertEquals("text/html; charset=UTF-8",
                    page.headers().firstValue("content-type").orElseThrow());
            assertTrue(page.body().contains("new WebSocket(url.href)"));
            var sockets = new ArrayList<WebSocket>();
            var listeners = new ArrayList<Listener>();
            try {
                var pending = new ArrayList<CompletableFuture<WebSocket>>();
                for (int i = 0; i < 32; i++) {
                    var listener = new Listener();
                    listeners.add(listener);
                    pending.add(client.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(3))
                            .buildAsync(URI.create("ws://127.0.0.1:" + port + "/ws"), listener));
                }
                for (var future : pending) {
                    sockets.add(future.get(5, TimeUnit.SECONDS));
                }
                for (int i = 0; i < sockets.size(); i++) {
                    sockets.get(i).sendText("hello-" + i, true).get(3, TimeUnit.SECONDS);
                }
                for (int i = 0; i < sockets.size(); i++) {
                    assertEquals("hello-" + i, listeners.get(i).text.poll(5, TimeUnit.SECONDS));
                }
                var socket = sockets.get(0);
                var listener = listeners.get(0);
                socket.sendBinary(ByteBuffer.wrap(new byte[]{1, 2, 3}), true).get(3, TimeUnit.SECONDS);
                assertArrayEquals(new byte[]{1, 2, 3}, listener.binary.poll(3, TimeUnit.SECONDS));
                socket.sendText("frag-", false).get(3, TimeUnit.SECONDS);
                socket.sendText("end", true).get(3, TimeUnit.SECONDS);
                assertEquals("frag-end", listener.text.poll(3, TimeUnit.SECONDS));
                socket.sendPing(ByteBuffer.wrap(new byte[]{9})).get(3, TimeUnit.SECONDS);
                assertArrayEquals(new byte[]{9}, listener.pongs.poll(3, TimeUnit.SECONDS));
                socket.sendClose(1000, "bye").get(3, TimeUnit.SECONDS);
                assertEquals(1000, listener.closed.get(3, TimeUnit.SECONDS));
            } finally {
                sockets.forEach(WebSocket::abort);
            }
        }
    }

    @Test
    void allowsConfiguredBrowserOrigin() throws Exception {
        try (var server = server(16384, 65536, 60)) {
            int port = server.start();
            var listener = new Listener();
            var socket = HttpClient.newHttpClient().newWebSocketBuilder()
                    .header("Origin", "https://trusted.example")
                    .buildAsync(URI.create("ws://127.0.0.1:" + port + "/ws"), listener)
                    .get(3, TimeUnit.SECONDS);
            try {
                socket.sendText("browser-origin", true).get(3, TimeUnit.SECONDS);
                assertEquals("browser-origin", listener.text.poll(3, TimeUnit.SECONDS));
            } finally {
                socket.abort();
            }
        }
    }

    @Test
    void realNetworkRejectsOriginAndOversizedFrame() throws Exception {
        try (var server = server(8, 16, 60)) {
            int port = server.start();
            var client = HttpClient.newHttpClient();
            var uri = URI.create("ws://127.0.0.1:" + port + "/ws");
            var failure = assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> client.newWebSocketBuilder().header("Origin", "https://untrusted.example")
                            .buildAsync(uri, new Listener()).get(3, TimeUnit.SECONDS));
            var handshake = assertInstanceOf(WebSocketHandshakeException.class, failure.getCause());
            assertEquals(403, handshake.getResponse().statusCode());
            var listener = new Listener();
            var socket = client.newWebSocketBuilder().buildAsync(uri, listener).get(3, TimeUnit.SECONDS);
            try {
                socket.sendText("123456789", true).get(3, TimeUnit.SECONDS);
                assertEquals(1009, listener.closed.get(3, TimeUnit.SECONDS));
            } finally {
                socket.abort();
            }
        }
    }

    @Test
    void realNetworkEnforcesCumulativeMessageLimit() throws Exception {
        try (var server = server(8, 16, 60)) {
            int port = server.start();
            var listener = new Listener();
            var socket = HttpClient.newHttpClient().newWebSocketBuilder()
                    .buildAsync(URI.create("ws://127.0.0.1:" + port + "/ws"), listener)
                    .get(3, TimeUnit.SECONDS);
            try {
                socket.sendText("12345678", false).get(3, TimeUnit.SECONDS);
                socket.sendText("12345678", false).get(3, TimeUnit.SECONDS);
                socket.sendText("9", true).get(3, TimeUnit.SECONDS);
                assertEquals(1009, listener.closed.get(3, TimeUnit.SECONDS));
                assertTrue(listener.text.isEmpty());
            } finally {
                socket.abort();
            }
        }
    }

    @Test
    void realNetworkClosesReadIdleConnectionsAndShutdownClosesClients() throws Exception {
        var server = server(16384, 65536, 1);
        try {
            int port = server.start();
            var client = HttpClient.newHttpClient();
            var uri = URI.create("ws://127.0.0.1:" + port + "/ws");
            var idle = new Listener();
            var socket = client.newWebSocketBuilder().buildAsync(uri, idle).get(3, TimeUnit.SECONDS);
            try {
                assertEquals(1000, idle.closed.get(4, TimeUnit.SECONDS));
            } finally {
                socket.abort();
            }
            var shutdown = new Listener();
            socket = client.newWebSocketBuilder().buildAsync(uri, shutdown).get(3, TimeUnit.SECONDS);
            try {
                server.close();
                assertEquals(1000, shutdown.closed.get(3, TimeUnit.SECONDS));
            } finally {
                socket.abort();
            }
        } finally {
            server.close();
        }
    }

    private static WebSocketServer server(int frameBytes, int messageBytes, int idleSeconds) {
        return new WebSocketServer(new ServerConfig("127.0.0.1", 0, 2, 100,
                frameBytes, messageBytes, idleSeconds, 128, 0, 0, Set.of("https://trusted.example")));
    }

    @Test
    void metricsExposeConnectionGauge() throws Exception {
        try (var server = server(16384, 65536, 60)) {
            int port = server.start();
            var response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                            URI.create("http://127.0.0.1:" + port + "/metrics")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            assertTrue(response.body().contains("# TYPE ws_connections gauge"));
            assertTrue(response.body().contains("ws_connections_accepted_total"));
        }
    }

    @Test
    void loadClientOpensConnectionsAndReceivesEcho() throws Exception {
        try (var server = server(16384, 65536, 60)) {
            int port = server.start();
            var client = new com.example.ws.load.LoadClient(new com.example.ws.load.ClientConfig(
                    "ws://127.0.0.1:" + port + "/ws", 2, 10, 30, 0, 4, 30, 1, 0, 30, 0, 0, java.util.List.of()));
            try {
                client.start();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (client.connected() < 2 && System.nanoTime() < deadline) {
                    Thread.sleep(50);
                }
                assertEquals(2, client.connected());
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (client.received() < 2 && System.nanoTime() < deadline) {
                    Thread.sleep(50);
                }
                assertTrue(client.received() >= 2);
            } finally {
                client.close();
            }
        }
    }

    private static final class Listener implements WebSocket.Listener {
        final LinkedBlockingQueue<String> text = new LinkedBlockingQueue<>();
        final LinkedBlockingQueue<byte[]> binary = new LinkedBlockingQueue<>();
        final LinkedBlockingQueue<byte[]> pongs = new LinkedBlockingQueue<>();
        final CompletableFuture<Integer> closed = new CompletableFuture<>();
        private final StringBuilder fragments = new StringBuilder();

        @Override
        public void onOpen(WebSocket webSocket) {
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            fragments.append(data);
            if (last) {
                text.add(fragments.toString());
                fragments.setLength(0);
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
            byte[] bytes = new byte[data.remaining()];
            data.get(bytes);
            binary.add(bytes);
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
            byte[] bytes = new byte[message.remaining()];
            message.get(bytes);
            pongs.add(bytes);
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            closed.complete(statusCode);
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            closed.completeExceptionally(error);
        }
    }
}
