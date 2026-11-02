package com.example.ws.load;

import com.example.ws.Settings;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolConfig;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.CharsetUtil;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

/**
 * Netty WebSocket load client. One process opens {@code CONNECTIONS} sockets, ramped so the server
 * accept queue is not the only thing under test. Metrics are process-wide counters, scraped at
 * {@code METRICS_PORT /metrics} and printed as one JSON line every {@code REPORT_SECONDS}.
 */
public final class LoadClient implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(LoadClient.class.getName());
    private static final int OPEN = 0;
    private static final int UP = 1;
    private static final int DONE = 2;

    private final ClientConfig config;
    private final EventLoopGroup group;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "ws-load-ramp");
        thread.setDaemon(true);
        return thread;
    });
    private final LongAdder connected = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final LongAdder closed = new LongAdder();
    private final LongAdder received = new LongAdder();
    private final AtomicInteger inflight = new AtomicInteger();
    private final AtomicInteger addressCursor = new AtomicInteger();
    private final AtomicBoolean stopping = new AtomicBoolean();
    private volatile Channel metricsChannel;
    private volatile boolean started;

    public LoadClient(ClientConfig config) {
        this.config = config;
        this.group = new NioEventLoopGroup(config.workers());
    }

    public static void main(String[] args) throws Exception {
        var config = ClientConfig.from(Settings.resolve(args));
        LOG.info("WebSocket load client " + config.summary());
        try (var client = new LoadClient(config)) {
            client.start();
            Thread.sleep(config.holdSeconds() * 1000L);
        }
    }

    public synchronized void start() throws InterruptedException {
        if (started) {
            return;
        }
        started = true;
        if (config.metricsPort() > 0) {
            metricsChannel = bindMetrics(config.metricsPort());
        }
        scheduler.scheduleAtFixedRate(this::report, config.reportSeconds(), config.reportSeconds(), TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::openBatch, 0, 1, TimeUnit.SECONDS);
    }

    public long connected() {
        return connected.sum();
    }

    public long received() {
        return received.sum();
    }

    public long failed() {
        return failed.sum();
    }

    private void openBatch() {
        if (stopping.get()) {
            return;
        }
        int room = config.connections() - (int) Math.min(Integer.MAX_VALUE, connected.sum()) - inflight.get();
        int batch = Math.min(config.rampPerSecond(), Math.max(0, room));
        for (int i = 0; i < batch && !stopping.get(); i++) {
            inflight.incrementAndGet();
            connect();
        }
    }

    private void connect() {
        var state = new AtomicInteger(OPEN);
        var bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        var protocol = WebSocketClientProtocolConfig.newBuilder()
                                .webSocketUri(URI.create(config.target()))
                                .allowExtensions(false)
                                .generateOriginHeader(false)
                                .handleCloseFrames(true)
                                .handshakeTimeoutMillis(5000)
                                .maxFramePayloadLength(65536)
                                .build();
                        ch.pipeline()
                                .addLast(new HttpClientCodec())
                                .addLast(new HttpObjectAggregator(8192))
                                .addLast(new WebSocketClientProtocolHandler(protocol))
                                .addLast(new Session(state));
                    }
                });
        if (config.socketReceiveBuffer() > 0) {
            bootstrap.option(ChannelOption.SO_RCVBUF, config.socketReceiveBuffer());
        }
        if (config.socketSendBuffer() > 0) {
            bootstrap.option(ChannelOption.SO_SNDBUF, config.socketSendBuffer());
        }
        var remote = socketAddress(URI.create(config.target()));
        var future = config.localAddresses().isEmpty()
                ? bootstrap.connect(remote)
                : bootstrap.connect(remote, localAddress());
        future.addListener(result -> {
            if (!result.isSuccess()) {
                finish(state, false);
            }
        });
    }

    private void finish(AtomicInteger state, boolean up) {
        if (up) {
            if (state.compareAndSet(OPEN, UP)) {
                inflight.decrementAndGet();
                connected.increment();
            }
            return;
        }
        int previous = state.getAndSet(DONE);
        if (previous == UP) {
            connected.decrement();
            closed.increment();
        } else if (previous == OPEN) {
            inflight.decrementAndGet();
            failed.increment();
        }
    }

    private InetSocketAddress localAddress() {
        var addresses = config.localAddresses();
        String address = addresses.get(Math.floorMod(addressCursor.getAndIncrement(), addresses.size()));
        return new InetSocketAddress(address, 0);
    }

    private static InetSocketAddress socketAddress(URI uri) {
        int port = uri.getPort() == -1 ? ("wss".equalsIgnoreCase(uri.getScheme()) ? 443 : 80) : uri.getPort();
        return new InetSocketAddress(uri.getHost(), port);
    }

    private void report() {
        LOG.info("{\"connected\":" + connected.sum()
                + ",\"inflight\":" + inflight.get()
                + ",\"failed\":" + failed.sum()
                + ",\"closed\":" + closed.sum()
                + ",\"received\":" + received.sum()
                + ",\"target\":" + config.connections() + "}");
    }

    private String scrape() {
        return "# TYPE ws_client_connected gauge\nws_client_connected " + connected.sum()
                + "\n# TYPE ws_client_target gauge\nws_client_target " + config.connections()
                + "\n# TYPE ws_client_inflight gauge\nws_client_inflight " + inflight.get()
                + "\n# TYPE ws_client_failed_total counter\nws_client_failed_total " + failed.sum()
                + "\n# TYPE ws_client_closed_total counter\nws_client_closed_total " + closed.sum()
                + "\n# TYPE ws_client_messages_received_total counter\nws_client_messages_received_total "
                + received.sum() + "\n";
    }

    private Channel bindMetrics(int port) throws InterruptedException {
        return new ServerBootstrap()
                .group(group)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new HttpServerCodec(), new HttpObjectAggregator(8192),
                                new ChannelInboundHandlerAdapter() {
                                    @Override
                                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                        if (msg instanceof FullHttpRequest request) {
                                            byte[] body = scrape().getBytes(CharsetUtil.UTF_8);
                                            var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                                                    HttpResponseStatus.OK, Unpooled.wrappedBuffer(body));
                                            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; version=0.0.4");
                                            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, body.length);
                                            response.headers().set(HttpHeaderNames.CONNECTION, "close");
                                            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
                                            request.release();
                                        } else {
                                            ctx.fireChannelRead(msg);
                                        }
                                    }
                                });
                    }
                }).bind(port).sync().channel();
    }

    @Override
    public void close() {
        if (!stopping.compareAndSet(false, true)) {
            return;
        }
        scheduler.shutdownNow();
        if (metricsChannel != null) {
            metricsChannel.close().awaitUninterruptibly();
        }
        group.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
    }

    private final class Session extends SimpleChannelInboundHandler<WebSocketFrame> {
        private final AtomicInteger state;

        private Session(AtomicInteger state) {
            this.state = state;
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
            if (event instanceof WebSocketClientProtocolHandler.ClientHandshakeStateEvent stateEvent
                    && stateEvent == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                finish(state, true);
                if (config.messageBytes() > 0) {
                    sendPayload(ctx);
                    ctx.executor().scheduleAtFixedRate(() -> sendPayload(ctx),
                            config.messageIntervalSeconds(), config.messageIntervalSeconds(), TimeUnit.SECONDS);
                }
                if (config.pingSeconds() > 0) {
                    ctx.executor().scheduleAtFixedRate(() -> {
                        if (ctx.channel().isActive()) {
                            ctx.writeAndFlush(new PingWebSocketFrame());
                        }
                    }, config.pingSeconds(), config.pingSeconds(), TimeUnit.SECONDS);
                }
            }
            super.userEventTriggered(ctx, event);
        }

        private void sendPayload(ChannelHandlerContext ctx) {
            if (ctx.channel().isActive()) {
                ctx.writeAndFlush(new TextWebSocketFrame(Unpooled.wrappedBuffer(new byte[config.messageBytes()])));
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
            if (frame instanceof TextWebSocketFrame) {
                received.increment();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            finish(state, false);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }
}

