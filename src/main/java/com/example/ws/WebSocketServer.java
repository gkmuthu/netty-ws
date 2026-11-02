package com.example.ws;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketDecoderConfig;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolConfig;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-concurrency WebSocket server supporting Linux native Epoll (edge-triggered, no JDK
 * selector lock contention, SO_REUSEPORT support) and standard Java NIO transport.
 */
public final class WebSocketServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(WebSocketServer.class);
    private final ServerConfig config;
    private final EventLoopGroup acceptor;
    private final EventLoopGroup workers;
    private final boolean epollActive;
    private final ChannelGroup clients = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);
    private final AtomicInteger connections = new AtomicInteger();
    private final ServerMetrics metrics;
    private Channel listener;
    private boolean closed;

    public WebSocketServer(ServerConfig config) {
        this.config = config;
        boolean epollRequested = "epoll".equalsIgnoreCase(config.transport());
        boolean autoRequested = "auto".equalsIgnoreCase(config.transport());

        if (epollRequested) {
            if (!Epoll.isAvailable()) {
                throw new IllegalStateException("Epoll transport requested but not available: "
                        + Epoll.unavailabilityCause(), Epoll.unavailabilityCause());
            }
            this.epollActive = true;
        } else if (autoRequested) {
            this.epollActive = Epoll.isAvailable();
            if (!this.epollActive && LOG.isDebugEnabled()) {
                LOG.debug("Epoll transport not available ({}); defaulting to Java NIO",
                        Epoll.unavailabilityCause() != null ? Epoll.unavailabilityCause().getMessage() : "unsupported platform");
            }
        } else {
            this.epollActive = false;
        }

        if (this.epollActive) {
            this.acceptor = new EpollEventLoopGroup(config.acceptors());
            this.workers = new EpollEventLoopGroup(config.workers());
        } else {
            this.acceptor = new NioEventLoopGroup(config.acceptors());
            this.workers = new NioEventLoopGroup(config.workers());
        }
        this.metrics = new ServerMetrics(connections);
        this.metrics.setEpollActive(this.epollActive);
    }

    public boolean isEpollActive() {
        return epollActive;
    }

    public static void main(String[] args) throws Exception {
        var settings = Settings.resolve(args);
        if (Settings.clientMode(args, settings)) {
            com.example.ws.load.LoadClient.main(args);
            return;
        }
        var config = ServerConfig.from(settings);
        try (var server = new WebSocketServer(config)) {
            Runtime.getRuntime().addShutdownHook(new Thread(server::close, "ws-shutdown"));
            int port = server.start();
            LOG.info("WebSocket server listening on {}:{} using [{}] transport (/ws, /health, /metrics); {}",
                    config.host(), port, server.isEpollActive() ? "EPOLL (native)" : "NIO (jdk)", config.summary());
            server.listener.closeFuture().sync();
        }
    }

    public ServerMetrics metrics() {
        return metrics;
    }

    public synchronized int start() throws InterruptedException {
        if (closed || listener != null) {
            throw new IllegalStateException("Server is closed or already started");
        }
        var allocator = new PooledByteBufAllocator(true, 0, Math.min(config.workers(), 4),
                8192, 6, 0, 0, false);
        var protocol = WebSocketServerProtocolConfig.newBuilder()
                .websocketPath("/ws")
                .checkStartsWith(false)
                .handshakeTimeoutMillis(5000)
                .forceCloseTimeoutMillis(1000)
                .decoderConfig(WebSocketDecoderConfig.newBuilder()
                        .maxFramePayloadLength(config.maxFrameBytes())
                        .allowExtensions(false)
                        .expectMaskedFrames(true)
                        .allowMaskMismatch(false)
                        .withUTF8Validator(true)
                        .build())
                .build();
        int highWater = Math.max(32768, config.maxFrameBytes() + 1024);
        var bootstrap = new ServerBootstrap()
                .group(acceptor, workers)
                // channel( NioServerSocketChannel.class ) is the default, but we want to use EpollServerSocketChannel if available
                .channel(epollActive ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_BACKLOG, config.backlog())
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.ALLOCATOR, allocator)
                .childOption(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(4096))
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(8192, highWater));
        // epoll enabled
        if (epollActive && config.reusePort()) {
            bootstrap.option(EpollChannelOption.SO_REUSEPORT, true);
        }
        if (config.socketReceiveBuffer() > 0) {
            bootstrap.childOption(ChannelOption.SO_RCVBUF, config.socketReceiveBuffer());
        }
        if (config.socketSendBuffer() > 0) {
            bootstrap.childOption(ChannelOption.SO_SNDBUF, config.socketSendBuffer());
        }
        listener = bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ch.pipeline()
                        .addLast(new IdleStateHandler(config.idleSeconds(), 0, 0))
                        .addLast(new HttpServerCodec(4096, 8192, 4096))
                        .addLast(new HttpObjectAggregator(8192))
                        .addLast(new HttpRouter(config.allowedOrigins(), metrics))
                        .addLast(new WebSocketServerProtocolHandler(protocol))
                        .addLast(new EchoHandler(config.maxMessageBytes(), metrics))
                        .addLast(new ConnectionHandler(connections, config.maxConnections(), clients, metrics, config.handshakeTimeoutSeconds()));
            }
        }).bind(config.host(), config.port()).sync().channel();
        return ((InetSocketAddress) listener.localAddress()).getPort();
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        LOG.info("Shutting down WebSocket server...");
        if (listener != null) {
            listener.close().awaitUninterruptibly();
        }
        clients.close().awaitUninterruptibly();
        acceptor.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
        workers.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
        LOG.info("WebSocket server stopped.");
    }
}

