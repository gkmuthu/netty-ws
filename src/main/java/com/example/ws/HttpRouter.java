package com.example.ws;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * HTTP routes used before upgrade. A metrics scrape is a new short HTTP connection, so it still
 * hits this handler. Upgraded sockets remove it. The scrape does not walk the connection table.
 */
final class HttpRouter extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static final byte[] CONNECTOR = loadConnector();
    private final Set<String> allowedOrigins;
    private final ServerMetrics metrics;

    HttpRouter(Set<String> allowedOrigins) {
        this(allowedOrigins, null);
    }

    HttpRouter(Set<String> allowedOrigins, ServerMetrics metrics) {
        this.allowedOrigins = allowedOrigins;
        this.metrics = metrics;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        String uri = request.uri();
        if (!request.decoderResult().isSuccess()) {
            respond(ctx, HttpResponseStatus.BAD_REQUEST, "Bad request\n");
        } else if (!HttpMethod.GET.equals(request.method())) {
            respond(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED, "GET required\n");
        } else if ("/health".equals(uri)) {
            respond(ctx, HttpResponseStatus.OK, "ok\n");
        } else if ("/metrics".equals(uri)) {
            if (metrics == null) {
                respond(ctx, HttpResponseStatus.NOT_FOUND, "Not found\n");
            } else {
                respond(ctx, HttpResponseStatus.OK, metrics.scrape().getBytes(StandardCharsets.UTF_8),
                        "text/plain; version=0.0.4; charset=UTF-8");
            }
        }  else if ("/metrics2".equals(uri)) {
            if (metrics == null) {
                respond(ctx, HttpResponseStatus.NOT_FOUND, "Not found\n");
            } else {
                respond(ctx, HttpResponseStatus.OK, metrics.scrapeToJson().getBytes(StandardCharsets.UTF_8),
                        "application/json; charset=utf-8");
            }
        } else if ("/sample-connector.html".equals(uri)) {
            respond(ctx, HttpResponseStatus.OK, CONNECTOR, "text/html; charset=UTF-8");
        } else if (!"/ws".equals(uri)) {
            respond(ctx, HttpResponseStatus.NOT_FOUND, "Not found\n");
        } else if (request.headers().contains(HttpHeaderNames.ORIGIN)
                && (request.headers().getAll(HttpHeaderNames.ORIGIN).size() != 1
                || !allowedOrigins.contains(request.headers().get(HttpHeaderNames.ORIGIN)))) {
            respond(ctx, HttpResponseStatus.FORBIDDEN, "Origin not allowed\n");
        } else if (!"websocket".equalsIgnoreCase(request.headers().get(HttpHeaderNames.UPGRADE))) {
            respond(ctx, HttpResponseStatus.UPGRADE_REQUIRED, "WebSocket upgrade required\n");
        } else {
            ctx.fireChannelRead(request.retain());
        }
    }

    private static void respond(ChannelHandlerContext ctx, HttpResponseStatus status, String text) {
        respond(ctx, status, text.getBytes(StandardCharsets.UTF_8), "text/plain; charset=UTF-8");
    }

    private static void respond(ChannelHandlerContext ctx, HttpResponseStatus status, byte[] body,
                                String contentType) {
        var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(body));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
        response.headers().set(HttpHeaderNames.CONNECTION, "close");
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    private static byte[] loadConnector() {
        try (var input = HttpRouter.class.getResourceAsStream("/sample-connector.html")) {
            if (input == null) {
                throw new IllegalStateException("Missing sample-connector.html classpath resource");
            }
            return input.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot load sample connector", e);
        }
    }
}

