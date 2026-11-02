package com.example.ws;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;

/**
 * Echoes data frames without copying the payload. Six event loops share every socket, so idle
 * connections cost no echo CPU. A 6-core host can hold a large idle set; it cannot echo one
 * message per second on every socket of a 1M set.
 */
final class EchoHandler extends SimpleChannelInboundHandler<WebSocketFrame> {
    private final int maxMessageBytes;
    private final ServerMetrics metrics;
    private long messageBytes;
    private boolean closing;

    EchoHandler(int maxMessageBytes) {
        this(maxMessageBytes, null);
    }

    EchoHandler(int maxMessageBytes, ServerMetrics metrics) {
        this.maxMessageBytes = maxMessageBytes;
        this.metrics = metrics;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
        if (closing || !ctx.channel().isActive()) {
            return;
        }
        if (frame instanceof TextWebSocketFrame || frame instanceof BinaryWebSocketFrame
                || frame instanceof ContinuationWebSocketFrame) {
            int size = frame.content().readableBytes();
            messageBytes += size;
            if (messageBytes > maxMessageBytes) {
                closing = true;
                ctx.writeAndFlush(new CloseWebSocketFrame(1009, "Message too large"))
                        .addListener(ChannelFutureListener.CLOSE);
                return;
            }
            if (frame.isFinalFragment()) {
                messageBytes = 0;
            }
            if (metrics != null) {
                metrics.messagesIn.increment();
                metrics.bytesIn.add(size);
                metrics.messagesOut.increment();
                metrics.bytesOut.add(size);
            }
            ctx.write(frame.retain()).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
        ctx.fireChannelReadComplete();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
            ctx.pipeline().remove(HttpRouter.class);
        }
        super.userEventTriggered(ctx, event);
    }
}

