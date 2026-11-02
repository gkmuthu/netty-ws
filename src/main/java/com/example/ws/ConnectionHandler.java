package com.example.ws;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Admits sockets up to the process cap, then accounts for handshake, idle, and slow-consumer closes.
 * The cap is the container setting {@code WS_MAX_CONNECTIONS}. The check is one atomic increment.
 */
final class ConnectionHandler extends ChannelDuplexHandler {
    private static final Logger LOG = LoggerFactory.getLogger( ConnectionHandler.class );
    private final AtomicInteger connections;
    private final int limit;
    private final ChannelGroup channels;
    private final ServerMetrics metrics;
    private final int handshakeTimeoutSeconds;
    private boolean counted;
    private boolean wsUpgraded;
    private ScheduledFuture<?> handshakeDeadline;

    ConnectionHandler( AtomicInteger connections, int limit, ChannelGroup channels ) {
        this( connections, limit, channels, null, 5 );
    }

    ConnectionHandler( AtomicInteger connections, int limit, ChannelGroup channels, ServerMetrics metrics ) {
        this( connections, limit, channels, metrics, 5 );
    }

    ConnectionHandler( AtomicInteger connections, int limit, ChannelGroup channels, ServerMetrics metrics, int handshakeTimeoutSeconds ) {
        this.connections = connections;
        this.limit = limit;
        this.channels = channels;
        this.metrics = metrics;
        this.handshakeTimeoutSeconds = handshakeTimeoutSeconds;
    }

    @Override
    public void channelActive( ChannelHandlerContext ctx ) throws Exception {
        int current = connections.incrementAndGet();
        if ( current > limit ) {
            connections.decrementAndGet();
            if ( metrics != null ) {
                metrics.rejected.increment();
            }
            LOG.warn( "Connection limit [{}] reached; rejecting channel [{}]", limit, ctx.channel().remoteAddress() );
            ctx.close();
            return;
        }
        counted = true;
        if ( metrics != null ) {
            metrics.accepted.increment();
        }
        channels.add( ctx.channel() );
        LOG.debug( "TCP connected: remote={}, activeConnections={}/{}", ctx.channel().remoteAddress(), current, limit );
        handshakeDeadline = ctx.executor().schedule( () -> {
            if ( metrics != null ) {
                metrics.handshakeExpired.increment();
            }
            LOG.warn( "Closing connection: handshake deadline exceeded, remote={}", ctx.channel().remoteAddress() );
            ctx.close();
        }, handshakeTimeoutSeconds, TimeUnit.SECONDS );
        super.channelActive( ctx );
    }

    @Override
    public void channelInactive( ChannelHandlerContext ctx ) throws Exception {
        cancelHandshakeDeadline();
        if ( counted ) {
            counted = false;
            int remaining = connections.decrementAndGet();
            if ( wsUpgraded ) {
                if ( metrics != null ) {
                    int activeWs = metrics.activeWebSockets.decrementAndGet();
                    LOG.info( "WebSocket disconnected: remote={}, activeWebSockets={}, totalTcp={}",
                            ctx.channel().remoteAddress(), activeWs, remaining );
                } else {
                    LOG.info( "WebSocket disconnected: remote={}, totalTcp={}",
                            ctx.channel().remoteAddress(), remaining );
                }
            } else {
                LOG.debug( "HTTP connection closed: remote={}, totalTcp={}",
                        ctx.channel().remoteAddress(), remaining );
            }
        }
        super.channelInactive( ctx );
    }

    @Override
    public void channelWritabilityChanged( ChannelHandlerContext ctx ) throws Exception {
        if ( !ctx.channel().isWritable() ) {
            if ( metrics != null ) {
                metrics.slowConsumers.increment();
            }
            LOG.warn( "Closing slow consumer: outbound high watermark exceeded, remote={}", ctx.channel().remoteAddress() );
            ctx.close();
        }
        super.channelWritabilityChanged( ctx );
    }

    @Override
    public void userEventTriggered( ChannelHandlerContext ctx, Object event ) throws Exception {
        if ( event instanceof WebSocketServerProtocolHandler.HandshakeComplete ) {
            cancelHandshakeDeadline();
            wsUpgraded = true;
            if ( metrics != null ) {
                metrics.handshakes.increment();
                int activeWs = metrics.activeWebSockets.incrementAndGet();

                if( connections.get() % 50000 == 0)
                LOG.info( "WebSocket connected: remote={}, activeWebSockets={}, totalTcp={}",
                        ctx.channel().remoteAddress(), activeWs, connections.get() );
            } else {

                if( connections.get() % 50000 == 0)
                LOG.info( "WebSocket connected: remote={}, totalTcp={}",
                        ctx.channel().remoteAddress(), connections.get() );
            }
            super.userEventTriggered( ctx, event );
        } else if ( event instanceof IdleStateEvent ) {
            if ( metrics != null ) {
                metrics.idleClosed.increment();
            }
            LOG.info( "Closing read-idle connection: remote={}", ctx.channel().remoteAddress() );
            ctx.close();
        } else {
            super.userEventTriggered( ctx, event );
        }
    }

    private void cancelHandshakeDeadline() {
        if ( handshakeDeadline != null ) {
            handshakeDeadline.cancel( false );
            handshakeDeadline = null;
        }
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause ) {
        if ( metrics != null ) {
            metrics.protocolErrors.increment();
        }
        if ( cause instanceof java.io.IOException ) {
            LOG.warn( "I/O connection error: remote={}, message={}", ctx.channel().remoteAddress(), cause.getMessage() );
        } else {
            LOG.error( "Closing connection after protocol failure: remote={}", ctx.channel().remoteAddress(), cause );
        }
        ctx.close();
    }
}

