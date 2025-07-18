/*
 * Copyright 2015-2017 floragunn GmbH
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.floragunn.searchguard.ssl.transport;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.netty4.Netty4Transport;

import com.floragunn.searchguard.ssl.SearchGuardKeyStore;
import com.floragunn.searchguard.ssl.SslExceptionHandler;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.ssl.SslHandler;
import org.elasticsearch.transport.netty4.SharedGroupFactory;

public class SearchGuardSSLNettyTransport extends Netty4Transport {

    private static final Logger logger = LogManager.getLogger(SearchGuardSSLNettyTransport.class);
    private final SearchGuardKeyStore sgks;
    private final SslExceptionHandler errorHandler;

    public SearchGuardSSLNettyTransport(final Settings settings, final TransportVersion transportVersion, final ThreadPool threadPool, final NetworkService networkService,
                                        final PageCacheRecycler pageCacheRecycler, final NamedWriteableRegistry namedWriteableRegistry,
                                        final CircuitBreakerService circuitBreakerService, SharedGroupFactory sharedGroupFactory, final SearchGuardKeyStore sgks, final SslExceptionHandler errorHandler) {
        super(settings, transportVersion, threadPool, networkService, pageCacheRecycler, namedWriteableRegistry, circuitBreakerService, sharedGroupFactory);

        this.sgks = sgks;
        this.errorHandler = errorHandler;
    }

    @Override
    public void onException(TcpChannel channel, Exception e) {

        Throwable cause = e;

        if (e instanceof DecoderException && e != null) {
            cause = e.getCause();
        }

        errorHandler.logError(cause, false);
        logger.error("Exception during establishing a SSL connection: " + cause, cause);

        super.onException(channel, e);
    }

    @Override
    protected ChannelHandler getServerChannelInitializer(String name) {
        return new SSLServerChannelInitializer(name);
    }

    @Override
    protected ChannelHandler getClientChannelInitializer(DiscoveryNode node, ConnectionProfile connectionProfile) {
        return new SSLClientChannelInitializer(node);
    }

    protected class SSLServerChannelInitializer extends Netty4Transport.ServerChannelInitializer {

        public SSLServerChannelInitializer(String name) {
            super(name);
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);
            final SslHandler sslHandler = new SslHandler(sgks.createServerTransportSSLEngine());
            ch.pipeline().addFirst("ssl_server", sslHandler);
        }
        
        @Override
        public final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (cause instanceof DecoderException && cause != null) {
                cause = cause.getCause();
            }

            errorHandler.logError(cause, false);
            logger.error("Exception during establishing a SSL connection: " + cause, cause);

            super.exceptionCaught(ctx, cause);
        }
    }

    protected static class ClientSSLHandler extends ChannelOutboundHandlerAdapter {
        private final Logger log = LogManager.getLogger(this.getClass());
        private final SearchGuardKeyStore sgks;
        private final boolean hostnameVerificationEnabled;
        private final boolean hostnameVerificationResovleHostName;
        private final SslExceptionHandler errorHandler;
        

        private ClientSSLHandler(final SearchGuardKeyStore sgks, final boolean hostnameVerificationEnabled,
                final boolean hostnameVerificationResovleHostName, final SslExceptionHandler errorHandler) {
            this.sgks = sgks;
            this.hostnameVerificationEnabled = hostnameVerificationEnabled;
            this.hostnameVerificationResovleHostName = hostnameVerificationResovleHostName;
            this.errorHandler = errorHandler;
        }
        

        @Deprecated
        @Override
        public final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (cause instanceof DecoderException && cause != null) {
                cause = cause.getCause();
            }

            errorHandler.logError(cause, false);
            logger.error("Exception during establishing a SSL connection: " + cause, cause);

            super.exceptionCaught(ctx, cause);
        }

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
            SSLEngine engine = null;
            try {
                if (hostnameVerificationEnabled) {
                    final InetSocketAddress inetSocketAddress = (InetSocketAddress) remoteAddress;
                    String hostname = null;
                    if (hostnameVerificationResovleHostName) {
                        hostname = inetSocketAddress.getHostName();
                    } else {
                        hostname = inetSocketAddress.getHostString();
                    }

                    if(log.isDebugEnabled()) {
                        log.debug("Hostname of peer is {} ({}/{}) with hostnameVerificationResovleHostName: {}", hostname, inetSocketAddress.getHostName(), inetSocketAddress.getHostString(), hostnameVerificationResovleHostName);
                    }
                    
                    engine = sgks.createClientTransportSSLEngine(hostname, inetSocketAddress.getPort());
                } else {
                    engine = sgks.createClientTransportSSLEngine(null, -1);
                }
            } catch (final SSLException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }
            final SslHandler sslHandler = new SslHandler(engine);
            ctx.pipeline().replace(this, "ssl_client", sslHandler);
            super.connect(ctx, remoteAddress, localAddress, promise);
        }
    }

    protected class SSLClientChannelInitializer extends Netty4Transport.ClientChannelInitializer {
        private final boolean hostnameVerificationEnabled;
        private final boolean hostnameVerificationResovleHostName;

        public SSLClientChannelInitializer(DiscoveryNode node) {
            hostnameVerificationEnabled = settings.getAsBoolean(
                    SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, true);
            hostnameVerificationResovleHostName = settings.getAsBoolean(
                    SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME, true);
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);
            ch.pipeline().addFirst("client_ssl_handler", new ClientSSLHandler(sgks, hostnameVerificationEnabled,
                    hostnameVerificationResovleHostName, errorHandler));
        }
        
        @Override
        public final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

            if (cause instanceof DecoderException && cause != null) {
                cause = cause.getCause();
            }

            errorHandler.logError(cause, false);
            logger.error("Exception during establishing a SSL connection: " + cause, cause);

            super.exceptionCaught(ctx, cause);
        }
    }
}
