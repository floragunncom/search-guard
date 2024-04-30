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

package com.floragunn.searchguard.ssl.http.netty;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpHandlingSettings;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.transport.netty4.TLSConfig;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import com.floragunn.searchguard.ssl.SearchGuardKeyStore;
import com.floragunn.searchguard.ssl.SslExceptionHandler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.ssl.SslHandler;

public class SearchGuardSSLNettyHttpServerTransport extends Netty4HttpServerTransport {

    private static final Logger logger = LogManager.getLogger(SearchGuardSSLNettyHttpServerTransport.class);
    private final SearchGuardKeyStore sgks;
    private final SslExceptionHandler errorHandler;

    public SearchGuardSSLNettyHttpServerTransport(final Settings settings, final NetworkService networkService,
                                                  final ThreadPool threadPool, final SearchGuardKeyStore sgks, final NamedXContentRegistry namedXContentRegistry,
                                                  final Dispatcher dispatcher, ClusterSettings clusterSettings, SharedGroupFactory sharedGroupFactory,
                                                  final SslExceptionHandler errorHandler, Tracer tracer) {
        super(settings, networkService, threadPool, namedXContentRegistry, dispatcher, clusterSettings, sharedGroupFactory, tracer, TLSConfig.noTLS(), null);
        this.sgks = sgks;
        this.errorHandler = errorHandler;
    }

    @Override
    public ChannelHandler configureServerChannelHandler() {
        return new SSLHttpChannelHandler(this, handlingSettings, sgks);
    }

    @Override
    public void onException(HttpChannel channel, Exception cause0) {

        Throwable cause = cause0;

        if (cause0 instanceof DecoderException && cause0 != null) {
            cause = cause0.getCause();
        }

        errorHandler.logError(cause, true);
        
        if (logger.isDebugEnabled()) {
            logger.debug("Exception during establishing a SSL connection: " + cause, cause);
        }
        
        super.onException(channel, cause0);
    }

    protected class SSLHttpChannelHandler extends Netty4HttpServerTransport.HttpChannelHandler {

        protected SSLHttpChannelHandler(Netty4HttpServerTransport transport, final HttpHandlingSettings handlingSettings,
                final SearchGuardKeyStore sgks) {
            super(transport, handlingSettings, TLSConfig.noTLS(), null);
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);
            final SslHandler sslHandler = new SslHandler(SearchGuardSSLNettyHttpServerTransport.this.sgks.createHTTPSSLEngine());
            ch.pipeline().addFirst("ssl_http", sslHandler);
        }
    }
}
