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

package com.floragunn.searchguard.http;

import com.floragunn.searchguard.ssl.http.AttributedHttpRequest;
import io.netty.handler.ssl.SslHandler;
import org.elasticsearch.action.bulk.IncrementalBulkService;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpHandlingSettings;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.netty4.Netty4HttpChannel;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.transport.netty4.TLSConfig;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

import java.util.function.BiConsumer;

public class SearchGuardNonSslHttpServerTransport extends Netty4HttpServerTransport {

    private final BiConsumer<HttpPreRequest, ThreadContext> perRequestThreadContext;
    private final IncrementalBulkService.Enabled incremantalBulkServiceEnabled;

    public SearchGuardNonSslHttpServerTransport(final Settings settings, final NetworkService networkService,
                                                final ThreadPool threadPool, final NamedXContentRegistry namedXContentRegistry, final Dispatcher dispatcher,
                                                BiConsumer<HttpPreRequest, ThreadContext> perRequestThreadContext, final ClusterSettings clusterSettings, SharedGroupFactory sharedGroupFactory, Tracer tracer) {
        super(settings, networkService, threadPool, namedXContentRegistry, dispatcher, clusterSettings, sharedGroupFactory, tracer, TLSConfig.noTLS(), null, null);
        this.perRequestThreadContext = perRequestThreadContext;
        this.incremantalBulkServiceEnabled = new IncrementalBulkService.Enabled(clusterSettings);
    }

    @Override
    protected void populatePerRequestThreadContext(RestRequest restRequest, ThreadContext threadContext) {
        perRequestThreadContext.accept(restRequest.getHttpRequest(), threadContext);
    }

    @Override
    public void incomingRequest(HttpRequest httpRequest, HttpChannel httpChannel) {
        Channel nettyChannel = ((Netty4HttpChannel) httpChannel).getNettyChannel();
        SslHandler sslhandler = (SslHandler) nettyChannel.pipeline().get("ssl_http");
        super.incomingRequest(AttributedHttpRequest.create(httpRequest, sslhandler, nettyChannel.eventLoop()), httpChannel);
    }

    @Override
    public ChannelHandler configureServerChannelHandler() {
        return new NonSslHttpChannelHandler(this, handlingSettings);
    }

    protected class NonSslHttpChannelHandler extends Netty4HttpServerTransport.HttpChannelHandler {

        protected NonSslHttpChannelHandler(Netty4HttpServerTransport transport, final HttpHandlingSettings handlingSettings) {
            super(transport, handlingSettings, TLSConfig.noTLS(), null, null, incremantalBulkServiceEnabled);
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);
        }
    }
}
