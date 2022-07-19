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

import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpHandlingSettings;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

public class SearchGuardNonSslHttpServerTransport extends Netty4HttpServerTransport {

    public SearchGuardNonSslHttpServerTransport(final Settings settings, final NetworkService networkService,
            final ThreadPool threadPool, final NamedXContentRegistry namedXContentRegistry, final Dispatcher dispatcher,
            final ClusterSettings clusterSettings, SharedGroupFactory sharedGroupFactory) {
        super(settings, networkService, threadPool, namedXContentRegistry, dispatcher, clusterSettings, sharedGroupFactory);
    }

    @Override
    public void incomingRequest(HttpRequest httpRequest, HttpChannel httpChannel) {
//        final HttpPipelinedRequest pipelinedRequest = (HttpPipelinedRequest) httpRequest;
//        final HttpPipelinedRequest copyPipelinedRequest =
//                new HttpPipelinedRequest(pipelinedRequest.getSequence()
//                        , pipelinedRequest.getDelegateRequest().releaseAndCopy());

        super.incomingRequest(httpRequest, httpChannel);
    }

    @Override
    public ChannelHandler configureServerChannelHandler() {
        return new NonSslHttpChannelHandler(this, handlingSettings);
    }

    protected class NonSslHttpChannelHandler extends Netty4HttpServerTransport.HttpChannelHandler {
        
        protected NonSslHttpChannelHandler(Netty4HttpServerTransport transport, final HttpHandlingSettings handlingSettings) {
            super(transport, handlingSettings);
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);
        }
    }
}
