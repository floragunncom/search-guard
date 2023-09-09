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

import java.util.List;

import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpHandlingSettings;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.http.netty4.internal.HttpValidator;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.SharedGroupFactory;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

public class SearchGuardNonSslHttpServerTransport extends Netty4HttpServerTransport {

    public SearchGuardNonSslHttpServerTransport(Settings settings, NetworkService networkService, BigArrays bigArrays, ThreadPool threadPool, NamedXContentRegistry xContentRegistry, Dispatcher dispatcher, ClusterSettings clusterSettings, SharedGroupFactory sharedGroupFactory, HttpValidator httpValidator) {
        super(settings, networkService, bigArrays, threadPool, xContentRegistry, dispatcher, clusterSettings, sharedGroupFactory, httpValidator);
    }

    @Override
    public ChannelHandler configureServerChannelHandler() {
        return new NonSslHttpChannelHandler(this, handlingSettings);
    }

    @Override
    protected void populatePerRequestThreadContext(RestRequest restRequest, ThreadContext threadContext) {
        for(String headerName: restRequest.getHeaders().keySet()) {
            final List<String> headerValues = restRequest.getHeaders().get(headerName);

            if (headerValues != null && !headerValues.isEmpty()) {
                threadContext.putHeader(headerName, String.join(",", headerValues));
            }
        }
    }

    protected class NonSslHttpChannelHandler extends Netty4HttpServerTransport.HttpChannelHandler {
        
        protected NonSslHttpChannelHandler(Netty4HttpServerTransport transport, final HttpHandlingSettings handlingSettings) {
            super(transport, handlingSettings, null);
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);
        }
    }
}
