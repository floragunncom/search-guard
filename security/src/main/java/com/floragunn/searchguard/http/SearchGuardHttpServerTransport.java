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

import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.SharedGroupFactory;

import com.floragunn.searchguard.ssl.SearchGuardKeyStore;
import com.floragunn.searchguard.ssl.SslExceptionHandler;
import com.floragunn.searchguard.ssl.http.netty.SearchGuardSSLNettyHttpServerTransport;
import com.floragunn.searchguard.ssl.http.netty.ValidatingDispatcher;

public class SearchGuardHttpServerTransport extends SearchGuardSSLNettyHttpServerTransport {

    public SearchGuardHttpServerTransport(final Settings settings, final NetworkService networkService, final BigArrays bigArrays,
            final ThreadPool threadPool, final SearchGuardKeyStore sgks, final SslExceptionHandler sslExceptionHandler,
            final NamedXContentRegistry namedXContentRegistry, final ValidatingDispatcher dispatcher, ClusterSettings clusterSettings,
            SharedGroupFactory sharedGroupFactory) {
        super(settings, networkService, bigArrays, threadPool, sgks, namedXContentRegistry, dispatcher, clusterSettings, sharedGroupFactory, sslExceptionHandler);
    }
}
