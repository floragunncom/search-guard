/*
 * Copyright 2025 floragunn GmbH
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
package com.floragunn.searchguard.authc.rest;

import com.floragunn.searchsupport.rest.AttributedHttpRequest;
import io.netty.channel.EventLoop;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpServerTransport.Dispatcher;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;

import static com.floragunn.searchsupport.rest.AttributedHttpRequest.ATTRIBUTE_EVENT_LOOP;

class ExecuteInNettyEventLoopDispatcher implements Dispatcher {

    private static final Logger log = LogManager.getLogger(ExecuteInNettyEventLoopDispatcher.class);

    private final Dispatcher original;
    private final ThreadPool threadPool;

    public ExecuteInNettyEventLoopDispatcher(Dispatcher originalDispatcher, ThreadPool threadPool) {
        this.original = Objects.requireNonNull(originalDispatcher, "originalDispatcher must not be null");
        this.threadPool = Objects.requireNonNull(threadPool, "threadPool must not be null");
    }

    @Override
    public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
        Runnable runnable = () -> original.dispatchRequest(request, channel, threadContext);
        InNettyEventLoopRequestHandler.handleInNettyEventLoop(request, runnable, threadPool);
    }

    @Override
    public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
        Runnable runnable = () -> original.dispatchBadRequest(channel, threadContext, cause);
        InNettyEventLoopRequestHandler.handleInNettyEventLoop(channel.request(), runnable, threadPool);
    }
}
