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

import com.floragunn.searchguard.ssl.http.AttributedHttpRequest;
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
        dispatch(request, runnable);
    }

    @Override
    public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
        Runnable runnable = () -> original.dispatchBadRequest(channel, threadContext, cause);
        dispatch(channel.request(), runnable);
    }

    private void dispatch(RestRequest request, Runnable runnable) {
        HttpRequest httpRequest = request.getHttpRequest();
        if (httpRequest instanceof AttributedHttpRequest attributedHttpRequest) {
            EventLoop eventLoop = attributedHttpRequest.getEventLoop();
            if (eventLoop.inEventLoop()) {
                // already in the correct thread
                runnable.run();
            } else {
                Runnable runnableWithContext = threadPool.getThreadContext().preserveContext(runnable);
                eventLoop.execute(runnableWithContext);
            }
        } else {
            assert false : "Expected AttributedHttpRequest but got " + httpRequest.getClass();
            log.error("Netty event loop not present, invalid type of request '{}'", httpRequest);
            runnable.run();
        }
    }
}
