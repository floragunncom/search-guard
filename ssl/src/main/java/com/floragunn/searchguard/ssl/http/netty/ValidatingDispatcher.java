/*
 * Copyright 2017 floragunn GmbH
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

import java.nio.file.Path;

import javax.net.ssl.SSLPeerUnverifiedException;

import com.floragunn.searchguard.ssl.http.AttributedHttpRequest;
import io.netty.channel.EventLoop;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpServerTransport.Dispatcher;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.searchguard.ssl.SslExceptionHandler;
import com.floragunn.searchguard.ssl.util.ExceptionUtils;
import com.floragunn.searchguard.ssl.util.SSLRequestHelper;
import org.elasticsearch.threadpool.ThreadPool;

public class ValidatingDispatcher implements Dispatcher {

    private static final Logger logger = LogManager.getLogger(ValidatingDispatcher.class);

    private final ThreadContext threadContext;
    private final Dispatcher originalDispatcher;
    private final SslExceptionHandler errorHandler;
    private final Settings settings;
    private final Path configPath;
    private final ThreadPool threadPool;

    public ValidatingDispatcher(final ThreadContext threadContext, final Dispatcher originalDispatcher, 
            final Settings settings, final Path configPath, final SslExceptionHandler errorHandler, ThreadPool threadPool) {
        super();
        this.threadContext = threadContext;
        this.originalDispatcher = originalDispatcher;
        this.settings = settings;
        this.configPath = configPath;
        this.errorHandler = errorHandler;
        this.threadPool = threadPool;
    }

    @Override
    public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
        checkRequest(request, channel);
        HttpRequest httpRequest = request.getHttpRequest();
        if(httpRequest instanceof AttributedHttpRequest attributedHttpRequest) {
            EventLoop eventLoop = attributedHttpRequest.getEventLoop();

            Runnable runnableWithContext = threadPool.getThreadContext()
                    .preserveContext(() -> originalDispatcher.dispatchRequest(request, channel, threadContext));
            eventLoop.execute(runnableWithContext);
        } else {
            assert false : "Expected AttributedHttpRequest but got " + httpRequest.getClass();
            logger.error("Netty event loop not present, invalid type of request '{}'", httpRequest);
        }
    }

    @Override
    public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
        checkRequest(channel.request(), channel);
        originalDispatcher.dispatchBadRequest(channel, threadContext, cause);
    }
    
    protected void checkRequest(final RestRequest request, final RestChannel channel) {
        
        if(SSLRequestHelper.containsBadHeader(threadContext, "_sg_ssl_")) {
            final ElasticsearchException exception = ExceptionUtils.createBadHeaderException();
            errorHandler.logError(exception, request, 1);
            throw exception;
        }
        
        try {
            if(SSLRequestHelper.getSSLInfo(settings, configPath, request, null) == null) {
                logger.error("Not an SSL request");
                throw new ElasticsearchSecurityException("Not an SSL request", RestStatus.INTERNAL_SERVER_ERROR);
            }
        } catch (SSLPeerUnverifiedException e) {
            logger.error("No client certificates found but such are needed (SG 8).");
            errorHandler.logError(e, request, 0);
            throw ExceptionsHelper.convertToElastic(e);
        }
    }
}
