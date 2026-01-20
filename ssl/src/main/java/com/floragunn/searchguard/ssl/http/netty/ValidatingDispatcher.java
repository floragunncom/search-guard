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

import java.util.Map;

import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpServerTransport.Dispatcher;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.searchguard.ssl.SslExceptionHandler;
import com.floragunn.searchguard.ssl.util.ExceptionUtils;

public class ValidatingDispatcher implements Dispatcher {

    private static final Logger logger = LogManager.getLogger(ValidatingDispatcher.class);

    private final ThreadContext threadContext;
    private final Dispatcher originalDispatcher;
    private final SslExceptionHandler errorHandler;

    public ValidatingDispatcher(final ThreadContext threadContext, final Dispatcher originalDispatcher, final SslExceptionHandler errorHandler) {
        super();
        this.threadContext = threadContext;
        this.originalDispatcher = originalDispatcher;
        this.errorHandler = errorHandler;
    }

    @Override
    public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
        checkRequest(request, channel);
        originalDispatcher.dispatchRequest(request, channel, threadContext);
    }

    @Override
    public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
        checkRequest(channel.request(), channel);
        originalDispatcher.dispatchBadRequest(channel, threadContext, cause);
    }
    
    protected void checkRequest(final RestRequest request, final RestChannel channel) {
        for (final Map.Entry<String, String> header : threadContext.getHeaders().entrySet()) {
            if (header != null && header.getKey() != null && header.getKey().trim().toLowerCase().startsWith(SSLConfigConstants.SG_SSL_PREFIX)) {
                final ElasticsearchException exception = ExceptionUtils.createBadHeaderException();
                errorHandler.logError(exception, request, 1);
                throw exception;
            }
        }

        if (threadContext.getTransient(SSLConfigConstants.SG_SSL_PEER_CERTIFICATES) == null) {
            logger.error("Not an SSL request");
            throw new ElasticsearchSecurityException("Not an SSL request", RestStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
