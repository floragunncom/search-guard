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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import com.floragunn.searchsupport.rest.AttributedHttpRequest;
import io.netty.handler.ssl.SslHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.http.netty4.Netty4HttpChannel;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import com.floragunn.codova.documents.ContentType;
import com.floragunn.codova.documents.Format.UnknownDocTypeException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.ssl.SearchGuardKeyStore;
import com.floragunn.searchguard.ssl.SslExceptionHandler;
import com.floragunn.searchguard.ssl.http.netty.SearchGuardSSLNettyHttpServerTransport;

public class SearchGuardHttpServerTransport extends SearchGuardSSLNettyHttpServerTransport {
    private static final Logger log = LogManager.getLogger(SearchGuardHttpServerTransport.class);

    public SearchGuardHttpServerTransport(final Settings settings, final NetworkService networkService,
                                          final ThreadPool threadPool, final SearchGuardKeyStore sgks, final SslExceptionHandler sslExceptionHandler,
                                          final NamedXContentRegistry namedXContentRegistry, final Dispatcher dispatcher, ClusterSettings clusterSettings,
                                          SharedGroupFactory sharedGroupFactory, Tracer tracer, BiConsumer<HttpPreRequest, ThreadContext> perRequestThreadContext) {
        super(settings, networkService, threadPool, sgks, namedXContentRegistry, dispatcher, clusterSettings, sharedGroupFactory, sslExceptionHandler, tracer, perRequestThreadContext);
    }

    @Override
    public void incomingRequest(HttpRequest httpRequest, HttpChannel httpChannel) {
        final SslHandler sslhandler = (SslHandler) ((Netty4HttpChannel) httpChannel).getNettyChannel().pipeline().get("ssl_http");
        ImmutableMap<String, Object> attributes = ImmutableMap.of("sg_ssl_handler", sslhandler);
        HttpRequest fixedRequest = fixNonStandardContentType(httpRequest);
        super.incomingRequest(AttributedHttpRequest.create(fixedRequest, attributes), httpChannel);
    }

    /**
     * Elasticsearch has normally a very limited choice of allowed Content-Type headers in requests. In order to support
     * any Content-Type header in our REST APIs, we preempt those requests here and save the original Content-Type in 
     * X-SG-Original-Content-Type and set Content-Type to a supported header.
     */
    private HttpRequest fixNonStandardContentType(HttpRequest httpRequest) {
        try {

            BytesReference content = httpRequest.content();

            if (content == null || content.length() == 0) {
                return httpRequest;
            }

            Map<String, List<String>> headers = httpRequest.getHeaders();

            List<String> contentTypeHeader = headers.get("Content-Type");

            if (contentTypeHeader == null || contentTypeHeader.size() != 1) {
                return httpRequest;
            }

            if (RestRequest.parseContentType(contentTypeHeader) != null) {
                return httpRequest;
            }

            ContentType contentType = ContentType.parseHeader(contentTypeHeader.get(0));
            Map<String, List<String>> modifiedHeaders = ImmutableMap.of(headers, "Content-Type",
                    Collections.singletonList(contentType.getFormat().getMediaType()), "X-SG-Original-Content-Type", contentTypeHeader);

            return new HttpRequest() {

                @Override
                public String uri() {
                    return httpRequest.uri();
                }

                @Override
                public List<String> strictCookies() {
                    return httpRequest.strictCookies();
                }

                @Override
                public HttpRequest removeHeader(String header) {
                    return httpRequest.removeHeader(header);
                }

                @Override
                public HttpRequest releaseAndCopy() {
                    return httpRequest.releaseAndCopy();
                }

                @Override
                public void release() {
                    httpRequest.release();
                }

                @Override
                public HttpVersion protocolVersion() {
                    return httpRequest.protocolVersion();
                }

                @Override
                public Method method() {
                    return httpRequest.method();
                }

                @Override
                public Exception getInboundException() {
                    return httpRequest.getInboundException();
                }

                @Override
                public Map<String, List<String>> getHeaders() {
                    return modifiedHeaders;
                }

                @Override
                public HttpResponse createResponse(RestStatus status, BytesReference content) {
                    return httpRequest.createResponse(status, content);
                }

                @Override
                public HttpResponse createResponse(RestStatus status, ChunkedRestResponseBodyPart content) {
                    return httpRequest.createResponse(status, content);
                }

                @Override
                public BytesReference content() {
                    return httpRequest.content();
                }
            };

        } catch (UnknownDocTypeException e) {
            log.debug("Unknown content type", e);
            return httpRequest;
        } catch (Exception e) {
            log.error("Error in fixNonStandardContentType(" + httpRequest + ")", e);
            return httpRequest;
        }
    }
}
