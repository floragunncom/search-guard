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

package com.floragunn.searchguard.ssl.http;

import io.netty.handler.ssl.SslHandler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AttributedHttpRequest implements HttpRequest {

    private final HttpRequest httpRequest;

    private final SslHandler sslHandler;


    private AttributedHttpRequest(HttpRequest httpRequest, SslHandler sslHandler) {
        this.httpRequest = Objects.requireNonNull(httpRequest, "Http request is required.");
        this.sslHandler = sslHandler;
    }

    public static AttributedHttpRequest create(HttpRequest httpRequest, SslHandler sslhandler) {
        Objects.requireNonNull(httpRequest, "Http request is required.");
        if(httpRequest instanceof AttributedHttpRequest request) {
            return request.withSslHandler(sslhandler);
        }
        return new AttributedHttpRequest(httpRequest, sslhandler);
    }

    private AttributedHttpRequest withSslHandler(SslHandler sslhandler) {
        return new AttributedHttpRequest(this.httpRequest, sslhandler);
    }

    public SslHandler getSslHandler() {
        return sslHandler;
    }

    @Override
    public String uri() {
        return httpRequest.uri();
    }

    @Override
    public HttpBody body() {
        return httpRequest.body();
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
    public void release() {
        httpRequest.release();
    }

    @Override
    public HttpVersion protocolVersion() {
        return httpRequest.protocolVersion();
    }

    @Override
    public RestRequest.Method method() {
        return httpRequest.method();
    }

    @Override
    public Exception getInboundException() {
        return httpRequest.getInboundException();
    }

    @Override
    public Map<String, List<String>> getHeaders() {
        return httpRequest.getHeaders();
    }

    @Override
    public HttpResponse createResponse(RestStatus status, BytesReference content) {
        return httpRequest.createResponse(status, content);
    }

    @Override
    public HttpResponse createResponse(RestStatus status, ChunkedRestResponseBodyPart content) {
        return httpRequest.createResponse(status, content);
    }

}