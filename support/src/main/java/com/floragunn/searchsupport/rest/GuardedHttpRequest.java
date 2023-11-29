package com.floragunn.searchsupport.rest;

import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.rest.ChunkedRestResponseBody;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class GuardedHttpRequest implements HttpRequest {

    private final HttpRequest httpRequest;

    private ImmutableMap<String, Object> attributes;


    public GuardedHttpRequest(HttpRequest httpRequest, ImmutableMap<String, Object> attributes) {
        // TODO provide type safe attribute implementation
        this.httpRequest = Objects.requireNonNull(httpRequest, "Http request is required.");
        this.attributes = attributes;
    }

    public Optional<Object> getAttribute(String name) {
        Objects.requireNonNull(name, "Attribute name is required.");
        return Optional.ofNullable(attributes.get(name));
    }

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
    public HttpResponse createResponse(RestStatus status, ChunkedRestResponseBody content) {
        return httpRequest.createResponse(status, content);
    }

    @Override
    public BytesReference content() {
        return httpRequest.content();
    }
}