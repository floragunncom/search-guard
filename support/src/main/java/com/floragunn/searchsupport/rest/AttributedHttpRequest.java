package com.floragunn.searchsupport.rest;

import com.floragunn.fluent.collections.ImmutableMap;
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
import java.util.Optional;

public class AttributedHttpRequest implements HttpRequest {

    public static final String ATTRIBUTE_EVENT_LOOP = "sg_event_loop";

    private final HttpRequest httpRequest;

    private ImmutableMap<String, Object> attributes;


    private AttributedHttpRequest(HttpRequest httpRequest, ImmutableMap<String, Object> attributes) {
        // TODO provide type safe attribute implementation
        this.httpRequest = Objects.requireNonNull(httpRequest, "Http request is required.");
        this.attributes = Objects.requireNonNull(attributes, "Request attributes are required");
    }

    public static AttributedHttpRequest create(HttpRequest httpRequest, ImmutableMap<String, Object> attributes) {
        Objects.requireNonNull(httpRequest, "Http request is required.");
        Objects.requireNonNull(attributes, "Request attributes are required");
        if(httpRequest instanceof AttributedHttpRequest request) {
            ImmutableMap<String, Object> commonAttributes = request.attributes.with(attributes);
            return request.withAttributes(commonAttributes);
        }
        return new AttributedHttpRequest(httpRequest, attributes);
    }

    private AttributedHttpRequest withAttributes(ImmutableMap<String, Object> commonAttributes) {
        return new AttributedHttpRequest(this.httpRequest, commonAttributes);
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
    public HttpBody body() {
        return httpRequest.body();
    }

    @Override
    public void setBody(HttpBody body) {
        httpRequest.setBody(body);
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
    public boolean hasContent() {
        return httpRequest.hasContent();
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