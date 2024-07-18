package com.floragunn.searchguard.authc.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * This class should be removed together with legacy code in the legacy-security module.
 * @see <a href="https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/issues/352">Issue 352</a>
 */
@Deprecated
class SendOnceRestChannelWrapper implements RestChannel {
    private static final Logger log = LogManager.getLogger(SendOnceRestChannelWrapper.class);

    private final RestChannel delegate;
    private volatile boolean sent = false;

    SendOnceRestChannelWrapper(RestChannel delegate) {
        this.delegate = Objects.requireNonNull(delegate, "Rest channel delegate must not be null");
    }

    @Override
    public XContentBuilder newBuilder() throws IOException {
        return delegate.newBuilder();
    }

    @Override
    public XContentBuilder newErrorBuilder() throws IOException {
        return delegate.newErrorBuilder();
    }

    @Override
    public XContentBuilder newBuilder(XContentType xContentType, boolean useFiltering) throws IOException {
        return delegate.newBuilder(xContentType, useFiltering);
    }

    @Override
    public XContentBuilder newBuilder(XContentType xContentType, XContentType responseContentType, boolean useFiltering)
        throws IOException {
        return delegate.newBuilder(xContentType, responseContentType, useFiltering);
    }

    @Override
    public XContentBuilder newBuilder(
        XContentType xContentType, XContentType responseContentType, boolean useFiltering, OutputStream out) throws IOException {
        return delegate.newBuilder(xContentType, responseContentType, useFiltering, out);
    }

    @Override
    public BytesStream bytesOutput() {
        return delegate.bytesOutput();
    }

    @Override
    public void releaseOutputBuffer() {
        delegate.releaseOutputBuffer();
    }

    @Override
    public RestRequest request() {
        return delegate.request();
    }

    @Override
    public boolean detailedErrorsEnabled() {
        return delegate.detailedErrorsEnabled();
    }

    @Override
    public void sendResponse(RestResponse response) {
        if (sent) {
            RestRequest request = request();
            long requestId = request.getRequestId();
            String spanId = request.getSpanId();
            String path = request.path();
            Method method = request.method();
            log.debug(
                "Rest response related to request '{} {}' ( with id '{}', spanId '{}') has already been sent",
                method,
                path,
                requestId,
                spanId);
        } else {
            sent = true;
            delegate.sendResponse(response);
        }
    }
}
