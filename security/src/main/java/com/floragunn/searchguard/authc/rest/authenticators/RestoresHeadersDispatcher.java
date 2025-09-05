package com.floragunn.searchguard.authc.rest.authenticators;

import com.floragunn.fluent.collections.ImmutableSet;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;

import java.util.Objects;

public class RestoresHeadersDispatcher implements HttpServerTransport.Dispatcher {

    private final HttpServerTransport.Dispatcher originalDispatcher;
    private final ImmutableSet<String> headersToRestore;

    public RestoresHeadersDispatcher(HttpServerTransport.Dispatcher originalDispatcher, ImmutableSet<String> headersToRestore) {
        this.originalDispatcher = Objects.requireNonNull(originalDispatcher, "Original dispatcher must not be null");
        this.headersToRestore = Objects.requireNonNull(headersToRestore, "Headers to restore must not be null");
    }

    @Override
    public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
        restoreHeaders(threadContext);
        originalDispatcher.dispatchRequest(request, channel, threadContext);
    }

    @Override
    public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
        restoreHeaders(threadContext);
        originalDispatcher.dispatchBadRequest(channel, threadContext, cause);
    }

    private void restoreHeaders(ThreadContext threadContext) {
        for (String header : headersToRestore) {
            Object value = threadContext.getTransient(header);
            if (value != null) {
                threadContext.putTransient(header, value);
            }
        }
    }
}
