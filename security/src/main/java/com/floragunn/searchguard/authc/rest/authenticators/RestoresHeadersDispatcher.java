package com.floragunn.searchguard.authc.rest.authenticators;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;

import java.util.Objects;
import java.util.function.Supplier;

public class RestoresHeadersDispatcher implements HttpServerTransport.Dispatcher {

    private final HttpServerTransport.Dispatcher originalDispatcher;
    private final Supplier<ImmutableMap<String, Object>> contextSupplier;

    public RestoresHeadersDispatcher(HttpServerTransport.Dispatcher originalDispatcher, Supplier<ImmutableMap<String, Object>> contextSupplier) {
        this.originalDispatcher = Objects.requireNonNull(originalDispatcher, "Original dispatcher must not be null");
        this.contextSupplier = Objects.requireNonNull(contextSupplier, "Context to restore must not be null");
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
        ImmutableMap<String, Object> contextToRestore = contextSupplier.get();
        contextToRestore.forEach(threadContext::putTransient);
    }
}
