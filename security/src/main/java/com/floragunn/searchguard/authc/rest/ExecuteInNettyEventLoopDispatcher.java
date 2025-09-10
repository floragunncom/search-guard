package com.floragunn.searchguard.authc.rest;

import com.floragunn.searchsupport.rest.AttributedHttpRequest;
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

import static com.floragunn.searchsupport.rest.AttributedHttpRequest.ATTRIBUTE_EVENT_LOOP;

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
        Runnable dispatch = () -> original.dispatchRequest(request, channel, threadContext);
        dispatch(request, dispatch);
    }

    @Override
    public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
        Runnable dispatch = () -> original.dispatchBadRequest(channel, threadContext, cause);
        dispatch(channel.request(), dispatch);
    }

    private void dispatch(RestRequest request, Runnable runnable) {
        HttpRequest httpRequest = request.getHttpRequest();
        if (httpRequest instanceof AttributedHttpRequest attributedHttpRequest) {
            EventLoop eventLoop = getEventLoop(attributedHttpRequest);
            if (eventLoop != null) {
                if (eventLoop.inEventLoop()) {
                    // already in the correct thread
                    runnable.run();
                } else {
                    Runnable runnableWithContext = threadPool.getThreadContext().preserveContext(runnable);
                    eventLoop.execute(runnableWithContext);
                }
            } else {
                log.error("Netty event loop not present, request '{}'", request);
                assert false : "Netty event loop not present, cannot use correct thread";
                runnable.run();
            }
        } else {
            assert false : "Expected AttributedHttpRequest but got " + httpRequest.getClass();
            log.error("Netty event loop not present, invalid type of request '{}'", httpRequest);
            runnable.run();
        }
    }

    private EventLoop getEventLoop(AttributedHttpRequest request) {
        return request.getAttribute(ATTRIBUTE_EVENT_LOOP) //
                .filter(EventLoop.class::isInstance) //
                .map(EventLoop.class::cast) //
                .orElse(null);
    }
}
