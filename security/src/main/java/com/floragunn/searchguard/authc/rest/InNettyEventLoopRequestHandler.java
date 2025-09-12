package com.floragunn.searchguard.authc.rest;

import com.floragunn.searchsupport.rest.AttributedHttpRequest;
import io.netty.channel.EventLoop;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.threadpool.ThreadPool;

import static com.floragunn.searchsupport.rest.AttributedHttpRequest.ATTRIBUTE_EVENT_LOOP;

public class InNettyEventLoopRequestHandler {

    private static final Logger log = LogManager.getLogger(InNettyEventLoopRequestHandler.class);

    public static void handleInNettyEventLoop(RestRequest request, Runnable requestHandler, ThreadPool threadPool) {
        HttpRequest httpRequest = request.getHttpRequest();
        if (httpRequest instanceof AttributedHttpRequest attributedHttpRequest) {
            EventLoop eventLoop = getEventLoop(attributedHttpRequest);
            if (eventLoop != null) {
                if (eventLoop.inEventLoop()) {
                    // already in the correct thread
                    requestHandler.run();
                } else {
                    Runnable runnableWithContext = threadPool.getThreadContext().preserveContext(requestHandler);
                    eventLoop.execute(runnableWithContext);
                }
            } else {
                log.error("Netty event loop not present, request '{}'", request);
                assert false : "Netty event loop not present, cannot use correct thread";
                requestHandler.run();
            }
        } else {
            assert false : "Expected AttributedHttpRequest but got " + httpRequest.getClass();
            log.error("Netty event loop not present, invalid type of request '{}'", httpRequest);
            requestHandler.run();
        }
    }

    private static EventLoop getEventLoop(AttributedHttpRequest request) {
        return request.getAttribute(ATTRIBUTE_EVENT_LOOP) //
                .filter(EventLoop.class::isInstance) //
                .map(EventLoop.class::cast) //
                .orElse(null);
    }

}
