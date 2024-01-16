package com.floragunn.searchguard.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.List;
import java.util.Map;

public class ContextLogger {

    private static final Logger log = LogManager.getLogger(ContextLogger.class);
    public static void logContext(String message, ThreadContext threadContext) {
        Map<String, String> requestHeaders = threadContext.getHeaders();
        Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
        String threadName = Thread.currentThread().getName();
        StringBuilder stringBuilder = new StringBuilder("Current thread ").append(threadName).append("\t").append(message).append("\n")
            .append("\n\tRequest headers: ");
        for(Map.Entry<String, String> entry : requestHeaders.entrySet()) {
            stringBuilder.append("\t\t").append(entry.getKey()).append("=").append(entry.getValue()).append(", ");
        }
        stringBuilder.append(".\n\tResponse headers: ");
        for(Map.Entry<String, List<String>> entry : responseHeaders.entrySet()) {
            stringBuilder.append("\t\t")
                .append(entry.getKey())
                .append("[")
                .append(String.join(", ", entry.getValue()))
                .append("], ");
        }
        Map<String, Object> transientHeaders = threadContext.getTransientHeaders();
        stringBuilder.append("\n\tTransient headers: ");
        for(Map.Entry<String, Object> entry : transientHeaders.entrySet()) {
            stringBuilder.append("\t\t").append(entry.getKey()).append("=").append(entry.getValue());
        }
        log.error(stringBuilder.toString());
    }
}
