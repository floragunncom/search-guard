/*
 * Includes code from the following Apache 2 licensed files from Elasticsearch 7.10.2:
 * 
 * - /server/src/main/java/org/elasticsearch/action/support/ContextPreservingActionListener.java
 *    https://github.com/elastic/elasticsearch/blob/7.10/server/src/main/java/org/elasticsearch/action/support/ContextPreservingActionListener.java
 * 
 * Original license notice for all of the noted files:
 *
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.floragunn.searchsupport.diag;

import java.util.Map;
import java.util.function.Supplier;

import org.apache.logging.log4j.CloseableThreadContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;

/**
 * Restores the given {@link org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext} and the given Log4j thread context
 * once the listener is invoked
 */
public final class LogContextPreservingActionListener<R> implements ActionListener<R> {

    private final ActionListener<R> delegate;
    private final Supplier<ThreadContext.StoredContext> context;
    private final Map<String, String> logThreadContextMap;

    public LogContextPreservingActionListener(Supplier<ThreadContext.StoredContext> contextSupplier, Map<String, String> logThreadContextMap,
            ActionListener<R> delegate) {
        this.delegate = delegate;
        this.context = contextSupplier;
        this.logThreadContextMap = logThreadContextMap;
    }

    @Override
    public void onResponse(R r) {
        try (ThreadContext.StoredContext ignore = context.get();
                CloseableThreadContext.Instance ctc = CloseableThreadContext.putAll(logThreadContextMap)) {
            delegate.onResponse(r);
        }
    }

    @Override
    public void onFailure(Exception e) {
        try (ThreadContext.StoredContext ignore = context.get();
                CloseableThreadContext.Instance ctc = CloseableThreadContext.putAll(logThreadContextMap)) {
            delegate.onFailure(e);
        }
    }

    @Override
    public String toString() {
        return getClass().getName() + "/" + delegate.toString();
    }

    /**
     * Wraps the provided action listener in a {@link ContextPreservingActionListener} that will
     * also copy the response headers when the {@link ThreadContext.StoredContext} is closed
     */
    public static <R> LogContextPreservingActionListener<R> wrapPreservingContext(ActionListener<R> listener, ThreadContext threadContext) {
        return new LogContextPreservingActionListener<>(threadContext.newRestorableContext(true),
                org.apache.logging.log4j.ThreadContext.getImmutableContext(), listener);
    }
}
