/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchsupport.diag;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.ThreadContext.StoredContext;
import org.opensearch.tasks.Task;

public final class DiagnosticContext {
    private static final Logger log = LogManager.getLogger(DiagnosticContext.class);

    public static final Setting<Boolean> ACTION_STACK_ENABLED = Setting.boolSetting("searchguard.diagnosis.action_stack.enabled", false,
            Property.NodeScope);

    public static final Setting<Boolean> ADD_EXTENDED_HEADERS_TO_LOG_CONTEXT = Setting.boolSetting("searchguard.logging.context.extended", false,
            Property.NodeScope);

    public static final List<Setting<?>> SETTINGS = Arrays.asList(ACTION_STACK_ENABLED, ADD_EXTENDED_HEADERS_TO_LOG_CONTEXT);

    public static final String ACTION_STACK_HEADER = "x_action_stack";
    public static final String ACTION_STACK_EXTENSION_TRANSIENT = "x_action_stack_ext";
    public static final Collection<String> CLEAR_TRANSIENT_HEADERS_ON_APPLY = Arrays.asList(DiagnosticContext.ACTION_STACK_EXTENSION_TRANSIENT);

    private final boolean trackActionStack;
    private final boolean addExtendedHeadersToLogContext;
    private final ThreadContext threadContext;

    public DiagnosticContext(Settings settings, ThreadContext threadContext) {
        this.trackActionStack = ACTION_STACK_ENABLED.get(settings);
        this.addExtendedHeadersToLogContext = ADD_EXTENDED_HEADERS_TO_LOG_CONTEXT.get(settings);
        this.threadContext = threadContext;
    }

    public void traceActionStack(String action) {
        if (!trackActionStack || action == null || action.length() == 0) {
            return;
        }

        String currentActionStack = getActionStack();

        traceActionStack(currentActionStack, action);
    }

    public void traceActionStack(String currentActionStack, String action) {
        if (!trackActionStack || action == null || action.length() == 0) {
            return;
        }

        String newActionStack = pushToActionStack(currentActionStack, action);

        if (threadContext.getTransient(ACTION_STACK_EXTENSION_TRANSIENT) == null) {
            threadContext.putTransient(ACTION_STACK_EXTENSION_TRANSIENT, newActionStack);
        } else {
            log.error("Could not set new action stack to ThreadContext. Make sure you pushed the context before. currentActionStack: "
                    + currentActionStack + "; action: " + action, new Exception());
        }

        org.apache.logging.log4j.ThreadContext.put("action_stack", newActionStack);
    }

    public Handle pushActionStack(String action) {
        if (!trackActionStack || action == null || action.length() == 0) {
            return NOP_CLOSEABLE;
        }

        String currentActionStack = getActionStack();

        StoredContext ctx = threadContext.newStoredContext(true, DiagnosticContext.CLEAR_TRANSIENT_HEADERS_ON_APPLY);

        try {

            String newActionStack = pushToActionStack(currentActionStack, action);

            threadContext.putTransient(ACTION_STACK_EXTENSION_TRANSIENT, newActionStack);
            org.apache.logging.log4j.ThreadContext.put("action_stack", newActionStack);

            return new Handle() {

                @Override
                public void close() {
                    ctx.close();
                    org.apache.logging.log4j.ThreadContext.put("action_stack", currentActionStack);
                }
            };
        } catch (RuntimeException e) {
            ctx.close();
            throw e;
        }
    }

    public String getActionStack() {
        return getActionStack(threadContext);
    }

    public void addHeadersToLogContext(ClusterService clusterService, ThreadContext threadContext) {
        if (!addExtendedHeadersToLogContext) {
            return;
        }

        ClusterName clusterName = clusterService.getClusterName();
        
        if (clusterName != null) {
            org.apache.logging.log4j.ThreadContext.put("cluster_name", clusterName.value());
        }
        
        org.apache.logging.log4j.ThreadContext.put("node_name", clusterService.getNodeName());        
        org.apache.logging.log4j.ThreadContext.put("sg_origin", threadContext.getTransient("_sg_origin"));
        org.apache.logging.log4j.ThreadContext.put("sg_channel_type", threadContext.getTransient("_sg_channel_type"));
    }

    private String pushToActionStack(String currentActionStack, String action) {
        if (currentActionStack != null) {
            return currentActionStack + " > " + action;
        } else {
            return action;
        }
    }

    private final static Handle NOP_CLOSEABLE = new Handle() {

    };

    public static class Handle implements AutoCloseable {

        public void close() {

        }

    }

    public ActionTraceFilter getActionTraceFilter() {
        if (trackActionStack) {
            return new ActionTraceFilter();
        } else {
            return null;
        }
    }

    public static void fixupLoggingContext(ThreadContext threadContext) {
        org.apache.logging.log4j.ThreadContext.put("action_stack", getActionStack(threadContext));
    }

    public static String getActionStack(ThreadContext threadContext) {
        String currentActionStack = threadContext.getTransient(ACTION_STACK_EXTENSION_TRANSIENT);

        if (currentActionStack != null) {
            return currentActionStack;
        } else {
            return threadContext.getHeader(ACTION_STACK_HEADER);
        }
    }

    private class ActionTraceFilter implements ActionFilter {

        @Override
        public int order() {
            return 0;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task, String action, Request request,
                ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
            String actionStack = getActionStack();
            LogContextPreservingActionListener<Response> wrappedListener = LogContextPreservingActionListener.wrapPreservingContext(listener,
                    threadContext);

            try (StoredContext ctx = threadContext.newStoredContext(true, DiagnosticContext.CLEAR_TRANSIENT_HEADERS_ON_APPLY)) {
                traceActionStack(actionStack, action);

                chain.proceed(task, action, request, wrappedListener);
            }
        }
    }
}
