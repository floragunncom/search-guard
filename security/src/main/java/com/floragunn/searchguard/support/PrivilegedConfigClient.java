/*
 * Copyright 2020-2022 floragunn GmbH
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

package com.floragunn.searchguard.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;

import com.floragunn.searchguard.internalauthtoken.InternalAuthTokenProvider;
import com.floragunn.searchsupport.diag.DiagnosticContext;
import com.floragunn.searchsupport.diag.LogContextPreservingActionListener;

public final class PrivilegedConfigClient extends FilterClient {
    
    private PrivilegedConfigClient(Client in) {
        super(in);
    }
    
    public static PrivilegedConfigClient adapt(Client client) {
        if (client instanceof PrivilegedConfigClient) {
            return (PrivilegedConfigClient) client;
        } else {
            return new PrivilegedConfigClient(client);
        }
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action, Request request,
                                                                                              ActionListener<Response> listener) {
        ThreadContext threadContext = threadPool().getThreadContext();
        LogContextPreservingActionListener<Response> wrappedListener = LogContextPreservingActionListener.wrapPreservingContext(listener, threadContext);
        String actionStack = DiagnosticContext.getActionStack(threadContext);
        try (StoredContext ctx = PrivilegedConfigContext.initPrivilegedContext(threadContext)) {
            threadContext.putHeader(InternalAuthTokenProvider.TOKEN_HEADER, "");
            threadContext.putHeader(InternalAuthTokenProvider.AUDIENCE_HEADER, "");

            if (actionStack != null) {
                threadContext.putHeader(DiagnosticContext.ACTION_STACK_HEADER, actionStack);
                DiagnosticContext.fixupLoggingContext(threadContext);
            }
            
            super.doExecute(action, request, wrappedListener);
        }
    }

}
