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

import com.floragunn.searchguard.internalauthtoken.InternalAuthTokenProvider;
import com.floragunn.searchsupport.diag.DiagnosticContext;
import com.floragunn.searchsupport.diag.LogContextPreservingActionListener;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;

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
        LogContextPreservingActionListener<Response> wrappedListener = LogContextPreservingActionListener.wrapPreservingContext(listener,
                threadContext);
        String actionStack = DiagnosticContext.getActionStack(threadContext);
        Object user = threadContext.getTransient(ConfigConstants.SG_USER);
        Object remoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
        Object origin = threadContext.getTransient(ConfigConstants.SG_ORIGIN);

        try (StoredContext ctx = threadContext.stashContext()) {
            threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
            threadContext.putHeader(InternalAuthTokenProvider.TOKEN_HEADER, "");
            threadContext.putHeader(InternalAuthTokenProvider.AUDIENCE_HEADER, "");

            if (user != null) {
                threadContext.putTransient(ConfigConstants.SG_USER, user);
            }

            if (remoteAddress != null) {
                threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);
            }

            if (origin != null) {
                threadContext.putTransient(ConfigConstants.SG_ORIGIN, origin);
            }

            if (actionStack != null) {
                threadContext.putHeader(DiagnosticContext.ACTION_STACK_HEADER, actionStack);
                DiagnosticContext.fixupLoggingContext(threadContext);
            }

            super.doExecute(action, request, wrappedListener);
        }
    }

}
