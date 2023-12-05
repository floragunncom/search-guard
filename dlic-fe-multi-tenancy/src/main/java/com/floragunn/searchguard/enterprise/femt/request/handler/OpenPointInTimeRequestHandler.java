/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.searchguard.enterprise.femt.request.handler;

import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.SyncAuthorizationFilter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.OpenPointInTimeAction;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Objects;

import static com.floragunn.searchguard.enterprise.femt.MultiTenancyAuthorizationFilter.SG_FILTER_LEVEL_FEMT_DONE;

class OpenPointInTimeRequestHandler extends RequestHandler<OpenPointInTimeRequest>{

    private final Client nodeClient;
    private final ThreadContext threadContext;

    public OpenPointInTimeRequestHandler(Client nodeClient, ThreadContext threadContext) {
        this.nodeClient = Objects.requireNonNull(nodeClient, "Node client is required");
        this.threadContext = Objects.requireNonNull(threadContext, "Thread context is required");
    }

    @Override
    public SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant,
        OpenPointInTimeRequest request, ActionListener<?> listener) {
        log.debug("Open point in time request");
        try (ThreadContext.StoredContext storedContext = threadContext.newStoredContext()) {
            threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, request.toString());
            nodeClient.execute(OpenPointInTimeAction.INSTANCE, request, (ActionListener<OpenPointInTimeResponse>)listener);
        }
        return SyncAuthorizationFilter.Result.INTERCEPTED;
    }
}
