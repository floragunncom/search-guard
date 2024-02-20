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
import com.floragunn.searchguard.enterprise.femt.request.mapper.UpdateByQueryMapper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;

import java.util.Objects;

import static com.floragunn.searchguard.enterprise.femt.MultiTenancyAuthorizationFilter.SG_FILTER_LEVEL_FEMT_DONE;

public class UpdateByQueryRequestHandler extends RequestHandler<UpdateByQueryRequest> {

    private final Client nodeClient;
    private final ThreadContext threadContext;
    private final UpdateByQueryMapper updateByQueryMapper;
    public UpdateByQueryRequestHandler(Client nodeClient, ThreadContext threadContext, UpdateByQueryMapper updateByQueryMapper) {
        this.nodeClient = Objects.requireNonNull(nodeClient, "nodeClient is required");
        this.threadContext = Objects.requireNonNull(threadContext, "threadContext is required");
        this.updateByQueryMapper = Objects.requireNonNull(updateByQueryMapper, "updateByQueryMapper is required");
    }

    @Override
    public SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant,
                                                 UpdateByQueryRequest request, ActionListener<?> listener) {
        log.debug("Handle update by query request");

        try (ThreadContext.StoredContext storedContext = threadContext.newStoredContext()) {
            threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, request.toString());
            UpdateByQueryRequest scoped = updateByQueryMapper.toScopedUpdateByQueryRequest(request, requestedTenant);

            TenantScopedActionListenerWrapper<BulkByScrollResponse> listenerWrapper = new TenantScopedActionListenerWrapper<>(
                    listener,
                    (response) -> storedContext.restore(),
                    updateByQueryMapper::toUnscopedBulkByScrollResponse,
                    (ex) -> {
                        log.error("An error occurred while sending update request", ex);
                        storedContext.restore();
                    }
            );

            nodeClient.execute(UpdateByQueryAction.INSTANCE, scoped, listenerWrapper);

            return SyncAuthorizationFilter.Result.INTERCEPTED;
        }
    }
}
