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
import com.floragunn.searchguard.enterprise.femt.request.mapper.BulkMapper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Objects;

import static com.floragunn.searchguard.enterprise.femt.MultiTenancyAuthorizationFilter.SG_FILTER_LEVEL_FEMT_DONE;

public class BulkRequestHandler extends RequestHandler<BulkRequest> {

    private final Client nodeClient;
    private final ThreadContext threadContext;
    private final BulkMapper bulkMapper;
    public BulkRequestHandler(Client nodeClient, ThreadContext threadContext) {
        this.nodeClient = Objects.requireNonNull(nodeClient, "nodeClient is required");
        this.threadContext = Objects.requireNonNull(threadContext, "threadContext is required");
        this.bulkMapper = new BulkMapper();
    }

    @Override
    public SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, BulkRequest request, ActionListener<?> listener) {
        log.debug("Handle bulk request");
        try (ThreadContext.StoredContext storedContext = threadContext.newStoredContext()) {
            threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, request.toString());
            BulkRequest scoped = bulkMapper.toScopedBulkRequest(request, requestedTenant);

            var listenerWrapper = new TenantScopedActionListenerWrapper<>(listener, storedContext, bulkMapper);

            nodeClient.bulk(scoped, listenerWrapper);

            return SyncAuthorizationFilter.Result.INTERCEPTED;
        }
    }

}
