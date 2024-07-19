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
import com.floragunn.searchguard.enterprise.femt.request.mapper.MultiGetMapper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Objects;

import static com.floragunn.searchguard.enterprise.femt.MultiTenancyAuthorizationFilter.SG_FILTER_LEVEL_FEMT_DONE;

public class MultiGetRequestHandler extends RequestHandler<MultiGetRequest> {

    private final Client nodeClient;
    private final ThreadContext threadContext;
    private final MultiGetMapper multiGetMapper;

    public MultiGetRequestHandler(Client nodeClient, ThreadContext threadContext, MultiGetMapper multiGetMapper) {
        this.nodeClient = Objects.requireNonNull(nodeClient, "nodeClient is required");
        this.threadContext = Objects.requireNonNull(threadContext, "threadContext is required");
        this.multiGetMapper = Objects.requireNonNull(multiGetMapper, "multiGetMapper is required");
    }

    @Override
    public SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, MultiGetRequest request, ActionListener<?> listener) {
        log.debug("Handle multi get request");

        try (ThreadContext.StoredContext storedContext = threadContext.newStoredContext()) {
            threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, request.toString());
            MultiGetRequest scopedRequest = multiGetMapper.toScopedMultiGetRequest(request, requestedTenant);

            var listenerWrapper = new TenantScopedActionListenerWrapper<>(listener, storedContext, multiGetMapper);

            nodeClient.multiGet(scopedRequest, listenerWrapper);

            return SyncAuthorizationFilter.Result.INTERCEPTED;
        }
    }


}
