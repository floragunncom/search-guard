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
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import static com.floragunn.searchguard.enterprise.femt.PrivilegesInterceptorImpl.SG_FILTER_LEVEL_FEMT_DONE;

public class MultiGetRequestHandler extends RequestHandler<MultiGetRequest> {

    private final Client nodeClient;
    private final ThreadContext threadContext;
    private final MultiGetMapper multiGetMapper;

    public MultiGetRequestHandler(Client nodeClient, ThreadContext threadContext) {
        this.nodeClient = nodeClient;
        this.threadContext = threadContext;
        this.multiGetMapper = new MultiGetMapper();
    }

    @Override
    public SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, MultiGetRequest request, ActionListener<?> listener) {
        log.debug("Handle multi get request");
        threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, request.toString());

        try (ThreadContext.StoredContext storedContext = threadContext.newStoredContext()) {
            MultiGetRequest scopedRequest = multiGetMapper.toScopedMultiGetRequest(request, requestedTenant);

            nodeClient.multiGet(scopedRequest, new ActionListener<>() {
                @Override
                public void onResponse(MultiGetResponse multiGetItemResponses) {
                    try {
                        storedContext.restore();

                        MultiGetResponse response = multiGetMapper.toUnscopedMultiGetResponse(multiGetItemResponses);
                        @SuppressWarnings("unchecked")
                        ActionListener<MultiGetResponse> multiGetListener = (ActionListener<MultiGetResponse>) listener;
                        multiGetListener.onResponse(response);
                    } catch (Exception e) {
                        log.error("An error occurred while handling multi get response", e);
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    log.error("An error occurred while sending multi get request", e);
                    listener.onFailure(e);
                }
            });

            return SyncAuthorizationFilter.Result.INTERCEPTED;
        }
    }


}
