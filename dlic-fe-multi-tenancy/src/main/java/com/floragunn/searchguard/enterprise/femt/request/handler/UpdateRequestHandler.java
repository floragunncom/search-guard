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
import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import static com.floragunn.searchguard.enterprise.femt.PrivilegesInterceptorImpl.SG_FILTER_LEVEL_FEMT_DONE;

public class UpdateRequestHandler extends RequestHandler<UpdateRequest> {

    private final Client nodeClient;
    private final ThreadContext threadContext;
    public UpdateRequestHandler(Logger log, Client nodeClient, ThreadContext threadContext) {
        super(log);
        this.nodeClient = nodeClient;
        this.threadContext = threadContext;
    }

    @Override
    public SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, UpdateRequest request, ActionListener<?> listener) {
        threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, request.toString());

        try (ThreadContext.StoredContext storedContext = threadContext.newStoredContext()) {
            String newId = RequestResponseTenantData.scopeIdIfNeeded(request.id(), requestedTenant);
            request.id(newId);
            nodeClient.update(request, new ActionListener<>() {

                @Override
                public void onResponse(UpdateResponse updateResponse) {
                    try {
                        storedContext.restore();
                        @SuppressWarnings("unchecked")
                        ActionListener<UpdateResponse> updateListener = (ActionListener<UpdateResponse>) listener;
                        updateListener.onResponse(handleUpdateResponse(updateResponse));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    log.debug("Interception of update request failed", e);
                    listener.onFailure(e);
                }
            });
            return SyncAuthorizationFilter.Result.INTERCEPTED;
        }
    }

}
