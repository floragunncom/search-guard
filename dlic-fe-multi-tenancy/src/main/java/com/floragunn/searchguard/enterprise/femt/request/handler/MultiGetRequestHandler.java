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
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.get.GetResult;

import java.util.Arrays;
import java.util.Optional;

import static com.floragunn.searchguard.enterprise.femt.PrivilegesInterceptorImpl.SG_FILTER_LEVEL_FEMT_DONE;

public class MultiGetRequestHandler extends RequestHandler<MultiGetRequest> {

    private final Client nodeClient;
    private final ThreadContext threadContext;
    public MultiGetRequestHandler(Logger log, Client nodeClient, ThreadContext threadContext) {
        super(log);
        this.nodeClient = nodeClient;
        this.threadContext = threadContext;
    }

    @Override
    public SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, MultiGetRequest request, ActionListener<?> listener) {
        threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, request.toString());

        try (ThreadContext.StoredContext storedContext = threadContext.newStoredContext()) {
            MultiGetRequest interceptedRequest = new MultiGetRequest() //
                    .preference(request.preference()) //
                    .realtime(request.realtime()) //
                    .refresh(request.refresh());
            interceptedRequest.setForceSyntheticSource(request.isForceSyntheticSource());

            request.getItems() //
                    .stream() //
                    .map(item -> addTenantScopeToMultiGetItem(item, requestedTenant)) //
                    .forEach(interceptedRequest::add);

            nodeClient.multiGet(interceptedRequest, new ActionListener<>() {
                @Override
                public void onResponse(MultiGetResponse multiGetItemResponses) {
                    try {
                        storedContext.restore();
                        MultiGetItemResponse[] items = Arrays.stream(multiGetItemResponses.getResponses()) //
                                .map(MultiGetRequestHandler.this::unscopeIdInMultiGetResponseItem)
                                .toArray(size -> new MultiGetItemResponse[size]);
                        MultiGetResponse response = new MultiGetResponse(items);
                        @SuppressWarnings("unchecked")
                        ActionListener<MultiGetResponse> multiGetListener = (ActionListener<MultiGetResponse>) listener;
                        multiGetListener.onResponse(response);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    log.debug("MultiGet response failure during multi tenancy interception", e);
                    listener.onFailure(e);
                }
            });

            return SyncAuthorizationFilter.Result.INTERCEPTED;
        }
    }

    private MultiGetRequest.Item addTenantScopeToMultiGetItem(MultiGetRequest.Item item, String tenant) {
        return new MultiGetRequest.Item(item.index(), scopedId(item.id(), tenant)) //
                .routing(item.routing()) //
                .storedFields(item.storedFields()) //
                .version(item.version()) //
                .versionType(item.versionType()) //
                .fetchSourceContext(item.fetchSourceContext());
    }

    private MultiGetItemResponse unscopeIdInMultiGetResponseItem(MultiGetItemResponse multiGetItemResponse) {
        GetResponse successResponse = Optional.ofNullable(multiGetItemResponse.getResponse()) //
                .map(response -> new GetResult(response.getIndex(),
                        unscopedId(response.getId()),
                        response.getSeqNo(),
                        response.getPrimaryTerm(),
                        response.getVersion(),
                        response.isExists(),
                        response.getSourceAsBytesRef(),
                        response.getFields(),
                        null)) //
                .map(GetResponse::new)
                .orElse(null);

        MultiGetResponse.Failure failure = Optional.ofNullable(multiGetItemResponse.getFailure()) //
                .map(fault -> new MultiGetResponse.Failure(fault.getIndex(), unscopedId(fault.getId()), fault.getFailure())) //
                .orElse(null);
        return new MultiGetItemResponse(successResponse, failure);
    }
}
