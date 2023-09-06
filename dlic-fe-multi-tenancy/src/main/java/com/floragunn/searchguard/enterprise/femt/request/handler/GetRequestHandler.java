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
import com.floragunn.searchguard.enterprise.femt.request.mapper.GetMapper;
import com.floragunn.searchguard.enterprise.femt.request.mapper.MappingException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.IndicesService;

import static com.floragunn.searchguard.enterprise.femt.PrivilegesInterceptorImpl.SG_FILTER_LEVEL_FEMT_DONE;

public class GetRequestHandler extends RequestHandler<GetRequest> {

    private final Client nodeClient;
    private final ThreadContext threadContext;
    private final GetMapper getMapper;

    public GetRequestHandler(Client nodeClient, ThreadContext threadContext, ClusterService clusterService, IndicesService indicesService) {
        this.nodeClient = nodeClient;
        this.threadContext = threadContext;
        this.getMapper = new GetMapper(clusterService, indicesService);
    }

    @Override
    public SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, GetRequest request, ActionListener<?> listener) {
        log.debug("Handle get request");
        threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, request.toString());

        try (ThreadContext.StoredContext storedContext = threadContext.newStoredContext()) {
            SearchRequest searchRequest = getMapper.toScopedSearchRequest(request, requestedTenant);

            nodeClient.search(searchRequest, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse response) {
                    try {

                        storedContext.restore();

                        GetResponse getResponse = getMapper.toUnscopedGetResponse(response, request);
                        @SuppressWarnings("unchecked")
                        ActionListener<GetResponse> getListener = (ActionListener<GetResponse>) listener;
                        getListener.onResponse(getResponse);

                    } catch (MappingException e) {
                        log.error("An error occurred while handling search response", e);
                        listener.onFailure(new ElasticsearchSecurityException("Internal error during multi tenancy interception"));
                    } catch (Exception e) {
                        log.error("An error occurred while handling search response", e);
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {

                    log.error("An error occurred while sending search request", e);
                    listener.onFailure(e);
                }
            });

            return SyncAuthorizationFilter.Result.INTERCEPTED;
        }
    }
}
