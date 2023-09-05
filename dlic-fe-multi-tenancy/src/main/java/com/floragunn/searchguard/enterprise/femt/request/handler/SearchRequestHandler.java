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
import com.floragunn.searchguard.enterprise.femt.request.mapper.SearchMapper;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import static com.floragunn.searchguard.enterprise.femt.PrivilegesInterceptorImpl.SG_FILTER_LEVEL_FEMT_DONE;

public class SearchRequestHandler extends RequestHandler<SearchRequest> {

    private final Client nodeClient;
    private final ThreadContext threadContext;
    public SearchRequestHandler(Logger log, Client nodeClient, ThreadContext threadContext) {
        super(log);
        this.nodeClient = nodeClient;
        this.threadContext = threadContext;
    }

    @Override
    public SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, SearchRequest request, ActionListener<?> listener) {
        log.debug("Handle search request");
        threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, request.toString());

        try (ThreadContext.StoredContext storedContext = threadContext.newStoredContext()) {

            SearchMapper searchMapper = new SearchMapper();
            SearchRequest scopedRequest = searchMapper.toScopedSearchRequest(request, requestedTenant);

            nodeClient.search(scopedRequest, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse response) {
                    try {
                        storedContext.restore();

                        SearchResponse unscoped = searchMapper.toUnscopedSearchResponse(response);
                        @SuppressWarnings("unchecked")
                        ActionListener<SearchResponse> searchListener = (ActionListener<SearchResponse>) listener;

                        searchListener.onResponse(unscoped);
                    } catch (Exception e) {
                        if (log.isErrorEnabled()) {
                            log.error("Error during handling search response", e);
                        }
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });

            return SyncAuthorizationFilter.Result.INTERCEPTED;
        }
    }
}
