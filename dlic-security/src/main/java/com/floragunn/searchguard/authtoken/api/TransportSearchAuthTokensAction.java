/*
 * Copyright 2020 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.authtoken.api;

import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.floragunn.searchguard.auth.AuthInfoService;
import com.floragunn.searchguard.authtoken.AuthTokenService;
import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.user.User;

public class TransportSearchAuthTokensAction extends AbstractTransportAuthTokenAction<SearchAuthTokensRequest, SearchAuthTokensResponse> {

    private final AuthTokenService authTokenService;
    private final AuthInfoService authInfoService;
    private final PrivilegedConfigClient privilegedConfigClient;
    private final ThreadPool threadPool;

    @Inject
    public TransportSearchAuthTokensAction(AuthTokenService authTokenService, AuthInfoService authInfoService, TransportService transportService,
            ActionFilters actionFilters, Client client, PrivilegesEvaluator privilegesEvaluator, ThreadPool threadPool) {
        super(SearchAuthTokensAction.NAME, transportService, actionFilters, SearchAuthTokensRequest::new, privilegesEvaluator);

        this.authTokenService = authTokenService;
        this.authInfoService = authInfoService;
        this.privilegedConfigClient = PrivilegedConfigClient.adapt(client);
        this.threadPool = threadPool;
    }

    @Override
    protected final void doExecute(Task task, SearchAuthTokensRequest request, ActionListener<SearchAuthTokensResponse> listener) {
        User user = authInfoService.getCurrentUser();

        SearchRequest searchRequest = new SearchRequest(authTokenService.getIndexName());

        if (request.getScroll() != null) {
            searchRequest.scroll(request.getScroll());
        }

        SearchSourceBuilder searchSourceBuilder = request.getSearchSourceBuilder();

        TransportAddress userRemoteAddress = (TransportAddress) this.threadPool.getThreadContext().getTransient(ConfigConstants.SG_REMOTE_ADDRESS);

        if (searchSourceBuilder == null) {
            searchSourceBuilder = new SearchSourceBuilder();

            if (!isAllowedToAccessAll(user, userRemoteAddress)) {
                searchSourceBuilder.query(QueryBuilders.termQuery("user_name", user.getName()));
            } else {
                searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            }
        } else if (!isAllowedToAccessAll(user, userRemoteAddress)) {
            QueryBuilder originalQuery = searchSourceBuilder.query();
            BoolQueryBuilder newQuery = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("user_name", user.getName()));

            if (originalQuery != null) {
                newQuery.must(originalQuery);
            }

            searchSourceBuilder.query(newQuery);
        }

        if (request.getFrom() != -1) {
            searchSourceBuilder.from(request.getFrom());
        }

        if (request.getSize() != -1) {
            searchSourceBuilder.size(request.getSize());
        }

        searchRequest.source(searchSourceBuilder);

        privilegedConfigClient.execute(SearchAction.INSTANCE, searchRequest, new ActionListener<SearchResponse>() {

            @Override
            public void onResponse(SearchResponse response) {
                listener.onResponse(new SearchAuthTokensResponse(response));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

        });

    }

}