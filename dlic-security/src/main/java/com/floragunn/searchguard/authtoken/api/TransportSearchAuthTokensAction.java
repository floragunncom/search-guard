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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.auth.AuthInfoService;
import com.floragunn.searchguard.authtoken.AuthTokenService;
import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.user.User;

public class TransportSearchAuthTokensAction extends AbstractTransportAuthTokenAction<SearchAuthTokensRequest, SearchAuthTokensResponse> {

    private final AuthTokenService authTokenService;
    private final AuthInfoService authInfoService;
    private final PrivilegedConfigClient privilegedConfigClient;

    @Inject
    public TransportSearchAuthTokensAction(AuthTokenService authTokenService, AuthInfoService authInfoService, TransportService transportService,
            ActionFilters actionFilters, Client client, PrivilegesEvaluator privilegesEvaluator) {
        super(SearchAuthTokensAction.NAME, transportService, actionFilters, SearchAuthTokensRequest::new, privilegesEvaluator);

        this.authTokenService = authTokenService;
        this.authInfoService = authInfoService;
        this.privilegedConfigClient = PrivilegedConfigClient.adapt(client);
    }

    @Override
    protected final void doExecute(Task task, SearchAuthTokensRequest request, ActionListener<SearchAuthTokensResponse> listener) {
        User user = authInfoService.getCurrentUser();

        SearchRequest searchRequest = new SearchRequest(authTokenService.getIndexName());

        if (request.getScroll() != null) {
            searchRequest.scroll(request.getScroll());
        }

        SearchSourceBuilder searchSourceBuilder = request.getSearchSourceBuilder();

        if (searchSourceBuilder == null) {
            searchSourceBuilder = new SearchSourceBuilder();

            if (!isAllowedToAccessAll(user)) {
                searchSourceBuilder.query(QueryBuilders.termQuery("user_name", user.getName()));
            } else {
                searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            }
        } else if (!isAllowedToAccessAll(user)) {
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