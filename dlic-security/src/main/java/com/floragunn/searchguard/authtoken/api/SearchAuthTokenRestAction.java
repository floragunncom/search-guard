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

import static org.elasticsearch.core.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.google.common.collect.ImmutableList;

public class SearchAuthTokenRestAction extends BaseRestHandler {

    public SearchAuthTokenRestAction() {
        super();
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_searchguard/authtoken/_search"), new Route(POST, "/_searchguard/authtoken/_search"));
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String scroll = request.param("scroll");
        int from = request.paramAsInt("from", -1);
        int size = request.paramAsInt("size", -1);

        SearchAuthTokensRequest searchWatchRequest = new SearchAuthTokensRequest();

        if (scroll != null) {
            searchWatchRequest.setScroll(new Scroll(parseTimeValue(scroll, null, "scroll")));
        }

        searchWatchRequest.setFrom(from);
        searchWatchRequest.setSize(size);

        if (request.hasContent()) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource().parseXContent(request.contentParser(), true);

            searchWatchRequest.setSearchSourceBuilder(searchSourceBuilder);
        }

        return channel -> client.execute(SearchAuthTokensAction.INSTANCE, searchWatchRequest,
                new RestToXContentListener<SearchAuthTokensResponse>(channel, SearchAuthTokensResponse::status));
    }

    @Override
    public String getName() {
        return "Search AuthToken Action";
    }
}
