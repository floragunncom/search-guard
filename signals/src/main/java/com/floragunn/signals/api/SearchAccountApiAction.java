/*
 * Copyright 2020-2026 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.signals.api;

import static org.elasticsearch.core.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.floragunn.signals.actions.account.search.SearchAccountAction;
import com.floragunn.signals.actions.account.search.SearchAccountRequest;
import com.floragunn.signals.actions.account.search.SearchAccountResponse;
import com.floragunn.signals.actions.account.search.TenantSearchAccountAction;
import com.google.common.collect.ImmutableList;

public class SearchAccountApiAction extends SignalsTenantAwareRestHandler {

    private final Predicate<NodeFeature> clusterSupportsFeature;

    public SearchAccountApiAction(Settings settings, Predicate<NodeFeature> clusterSupportsFeature) {
        super(settings);
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

    @Override
    public List<Route> routes() {
        // The route shapes are /_search for global accounts and /{tenant}/_search for a tenant plus global accounts. PathTrie requires the
        // wildcard at this position to use the same name as the CRUD routes, where its value can instead be an account type.
        return ImmutableList.of(new Route(GET, "/_signals/account/_search"), new Route(POST, "/_signals/account/_search"),
                new Route(GET, "/_signals/account/{tenantOrAccountType}/_search"),
                new Route(POST, "/_signals/account/{tenantOrAccountType}/_search"));
    }

    @Override
    protected final RestChannelConsumer getRestChannelConsumer(RestRequest request, NodeClient client) throws IOException {
        boolean tenantScoped = request.param("tenantOrAccountType") != null;
        String scroll = request.param("scroll");
        int from = request.paramAsInt("from", -1);
        int size = request.paramAsInt("size", -1);

        SearchAccountRequest searchDestinationRequest = new SearchAccountRequest();

        if (scroll != null) {
            searchDestinationRequest.setScroll(parseTimeValue(scroll, null, "scroll"));
        }

        searchDestinationRequest.setFrom(from);
        searchDestinationRequest.setSize(size);

        if (request.hasContent()) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource().parseXContent(request.contentParser(), true, clusterSupportsFeature);

            searchDestinationRequest.setSearchSourceBuilder(searchSourceBuilder);
        }

        return channel -> client.execute(tenantScoped ? TenantSearchAccountAction.INSTANCE : SearchAccountAction.INSTANCE, searchDestinationRequest,
                new RestToXContentListener<>(channel, SearchAccountResponse::status));

    }

    @Override
    public String getName() {
        return "Search Account Action";
    }
}
