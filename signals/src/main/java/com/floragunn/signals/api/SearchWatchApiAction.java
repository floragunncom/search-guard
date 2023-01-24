/*
 * Copyright 2023 floragunn GmbH
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

import com.floragunn.searchguard.authc.rest.TenantAwareRestHandler;
import com.floragunn.signals.actions.watch.search.SearchWatchAction;
import com.floragunn.signals.actions.watch.search.SearchWatchRequest;
import com.floragunn.signals.actions.watch.search.SearchWatchResponse;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class SearchWatchApiAction extends BaseRestHandler implements TenantAwareRestHandler {

    public SearchWatchApiAction() {
        super();
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_signals/watch/{tenant}/_search"), new Route(POST, "/_signals/watch/{tenant}/_search"));
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String scroll = request.param("scroll");
        int from = request.paramAsInt("from", -1);
        int size = request.paramAsInt("size", -1);

        SearchWatchRequest searchWatchRequest = new SearchWatchRequest();

        if (scroll != null) {
            searchWatchRequest.setScroll(new Scroll(parseTimeValue(scroll, null, "scroll")));
        }

        searchWatchRequest.setFrom(from);
        searchWatchRequest.setSize(size);

        if (request.hasContent()) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(request.contentParser());

            searchWatchRequest.setSearchSourceBuilder(searchSourceBuilder);
        }

        return channel -> client.execute(SearchWatchAction.INSTANCE, searchWatchRequest,
                new RestStatusToXContentListener<SearchWatchResponse>(channel));

    }

    @Override
    public String getName() {
        return "Search Watch Action";
    }
}
