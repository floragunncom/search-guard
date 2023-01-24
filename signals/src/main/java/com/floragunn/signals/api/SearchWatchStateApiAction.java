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
import com.floragunn.signals.actions.watch.state.search.SearchWatchStateAction;
import com.floragunn.signals.actions.watch.state.search.SearchWatchStateRequest;
import com.floragunn.signals.actions.watch.state.search.SearchWatchStateResponse;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class SearchWatchStateApiAction extends BaseRestHandler implements TenantAwareRestHandler {

    public SearchWatchStateApiAction() {

    }

    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_signals/watch/{tenant}/_search/_state"),
                new Route(POST, "/_signals/watch/{tenant}/_search/_state"));
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String scroll = request.param("scroll");
        int from = request.paramAsInt("from", -1);
        int size = request.paramAsInt("size", -1);

        SearchWatchStateRequest searchWatchRequest = new SearchWatchStateRequest();

        if (scroll != null) {
            searchWatchRequest.setScroll(new Scroll(parseTimeValue(scroll, null, "scroll")));
        }

        searchWatchRequest.setFrom(from);
        searchWatchRequest.setSize(size);

        if (request.hasContent()) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(request.contentParser());

            searchWatchRequest.setSearchSourceBuilder(searchSourceBuilder);
        }

        return channel -> client.execute(SearchWatchStateAction.INSTANCE, searchWatchRequest,
                new RestStatusToXContentListener<SearchWatchStateResponse>(channel));

    }

    @Override
    public String getName() {
        return "Search Watch State Action";
    }
}
