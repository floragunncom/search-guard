package com.floragunn.signals.api;

import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.floragunn.searchguard.filter.TenantAwareRestHandler;
import com.floragunn.signals.actions.watch.search.SearchWatchAction;
import com.floragunn.signals.actions.watch.search.SearchWatchRequest;
import com.floragunn.signals.actions.watch.search.SearchWatchResponse;
import com.google.common.collect.ImmutableList;

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

        return channel -> {
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

            client.execute(SearchWatchAction.INSTANCE, searchWatchRequest, new RestStatusToXContentListener<SearchWatchResponse>(channel));
        };

    }

    @Override
    public String getName() {
        return "Search Watch Action";
    }
}
