package com.floragunn.signals.api;

import static org.elasticsearch.core.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.floragunn.signals.actions.watch.search.SearchWatchAction;
import com.floragunn.signals.actions.watch.search.SearchWatchRequest;
import com.floragunn.signals.actions.watch.search.SearchWatchResponse;
import com.google.common.collect.ImmutableList;

public class SearchWatchApiAction extends SignalsTenantAwareRestHandler {

    public SearchWatchApiAction(Settings settings) {
        super(settings);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_signals/watch/{tenant}/_search"), new Route(POST, "/_signals/watch/{tenant}/_search"));
    }

    @Override
    protected final RestChannelConsumer getRestChannelConsumer(RestRequest request, NodeClient client) throws IOException {
        String scroll = request.param("scroll");
        int from = request.paramAsInt("from", -1);
        int size = request.paramAsInt("size", -1);

        //we need to consume the tenant param here because
        //if not ES 8 throws an exception
        request.param("tenant");

        SearchWatchRequest searchWatchRequest = new SearchWatchRequest();

        if (scroll != null) {
            searchWatchRequest.setScroll(new Scroll(parseTimeValue(scroll, null, "scroll")));
        }

        searchWatchRequest.setFrom(from);
        searchWatchRequest.setSize(size);

        if (request.hasContent()) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource().parseXContent(request.contentParser(), true);

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
