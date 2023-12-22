package com.floragunn.signals.api;

import static org.elasticsearch.core.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.floragunn.searchguard.authc.rest.TenantAwareRestHandler;
import com.floragunn.signals.actions.watch.state.search.SearchWatchStateAction;
import com.floragunn.signals.actions.watch.state.search.SearchWatchStateRequest;
import com.floragunn.signals.actions.watch.state.search.SearchWatchStateResponse;
import com.google.common.collect.ImmutableList;

public class SearchWatchStateApiAction extends SignalsTenantAwareRestHandler {

    public SearchWatchStateApiAction(Settings settings) {
        super(settings);

    }

    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_signals/watch/{tenant}/_search/_state"),
                new Route(POST, "/_signals/watch/{tenant}/_search/_state"));
    }

    @Override
    protected final RestChannelConsumer getRestChannelConsumer(RestRequest request, NodeClient client) throws IOException {
        String scroll = request.param("scroll");
        int from = request.paramAsInt("from", -1);
        int size = request.paramAsInt("size", -1);

        //we need to consume the tenant param here because
        //if not ES 8 throws an exception
        request.param("tenant");

        SearchWatchStateRequest searchWatchRequest = new SearchWatchStateRequest();

        if (scroll != null) {
            searchWatchRequest.setScroll(new Scroll(parseTimeValue(scroll, null, "scroll")));
        }

        searchWatchRequest.setFrom(from);
        searchWatchRequest.setSize(size);

        if (request.hasContent()) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource().parseXContent(request.contentParser(), true);

            searchWatchRequest.setSearchSourceBuilder(searchSourceBuilder);
        }

        return channel -> client.execute(SearchWatchStateAction.INSTANCE, searchWatchRequest,
                new RestToXContentListener<>(channel, SearchWatchStateResponse::status));

    }

    @Override
    public String getName() {
        return "Search Watch State Action";
    }
}
