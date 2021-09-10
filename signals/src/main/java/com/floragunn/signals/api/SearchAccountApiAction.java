package com.floragunn.signals.api;

import static org.opensearch.common.unit.TimeValue.parseTimeValue;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.search.Scroll;
import org.opensearch.search.builder.SearchSourceBuilder;

import com.floragunn.signals.actions.account.search.SearchAccountAction;
import com.floragunn.signals.actions.account.search.SearchAccountRequest;
import com.floragunn.signals.actions.account.search.SearchAccountResponse;
import com.google.common.collect.ImmutableList;

public class SearchAccountApiAction extends BaseRestHandler {

    public SearchAccountApiAction() {

    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_signals/account/_search"), new Route(POST, "/_signals/account/_search"));
    }

    @Override
    public List<DeprecatedRoute> deprecatedRoutes() {
        return ImmutableList.of(new DeprecatedRoute(GET, "/_signals/destination/_search", "Use /_signals/account/_search instead"),
                new DeprecatedRoute(POST, "/_signals/destination/_search", "Use /_signals/account/_search instead"));
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String scroll = request.param("scroll");
        int from = request.paramAsInt("from", -1);
        int size = request.paramAsInt("size", -1);

        SearchAccountRequest searchDestinationRequest = new SearchAccountRequest();

        if (scroll != null) {
            searchDestinationRequest.setScroll(new Scroll(parseTimeValue(scroll, null, "scroll")));
        }

        searchDestinationRequest.setFrom(from);
        searchDestinationRequest.setSize(size);

        if (request.hasContent()) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(request.contentParser());

            searchDestinationRequest.setSearchSourceBuilder(searchSourceBuilder);
        }

        return channel -> client.execute(SearchAccountAction.INSTANCE, searchDestinationRequest,
                new RestStatusToXContentListener<SearchAccountResponse>(channel));

    }

    @Override
    public String getName() {
        return "Search Account Action";
    }
}
