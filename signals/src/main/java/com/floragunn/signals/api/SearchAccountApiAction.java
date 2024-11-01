package com.floragunn.signals.api;

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
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource().parseXContent(request.contentParser(), true);

            searchDestinationRequest.setSearchSourceBuilder(searchSourceBuilder);
        }

        return channel -> client.execute(SearchAccountAction.INSTANCE, searchDestinationRequest,
                new RestToXContentListener<>(channel, SearchAccountResponse::status));

    }

    @Override
    public String getName() {
        return "Search Account Action";
    }
}
