package com.floragunn.signals.api;

import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.signals.actions.account.search.SearchAccountAction;
import com.floragunn.signals.actions.account.search.SearchAccountRequest;
import com.floragunn.signals.actions.account.search.SearchAccountResponse;

public class SearchDestinationApiAction extends BaseRestHandler {

    public SearchDestinationApiAction(final Settings settings, final RestController controller, final ThreadPool threadPool) {
        controller.registerHandler(GET, "/_signals/destination/_search", this);
        controller.registerHandler(POST, "/_signals/destination/_search", this);
        controller.registerHandler(GET, "/_signals/account/_search", this);
        controller.registerHandler(POST, "/_signals/account/_search", this);
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String scroll = request.param("scroll");
        int from = request.paramAsInt("from", -1);
        int size = request.paramAsInt("size", -1);

        return channel -> {
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

            client.execute(SearchAccountAction.INSTANCE, searchDestinationRequest, new RestStatusToXContentListener<SearchAccountResponse>(channel));
        };

    }

    @Override
    public String getName() {
        return "Search Account Action";
    }
}
