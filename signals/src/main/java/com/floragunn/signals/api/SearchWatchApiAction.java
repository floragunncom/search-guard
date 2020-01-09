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

import com.floragunn.searchguard.filter.TenantAwareRestHandler;
import com.floragunn.signals.actions.watch.search.SearchWatchAction;
import com.floragunn.signals.actions.watch.search.SearchWatchRequest;
import com.floragunn.signals.actions.watch.search.SearchWatchResponse;

public class SearchWatchApiAction extends BaseRestHandler implements TenantAwareRestHandler {

    public SearchWatchApiAction(final Settings settings, final RestController controller, final ThreadPool threadPool) {
        super();
        controller.registerHandler(GET, "/_signals/watch/{tenant}/_search", this);
        controller.registerHandler(POST, "/_signals/watch/{tenant}/_search", this);
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
