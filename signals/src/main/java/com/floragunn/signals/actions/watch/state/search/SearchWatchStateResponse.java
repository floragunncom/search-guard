package com.floragunn.signals.actions.watch.state.search;

import java.io.IOException;

import org.opensearch.action.ActionResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;

import com.floragunn.signals.watch.Watch;

public class SearchWatchStateResponse extends ActionResponse implements StatusToXContentObject {

    private SearchResponse searchResponse;

    public SearchWatchStateResponse() {
    }

    public SearchWatchStateResponse(SearchResponse searchResponse) {
        this.searchResponse = searchResponse;
    }

    public SearchWatchStateResponse(StreamInput in) throws IOException {
        this.searchResponse = new SearchResponse(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.searchResponse.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        this.searchResponse.toXContent(builder, new DelegatingMapParams(Watch.WITHOUT_AUTH_TOKEN_PARAM_MAP, params));
        return builder;
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    @Override
    public RestStatus status() {
        return searchResponse.status();
    }

}
