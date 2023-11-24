package com.floragunn.signals.actions.account.search;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.signals.watch.Watch;

public class SearchAccountResponse extends ActionResponse implements ToXContentObject {

    private SearchResponse searchResponse;

    public SearchAccountResponse() {
    }

    public SearchAccountResponse(SearchResponse searchResponse) {
        this.searchResponse = searchResponse;
    }

    public SearchAccountResponse(StreamInput in) throws IOException {
        super(in);
        this.searchResponse = new SearchResponse(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.searchResponse.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        ChunkedToXContentObject.wrapAsToXContentObject(this.searchResponse)
                .toXContent(builder, new DelegatingMapParams(Watch.WITHOUT_AUTH_TOKEN_PARAM_MAP, params));
        return builder;
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    public RestStatus status() {
        return searchResponse.status();
    }

}
