package com.floragunn.searchguard.authtoken.api;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

public class SearchAuthTokensResponse extends ActionResponse implements StatusToXContentObject {

    private SearchResponse searchResponse;

    public SearchAuthTokensResponse() {
    }

    public SearchAuthTokensResponse(SearchResponse searchResponse) {
        this.searchResponse = searchResponse;
    }

    public SearchAuthTokensResponse(StreamInput in) throws IOException {
        super(in);
        this.searchResponse = new SearchResponse(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.searchResponse.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        this.searchResponse.toXContent(builder, params);
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
