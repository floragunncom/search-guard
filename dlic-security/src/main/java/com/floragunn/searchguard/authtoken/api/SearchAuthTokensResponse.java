/*
 * Copyright 2020 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.searchguard.authtoken.api;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

public class SearchAuthTokensResponse extends ActionResponse implements ToXContentObject {

    private SearchResponse searchResponse;

    public SearchAuthTokensResponse() {
    }

    public SearchAuthTokensResponse(SearchResponse searchResponse) {
        this.searchResponse = searchResponse;
        this.searchResponse.incRef();
    }

    public SearchAuthTokensResponse(StreamInput in) throws IOException {
        this.searchResponse = new SearchResponse(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.searchResponse.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        ChunkedToXContentObject.wrapAsToXContentObject(this.searchResponse).toXContent(builder, params);
        return builder;
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    public RestStatus status() {
        return searchResponse.status();
    }

    @Override
    public boolean decRef() {
        return this.searchResponse.decRef();
    }
}
