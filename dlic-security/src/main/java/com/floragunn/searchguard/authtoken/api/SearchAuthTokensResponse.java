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

import org.opensearch.action.ActionResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;

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
