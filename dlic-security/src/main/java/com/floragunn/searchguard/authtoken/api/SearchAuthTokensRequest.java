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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class SearchAuthTokensRequest extends ActionRequest {

    private SearchSourceBuilder searchSourceBuilder;
    private TimeValue scroll;
    private int from = -1;
    private int size = -1;

    public SearchAuthTokensRequest() {
        super();
    }

    public SearchAuthTokensRequest(SearchSourceBuilder searchSourceBuilder, TimeValue scroll) {
        super();
        this.searchSourceBuilder = searchSourceBuilder;
        this.scroll = scroll;
    }

    public SearchAuthTokensRequest(StreamInput in) throws IOException {
        super(in);
//        scroll = in.readOptionalWriteable(Scroll::new); // TODO ES9 class does not exist, backward compatibility issue
        String stringScroll = in.readOptionalString();
        scroll = stringScroll == null ? null : TimeValue.parseTimeValue(stringScroll, "SearchAuthTokenRequest scroll");
        from = in.readInt();
        size = in.readInt();
        searchSourceBuilder = in.readOptionalWriteable(SearchSourceBuilder::new);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
//        out.writeOptionalWriteable(null);// TODO ES9 class Scroll does not exist, backward compatibility issue
        out.writeOptionalString(scroll == null ? null : scroll.toString());
        out.writeInt(from);
        out.writeInt(size);
        out.writeOptionalWriteable(searchSourceBuilder);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public SearchSourceBuilder getSearchSourceBuilder() {
        return searchSourceBuilder;
    }

    public TimeValue getScroll() {
        return scroll;
    }

    public void setSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder) {
        this.searchSourceBuilder = searchSourceBuilder;
    }

    public void setScroll(TimeValue scroll) {
        this.scroll = scroll;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

}
