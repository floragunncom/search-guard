package com.floragunn.signals.actions.account.search;

import java.io.IOException;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class SearchAccountRequest extends ActionRequest {

    private SearchSourceBuilder searchSourceBuilder;
    private Scroll scroll;
    private int from = -1;
    private int size = -1;

    public SearchAccountRequest() {
        super();
    }

    public SearchAccountRequest(SearchSourceBuilder searchSourceBuilder, Scroll scroll) {
        super();
        this.searchSourceBuilder = searchSourceBuilder;
        this.scroll = scroll;
    }

    public SearchAccountRequest(StreamInput in) throws IOException {
        super(in);
        scroll = in.readOptionalWriteable(Scroll::new);
        from = in.readInt();
        size = in.readInt();
        searchSourceBuilder = in.readOptionalWriteable(SearchSourceBuilder::new);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(scroll);
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

    public Scroll getScroll() {
        return scroll;
    }

    public void setSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder) {
        this.searchSourceBuilder = searchSourceBuilder;
    }

    public void setScroll(Scroll scroll) {
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
