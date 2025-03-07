package com.floragunn.signals.actions.watch.search;

import java.io.IOException;

import com.google.common.base.Strings;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class SearchWatchRequest extends ActionRequest {

    private SearchSourceBuilder searchSourceBuilder;
    private TimeValue scroll;
    private int from = -1;
    private int size = -1;

    public SearchWatchRequest() {
        super();
    }

    public SearchWatchRequest(SearchSourceBuilder searchSourceBuilder, TimeValue scroll) {
        super();
        this.searchSourceBuilder = searchSourceBuilder;
        this.scroll = scroll;
    }

    public SearchWatchRequest(StreamInput in) throws IOException {
        super(in);
//        scroll = in.readOptionalWriteable(Scroll::new);// TODO ES9 class does not exist, backward compatibility issue
        String stringScroll = in.readOptionalString();
        scroll = Strings.isNullOrEmpty(stringScroll) ? null : TimeValue.parseTimeValue(stringScroll, "SearchAccountRequest scroll");
        from = in.readInt();
        size = in.readInt();
        searchSourceBuilder = in.readOptionalWriteable(SearchSourceBuilder::new);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
//        out.writeOptionalWriteable(scroll); // TODO ES9 class Scroll does not exist, backward compatibility issue
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
