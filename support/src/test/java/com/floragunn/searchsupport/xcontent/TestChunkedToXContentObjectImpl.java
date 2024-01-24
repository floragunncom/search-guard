package com.floragunn.searchsupport.xcontent;

import com.floragunn.fluent.collections.ImmutableList;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Iterator;

class TestChunkedToXContentObjectImpl implements ChunkedToXContentObject {

    private final String first;
    private final String second;

    public TestChunkedToXContentObjectImpl(String first, String second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        ToXContent firstBuilder = (builder, p) -> {
            builder.startObject();
            builder.field("first", first);
            return builder;
        };
        ToXContent secondBuilder = (builder, p) -> {
            builder.field("second", second);
            builder.endObject();
            return builder;
        };
        return ImmutableList.of(firstBuilder, secondBuilder).iterator();
    }
}
