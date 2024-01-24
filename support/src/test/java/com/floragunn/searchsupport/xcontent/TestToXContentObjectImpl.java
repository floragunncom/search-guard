package com.floragunn.searchsupport.xcontent;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

class TestToXContentObjectImpl implements ToXContentObject {

    private final String message;

    public TestToXContentObjectImpl(String message) {
        this.message = message;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("msg", message);
        builder.endObject();
        return builder;
    }
}
