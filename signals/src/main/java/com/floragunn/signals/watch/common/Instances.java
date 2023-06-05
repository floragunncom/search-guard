package com.floragunn.signals.watch.common;

import com.floragunn.fluent.collections.ImmutableList;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class Instances implements ToXContentObject {

    private final boolean enabled;

    private final ImmutableList<String> params;

    public Instances(boolean enabled, ImmutableList<String> params) {
        this.enabled = enabled;
        this.params = Objects.requireNonNull(params, "Params are required");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        return null;
    }
}
