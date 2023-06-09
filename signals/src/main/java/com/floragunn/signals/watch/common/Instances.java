package com.floragunn.signals.watch.common;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class Instances implements ToXContentObject {

    public static final Instances EMPTY = new Instances(false, ImmutableList.empty());

    public final static String FIELD_ENABLED = "enabled";
    public final static String FIELD_PARAMS = "params";

    private final boolean enabled;

    private final ImmutableList<String> params;

    public Instances(boolean enabled, ImmutableList<String> params) {
        this.enabled = enabled;
        this.params = Objects.requireNonNull(params, "Params are required");
    }

    public boolean isEnabled() {
        return enabled;
    }

    public ImmutableList<String> getParams() {
        return params;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params xContentParams) throws IOException {
        xContentBuilder.startObject();
        xContentBuilder.field(FIELD_ENABLED, enabled);
        xContentBuilder.array(FIELD_PARAMS, params.toArray(new String[0]));
        xContentBuilder.endObject();
        return xContentBuilder;
    }
}
