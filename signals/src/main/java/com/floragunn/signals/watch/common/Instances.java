package com.floragunn.signals.watch.common;

import com.floragunn.fluent.collections.ImmutableList;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

public class Instances implements ToXContentObject {

    public static final Instances EMPTY = new Instances(false, ImmutableList.empty());

    private static final Pattern PARAMETER_NAME_PATTERN = Pattern.compile("[_a-zA-Z][_a-zA-Z0-9]*");

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

    public static boolean isValidParameterName(String parameterName) {
        if((parameterName == null) || parameterName.isEmpty()) {
            return false;
        }
        return PARAMETER_NAME_PATTERN.matcher(parameterName).matches();
    }

    private int getNumberOfParameters() {
        return params.size();
    }

    public boolean hasSameParameterList(Instances previousInstance) {
        Objects.requireNonNull(previousInstance, "Previous instance is required");
        if(getNumberOfParameters() != previousInstance.getNumberOfParameters()) {
            return false;
        }
        Set<String> parameterNames = new HashSet<>(getParams());
        previousInstance.getParams().forEach(parameterNames::remove);
        return parameterNames.size() == 0;
    }

    @Override public String toString() {
        return "Instances{" + "enabled=" + enabled + ", params=" + params + '}';
    }
}
