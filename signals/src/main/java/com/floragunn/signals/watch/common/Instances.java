/*
 * Copyright 2019-2023 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
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

    /**
     * This regexp is used for validation of parameter names and generic watch instance ID. Therefore, is crucial that valid instance id
     * cannot contain instance separator which is defined by WatchInstanceIdService#INSTANCE_ID_SEPARATOR
     */
    private static final Pattern PARAMETER_NAME_PATTERN = Pattern.compile("[_a-zA-Z][_a-zA-Z0-9]*");

    /**
     * <code>id</code> parameter is reserved for watch instance id
     */
    private static final String RESERVED_PARAMETER_NAME = "id";

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
        if(RESERVED_PARAMETER_NAME.equalsIgnoreCase(parameterName)) {
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
