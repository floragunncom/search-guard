/*
 * Copyright 2020-2022 floragunn GmbH
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

package com.floragunn.signals.watch.checks;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.opensearch.common.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.support.NestedValueMap;

public class StaticInput extends AbstractInput {
    private Map<String, Object> value;

    static Check create(DocNode jsonObject) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonObject, validationErrors);

        vJsonNode.used("type");

        String name = vJsonNode.get("name").asString();
        String target = vJsonNode.get("target").asString();

        Map<String, Object> value = Collections.emptyMap();

        // value is the user-facing and legacy in-index attribute name
        // value_no_map is the new in-index attribute name, which needs to be used due to a bug in earlier versions.
        // See comment in toXContent() below regarding the attributes value_no_map and value
        if (vJsonNode.hasNonNull("value_no_map")) {
            value = jsonObject.getAsNode("value_no_map").toMap();
        } else if (vJsonNode.hasNonNull("value")) {
            value = jsonObject.getAsNode("value").toMap();
        }

        vJsonNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        StaticInput result = new StaticInput(name, target, value);

        return result;

    }

    public StaticInput(String name, String target, Map<String, Object> value) {
        super(name, target);
        this.value = Collections.unmodifiableMap(value);
    }

    public Map<String, Object> getValue() {
        return value;
    }

    @Override
    public boolean execute(WatchExecutionContext ctx) {
        if (this.value != null) {
            ctx.getContextData().getData().put(this.target, this.value);
        }

        return true;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", "static");
        builder.field("name", name);
        builder.field("target", target);

        // Note: This was once written to an attribute "value". However, dynamic index mapping was enabled for this attribute, which made using
        // different value structures impossible. As index mappings for specific fields cannot be changed retroactively, we need to write this
        // to a new field name. The parsing code above supports both attributes.
        builder.field("value_no_map", value);
        builder.endObject();
        return builder;
    }

    static void addIndexMappingProperties(NestedValueMap mapping) {
        mapping.put(new NestedValueMap.Path("value_no_map", "type"), "object");
        mapping.put(new NestedValueMap.Path("value_no_map", "dynamic"), true);
        mapping.put(new NestedValueMap.Path("value_no_map", "enabled"), false);
    }

    /**
     * Fixes bug in regard to index mapping. Dynamic index mapping was originally enabled for the attribute "value", which made using
     * different value structures impossible. As index mappings for specific fields cannot be changed retroactively, we need to write this   
     * to a new field name. 
     */
    public static void patchForIndexMappingBugFix(Map<String, Object> watchJson) {
        if (watchJson.get("checks") instanceof List) {
            for (Object checkObject : (List<?>) watchJson.get("checks")) {
                if (!(checkObject instanceof Map)) {
                    continue;
                }

                @SuppressWarnings("unchecked")
                Map<String, Object> check = (Map<String, Object>) checkObject;

                if ("static".equals(check.get("type"))) {
                    Object value = check.remove("value");

                    if (value != null) {
                        check.put("value_no_map", value);
                    }
                }
            }
        }

        if (watchJson.get("actions") instanceof List) {
            for (Object actionObject : (List<?>) watchJson.get("actions")) {
                if (!(actionObject instanceof Map)) {
                    continue;
                }

                Map<?, ?> action = (Map<?, ?>) actionObject;

                if (action.get("checks") instanceof List) {
                    for (Object checkObject : (List<?>) action.get("checks")) {
                        if (!(checkObject instanceof Map)) {
                            continue;
                        }

                        @SuppressWarnings("unchecked")
                        Map<String, Object> check = (Map<String, Object>) checkObject;

                        if ("static".equals(check.get("type"))) {
                            Object value = check.remove("value");

                            if (value != null) {
                                check.put("value_no_map", value);
                            }
                        }
                    }
                }
            }
        }
    }
    
    public static void unpatchForIndexMappingBugFix(Map<String, Object> watchJson) {
        if (watchJson.get("checks") instanceof List) {
            for (Object checkObject : (List<?>) watchJson.get("checks")) {
                if (!(checkObject instanceof Map)) {
                    continue;
                }

                @SuppressWarnings("unchecked")
                Map<String, Object> check = (Map<String, Object>) checkObject;

                if ("static".equals(check.get("type"))) {
                    Object value = check.remove("value_no_map");

                    if (value != null) {
                        check.put("value", value);
                    }
                }
            }
        }

        if (watchJson.get("actions") instanceof List) {
            for (Object actionObject : (List<?>) watchJson.get("actions")) {
                if (!(actionObject instanceof Map)) {
                    continue;
                }

                Map<?, ?> action = (Map<?, ?>) actionObject;

                if (action.get("checks") instanceof List) {
                    for (Object checkObject : (List<?>) action.get("checks")) {
                        if (!(checkObject instanceof Map)) {
                            continue;
                        }

                        @SuppressWarnings("unchecked")
                        Map<String, Object> check = (Map<String, Object>) checkObject;

                        if ("static".equals(check.get("type"))) {
                            Object value = check.remove("value_no_map");

                            if (value != null) {
                                check.put("value", value);
                            }
                        }
                    }
                }
            }
        }
    }
}
