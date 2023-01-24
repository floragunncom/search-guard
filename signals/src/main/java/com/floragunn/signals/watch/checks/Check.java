/*
 * Copyright 2023 floragunn GmbH
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

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.signals.execution.CheckExecutionException;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.common.WatchElement;
import com.floragunn.signals.watch.init.WatchInitializationService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class Check extends WatchElement {
    protected final String name;

    Check(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " " + name;
    }

    protected Map<String, Object> getTemplateScriptParamsAsMap(WatchExecutionContext ctx) {
        return ctx.getTemplateScriptParamsAsMap();
    }

    public abstract boolean execute(WatchExecutionContext ctx) throws CheckExecutionException;

    static Check create(WatchInitializationService watchInitService, DocNode jsonNode) throws ConfigValidationException {

        if (!jsonNode.hasNonNull("type")) {
            throw new ConfigValidationException(new MissingAttribute("type", jsonNode));
        }

        String type = jsonNode.getAsString("type");

        switch (type) {
        case "search":
            if (jsonNode.hasNonNull("template")) {
                return SearchTemplateInput.create(watchInitService, jsonNode);
            } else {
                return SearchInput.create(watchInitService, jsonNode);
            }
        case "static":
            return StaticInput.create(jsonNode);
        case "http":
            return HttpInput.create(watchInitService, jsonNode);
        case "condition":
        case "condition.script":
            return Condition.create(watchInitService, jsonNode);
        case "calc":
            return Calc.create(watchInitService, jsonNode);
        case "transform":
            return Transform.create(watchInitService, jsonNode);
        default:
            throw new ConfigValidationException(new InvalidAttributeValue("type", type, "search|static|http|condition|calc|transform", jsonNode));
        }
    }

    public static Map<String, Object> getIndexMapping() {
        NestedValueMap result = new NestedValueMap();

        result.put("dynamic", true);

        NestedValueMap properties = new NestedValueMap();
        SearchInput.addIndexMappingProperties(properties);
        StaticInput.addIndexMappingProperties(properties);

        result.put("properties", properties);

        return result;
    }

    public static Map<String, Object> getIndexMappingUpdate() {
        NestedValueMap result = new NestedValueMap();

        NestedValueMap properties = new NestedValueMap();
        StaticInput.addIndexMappingProperties(properties);

        result.put("properties", properties);

        return result;
    }

    public static List<Check> create(WatchInitializationService watchInitService, List<?> checkNodes) throws ConfigValidationException {
        ArrayList<Check> result = new ArrayList<>(checkNodes.size());

        ValidationErrors validationErrors = new ValidationErrors();

        for (Object o : checkNodes) {
            DocNode member = DocNode.wrap(o);
            try {
                result.add(create(watchInitService, member));
            } catch (ConfigValidationException e) {
                validationErrors.add(member.hasNonNull("name") ? "[" + member.getAsString("name") + "]" : "[]", e);
            }
        }

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }
}
