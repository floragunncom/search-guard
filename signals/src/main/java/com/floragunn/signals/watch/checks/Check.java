package com.floragunn.signals.watch.checks;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.signals.execution.CheckExecutionException;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.common.WatchElement;
import com.floragunn.signals.watch.init.WatchInitializationService;

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

    static Check create(WatchInitializationService watchInitService, ObjectNode jsonNode) throws ConfigValidationException {

        if (!jsonNode.hasNonNull("type")) {
            throw new ConfigValidationException(new MissingAttribute("type", jsonNode));
        }

        String type = jsonNode.get("type").textValue();

        switch (type) {
        case "search":
            if (jsonNode.has("template")) {
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
            throw new ConfigValidationException(
                    new InvalidAttributeValue("type", type, "search|static|http|condition|calc|transform", jsonNode));
        }
    }

    public static Map<String, Object> getIndexMapping() {
        NestedValueMap result = new NestedValueMap();

        result.put("dynamic", true);

        NestedValueMap properties = new NestedValueMap();
        SearchInput.addIndexMappingProperties(properties);

        result.put("properties", properties);

        return result;
    }

    public static List<Check> create(WatchInitializationService watchInitService, ArrayNode arrayNode) throws ConfigValidationException {
        ArrayList<Check> result = new ArrayList<>(arrayNode.size());

        ValidationErrors validationErrors = new ValidationErrors();

        for (JsonNode member : arrayNode) {
            if (member instanceof ObjectNode) {
                try {
                    result.add(create(watchInitService, (ObjectNode) member));
                } catch (ConfigValidationException e) {
                    validationErrors.add(member.hasNonNull("name") ? "[" + member.get("name").asText() + "]" : "[]", e);
                }
            }
        }

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }
}
