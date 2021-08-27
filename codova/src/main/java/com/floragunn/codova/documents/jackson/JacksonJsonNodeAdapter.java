/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.codova.documents.jackson;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingFunction;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.google.common.collect.Maps;

public class JacksonJsonNodeAdapter extends DocNode {

    private final JsonNode jsonNode;

    public JacksonJsonNodeAdapter(JsonNode jsonNode) {
        this.jsonNode = jsonNode;
    }

    public JacksonJsonNodeAdapter(JsonNode jsonNode, String key) {
        this.jsonNode = jsonNode;
        this.key = key;
    }

    @Override
    public Object get(String attribute) {
        if (attribute == null) {
            return toBaseType(this.jsonNode, key);
        } else {
            return toBaseType(this.jsonNode.get(attribute), attribute);
        }
    }

    @Override
    public DocNode getAsNode(String attribute) {
        if (attribute == null) {
            return this;
        } else {
            JsonNode subNode = this.jsonNode.get(attribute);

            if (subNode == null || subNode.isNull()) {
                return null;
            } else {
                return new JacksonJsonNodeAdapter(subNode, attribute);
            }
        }
    }

    @Override
    public List<DocNode> getListOfNodes(String attribute) throws ConfigValidationException {
        JsonNode jsonNode;

        if (attribute == null) {
            jsonNode = this.jsonNode;
        } else {
            jsonNode = this.jsonNode.get(attribute);
        }

        if (jsonNode == null || jsonNode.isNull() || jsonNode.isMissingNode()) {
            return null;
        }

        if (!jsonNode.isArray()) {
            throw new ConfigValidationException(new InvalidAttributeValue(attribute, jsonNode, "A list of values"));
        }

        List<DocNode> list = new ArrayList<>(jsonNode.size());

        for (JsonNode subNode : jsonNode) {
            list.add(new JacksonJsonNodeAdapter(subNode));
        }

        return list;
    }

    @Override
    public List<Object> toList() {
        if (this.jsonNode.isNull() || this.jsonNode.isMissingNode()) {
            return Collections.emptyList();
        } else if (this.jsonNode.isArray()) {
            List<Object> list = new ArrayList<>(jsonNode.size());

            for (JsonNode subNode : jsonNode) {
                list.add(toBaseType(subNode, null));
            }

            return list;
        } else {
            return Collections.singletonList(toBaseType(this.jsonNode, null));
        }
    }

    @Override
    public <R> List<R> getList(String attribute, ValidatingFunction<String, R> conversionFunction, Object expected) throws ConfigValidationException {

        JsonNode jsonNode;

        if (attribute == null) {
            jsonNode = this.jsonNode;
        } else {
            jsonNode = this.jsonNode.get(attribute);
        }

        if (jsonNode == null || jsonNode.isNull() || jsonNode.isMissingNode()) {
            return null;
        }

        List<R> result = new ArrayList<>(jsonNode.size());
        ValidationErrors validationErrors = new ValidationErrors();

        int index = 0;

        for (JsonNode subNode : jsonNode) {
            try {
                if (subNode.isNull() || subNode.isMissingNode()) {
                    result.add(null);
                } else {
                    result.add(conversionFunction.apply(subNode.textValue()));
                }

            } catch (Exception e) {
                validationErrors.add(new InvalidAttributeValue(attribute + "." + index, subNode.textValue(), expected).cause(e));
            }
            index++;
        }

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }

    public BigDecimal getBigDecimal(String attribute) throws ConfigValidationException {

        if (jsonNode.hasNonNull(attribute)) {
            JsonNode attributeNode = jsonNode.get(attribute);

            if (attributeNode.isNumber()) {
                return attributeNode.decimalValue();
            } else {
                throw new ConfigValidationException(new InvalidAttributeValue(attribute, attributeNode.toString(), "number"));
            }
        } else {
            return null;
        }
    }

    private static Object toBaseType(JsonNode jsonNode, String key) {
        if (jsonNode == null || jsonNode.isNull() || jsonNode.isMissingNode()) {
            return null;
        } else if (jsonNode.isNumber()) {
            return jsonNode.numberValue();
        } else if (jsonNode.isTextual()) {
            return jsonNode.textValue();
        } else if (jsonNode.isBoolean()) {
            return jsonNode.booleanValue();
        } else if (jsonNode.isObject()) {
            return new JacksonJsonNodeAdapter(jsonNode, key);
        } else if (jsonNode.isArray()) {
            return toListOfBaseType(jsonNode);
        } else {
            throw new RuntimeException("Unexpected type: " + jsonNode);
        }
    }

    private static List<Object> toListOfBaseType(JsonNode jsonNode) {
        List<Object> list = new ArrayList<>(jsonNode.size());

        for (JsonNode subNode : jsonNode) {
            list.add(toBaseType(subNode, null));
        }

        return list;
    }

    @Override
    public Map<String, Object> toMap() throws ConfigValidationException {
        return toMap(this.jsonNode);
    }

    @Override
    public boolean isMap() {
        return jsonNode instanceof ObjectNode;
    }

    @Override
    public boolean isList(String attribute) {
        if (attribute != null) {
            return jsonNode.path(attribute).isArray();
        } else {
            return jsonNode.isArray();
        }
    }

    @Override
    public boolean isList() {
        return jsonNode.isArray();
    }

    @Override
    public int size() {
        return jsonNode.size();
    }

    @Override
    public boolean isEmpty() {
        return jsonNode.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        if (jsonNode instanceof ObjectNode && key instanceof String) {
            return ((ObjectNode) jsonNode).has((String) key);
        } else {
            return false;
        }
    }

    @Override
    public Set<String> keySet() {
        if (jsonNode instanceof ObjectNode) {
            ObjectNode objectNode = (ObjectNode) jsonNode;

            Set<String> result = new HashSet<>(objectNode.size());

            for (Iterator<String> iter = objectNode.fieldNames(); iter.hasNext();) {
                result.add(iter.next());
            }

            return result;
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    public Collection<Object> values() {
        if (jsonNode instanceof ObjectNode) {

            List<Object> result = new ArrayList<>();

            for (String key : keySet()) {
                result.add(get(key));
            }

            return result;
        } else if (jsonNode instanceof ArrayNode) {
            return toListOfBaseType(jsonNode);
        } else if (!jsonNode.isNull() && !jsonNode.isMissingNode()) {
            return Collections.singleton(toBaseType(jsonNode, key));
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        if (jsonNode instanceof ObjectNode) {
            LinkedHashSet<Entry<String, Object>> result = new LinkedHashSet<>();

            Iterator<Entry<String, JsonNode>> iter = ((ObjectNode) jsonNode).fields();

            while (iter.hasNext()) {
                Entry<String, JsonNode> entry = iter.next();

                result.add(Maps.immutableEntry(entry.getKey(), toBaseType(entry.getValue(), entry.getKey())));
            }

            return result;
        } else {
            return Collections.emptySet();
        }
    }

    private static Map<String, Object> toMap(JsonNode jsonNode) {
        if (jsonNode == null) {
            return null;
        } else if (jsonNode instanceof ObjectNode) {
            ObjectNode objectNode = (ObjectNode) jsonNode;
            Map<String, Object> result = new LinkedHashMap<>(objectNode.size());
            Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();

            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> field = iter.next();

                result.put(field.getKey(), toObject(field.getValue()));
            }

            return result;
        } else {
            Map<String, Object> result = new LinkedHashMap<>(1);

            result.put("_value", toObject(jsonNode));

            return result;
        }
    }

    private static Object toObject(JsonNode jsonNode) {
        if (jsonNode == null) {
            return null;
        } else if (jsonNode instanceof ObjectNode) {
            return toMap(jsonNode);
        } else if (jsonNode instanceof ArrayNode) {
            ArrayNode arrayNode = (ArrayNode) jsonNode;
            List<Object> result = new ArrayList<>(arrayNode.size());

            for (JsonNode child : arrayNode) {
                result.add(toObject(child));
            }

            return result;
        } else if (jsonNode instanceof NullNode) {
            return null;
        } else if (jsonNode instanceof IntNode) {
            return ((NumericNode) jsonNode).asInt();
        } else if (jsonNode instanceof LongNode) {
            return ((NumericNode) jsonNode).asLong();
        } else if (jsonNode instanceof NumericNode) {
            return ((NumericNode) jsonNode).asDouble();
        } else if (jsonNode instanceof BooleanNode) {
            return ((BooleanNode) jsonNode).asBoolean();
        } else if (jsonNode instanceof TextNode) {
            return ((TextNode) jsonNode).asText();
        } else {
            return jsonNode.toString();
        }
    }

}