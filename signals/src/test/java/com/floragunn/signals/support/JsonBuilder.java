package com.floragunn.signals.support;

import java.io.IOException;
import java.math.BigDecimal;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.searchguard.DefaultObjectMapper;

public class JsonBuilder {

    public static class Object {
        private final ObjectNode node = JsonNodeFactory.instance.objectNode();

        public JsonBuilder.Object attr(String name, String value) {
            node.put(name, value);
            return this;
        }

        public JsonBuilder.Object attr(String name, boolean value) {
            node.put(name, value);
            return this;
        }

        public JsonBuilder.Object attr(String name, int value) {
            node.put(name, value);
            return this;
        }

        public JsonBuilder.Object attr(String name, float value) {
            node.put(name, value);
            return this;
        }

        public JsonBuilder.Object attr(String name, double value) {
            node.put(name, value);
            return this;
        }

        public JsonBuilder.Object attr(String name, BigDecimal value) {
            node.put(name, value);
            return this;
        }

        public JsonBuilder.Object attr(String name, JsonBuilder.Array array) {
            node.set(name, array.node);
            return this;
        }

        public JsonBuilder.Object attr(String name, JsonBuilder.Object object) {
            node.set(name, object.node);
            return this;
        }

        public ObjectNode getNode() {
            return node;
        }

        public String toJsonString() {
            try {
                return DefaultObjectMapper.writeJsonTree(node);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class Array {
        private final ArrayNode node = JsonNodeFactory.instance.arrayNode();

        public Array(java.lang.Object... elements) {
            for (java.lang.Object element : elements) {
                if (element instanceof String) {
                    node.add((String) element);
                } else if (element instanceof Integer) {
                    node.add(((Integer) element).intValue());
                } else if (element instanceof Long) {
                    node.add(((Long) element).longValue());
                } else if (element instanceof Short) {
                    node.add(((Short) element).longValue());
                } else if (element instanceof Float) {
                    node.add(((Float) element).floatValue());
                } else if (element instanceof Double) {
                    node.add(((Double) element).doubleValue());
                } else if (element instanceof BigDecimal) {
                    node.add((BigDecimal) element);
                } else if (element instanceof Boolean) {
                    node.add(((Boolean) element).booleanValue());
                } else if (element instanceof JsonBuilder.Object) {
                    node.add(((JsonBuilder.Object) element).node);
                } else if (element == null) {
                    node.add(JsonNodeFactory.instance.nullNode());
                } else {
                    node.add(String.valueOf(element));
                }
            }
        }

        public ArrayNode getNode() {
            return node;
        }

        public String toJsonString() {
            try {
                return DefaultObjectMapper.writeJsonTree(node);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
