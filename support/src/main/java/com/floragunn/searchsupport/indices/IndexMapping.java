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

package com.floragunn.searchsupport.indices;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.searchsupport.util.ImmutableMap;

public class IndexMapping implements Document {

    private final DocNode baseAttributes;
    private final List<Property> properties;

    public IndexMapping(Property... properties) {
        this.properties = Arrays.asList(properties);
        this.baseAttributes = DocNode.EMPTY;
    }

    public IndexMapping(DocNode baseAttributes, Property... properties) {
        this.properties = Arrays.asList(properties);
        this.baseAttributes = baseAttributes;
    }

    public static class Property implements Document {
        private String name;
        private String type;
        private boolean enabled;
        private boolean dynamic;

        Property(String name, String type) {
            this.name = name;
            this.type = type;
            this.enabled = true;
            this.dynamic = false;
        }

        Property(String name, String type, boolean enabled, boolean dynamic) {
            this.name = name;
            this.type = type;
            this.enabled = enabled;
            this.dynamic = dynamic;
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.ofNonNull("type", type, "enabled", enabled ? null : Boolean.FALSE, "dynamic", dynamic ? Boolean.TRUE : null);
        }

    }

    @Override
    public Object toBasicObject() {
        Map<String, Object> result = new LinkedHashMap<>();

        for (Property property : properties) {
            result.put(property.name, property.toBasicObject());
        }

        return this.baseAttributes.with(DocNode.of("properties", result));
    }

    public static class KeywordProperty extends Property {
        public KeywordProperty(String name) {
            super(name, "keyword");
        }
    }

    public static class BinaryProperty extends Property {
        public BinaryProperty(String name) {
            super(name, "binary");
        }
    }

    public static class ObjectProperty extends Property {
        private final List<Property> properties;

        public ObjectProperty(String name, Property... properties) {
            super(name, null);
            this.properties = Arrays.asList(properties);
        }

        @Override
        public Object toBasicObject() {
            DocNode baseDocNode = DocNode.wrap(super.toBasicObject());

            Map<String, Object> result = new LinkedHashMap<>();

            for (Property property : properties) {
                result.put(property.name, property.toBasicObject());
            }

            return baseDocNode.with(DocNode.of("properties", result));
        }

    }

    public static class DisabledIndexProperty extends Property {
        public DisabledIndexProperty(String name) {
            super(name, "object", false, false);
        }
    }

    public static class DynamicIndexMapping extends IndexMapping {
        public DynamicIndexMapping(Property... properties) {
            super(DocNode.of("dynamic", true), properties);
        }
    }

}
