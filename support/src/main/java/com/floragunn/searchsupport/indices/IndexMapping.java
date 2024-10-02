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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.ImmutableMap;

public class IndexMapping implements Document<IndexMapping> {

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

    public static class Property implements Document<Property> {
        private String name;
        private String type;
        private boolean enabled;
        private boolean dynamic;

        /**
         * Add support for multi fields, so that it is possible to index one fields a few times with a usage of various data types.
         * Most common use case is to index String with the following types <code>text</code> and <code>keyword</code>.
         *
         * @see TextWithKeywordProperty
         */
        private final List<Property> fields;

        Property(String name, String type) {
            this.name = name;
            this.type = type;
            this.enabled = true;
            this.dynamic = false;
            this.fields = new ArrayList<>();
        }

        Property(String name, String type, boolean enabled, boolean dynamic) {
            this.name = name;
            this.type = type;
            this.enabled = enabled;
            this.dynamic = dynamic;
            this.fields = new ArrayList<>();
        }

        @Override
        public Object toBasicObject() {
            Map<String, Object> fieldsMappings = new LinkedHashMap<>();

            for (Property property : fields) {
                fieldsMappings.put(property.name, property.toBasicObject());
            }
            ImmutableMap<String, Object> mappingsMap = ImmutableMap.ofNonNull("type", type, "enabled", //
                enabled ? null : Boolean.FALSE, "dynamic", dynamic ? Boolean.TRUE : null);
            return fieldsMappings.isEmpty() ? mappingsMap : mappingsMap.with("fields", fieldsMappings);
        }

        public Property withField(Property field) {
            fields.add(field);
            return this;
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

    public static class TextProperty extends Property {

        public TextProperty(String name) {
            super(name, "text");
        }
    }

    public static class LongProperty extends Property {
        public LongProperty(String name) {
            super(name, "long");
        }
    }

    public static class IntegerProperty extends Property {
        public IntegerProperty(String name) {
            super(name, "integer");
        }
    }

    /**
     * Fields which is indexed twice using the following data types text, keyword. Such field can be used to perform full text search just
     * with field name and match query. When the postfix <code>.keyword</code> is appended to the field name then it is possible to search
     * for the field exact value with terms query.
     */
    public static class TextWithKeywordProperty extends Property {
        public TextWithKeywordProperty(String name) {
            super(name, "text");
            withField(new KeywordProperty("keyword"));
        }
    }

    public static class DateProperty extends Property {

        public DateProperty(String name) {
            super(name, "date");
        }
    }

    public static class BinaryProperty extends Property {
        public BinaryProperty(String name) {
            super(name, "binary");
        }
    }

    public static class BooleanProperty extends Property {
        public BooleanProperty(String name) {
            super(name, "boolean");
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
