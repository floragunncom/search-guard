/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.codova.documents;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.floragunn.codova.documents.Parser.Context;

public interface Metadata<T> extends Document<Metadata<T>> {
    String description();

    Class<T> type();

    Parser<T, Parser.Context> parser();

    Map<String, Object> toBasicObject();

    List<Metadata.Attribute<?>> attributes();

    interface Attribute<AttributeType> {
        String name();

        String description();

        Metadata<AttributeType> meta();

        int minCardinality();

        int maxCardinality();

        String cardinalityAsString();

        AttributeType defaultValue();

        Attribute<AttributeType> defaultValue(AttributeType defaultValue);

        static <AttributeType> Metadata.Attribute<AttributeType> required(String name, Metadata<AttributeType> meta, String description) {
            return new MetadataAttributeImpl<>(name, description, meta, 1, 1, "1");
        }

        static <AttributeType> Metadata.Attribute<AttributeType> required(String name, Class<AttributeType> type, String description) {
            return required(name, Metadata.create(type, null, null), description);
        }

        static <AttributeType> Metadata.Attribute<AttributeType> optional(String name, Metadata<AttributeType> meta, String description) {
            return new MetadataAttributeImpl<>(name, description, meta, 0, 1, "0..1");
        }

        static <AttributeType> Metadata.Attribute<AttributeType> optional(String name, Class<AttributeType> type, String description) {
            return optional(name, Metadata.create(type, null, null), description);
        }

        static <AttributeType> Metadata.ListAttribute<AttributeType> list(String name, Metadata<AttributeType> meta, String description) {
            return new MetadataListAttributeImpl<>(name, description, meta, 0, Integer.MAX_VALUE, "0..n");
        }

        static <AttributeType> Metadata.ListAttribute<AttributeType> list(String name, Class<AttributeType> type, String description) {
            return list(name, Metadata.create(type, null, null), description);
        }
    }

    interface ListAttribute<AttributeType> extends Attribute<List<AttributeType>> {
        Metadata<AttributeType> elementMeta();
        ListAttribute<AttributeType> withEmptyListAsDefault();
    }

    static <T> Metadata<T> create(Class<T> type, String description, Parser<T, Parser.Context> parser, Metadata.Attribute<?>... attributes) {
        return create(type, type.getSimpleName(), description, parser, attributes);
    }

    static <T> Metadata<T> create(Class<T> type, String typeName, String description, Parser<T, Parser.Context> parser,
            Metadata.Attribute<?>... attributes) {
        List<Metadata.Attribute<?>> attributesList = Arrays.asList(attributes);
        LinkedHashMap<String, Object> basicObject = new LinkedHashMap<>(4);

        if (description != null) {
            basicObject.put("description", description);
        }

        if (typeName != null) {
            basicObject.put("type", typeName);
        }

        if (attributes != null && attributes.length > 0) {
            LinkedHashMap<String, Object> attributesBasicObject = new LinkedHashMap<>(attributes.length);

            for (Metadata.Attribute<?> attribute : attributes) {
                LinkedHashMap<String, Object> attributeBasicObject = new LinkedHashMap<>(4);

                if (attribute.description() != null) {
                    attributeBasicObject.put("description", attribute.description());
                } else if (attribute.meta().toBasicObject().containsKey("description")) {
                    attributeBasicObject.put("description", attribute.meta().toBasicObject().get("description"));

                }

                attributeBasicObject.put("cardinality", attribute.cardinalityAsString());

                for (Map.Entry<String, Object> entry : attribute.meta().toBasicObject().entrySet()) {
                    if (entry.getKey().equals("description")) {
                        continue;
                    }

                    attributeBasicObject.put(entry.getKey(), entry.getValue());
                }

                attributesBasicObject.put(attribute.name(), attributeBasicObject);
            }

            basicObject.put("attributes", attributesBasicObject);
        }

        return new Metadata<T>() {

            @Override
            public String description() {
                return description;
            }

            @Override
            public List<Metadata.Attribute<?>> attributes() {
                return attributesList;
            }

            @Override
            public Class<T> type() {
                return type;
            }

            @Override
            public Parser<T, Context> parser() {
                return parser;
            }

            @Override
            public Map<String, Object> toBasicObject() {
                return basicObject;
            }
        };
    }
}