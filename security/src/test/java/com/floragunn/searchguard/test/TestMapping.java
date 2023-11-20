/*
 * Copyright 2024 floragunn GmbH
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

package com.floragunn.searchguard.test;

import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.OrderedImmutableMap;

public class TestMapping implements Document<TestMapping> {
    
    private OrderedImmutableMap<String, Property> properties;

    public TestMapping(Property... properties) {
        this.properties = OrderedImmutableMap.map(ImmutableList.ofArray(properties), (p) -> ImmutableMap.entry(p.name, p));
    }

    public OrderedImmutableMap<String, Property> getProperties() {
        return properties;
    }

    @Override
    public Object toBasicObject() {
        return ImmutableMap.of("properties", this.properties);
    }

    public static class Property implements Document<TestMapping> {
        final String name;
        final String type;
        final String format;

        public Property(String name, String type, String format) {
            this.name = name;
            this.type = type;
            this.format = format;
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.ofNonNull("type", type, "format", format);
        }

    }

}
