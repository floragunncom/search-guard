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

import java.util.Collections;
import java.util.List;

import com.floragunn.codova.documents.Metadata.Attribute;
import com.floragunn.codova.documents.Metadata.ListAttribute;

class MetadataAttributeImpl<T> implements Metadata.Attribute<T> {

    private final String name;
    private final String description;
    private final Metadata<T> meta;
    private final int minCardinality;
    private final int maxCardinality;
    private final String cardinalityAsString;
    private final T defaultValue;

    MetadataAttributeImpl(String name, String description, Metadata<T> meta, int minCardinality, int maxCardinality, String cardinalityAsString) {
        super();
        this.name = name;
        this.description = description;
        this.meta = meta;
        this.minCardinality = minCardinality;
        this.maxCardinality = maxCardinality;
        this.cardinalityAsString = cardinalityAsString;
        this.defaultValue = null;
    }

    MetadataAttributeImpl(String name, String description, Metadata<T> meta, int minCardinality, int maxCardinality, String cardinalityAsString,
            T defaultValue) {
        this.name = name;
        this.description = description;
        this.meta = meta;
        this.minCardinality = minCardinality;
        this.maxCardinality = maxCardinality;
        this.cardinalityAsString = cardinalityAsString;
        this.defaultValue = defaultValue;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String description() {
        return description;
    }

    @Override
    public Metadata<T> meta() {
        return meta;
    }

    @Override
    public int minCardinality() {
        return minCardinality;
    }

    @Override
    public int maxCardinality() {
        return maxCardinality;
    }

    @Override
    public String cardinalityAsString() {
        return cardinalityAsString;
    }

    @Override
    public T defaultValue() {
        return defaultValue;
    }

    @Override
    public Attribute<T> defaultValue(T defaultValue) {
        return new MetadataAttributeImpl<>(name, description, meta, minCardinality, maxCardinality, cardinalityAsString, defaultValue);
    }

}

class MetadataListAttributeImpl<T> extends MetadataAttributeImpl<List<T>> implements Metadata.ListAttribute<T> {

    private final Metadata<T> elementMeta;

    public MetadataListAttributeImpl(String name, String description, Metadata<T> elementMeta, int minCardinality, int maxCardinality,
            String cardinalityAsString, List<T> defaultValue) {
        super(name, description, null, minCardinality, maxCardinality, cardinalityAsString, defaultValue);
        this.elementMeta = elementMeta;
    }

    public MetadataListAttributeImpl(String name, String description, Metadata<T> elementMeta, int minCardinality, int maxCardinality,
            String cardinalityAsString) {
        super(name, description, null, minCardinality, maxCardinality, cardinalityAsString);
        this.elementMeta = elementMeta;
    }

    @Override
    public Metadata<T> elementMeta() {
        return elementMeta;
    }

    @Override
    public ListAttribute<T> withEmptyListAsDefault() {
        return new MetadataListAttributeImpl<T>(name(), description(), elementMeta, minCardinality(), maxCardinality(), cardinalityAsString(),
                Collections.emptyList());
    }

}
