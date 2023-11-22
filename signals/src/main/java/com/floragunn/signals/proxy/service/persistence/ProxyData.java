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

package com.floragunn.signals.proxy.service.persistence;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchsupport.indices.IndexMapping;

import java.time.Instant;
import java.util.Objects;

public class ProxyData implements Document<ProxyData> {
    public static final String FIELD_NAME = "name";
    public static final String FIELD_URI = "uri";
    public static final String FIELD_STORE_TIME = "store_time";

    public final static IndexMapping.DynamicIndexMapping MAPPINGS = new IndexMapping.DynamicIndexMapping(
            new IndexMapping.TextWithKeywordProperty(ProxyData.FIELD_NAME),
            new IndexMapping.TextWithKeywordProperty(ProxyData.FIELD_URI),
            new IndexMapping.DateProperty(ProxyData.FIELD_STORE_TIME)
    );

    private final String id;
    private final String name;
    private final String uri;
    private final Instant storeTime;

    public ProxyData(String id, String name, String uri) {
        this.id = id;
        this.uri = uri;
        this.name = name;
        this.storeTime = Instant.now();
    }

    public ProxyData(String id, DocNode document) {
        Objects.requireNonNull(document, "Document containing proxy data is required");
        this.id = id;
        this.name = document.getAsString(FIELD_NAME);
        this.uri = document.getAsString(FIELD_URI);
        this.storeTime = Instant.now();
    }

    @Override
    public ImmutableMap<String, Object> toBasicObject() {
        return ImmutableMap.of(FIELD_NAME, name, FIELD_URI, uri, FIELD_STORE_TIME, storeTime);
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getUri() {
        return uri;
    }
}
