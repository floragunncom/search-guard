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

package com.floragunn.signals.proxy.rest;

import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.signals.proxy.service.persistence.ProxyData;

public class ProxyRepresentation implements Document<ProxyRepresentation> {

    private static final String FIELD_ID = "id";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_URI = "uri";

    public static ProxyRepresentation of(ProxyData proxyData) {
        return new ProxyRepresentation(proxyData.getId(), proxyData.getName(), proxyData.getUri());
    }

    private ProxyRepresentation(String id, String name, String uri) {
        this.id = id;
        this.name = name;
        this.uri = uri;
    }

    private final String id;
    private final String name;
    private final String uri;

    @Override
    public ImmutableMap<String, Object> toBasicObject() {
        return ImmutableMap.of(FIELD_ID, id, FIELD_NAME, name, FIELD_URI, uri);
    }
}
