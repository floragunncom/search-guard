/*
 * Copyright 2020-2023 floragunn GmbH
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
package com.floragunn.signals.truststore.rest;

import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;

import java.util.Objects;

public class TruststoreRepresentation implements Document<TruststoreRepresentation> {
    public static final String FIELD_ID = "id";
    public static final String FIELD_NAME = "name";
    public static final String FIELD_CERTIFICATES = "certificates";
    public static final String FIELD_PEM = "raw_pem";
    private final String id;
    private final String name;
    private final String pem;
    private final ImmutableList<CertificateRepresentation> certificates;

    public TruststoreRepresentation(String id, String name, String pem, ImmutableList<CertificateRepresentation> certificates) {
        this.id = id;
        this.name = name;
        this.pem = pem;
        this.certificates = Objects.requireNonNull(certificates, "Certificates list must not be null");
    }

    @Override
    public ImmutableMap<String, Object> toBasicObject() {
        return ImmutableMap.of(FIELD_ID, id, FIELD_NAME, name, FIELD_PEM, pem, FIELD_CERTIFICATES, certificates);
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getPem() {
        return pem;
    }

    public ImmutableList<CertificateRepresentation> getCertificates() {
        return certificates;
    }
}
