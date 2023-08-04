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
package com.floragunn.signals.truststore.service.persistence;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchsupport.indices.IndexMapping.DateProperty;
import com.floragunn.searchsupport.indices.IndexMapping.DynamicIndexMapping;
import com.floragunn.searchsupport.indices.IndexMapping.LongProperty;
import com.floragunn.searchsupport.indices.IndexMapping.ObjectProperty;
import com.floragunn.searchsupport.indices.IndexMapping.TextProperty;
import com.floragunn.searchsupport.indices.IndexMapping.TextWithKeywordProperty;

import java.time.Instant;
import java.util.List;
import java.util.Objects;


public class TruststoreData implements Document<TruststoreData> {
    public static final String FIELD_NAME = "name";
    public static final String FIELD_PEM = "raw_pem";
    public static final String FIELD_CERTIFICATES = "certificates";
    public static final String FIELD_STORE_TIME = "store_time";
    public static final String FIELD_SIZE = "size";

    public final static DynamicIndexMapping MAPPINGS = new DynamicIndexMapping(
        new TextWithKeywordProperty(TruststoreData.FIELD_NAME),//
        new TextProperty(TruststoreData.FIELD_PEM),//
        new DateProperty(TruststoreData.FIELD_STORE_TIME),//
        new LongProperty(TruststoreData.FIELD_SIZE),//
        new ObjectProperty("certificates",//
            new TextWithKeywordProperty(CertificateData.FIELD_ISSUER),//
            new TextWithKeywordProperty(CertificateData.FIELD_SUBJECT),//
            new TextWithKeywordProperty(CertificateData.FIELD_SERIAL_NUMBER),//
            new TextProperty(CertificateData.FIELD_PEM),//
            new DateProperty(CertificateData.FIELD_NOT_AFTER),//
            new DateProperty(CertificateData.FIELD_NOT_BEFORE)//
        )
    );
    private final String id;
    private final String name;
    private final String pem;

    private final Instant storeTime;
    private final ImmutableList<CertificateData> certificates;

    public TruststoreData(String id, String name, Instant storeTime, String pem, ImmutableList<CertificateData> certificates) {
        this.id = id;
        this.name = name;
        this.pem = pem;
        this.certificates = Objects.requireNonNull(certificates, "Certificates are required");
        this.storeTime = storeTime;
    }

    TruststoreData(String id, DocNode document) throws ConfigValidationException {
        Objects.requireNonNull(document, "Doc node is required to parse truststore data");
        this.id = id;
        this.name = document.getAsString(FIELD_NAME);
        this.storeTime = Instant.parse(document.getAsString(FIELD_STORE_TIME));
        this.pem = document.getAsString(FIELD_PEM);
        this.certificates = document.getAsListFromNodes(FIELD_CERTIFICATES, CertificateData::parse);
    }

    @Override
    public ImmutableMap<String, Object> toBasicObject() {
        return ImmutableMap.of(FIELD_NAME, name, FIELD_STORE_TIME, storeTime, FIELD_SIZE, certificates.size(), FIELD_PEM, pem,//
            FIELD_CERTIFICATES, certificates);
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

    public List<CertificateData> getCertificates() {
        return certificates;
    }
}
