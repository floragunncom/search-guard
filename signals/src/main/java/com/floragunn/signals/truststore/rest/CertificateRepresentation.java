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
import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.common.time.DateFormatter;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;

public class CertificateRepresentation implements Document<TruststoreRepresentation> {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);
    public static final String FIELD_SERIAL_NUMBER = "serial_number";
    public static final String FIELD_NOT_BEFORE = "not_before";
    public static final String FIELD_NOT_AFTER = "not_after";
    public static final String FIELD_ISSUER = "issuer";
    public static final String FIELD_SUBJECT = "subject";
    public static final String FIELD_PEM = "pem";

    private final String serialNumber;
    private final Instant notBefore;
    private final Instant notAfter;
    private final String issuer;
    private final String subject;
    private final String pem;

    public CertificateRepresentation(String serialNumber, Instant notBefore, Instant notAfter, String issuer,//
        String subject, String pem) {
        this.serialNumber = serialNumber;
        this.notBefore = notBefore;
        this.notAfter = notAfter;
        this.issuer = issuer;
        this.subject = subject;
        this.pem = pem;
    }

    public String getSerialNumber() {
        return serialNumber;
    }

    public Instant getNotBefore() {
        return notBefore;
    }

    public Instant getNotAfter() {
        return notAfter;
    }

    public String getIssuer() {
        return issuer;
    }

    public String getSubject() {
        return subject;
    }

    public String getPem() {
        return pem;
    }


    @Override
    public ImmutableMap<String, Object> toBasicObject() {
        return ImmutableMap.<String, Object>of(FIELD_SERIAL_NUMBER, serialNumber)//
            .with(FIELD_NOT_BEFORE, formatNullableDate(notBefore))//
            .with(FIELD_NOT_AFTER, formatNullableDate(notAfter))//
            .with(FIELD_ISSUER, issuer)//
            .with(FIELD_SUBJECT, subject)//
            .with(FIELD_PEM, pem);
    }

    private String formatNullableDate(TemporalAccessor date) {
        return date != null ? DATE_FORMATTER.format(date) : null;
    }

}
