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

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.UnparsedDocument;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class UploadCertificatesRequestTest {

    private final static String ENCAPSULATION_BOUNDARY_BEGINNING = "-----BEGIN CERTIFICATE-----";
    private final static String ENCAPSULATION_BOUNDARY_END = "-----END CERTIFICATE-----";

    private final static String CERTIFICATE_PATTERN = "\\A" + ENCAPSULATION_BOUNDARY_BEGINNING + "[A-Za-z0-9=+/\\v]+" + //
        ENCAPSULATION_BOUNDARY_END + "\\z";

    @Test
    public void shouldSplitOneCertificate() {
        String pemCertificates = TruststoreLoader.loadCertificates(TruststoreLoader.PEM_ONE_CERTIFICATES);
        CreateOrReplaceTruststoreAction.CreateOrReplaceTruststoreRequest uploadCertificatesRequest = request(pemCertificates);

        List<String> certificates = uploadCertificatesRequest.getCertificates();

        assertThat(certificates, hasSize(1));
        assertThat(certificates.get(0).matches(CERTIFICATE_PATTERN), equalTo(true));
    }

    @Test
    public void shouldSplitTwoCertificates() {
        String pemCertificates = TruststoreLoader.loadCertificates(TruststoreLoader.PEM_TWO_CERTIFICATES);
        CreateOrReplaceTruststoreAction.CreateOrReplaceTruststoreRequest uploadCertificatesRequest = request(pemCertificates);

        List<String> certificates = uploadCertificatesRequest.getCertificates();

        assertThat(certificates, hasSize(2));
        assertThat(certificates.get(0).matches(CERTIFICATE_PATTERN), equalTo(true));
        assertThat(certificates.get(1).matches(CERTIFICATE_PATTERN), equalTo(true));
    }

    @Test
    public void shouldSplitCertificates() {
        String pemCertificates = TruststoreLoader.loadCertificates(TruststoreLoader.PEM_THREE_CERTIFICATES);
        CreateOrReplaceTruststoreAction.CreateOrReplaceTruststoreRequest uploadCertificatesRequest = request(pemCertificates);

        List<String> certificates = uploadCertificatesRequest.getCertificates();

        assertThat(certificates, hasSize(3));
        assertThat(certificates.get(0).matches(CERTIFICATE_PATTERN), equalTo(true));
        assertThat(certificates.get(1).matches(CERTIFICATE_PATTERN), equalTo(true));
        assertThat(certificates.get(2).matches(CERTIFICATE_PATTERN), equalTo(true));
    }

    @Test
    public void shouldSplitCertificatesWithoutNewLines() {
        String pemCertificates = TruststoreLoader.loadCertificates(TruststoreLoader.PEM_THREE_CERTIFICATES)//
            .replaceAll("\\v", "");
        CreateOrReplaceTruststoreAction.CreateOrReplaceTruststoreRequest uploadCertificatesRequest = request(pemCertificates);

        List<String> certificates = uploadCertificatesRequest.getCertificates();

        assertThat(certificates, hasSize(3));
        assertThat(certificates.get(0).matches(CERTIFICATE_PATTERN), equalTo(true));
        assertThat(certificates.get(1).matches(CERTIFICATE_PATTERN), equalTo(true));
        assertThat(certificates.get(2).matches(CERTIFICATE_PATTERN), equalTo(true));
    }

    private CreateOrReplaceTruststoreAction.CreateOrReplaceTruststoreRequest request(String pem) {
        UnparsedDocument<?> document = Mockito.mock(UnparsedDocument.class);
        DocNode docNode = DocNode.of("name", "my name is certificate", "pem", pem);
        try {
            Mockito.when(document.parseAsDocNode()).thenReturn(docNode);
            return new CreateOrReplaceTruststoreAction.CreateOrReplaceTruststoreRequest("id", document);
        } catch (DocumentParseException e) {
            throw new RuntimeException("Cannot create UploadCertificateRequest", e);
        }
    }
}