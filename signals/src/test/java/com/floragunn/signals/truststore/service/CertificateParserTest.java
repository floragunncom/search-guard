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
package com.floragunn.signals.truststore.service;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.signals.truststore.rest.CertificateRepresentation;
import com.floragunn.signals.truststore.rest.CreateOrReplaceTruststoreAction;
import com.floragunn.signals.truststore.rest.TruststoreLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import static com.floragunn.signals.truststore.rest.TruststoreLoader.PEM_ONE_CERTIFICATES;
import static com.floragunn.signals.truststore.rest.TruststoreLoader.PEM_TWO_CERTIFICATES;
import static com.floragunn.signals.truststore.rest.TruststoreLoader.loadCertificates;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@RunWith(MockitoJUnitRunner.class)
public class CertificateParserTest {

    @Mock
    private UnparsedDocument<?> unparsedDocument;

    @Test
    public void shouldParseSingleCertificate() throws ConfigValidationException {
        CertificateParser certificateParser = new CertificateParser();
        String certificateInPemFormat = loadCertificates(PEM_ONE_CERTIFICATES);
        List<CertificateRepresentation> certificateRepresentations = certificateParser.parse(singletonList(certificateInPemFormat));

        assertThat(certificateRepresentations, hasSize(1));
        CertificateRepresentation certificate = certificateRepresentations.get(0);
        assertThat(certificate.getPem(), equalTo(certificateInPemFormat));
        assertThat(certificate.getSerialNumber(), equalTo("1"));
        assertThat(certificate.getIssuer(), equalTo(TruststoreLoader.CERT_1_ISSUER));
        assertThat(certificate.getSubject(), equalTo(TruststoreLoader.CERT_1_SUBJECT));
        Instant notBefore = ZonedDateTime.of(LocalDateTime.of(2023, 5, 18, 15, 43, 30), ZoneOffset.UTC).toInstant();
        assertThat(certificate.getNotBefore(), equalTo(notBefore));
        Instant notAfter = ZonedDateTime.of(LocalDateTime.of(2023, 6, 17, 15, 43, 30), ZoneOffset.UTC).toInstant();
        assertThat(certificate.getNotAfter(), equalTo(notAfter));
    }

    @Test
    public void shouldParseMultipleCertificates() throws ConfigValidationException {
        String certificateInPemFormat = loadCertificates(PEM_TWO_CERTIFICATES);
        DocNode docNode = DocNode.of("name", "name", "pem", certificateInPemFormat);
        Mockito.when(unparsedDocument.parseAsDocNode()).thenReturn(docNode);
        CreateOrReplaceTruststoreAction.CreateOrReplaceTruststoreRequest
            uploadCertificatesRequest = new CreateOrReplaceTruststoreAction.CreateOrReplaceTruststoreRequest("cert-id-1", unparsedDocument);
        List<String> splitCertificates = uploadCertificatesRequest.getCertificates();
        CertificateParser certificateParser = new CertificateParser();

        List<CertificateRepresentation> certificateRepresentations = certificateParser.parse(splitCertificates);

        assertThat(certificateRepresentations, hasSize(2));
        CertificateRepresentation certificate = certificateRepresentations.get(0);
        assertThat(certificate.getPem(), equalTo(splitCertificates.get(0)));
        assertThat(certificate.getSerialNumber(), equalTo("1"));
        assertThat(certificate.getIssuer(), equalTo(TruststoreLoader.CERT_1_ISSUER));
        assertThat(certificate.getSubject(), equalTo(TruststoreLoader.CERT_1_SUBJECT));
        Instant notBefore = ZonedDateTime.of(LocalDateTime.of(2023, 5, 18, 15, 43, 30), ZoneOffset.UTC).toInstant();
        assertThat(certificate.getNotBefore(), equalTo(notBefore));
        Instant notAfter = ZonedDateTime.of(LocalDateTime.of(2023, 6, 17, 15, 43, 30), ZoneOffset.UTC).toInstant();
        assertThat(certificate.getNotAfter(), equalTo(notAfter));
    }

    @Test(expected = ConfigValidationException.class)
    public void shouldNotParseCertificate() throws ConfigValidationException {
        CertificateParser certificateParser = new CertificateParser();
        String pem = "-----BEGIN CERTIFICATE-----iaminvalidcertificate-----END CERTIFICATE-----";
        List<String> splitCertificates = singletonList(pem);

        certificateParser.parse(splitCertificates);
    }

}