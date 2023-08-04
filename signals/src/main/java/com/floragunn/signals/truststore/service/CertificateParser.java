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

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.signals.CertificatesParser;
import com.floragunn.signals.truststore.rest.CertificateRepresentation;

import javax.security.auth.x500.X500Principal;
import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

class CertificateParser {

    public ImmutableList<CertificateRepresentation> parse(Collection<String> certificates) throws ConfigValidationException {
        List<CertificateRepresentation> list = new ArrayList<>();
        for (String certificate : certificates) {
            CertificateRepresentation certificateRepresentation = parseCertificate(certificate);
            list.add(certificateRepresentation);
        }
        return ImmutableList.of(list);
    }

    private CertificateRepresentation parseCertificate(String singleCertificate) throws ConfigValidationException {
        X509Certificate x509 = CertificatesParser.parseCertificates(singleCertificate)//
                .stream()//
                .filter(X509Certificate.class::isInstance)//
                .map(X509Certificate.class::cast)//
                .findFirst()//
                .get();
        return new CertificateRepresentation(readSerialNumberAsString(x509),
            dateToLocalDateTime(x509.getNotBefore()), dateToLocalDateTime(x509.getNotAfter()),
            principalToString(x509.getIssuerX500Principal()), principalToString(x509.getSubjectX500Principal()),
            singleCertificate);
    }

    private static String principalToString(X500Principal principal) {
        return principal == null ? null : principal.getName();
    }

    private static String readSerialNumberAsString(X509Certificate x509Certificate) {
        BigInteger serialNumber = x509Certificate.getSerialNumber();
        return serialNumber == null ? null : serialNumber.toString(16);
    }

    private static Instant dateToLocalDateTime(Date date) {
        return date == null ? null : Instant.ofEpochMilli(date.getTime());
    }
}
