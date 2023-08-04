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
package com.floragunn.signals;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.ValidationError;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Collection;

public class CertificatesParser {

    private static final Logger log = LogManager.getLogger(CertificatesParser.class);

    public static Collection<? extends Certificate> parseCertificates(String pem) throws ConfigValidationException {
        if (pem == null) {
            return null;
        }

        InputStream inputStream = new ByteArrayInputStream(pem.getBytes(StandardCharsets.US_ASCII));

        CertificateFactory fact;
        try {
            fact = CertificateFactory.getInstance("X.509");
        } catch (CertificateException e) {
            log.error("Could not initialize X.509", e);
            throw new ConfigValidationException(new ValidationError(null, "Could not initialize X.509").cause(e));
        }

        try {
            return fact.generateCertificates(inputStream);
        } catch (CertificateException e) {
            throw new ConfigValidationException(new InvalidAttributeValue(null, pem, "PEM File") //
                .message("Invalid PEM certificate. " + e.getMessage()).cause(e));
        }

    }

    public static KeyStore toTruststore(String trustCertificatesAliasPrefix, Collection<? extends Certificate> certificates)
        throws ConfigValidationException {

        if (certificates == null) {
            return null;
        }

        KeyStore keyStore;

        try {
            keyStore = KeyStore.getInstance("JKS");
            keyStore.load(null);
        } catch (Exception e) {
            log.error("Could not initialize JKS KeyStore", e);
            throw new RuntimeException("Could not create JKS KeyStore", e);
        }

        int i = 0;

        for (Certificate cert : certificates) {

            try {
                keyStore.setCertificateEntry(trustCertificatesAliasPrefix + "_" + i, cert);
            } catch (KeyStoreException e) {
                throw new RuntimeException("Cannot add certificates to JKS keystore", e);
            }
            i++;
        }

        return keyStore;
    }
}
