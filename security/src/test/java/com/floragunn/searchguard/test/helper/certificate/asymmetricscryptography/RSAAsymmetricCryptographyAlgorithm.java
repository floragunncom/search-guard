/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.test.helper.certificate.asymmetricscryptography;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;

public class RSAAsymmetricCryptographyAlgorithm implements AsymmetricCryptographyAlgorithm {

    private static final Logger log = LogManager.getLogger(RSAAsymmetricCryptographyAlgorithm.class);
    private final KeyPairGenerator generator;

    public RSAAsymmetricCryptographyAlgorithm(Provider securityProvider, int keySize) {
        try {
            this.generator = KeyPairGenerator.getInstance("RSA", securityProvider);
            log.debug("Initialize key pair generator with keySize: {}", keySize);
            this.generator.initialize(keySize);
        } catch (NoSuchAlgorithmException e) {
            log.error("Error while initializing", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getSignatureAlgorithmName() {
        return "SHA256withRSA";
    }

    @Override
    public KeyPair generateKeyPair() {
        return generator.generateKeyPair();
    }
}
