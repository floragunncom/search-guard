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

package com.floragunn.searchguard.test.helper.certificate.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.StringWriter;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509CRL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.openssl.PKCS8Generator;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8EncryptorBuilder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.OutputEncryptor;
import org.bouncycastle.util.io.pem.PemGenerationException;
import org.bouncycastle.util.io.pem.PemObject;

public class CertificateAndPrivateKeyWriter {

    private static final Logger log = LogManager.getLogger(CertificateAndPrivateKeyWriter.class);
    private static final SecureRandom secureRandom = new SecureRandom();

    public static String writeCertificate(X509CertificateHolder certificate) {
        StringWriter stringWriter = new StringWriter();
        try (JcaPEMWriter writer = new JcaPEMWriter(stringWriter)) {
            writer.writeObject(certificate);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return stringWriter.toString();
    }
    
    public static void saveCertificate(File certificateFile, X509CertificateHolder certificate) {
        try (JcaPEMWriter writer = new JcaPEMWriter(new FileWriter(certificateFile))) {
            writer.writeObject(certificate);
        } catch (Exception e) {
            log.error("Error while writing certificate to file: {}", certificateFile.getAbsolutePath(), e);
            throw new RuntimeException(String.format("Error while writing to file: %s", certificateFile.getAbsolutePath()), e);
        }
    }

    public static void saveCrl(File crlFile, X509CRL crl) {
        try (JcaPEMWriter writer = new JcaPEMWriter(new FileWriter(crlFile))) {
            writer.writeObject(crl);
        } catch (Exception e) {
            log.error("Error while writing crl to file: {}", crlFile.getAbsolutePath(), e);
            throw new RuntimeException(String.format("Error while writing to file: %s", crlFile.getAbsolutePath()), e);
        }
    }

    public static void savePrivateKey(File privateKeyFile, PrivateKey privateKey, String privateKeyPassword) {
        try (JcaPEMWriter writer = new JcaPEMWriter(new FileWriter(privateKeyFile))) {
            writer.writeObject(createPkcs8PrivateKeyPem(privateKey, privateKeyPassword));
        } catch (Exception e) {
            log.error("Error while writing private key to file: {}", privateKeyFile.getAbsolutePath(), e);
            throw new RuntimeException(String.format("Error while writing private key to file: %s", privateKeyFile.getAbsolutePath()), e);
        }
    }

    private static PemObject createPkcs8PrivateKeyPem(PrivateKey privateKey, String password) {
        try {
            OutputEncryptor outputEncryptor = getPasswordEncryptor(password);
            return new PKCS8Generator(PrivateKeyInfo.getInstance(privateKey.getEncoded()), outputEncryptor).generate();
        } catch (PemGenerationException | OperatorCreationException e) {
            log.error("Creating PKCS8 private key failed", e);
            throw new RuntimeException("Creating PKCS8 private key failed", e);
        }
    }

    private static OutputEncryptor getPasswordEncryptor(String password) throws OperatorCreationException {
        if (password != null) {
            JceOpenSSLPKCS8EncryptorBuilder encryptorBuilder = new JceOpenSSLPKCS8EncryptorBuilder(PKCS8Generator.PBE_SHA1_3DES);
            encryptorBuilder.setRandom(secureRandom);
            encryptorBuilder.setPassword(password.toCharArray());
            return encryptorBuilder.build();
        }
        return null;
    }
}
