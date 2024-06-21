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

package com.floragunn.searchguard.test.helper.certificate;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import com.floragunn.searchguard.test.helper.certificate.utils.CertificateAndPrivateKeyWriter;

public class TestCertificate {
    private static final Provider DEFAULT_SECURITY_PROVIDER = new BouncyCastleProvider();

    private final X509CertificateHolder certificate;
    private final KeyPair keyPair;
    private final String privateKeyPassword;
    private final File certificateFile;
    private final File privateKeyFile;
    private final CertificateType certificateType;
    private final File directory;
    private String certificateString;
    private File jksFile;

    public TestCertificate(X509CertificateHolder certificate, KeyPair keyPair, String privateKeyPassword, CertificateType certificateType,
            File directory) {
        this.certificate = certificate;
        this.keyPair = keyPair;
        this.privateKeyPassword = privateKeyPassword;
        this.certificateType = certificateType;
        this.directory = directory;

        switch (certificateType) {
        case ca: {
            this.certificateFile = new File(directory, "ca.pem");
            this.privateKeyFile = new File(directory, "ca-key.pem");
            break;
        }
        case admin_client: {
            this.certificateFile = new File(directory, String.format("admin-client-%s.pem", certificate.getSubject()));
            this.privateKeyFile = new File(directory, String.format("admin-client-%s-key.pem", certificate.getSubject()));
            break;
        }
        case client: {
            this.certificateFile = new File(directory, String.format("client-%s.pem", certificate.getSubject()));
            this.privateKeyFile = new File(directory, String.format("client-%s-key.pem", certificate.getSubject()));
            break;
        }
        case node_rest: {
            this.certificateFile = new File(directory, String.format("node-%s-rest.pem", certificate.getSubject()));
            this.privateKeyFile = new File(directory, String.format("node-%s-rest-key.pem", certificate.getSubject()));
            break;
        }
        case node_transport: {
            this.certificateFile = new File(directory, String.format("node-%s-transport.pem", certificate.getSubject()));
            this.privateKeyFile = new File(directory, String.format("node-%s-transport-key.pem", certificate.getSubject()));
            break;
        }
        case node_transport_rest: {
            this.certificateFile = new File(directory, String.format("node-%s-transport-rest.pem", certificate.getSubject()));
            this.privateKeyFile = new File(directory, String.format("node-%s-transport-rest-key.pem", certificate.getSubject()));
            break;
        }
        case other: {
            this.certificateFile = new File(directory, String.format("cert-%s.pem", certificate.getSubject()));
            this.privateKeyFile = new File(directory, String.format("cert-%s-key.pem", certificate.getSubject()));
            break;
        }
        default:
            throw new RuntimeException("Not supported");
        }
        certificateString = CertificateAndPrivateKeyWriter.writeCertificate(certificate);
        CertificateAndPrivateKeyWriter.saveCertificate(certificateFile, certificate);
        CertificateAndPrivateKeyWriter.savePrivateKey(privateKeyFile, keyPair.getPrivate(), privateKeyPassword);
    }

    public X509CertificateHolder getCertificate() {
        return certificate;
    }

    public KeyPair getKeyPair() {
        return keyPair;
    }

    public String getPrivateKeyPassword() {
        return privateKeyPassword;
    }

    public File getCertificateFile() {
        return certificateFile;
    }
    
    public String getCertificateString() {
        return certificateString;
    }

    public File getPrivateKeyFile() {
        return privateKeyFile;
    }

    public CertificateType getCertificateType() {
        return certificateType;
    }

    public File getJksFile() {
        if (jksFile == null) {
            jksFile = saveAsJksFile();
        }

        return jksFile;
    }

    public TestCertificate at(File directory) {
        return new TestCertificate(certificate, keyPair, privateKeyPassword, certificateType, directory);
    }
    
    private File saveAsJksFile() {
        try {
            File file = new File(directory, String.format("cert-%s.pem", certificate.getSubject()));
            X509Certificate x509certificate = new JcaX509CertificateConverter().setProvider(DEFAULT_SECURITY_PROVIDER).getCertificate(certificate);

            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(null, null);
            keyStore.setKeyEntry("cert", keyPair.getPrivate(), privateKeyPassword.toCharArray(), new X509Certificate[] { x509certificate });
            keyStore.store(new FileOutputStream(file), privateKeyPassword.toCharArray());
            return file;
        } catch (KeyStoreException | CertificateException | IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
