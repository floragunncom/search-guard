package com.floragunn.searchguard.test.helper.certificate;

import org.bouncycastle.cert.X509CertificateHolder;

import java.security.KeyPair;

public class CertificateWithKeyPair {

    private final X509CertificateHolder certificate;
    private final KeyPair keyPair;

    public CertificateWithKeyPair(X509CertificateHolder certificate, KeyPair keyPair) {
        this.certificate = certificate;
        this.keyPair = keyPair;
    }

    public X509CertificateHolder getCertificate() {
        return certificate;
    }

    public KeyPair getKeyPair() {
        return keyPair;
    }
}
