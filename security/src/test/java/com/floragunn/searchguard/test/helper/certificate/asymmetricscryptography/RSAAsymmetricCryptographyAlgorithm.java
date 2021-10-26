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
            log.info("Initialize key pair generator with keySize: {}", keySize);
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
        log.info("Create key pair");
        return generator.generateKeyPair();
    }
}
