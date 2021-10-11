package com.floragunn.searchguard.test.helper.certificate.asymmetricscryptography;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.*;
import java.security.spec.ECGenParameterSpec;

public class ECDSAAsymmetricCryptographyAlgorithm implements AsymmetricCryptographyAlgorithm {

    private static final Logger log = LogManager.getLogger(ECDSAAsymmetricCryptographyAlgorithm.class);
    private final KeyPairGenerator generator;

    public ECDSAAsymmetricCryptographyAlgorithm(Provider securityProvider, String ellipticCurve) {
        try {
            this.generator = KeyPairGenerator.getInstance("EC", securityProvider);
            log.info("Initialize key pair generator with elliptic curve: {}", ellipticCurve);
            ECGenParameterSpec ecsp = new ECGenParameterSpec(ellipticCurve);
            generator.initialize(ecsp);
        } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException e) {
            log.error("Error while initializing", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getSignatureAlgorithmName() {
        return "SHA256withECDSA";
    }

    @Override
    public KeyPair generateKeyPair() {
        log.info("Create key pair");
        return generator.generateKeyPair();
    }

}
