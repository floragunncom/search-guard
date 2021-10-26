package com.floragunn.searchguard.test.helper.certificate.asymmetricscryptography;

import java.security.KeyPair;

public interface AsymmetricCryptographyAlgorithm {

    String getSignatureAlgorithmName();

    KeyPair generateKeyPair();

}
