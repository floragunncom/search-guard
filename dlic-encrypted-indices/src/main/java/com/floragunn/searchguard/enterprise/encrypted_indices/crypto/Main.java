package com.floragunn.searchguard.enterprise.encrypted_indices.crypto;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Base64;

public class Main {

    public static void main(String[] args) throws Exception {

        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");

        kpg.initialize(2048);
        KeyPair keypair = kpg.generateKeyPair();
        PublicKey publickey = keypair.getPublic();
        PrivateKey privateKey = keypair.getPrivate();

        System.out.println(Base64.getEncoder().encodeToString(publickey.getEncoded()));
        System.out.println(Base64.getEncoder().encodeToString(privateKey.getEncoded()));


    }

}
