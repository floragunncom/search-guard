package com.floragunn.searchguard.enterprise.encrypted_indices.crypto;

import org.opensearch.client.Client;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.Index;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class IndexKeys {

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private byte[] key = new byte[16];

    private Client client;
    private ThreadContext threadContext;

    private Index index;

    public IndexKeys(Index index, Client client, ThreadContext threadContext) {
        this.client = Objects.requireNonNull(client,"client must not be null");
        this.threadContext = Objects.requireNonNull(threadContext, "threadContext must not be null");
        this.index = index;
    }

    public void extractPrivateKeyFromHeader() throws NoSuchAlgorithmException, InvalidKeySpecException {
        String pk = threadContext.getHeader("x-osec-pk");

        assert pk != null;

        PKCS8EncodedKeySpec spec =
                new PKCS8EncodedKeySpec(Base64.getDecoder().decode(pk));
        KeyFactory kf = KeyFactory.getInstance("RSA");
        PrivateKey p = kf.generatePrivate(spec);
    }

    public synchronized byte[] getOrCreateSymmetricKey() {
        System.out.println("Looking for sym key for "+index.getName());

        if(key != null) {
            return key.clone();
        }

        byte[] k = new byte[16];
        SECURE_RANDOM.nextBytes(k);

        key = k;

        return k.clone();
    }
}
