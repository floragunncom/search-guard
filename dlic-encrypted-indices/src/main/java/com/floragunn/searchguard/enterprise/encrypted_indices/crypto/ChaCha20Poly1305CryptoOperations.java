package com.floragunn.searchguard.enterprise.encrypted_indices.crypto;

import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.Index;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class ChaCha20Poly1305CryptoOperations extends CryptoOperations {
    private static final int NONCE_LENGTH = 12;

    ChaCha20Poly1305CryptoOperations(ClusterService clusterService, Index index, Client client, ThreadContext threadContext, String indexPublicKey, int keySize) throws Exception {
        super(clusterService, index, client, threadContext, indexPublicKey, keySize);
    }

    @Override
    protected byte[] doCrypt(byte[] in, byte[] key, String field, String id, int mode) throws Exception {
        Cipher cipher = Cipher.getInstance("ChaCha20-Poly1305");
        SecretKeySpec keySpec = new SecretKeySpec(key, "ChaCha20");
        IvParameterSpec ivParameterSpec = new IvParameterSpec(createNonce(field, id, NONCE_LENGTH));
        cipher.init(mode, keySpec, ivParameterSpec);
        return cipher.doFinal(in);
    }
}
