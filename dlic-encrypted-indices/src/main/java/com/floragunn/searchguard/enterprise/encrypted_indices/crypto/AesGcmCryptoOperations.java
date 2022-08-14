package com.floragunn.searchguard.enterprise.encrypted_indices.crypto;

import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.Index;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class AesGcmCryptoOperations extends CryptoOperations {
    private static final int GCM_NONCE_LENGTH = 12;
    private static final int GCM_TAG_LENGTH = 16;

    AesGcmCryptoOperations(ClusterService clusterService, Index index, Client client, ThreadContext threadContext, String indexPublicKey, int keySize) throws Exception {
        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
        super(clusterService, index, client, threadContext, indexPublicKey, keySize);

    }

    @Override
    protected byte[] doCrypt(byte[] in, byte[] key, String field, String id, int mode) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, createNonce(field, id, GCM_NONCE_LENGTH));
        cipher.init(mode, keySpec, gcmParameterSpec);
        return cipher.doFinal(in);
    }
}
