package com.floragunn.searchguard.enterprise.encrypted_indices.crypto;

import com.google.crypto.tink.BinaryKeysetReader;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.DeterministicAead;
import com.google.crypto.tink.KeyTemplates;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.daead.DeterministicAeadConfig;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.Index;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class AesSivCryptoOperations extends CryptoOperations {

    private static final byte[] EMPTY_BYTE = new byte[0];

    AesSivCryptoOperations(ClusterService clusterService, Index index, Client client, ThreadContext threadContext, String indexPublicKey, int keySize) throws Exception {
        super(clusterService, index, client, threadContext, indexPublicKey, keySize);
        DeterministicAeadConfig.register();
    }

    @Override
    protected byte[] doCrypt(byte[] in, byte[] key, String field, String id, int mode) throws Exception {

        KeysetHandle keysetHandle = CleartextKeysetHandle.read(BinaryKeysetReader.withBytes(key));

        DeterministicAead daead =
                keysetHandle.getPrimitive(DeterministicAead.class);

        if(mode == Cipher.ENCRYPT_MODE) {
            return daead.encryptDeterministically(in, EMPTY_BYTE);
        } else {
            return daead.decryptDeterministically(in, EMPTY_BYTE);
        }
    }
}
