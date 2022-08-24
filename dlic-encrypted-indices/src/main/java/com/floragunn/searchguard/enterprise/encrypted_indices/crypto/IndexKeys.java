package com.floragunn.searchguard.enterprise.encrypted_indices.crypto;

import com.floragunn.searchguard.enterprise.encrypted_indices.utils.KeyPairUtil;
import com.google.crypto.tink.BinaryKeysetReader;
import com.google.crypto.tink.BinaryKeysetWriter;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.KeyTemplates;
import com.google.crypto.tink.KeysetHandle;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.lucene.util.ArrayUtil;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;

import javax.crypto.Cipher;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

import static com.floragunn.searchguard.enterprise.encrypted_indices.utils.KeyPairUtil.isKeyPair;

public final class IndexKeys {

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static ByteBuffer secure = ByteBuffer.allocateDirect(32);

    static {
        byte[] b = new byte[32];
        SECURE_RANDOM.nextBytes(b);
        secure.put(b);
        Arrays.fill(b, (byte) 0);
        b = null;
    }

    public static final String INDEX = ".osei_keys";

    public static final String NODE_KEYS_INDEX = ".osei_nodes";

    private final Client client;
    private final ThreadContext threadContext;

    private final ClusterService clusterService;

    private final String indexName;

    private final PublicKey ownerPublicKey;

    //private final Map<String, byte[]> cache = new HashMap<>();

    public IndexKeys(ClusterService clusterService, String indexName, Client client, ThreadContext threadContext, PublicKey ownerPublicKey) {
        this.clusterService = Objects.requireNonNull(clusterService,"clusterService must not be null");
        this.client = Objects.requireNonNull(client,"client must not be null");
        this.threadContext = Objects.requireNonNull(threadContext, "threadContext must not be null");
        this.indexName = indexName;
        this.ownerPublicKey = Objects.requireNonNull(ownerPublicKey, "ownerPublicKey must not be null");

        //TODO
        //mitigate private key send as header (string) stored in heap
        //every node holds a secret (same for all nodes) in a direct byte buffer (offheap)
        //this secret will be store encrypted (with pubkey2) with in an index (encrypted index?) and the
        //private key 2 never leaves the client. clients retrieve the encrypted secret, decrypt, an encrypt
        //the x-osei-key header with this secret. node decrypts, sensitive keys are stored off heap
        //nodes need to have all the same secret circeling around in the cluster
        //new secret if full cluster restart
        //unclear how secret propagation works, master node action, cluster listener when nodes added etc


        //client.index(new IndexRequest(NODE_KEYS_INDEX).id(clusterService.localNode().getName())
        //        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        //        .source("node_key",encryptKey())).actionGet();


    }

    //private key request scoped
    //cached in memory only here in this class

    private PrivateKey extractPrivateKeyFromHeader() throws Exception {
        String pk = threadContext.getHeader("x-osec-pk");

        if(pk == null) {
            return null;
        }

        PKCS8EncodedKeySpec spec =
                new PKCS8EncodedKeySpec(Base64.getDecoder().decode(pk));
        KeyFactory kf = KeyFactory.getInstance("RSA");

        return kf.generatePrivate(spec);
    }

    //user u1 (with u1pub u1sec) creates index i1 + i11 with u1pub
    //u1 can index and search with u1sec for i1+i11

    //user u2 (with u2pub u2sec) creates index i2 with u2pub
    //u2 can index and search with u2sec

    //u1 gets the pub key u2pub from u2 and "uploads" it to grant u2 access to i1 but not to i11
    //--> "UPLOAD"
    //--> _upload/ with payload u2pub and u1sec
    //--> decrypt i1 index key with u1sec and reencrypt with u2pub

    //u2 can index and search with u2sec on i2, i1 (but not i11)

    //.osei_keys index

    //doc id(modulus_indexname) -> {encrypted sym index key of index i1;   }

    public void addKey(PublicKey publicKey, final ActionListener<IndexResponse> listener) throws Exception {
        PrivateKey pk = extractPrivateKeyFromHeader();

        if(pk == null) {
            throw new Exception("private key required");
        }

        if(!isKeyPair(ownerPublicKey, pk)) {
            //owner
            throw new Exception("wrong private key for this index");
        }

        if(!keysIndexExists()) {
            throw new Exception("keys index does not exist");
        }

        client.get(new GetRequest(INDEX, keyIDocId(pk)), new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if(getResponse.isExists()) {
                    try {
                        byte[] bytes = decryptKey(Base64.getDecoder().decode((String) getResponse.getSource().get("encrypted_key")), pk);

                        client.index(new IndexRequest(INDEX).id(keyIDocId(publicKey))
                                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                                        .source("encrypted_key",encryptKey(bytes, publicKey))
                                , listener);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                } else {
                    throw new RuntimeException("no such key found or unable to decrypt");
                }


            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException(e);
            }
        });

    }

    public synchronized byte[] getOrCreateSymmetricKey(int size) throws Exception {

        PrivateKey pk = extractPrivateKeyFromHeader();

        if(pk == null) {
            return null;
        }

        if(keysIndexExists()) {
            byte[] bytes = getDecryptedKeyFromKeysIndex(pk);

            if(bytes != null) {
                return bytes;
            }
        }


        if(isKeyPair(ownerPublicKey, pk)) {
            //owner
            return createRandomKeyAndEncryptAndStore(ownerPublicKey, size);
        }

        return null;
    }

    private byte[] createRandomKeyAndEncryptAndStore(PublicKey publicKey, int size) throws Exception {
        KeysetHandle keysetHandle = KeysetHandle.generateNew(
                KeyTemplates.get("AES256_SIV"));

        System.out.println(keysetHandle.size());
        System.out.println(keysetHandle.getKeysetInfo().getKeyInfoList());
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        CleartextKeysetHandle.write(keysetHandle, BinaryKeysetWriter.withOutputStream(bout));

        //byte[] k = new byte[size];
        //SECURE_RANDOM.nextBytes(k);

        client.index(new IndexRequest(INDEX).id(keyIDocId(publicKey))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source("encrypted_key",encryptKey(bout.toByteArray(), publicKey))
        ).actionGet();

        return bout.toByteArray();
    }

    private byte[] getDecryptedKeyFromKeysIndex(PrivateKey pk) throws Exception {
        GetResponse res = client.get(new GetRequest(INDEX, keyIDocId(pk))).actionGet();
        if(res.isExists()) {
            return decryptKey(Base64.getDecoder().decode((String) res.getSource().get("encrypted_key")), pk);
        }

        return null;
    }

    private boolean keysIndexExists() {
        return clusterService.state().getMetadata().hasIndex(INDEX);
    }

    private String keyIDocId(PublicKey publicKey) {
        return DigestUtils.sha256Hex(concatArrays(indexName.getBytes(StandardCharsets.UTF_8), KeyPairUtil.getModulus(publicKey).toByteArray()));
    }

    private String keyIDocId(PrivateKey privateKey) {
        return DigestUtils.sha256Hex(concatArrays(indexName.getBytes(StandardCharsets.UTF_8), KeyPairUtil.getModulus(privateKey).toByteArray()));
    }

    private byte[] decryptKey(byte[] encrypted_keys, PrivateKey pk) throws Exception {
        Cipher encryptCipher = Cipher.getInstance("RSA");
        encryptCipher.init(Cipher.DECRYPT_MODE, pk);
        return encryptCipher.doFinal(encrypted_keys);
    }

    private byte[] encryptKey(byte[] k, PublicKey publicKey) throws Exception {
        Cipher encryptCipher = Cipher.getInstance("RSA");
        encryptCipher.init(Cipher.ENCRYPT_MODE, publicKey);
        try {
            return encryptCipher.doFinal(k);
        } finally {
            Arrays.fill(k, (byte) 0);
        }
    }

    /**
     * Concatenate arrays
     *
     * @param first First array (not null)
     * @param second Second array (not null)
     * @param more additional arrays to be concatenated to the first two (some arrays can be null -
     *     they will be ignored)
     * @return The concatenated arrays
     */
    public static byte[] concatArrays(byte[] first, byte[] second, byte[]... more) {
        Objects.requireNonNull(first, "first");
        Objects.requireNonNull(second, "second");

        int newLength = first.length + second.length;

        if (more != null && more.length > 0) {
            for (final byte[] b : more) {
                if (b != null) {
                    newLength += b.length;
                }
            }
        }

        final byte[] result = ArrayUtil.growExact(first, newLength);
        System.arraycopy(second, 0, result, first.length, second.length);

        if (more != null && more.length > 0) {
            int offset = first.length + second.length;
            for (final byte[] b : more) {
                if (b != null && b.length > 0) {
                    System.arraycopy(b, 0, result, offset, b.length);
                    offset += b.length;
                }
            }
        }

        return result;
    }


}
