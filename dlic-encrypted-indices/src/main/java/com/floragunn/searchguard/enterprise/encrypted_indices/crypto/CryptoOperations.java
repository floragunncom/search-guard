package com.floragunn.searchguard.enterprise.encrypted_indices.crypto;

import com.floragunn.searchguard.enterprise.encrypted_indices.utils.MapUtils;
import com.google.common.base.Joiner;
import com.google.common.io.CharStreams;
import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.BytesRef;
import org.bouncycastle.crypto.digests.Blake2bDigest;
import org.bouncycastle.util.encoders.Hex;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.Index;

import javax.crypto.Cipher;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public abstract class CryptoOperations {

    private final IndexKeys indexKeys;

    protected final int keySize;
    protected CryptoOperations(ClusterService clusterService, Index index, Client client, ThreadContext threadContext, String indexPublicKey, int keySize) throws Exception {
        this.indexKeys = new IndexKeys(clusterService, index, client, threadContext, parsePublicKey(indexPublicKey));
        this.keySize = keySize;
    }

    public final String hashString(String toHash) throws Exception {
        byte[] key = getIndexKeys().getOrCreateSymmetricKey(keySize);

        if(key == null) {

        }

        return new String(blake2bHash(toHash.getBytes(StandardCharsets.UTF_8), key), StandardCharsets.UTF_8);
    }

    protected final byte[] createNonce(String field, String id, int nonceLenInByte) {
        if(field == null) {
            throw new IllegalArgumentException("unable to create nonce: field must not be null");
        }

        if(id == null) {
            throw new IllegalArgumentException("unable to create nonce: id must not be null");
        }

        if(field.isEmpty()) {
            throw new IllegalArgumentException("unable to create nonce: field must not be empty");
        }

        if(id.length() < 5) {
            throw new IllegalArgumentException("unable to create nonce: id must be len >= 5");
        }

        final byte[] nonce = (field+id).getBytes(StandardCharsets.UTF_8);

        if(nonce.length < nonceLenInByte) {
            return Arrays.copyOf(nonce, nonceLenInByte);
        } else if (nonce.length > nonceLenInByte) {
            return blake2bHashForNonce(nonce, nonceLenInByte);
        } else {
            return nonce;
        }
    }

    private byte[] blake2bHash(byte[] in, byte[] key) {
        final Blake2bDigest hash = new Blake2bDigest(key, 16, null, null);
        hash.update(in, 0, in.length);
        final byte[] out = new byte[hash.getDigestSize()];
        hash.doFinal(out, 0);
        return Hex.encode(out);
    }

    private byte[] blake2bHashForNonce(byte[] in, int nonceLenInByte) {
        final Blake2bDigest hash = new Blake2bDigest(null, nonceLenInByte, null, null);
        hash.update(in, 0, in.length);
        assert nonceLenInByte == hash.getDigestSize();
        final byte[] out = new byte[hash.getDigestSize()];
        hash.doFinal(out, 0);
        return out;
    }



    private static PublicKey parsePublicKey(String indexPublicKey) throws Exception {
        byte[] key = Base64.getDecoder().decode(indexPublicKey);
        X509EncodedKeySpec spec =
                new X509EncodedKeySpec(key);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePublic(spec);
    }

    protected final IndexKeys getIndexKeys() {
        return indexKeys;
    }

    public final void hashAttribute(CharTermAttribute termAtt) throws Exception {

        if (termAtt != null) {
            final String clearTextValue = termAtt.toString();

            if(clearTextValue != null) {
                termAtt.setEmpty().append(hashString(clearTextValue));
            }
        }
    }

    public final void hashAttribute(BytesTermAttribute bytesTermAtt) {
        // System.out.println("= Hash BytesTermAttribute");
        // if (bytesTermAtt != null) {
        //    bytesTermAtt.setBytesRef(crypto(bytesTermAtt.getBytesRef(), context));
        // }
    }

    public final void hashAttribute(TermToBytesRefAttribute termToBytesAtt) {
        //System.out.println("= Hash TermToBytesRefAttribute");
    }

  
    public final String encryptString(String stringValue, String field, String id) throws Exception {
        byte[] b = prepareCrypt(stringValue.getBytes(StandardCharsets.UTF_8),field,id, Cipher.ENCRYPT_MODE);
        return Base64.getEncoder().encodeToString(b);
    }


    public final String decryptString(String stringValue, String field, String id) throws Exception {
        byte[] b = Base64.getDecoder().decode(stringValue);
        return new String(prepareCrypt(b,field,id,Cipher.DECRYPT_MODE), StandardCharsets.UTF_8);
    }

    protected final byte[] prepareCrypt(byte[] in, String field, String id, int mode) throws Exception {
        byte[] key = getIndexKeys().getOrCreateSymmetricKey(keySize);

        if(key == null) {
            if(mode == Cipher.ENCRYPT_MODE) {
                throw new RuntimeException("need a key to encrypt");
            } else {
                return in;
            }
        }

        return doCrypt(in, key, field,id, mode);
    }

    protected abstract byte[] doCrypt(byte[] in, byte[] key, String field, String id, int mode) throws Exception;


    public final byte[] encryptByteArray(byte[] byteArray, String field, String id) throws Exception {
        return encryptByteArray(byteArray, 0, byteArray.length, field, id);
    }

    
    public final byte[] encryptByteArray(byte[] bytes, int offset, int length, String field, String id) throws Exception {
        return prepareCrypt(Arrays.copyOfRange(bytes, offset, offset+length), field,id,Cipher.ENCRYPT_MODE);
    }

  
    public final byte[] decryptByteArray(byte[] byteArray, String field, String id) throws Exception {
        return prepareCrypt(byteArray, field,id,Cipher.DECRYPT_MODE);
    }

    public final Reader encryptReader(Reader readerValue, String field, String id) throws Exception {
        if(readerValue == null) {
            return null;
        }
        String s = CharStreams.toString(readerValue);
        return new StringReader(encryptString(s, field, id));
    }

    public final BytesRef encryptBytesRef(BytesRef source, String field, String id) throws Exception {
        byte[] bytes = encryptByteArray(source.bytes, source.offset, source.length, field, id);
        return new BytesRef(bytes);
    }

//    public final BytesRef decryptBytesRef(BytesRef source) {
//        byte[] bytes = encryptByteArray(source.bytes, source.offset, source.length);
//        return new BytesRef(bytes);
//    }

    public final BytesRef encryptSource(BytesRef source, String id) throws Exception {
        return cryptoSourceToBytesRef(source, true, id);
    }

    public final BytesRef decryptSource(BytesRef source, String id) throws Exception {
        return cryptoSourceToBytesRef(source, false, id);
    }

    public final byte[] decryptSourceAsByteArray(byte[] source, String id) throws Exception {
        BytesRef bytesRef = cryptoSourceToBytesRef(new BytesRef(source), false, id);
        return Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset+bytesRef.length);
    }


    //#### private

    private BytesRef cryptoSourceToBytesRef(BytesRef source, boolean encrypt, String id) throws Exception {
        Map<String, Object> sourceAsMap = cryptoSource(new BytesArray(source.bytes, source.offset, source.length), encrypt, id);
        try {
            return BytesReference.bytes(XContentBuilder.builder(JsonXContent.jsonXContent).map(sourceAsMap)).toBytesRef();
        } catch (IOException e) {
            // can not happen
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> cryptoSource(BytesReference source, boolean encrypt, String id) throws Exception {
        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(source, false, XContentType.JSON).v2();
        MapUtils.deepTraverseMap(sourceAsMap, new CrypticCallback(this, encrypt, id));
        return sourceAsMap;
    }

    private static class CrypticCallback implements MapUtils.Callback {

        private static final String PREFIX = "_source.";

        private final CryptoOperations cryptoOperations;
        private final boolean encrypt;

        private final String id;

        private CrypticCallback(CryptoOperations cryptoOperations, boolean encrypt, String id) {
            this.cryptoOperations = cryptoOperations;
            this.encrypt = encrypt;
            this.id = id;
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Override
        public void call(String key, Map<String, Object> map, List<String> stack) throws Exception {
            Object v = map.get(key);

            if (v != null && (v instanceof List)) {
                final String field = stack.isEmpty() ? key : Joiner.on('.').join(stack) + "." + key;
                //final Optional<String> matchedPattern = WildcardMatcher.getFirstMatchingPattern(maskedFieldsKeySet, field);
                //if (matchedPattern.isPresent()) {
                    final List listField = (List) v;
                    for (ListIterator iterator = listField.listIterator(); iterator.hasNext();) {
                        final Object listFieldItem = iterator.next();

                        if (listFieldItem instanceof String) {
                            if(encrypt) {
                                iterator.set(cryptoOperations.encryptString((String) listFieldItem, PREFIX+field, id));
                            } else {
                                iterator.set(cryptoOperations.decryptString((String) listFieldItem, PREFIX+field, id));
                            }

                        } else if (listFieldItem instanceof byte[]) {
                            if(encrypt) {
                                iterator.set(cryptoOperations.encryptByteArray((byte[]) listFieldItem, PREFIX+field, id));
                            } else {
                                iterator.set(cryptoOperations.decryptByteArray((byte[]) listFieldItem, PREFIX+field, id));

                            }
                        }
                    }
                //}
            }

            if (v != null && (v instanceof String || v instanceof byte[])) {

                final String field = stack.isEmpty() ? key : Joiner.on('.').join(stack) + "." + key;
                //final Optional<String> matchedPattern = WildcardMatcher.getFirstMatchingPattern(maskedFieldsKeySet, field);
                //if (matchedPattern.isPresent()) {
                    if (v instanceof String) {
                        if(encrypt) {
                            map.replace(key, cryptoOperations.encryptString((String) v, PREFIX+field, id));
                        } else {
                            map.replace(key, cryptoOperations.decryptString((String) v, PREFIX+field, id));
                        }
                    } else {
                        if(encrypt) {
                            map.replace(key, cryptoOperations.encryptByteArray((byte[]) v, PREFIX+field, id));
                        } else {
                            map.replace(key, cryptoOperations.decryptByteArray((byte[]) v, PREFIX+field, id));

                        }
                    }
                //}
            }
        }

    }
}
