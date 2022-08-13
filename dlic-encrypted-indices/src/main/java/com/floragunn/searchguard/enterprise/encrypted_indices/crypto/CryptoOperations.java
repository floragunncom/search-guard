package com.floragunn.searchguard.enterprise.encrypted_indices.crypto;

import com.floragunn.searchguard.enterprise.encrypted_indices.utils.MapUtils;
import com.google.common.base.Joiner;
import com.google.common.io.CharStreams;
import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.BytesRef;
import org.opensearch.client.Client;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.Index;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public abstract class CryptoOperations {

    private final IndexKeys indexKeys;
    protected CryptoOperations(Index index, Client client, ThreadContext threadContext) {
        this.indexKeys = new IndexKeys(index, client, threadContext);
    }

    protected final IndexKeys getIndexKeys() {
        return indexKeys;
    }

    public final void hashAttribute(CharTermAttribute termAtt) {

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


    public abstract String hashString(String toHash);
    public abstract String encryptString(String stringValue, String field, String id) throws Exception;
    public abstract String decryptString(String stringValue, String field, String id) throws Exception;
    public final byte[] encryptByteArray(byte[] byteArray, String field, String id) throws Exception {
        return encryptByteArray(byteArray, 0, byteArray.length, field, id);
    }
    public abstract byte[] decryptByteArray(byte[] byteArray, String field, String id) throws Exception;

    public abstract byte[] encryptByteArray(byte[] bytes, int offset, int length, String field, String id) throws Exception;

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
                                iterator.set(cryptoOperations.encryptString((String) listFieldItem, field, id));
                            } else {
                                iterator.set(cryptoOperations.decryptString((String) listFieldItem, field, id));
                            }

                        } else if (listFieldItem instanceof byte[]) {
                            if(encrypt) {
                                iterator.set(cryptoOperations.encryptByteArray((byte[]) listFieldItem, field, id));
                            } else {
                                iterator.set(cryptoOperations.decryptByteArray((byte[]) listFieldItem, field, id));

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
                            map.replace(key, cryptoOperations.encryptString((String) v, field, id));
                        } else {
                            map.replace(key, cryptoOperations.decryptString((String) v, field, id));
                        }
                    } else {
                        if(encrypt) {
                            map.replace(key, cryptoOperations.encryptByteArray((byte[]) v, field, id));
                        } else {
                            map.replace(key, cryptoOperations.decryptByteArray((byte[]) v, field, id));

                        }
                    }
                //}
            }
        }

    }
}
