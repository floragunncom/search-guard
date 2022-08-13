package com.floragunn.searchguard.enterprise.encrypted_indices.crypto;

import com.floragunn.searchguard.enterprise.encrypted_indices.utils.MapUtils;
import com.google.common.io.CharStreams;
import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public abstract class CryptoOperations {

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
    public abstract String encryptString(String stringValue);
    public abstract String decryptString(String stringValue);
    public final byte[] encryptByteArray(byte[] byteArray) {
        return encryptByteArray(byteArray, 0, byteArray.length);
    }
    public abstract byte[] decryptByteArray(byte[] byteArray);

    public abstract byte[] encryptByteArray(byte[] bytes, int offset, int length);

    public final Reader encryptReader(Reader readerValue) throws IOException {
        if(readerValue == null) {
            return null;
        }
        String s = CharStreams.toString(readerValue);
        return new StringReader(encryptString(s));
    }

    public abstract boolean isEncrypted(String string);
    public abstract boolean isEncrypted(byte[] byteArray);

    public final BytesRef encryptBytesRef(BytesRef source) {
        byte[] bytes = encryptByteArray(source.bytes, source.offset, source.length);
        return new BytesRef(bytes);
    }

//    public final BytesRef decryptBytesRef(BytesRef source) {
//        byte[] bytes = encryptByteArray(source.bytes, source.offset, source.length);
//        return new BytesRef(bytes);
//    }

    public final BytesRef encryptSource(BytesRef source) {
        return cryptoSourceToBytesRef(source, true);
    }

    public final BytesRef decryptSource(BytesRef source) {
        return cryptoSourceToBytesRef(source, false);
    }

    public final byte[] decryptSourceAsByteArray(byte[] source) {
        BytesRef bytesRef = cryptoSourceToBytesRef(new BytesRef(source), false);
        return Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset+bytesRef.length);
    }


    //#### private

    private BytesRef cryptoSourceToBytesRef(BytesRef source, boolean encrypt) {
        Map<String, Object> sourceAsMap = cryptoSource(new BytesArray(source.bytes, source.offset, source.length), encrypt);
        try {
            return BytesReference.bytes(XContentBuilder.builder(JsonXContent.jsonXContent).map(sourceAsMap)).toBytesRef();
        } catch (IOException e) {
            // can not happen
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> cryptoSource(BytesReference source, boolean encrypt) {
        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(source, false, XContentType.JSON).v2();
        MapUtils.deepTraverseMap(sourceAsMap, new EnryptingCallback(this, encrypt));
        return sourceAsMap;
    }

    private static class EnryptingCallback implements MapUtils.Callback {

        private final CryptoOperations cryptoOperations;
        private final boolean encrypt;

        private EnryptingCallback(CryptoOperations cryptoOperations, boolean encrypt) {
            this.cryptoOperations = cryptoOperations;
            this.encrypt = encrypt;
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Override
        public void call(String key, Map<String, Object> map, List<String> stack) {
            Object v = map.get(key);

            if (v != null && (v instanceof List)) {
                //final String field = stack.isEmpty() ? key : Joiner.on('.').join(stack) + "." + key;
                //final Optional<String> matchedPattern = WildcardMatcher.getFirstMatchingPattern(maskedFieldsKeySet, field);
                //if (matchedPattern.isPresent()) {
                    final List listField = (List) v;
                    for (ListIterator iterator = listField.listIterator(); iterator.hasNext();) {
                        final Object listFieldItem = iterator.next();

                        if (listFieldItem instanceof String) {
                            if(encrypt) {
                                iterator.set(cryptoOperations.encryptString(((String) listFieldItem)));
                            } else {
                                iterator.set(cryptoOperations.decryptString(((String) listFieldItem)));
                            }

                        } else if (listFieldItem instanceof byte[]) {
                            if(encrypt) {
                                iterator.set(cryptoOperations.encryptByteArray(((byte[]) listFieldItem)));
                            } else {
                                iterator.set(cryptoOperations.decryptByteArray(((byte[]) listFieldItem)));

                            }
                        }
                    }
                //}
            }

            if (v != null && (v instanceof String || v instanceof byte[])) {

                //final String field = stack.isEmpty() ? key : Joiner.on('.').join(stack) + "." + key;
                //final Optional<String> matchedPattern = WildcardMatcher.getFirstMatchingPattern(maskedFieldsKeySet, field);
                //if (matchedPattern.isPresent()) {
                    if (v instanceof String) {
                        if(encrypt) {
                            map.replace(key, cryptoOperations.encryptString(((String) v)));
                        } else {
                            map.replace(key, cryptoOperations.decryptString(((String) v)));
                        }
                    } else {
                        if(encrypt) {
                            map.replace(key, cryptoOperations.encryptByteArray(((byte[]) v)));
                        } else {
                            map.replace(key, cryptoOperations.decryptByteArray(((byte[]) v)));

                        }
                    }
                //}
            }
        }

    }
}
