package com.floragunn.searchguard.enterprise.encrypted_indices.crypto;

import com.google.common.io.CharStreams;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

public class Cryptor {
    public static Cryptor dummy() {

        return new Cryptor() {
            @Override
            public void hash(CharTermAttribute termAtt) {

                if (termAtt != null) {
                    final String clearTextValue = termAtt.toString();
                    String md5 = DigestUtils.md5Hex(clearTextValue.getBytes(StandardCharsets.UTF_8));
                    System.out.println("= Hash CharTermAttribute " + termAtt + " to " + md5);
                    //new Exception("= Hash CharTermAttribute " + termAtt + " to " + md5).printStackTrace();
                    termAtt.setEmpty().append(md5+clearTextValue);
                }
            }

            @Override
            public void hash(BytesTermAttribute bytesTermAtt) {
                // System.out.println("= Hash BytesTermAttribute");
                // if (bytesTermAtt != null) {
                //    bytesTermAtt.setBytesRef(crypto(bytesTermAtt.getBytesRef(), context));
                // }
            }

            @Override
            public void hash(TermToBytesRefAttribute termToBytesAtt) {
                //System.out.println("= Hash TermToBytesRefAttribute");
            }

            @Override
            public String encryptString(String stringValue) {

                String res = StringUtils.reverse(stringValue);
                System.out.println("== crypto string value '" + stringValue + "' -> '" + res + "'");
                return res;
            }

            @Override
            public String decryptString(String stringValue) {

                String res = StringUtils.reverse(stringValue);
                System.out.println("== crypto string value '" + stringValue + "' -> '" + res + "'");
                return res;
            }

            @Override
            public Reader encryptReader(Reader readerValue) throws IOException {
                System.out.println("== crypto reader value");
                String s = CharStreams.toString(readerValue);
                return new StringReader(StringUtils.reverse(s));
            }

            @Override
            public boolean isSourceEncrypted(BytesRef binaryValue) {
                return binaryValue.utf8ToString().contains("Spuck") || binaryValue.utf8ToString().contains("Kork");
            }

            @Override
            public byte[] encryptBytesRef(BytesRef binaryValue, String fieldName) {
                System.out.println("== encryptBytesRef value for " + fieldName);
                byte[] bytes = Arrays.copyOfRange(binaryValue.bytes, binaryValue.offset, binaryValue.length + binaryValue.offset);

                if (fieldName.equals("_source")) {
                    //we maintain json

                    if (new String(bytes).contains("Spuck") || new String(bytes).contains("Kork")) {
                        new Exception("unable to encrypt " + new String(bytes)).printStackTrace();
                        throw new RuntimeException("unable to encrypt " + new String(bytes));
                    }
                    String s = new String(bytes)
                            .replace("Spock", "Spuck")
                            .replace("Kirk", "Kork");
                    return s.getBytes(StandardCharsets.UTF_8);

                } else {
                    reverseArray(bytes, bytes.length);
                    return bytes;
                }
            }

            @Override
            public byte[] decryptBytesRef(BytesRef binaryValue, String fieldName) {
                System.out.println("== decryptBytesRef value for " + fieldName);
                byte[] bytes = Arrays.copyOfRange(binaryValue.bytes, binaryValue.offset, binaryValue.length + binaryValue.offset);

                if (fieldName.equals("_source")) {

                    if (!new String(bytes).contains("Kork") && !new String(bytes).contains("Spuck")) {
                        new Exception("unable to decrypt " + new String(bytes)).printStackTrace();
                        throw new RuntimeException("unable to decrypt " + new String(bytes));
                    }

                    //we maintain json
                    String s = new String(bytes)
                            .replace("Spuck", "Spock")
                            .replace("Kork", "Kirk");
                    return s.getBytes(StandardCharsets.UTF_8);
                } else {
                    reverseArray(bytes, bytes.length);
                    return bytes;
                }
            }


        };

    }


    static void reverseArray(byte[] bytes, int size) {
        int i, k;
        byte temp;
        for (i = 0; i < size / 2; i++) {
            temp = bytes[i];
            bytes[i] = bytes[size - i - 1];
            bytes[size - i - 1] = temp;
        }
    }

    public void hash(CharTermAttribute termAtt) {
        throw new RuntimeException();
    }

    public void hash(BytesTermAttribute bytesTermAtt) {
        throw new RuntimeException();
    }

    public void hash(TermToBytesRefAttribute termToBytesAtt) {
        throw new RuntimeException();
    }

    public String encryptString(String stringValue) {
        throw new RuntimeException();
    }

    public String decryptString(String value) {
        throw new RuntimeException();
    }

    public Reader encryptReader(Reader readerValue) throws IOException {
        throw new RuntimeException();
    }

    public byte[] encryptBytesRef(BytesRef binaryValue, String fieldName) {
        throw new RuntimeException();
    }

    public byte[] decryptBytesRef(BytesRef binaryValue, String fieldName) {
        throw new RuntimeException();
    }

    public boolean isSourceEncrypted(BytesRef binaryValue) {
        throw new RuntimeException();
    }

}
