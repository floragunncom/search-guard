package com.floragunn.searchguard.enterprise.encrypted_indices.crypto;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class DefaultCryptoOperations extends CryptoOperations{

    private String indexEncryptionKey;

    DefaultCryptoOperations(String indexEncryptionKey) {
        this.indexEncryptionKey = indexEncryptionKey;
    }

    @Override
    public String hashString(String toHash) {
        return "HASH:"+reverseString(toHash);
    }

    @Override
    public String encryptString(String stringValue) {
        return reverseString(stringValue);
    }

    @Override
    public String decryptString(String stringValue) {
        return reverseString(stringValue);
    }

    @Override
    public byte[] encryptByteArray(byte[] bytes, int offset, int length) {
        return reverseBytes(Arrays.copyOfRange(bytes, offset, offset+length));
    }

    @Override
    public byte[] decryptByteArray(byte[] byteArray) {
        return reverseBytes(byteArray);
    }

    @Override
    public boolean isEncrypted(String string) {
        return false;
    }

    @Override
    public boolean isEncrypted(byte[] byteArray) {
        return false;
    }

    private static byte[] reverseBytes(byte[] bytes) {
        if(bytes != null) {
            reverseBytes(bytes, bytes.length);
        }

        return bytes;
    }

    private static String reverseString(String string) {
        if(string == null) {
            return null;
        }
        return new String(reverseBytes(string.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }

    private static void reverseBytes(byte[] bytes, int size) {
        int i, k;
        byte temp;
        for (i = 0; i < size / 2; i++) {
            temp = bytes[i];
            bytes[i] = bytes[size - i - 1];
            bytes[size - i - 1] = temp;
        }
    }
}
