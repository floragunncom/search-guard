/*
 * Based on https://github.com/bcgit/bc-java/blob/main/core/src/test/java/org/bouncycastle/crypto/test/Blake2bDigestTest.java from the Bouncy Castle project licensed under the Bouncy Castle License:
 * 
 * Copyright (c) 2000 - 2024 The Legion of the Bouncy Castle Inc. (https://www.bouncycastle.org)
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, 
 * including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished 
 * to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND 
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT 
 * OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.   
 *
 */

package com.floragunn.searchguard.enterprise.dlsfls;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

public class Blake2bDigestTest {

    private static final String[][] keyedTestVectors = { // input/message, key, hash

            // Vectors from BLAKE2 web site: https://blake2.net/blake2b-test.txt
            { "", "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
                    "10ebb67700b1868efb4417987acf4690ae9d972fb7a590c2f02871799aaa4786b5e996e8f0f4eb981fc214b005f42d2ff4233499391653df7aefcbc13fc51568" },

            { "00", "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
                    "961f6dd1e4dd30f63901690c512e78e4b45e4742ed197c3c5e45c549fd25f2e4187b0bc9fe30492b16b0d0bc4ef9b0f34c7003fac09a5ef1532e69430234cebd" },

            { "0001",
                    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
                    "da2cfbe2d8409a0f38026113884f84b50156371ae304c4430173d08a99d9fb1b983164a3770706d537f49e0c916d9f32b95cc37a95b99d857436f0232c88a965" },

            { "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d",
                    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
                    "f1aa2b044f8f0c638a3f362e677b5d891d6fd2ab0765f6ee1e4987de057ead357883d9b405b9d609eea1b869d97fb16d9b51017c553f3b93c0a1e0f1296fedcd" },

            { "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3",
                    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
                    "c230f0802679cb33822ef8b3b21bf7a9a28942092901d7dac3760300831026cf354c9232df3e084d9903130c601f63c1f4a4a4b8106e468cd443bbe5a734f45f" },

            { "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfe",
                    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
                    "142709d62e28fcccd0af97fad0f8465b971e82201dc51070faa0372aa43e92484be1c1e73ba10906d5d1853db6a4106e0a7bf9800d373d6dee2d46d62ef2a461" } };

    private final static String[][] unkeyedTestVectors = { // from: https://fossies.org/linux/john/src/rawBLAKE2_512_fmt_plug.c
            // hash, input/message
            // digests without leading $BLAKE2$
            { "4245af08b46fbb290222ab8a68613621d92ce78577152d712467742417ebc1153668f1c9e1ec1e152a32a9c242dc686d175e087906377f0c483c5be2cb68953e",
                    "blake2" },
            { "021ced8799296ceca557832ab941a50b4a11f83478cf141f51f933f653ab9fbcc05a037cddbed06e309bf334942c4e58cdf1a46e237911ccd7fcf9787cbc7fd0",
                    "hello world" },
            { "1f7d9b7c9a90f7bfc66e52b69f3b6c3befbd6aee11aac860e99347a495526f30c9e51f6b0db01c24825092a09dd1a15740f0ade8def87e60c15da487571bcef7",
                    "verystrongandlongpassword" },
            { "a8add4bdddfd93e4877d2746e62817b116364a1fa7bc148d95090bc7333b3673f82401cf7aa2e4cb1ecd90296e3f14cb5413f8ed77be73045b13914cdcd6a918",
                    "The quick brown fox jumps over the lazy dog" },
            { "786a02f742015903c6c6fd852552d272912f4740e15847618a86e217f71f5419d25e1031afee585313896444934eb04b903a685b1448b755d56f701afe9be2ce",
                    "" },
            { "ba80a53f981c4d0d6a2797b69f12f6e94c212f14685ac4b74b12bb6fdbffa2d17d87c5392aab792dc252d5de4533cc9518d38aa8dbf1925ab92386edd4009923",
                    "abc" }, };

    private void offsetTest(Blake2bDigest digest, byte[] input, byte[] expected) {
        byte[] resBuf = new byte[expected.length + 11];

        digest.update(input, 0, input.length);

        digest.doFinal(resBuf, 11);

        if (!Arrays.equals(Arrays.copyOfRange(resBuf, 11, resBuf.length), expected)) {
            fail("Offset failed got " + new String(Hex.encodeHexString(resBuf)));
        }
    }

    @Test
    public void performTest() throws Exception {
        // test keyed test vectors:

        Blake2bDigest blake2bkeyed = new Blake2bDigest(Hex.decodeHex(keyedTestVectors[0][1]));
        for (int tv = 0; tv < keyedTestVectors.length; tv++) {

            byte[] input = Hex.decodeHex(keyedTestVectors[tv][0]);
            blake2bkeyed.reset();

            blake2bkeyed.update(input, 0, input.length);
            byte[] keyedHash = new byte[64];
            blake2bkeyed.doFinal(keyedHash, 0);

            if (!Arrays.equals(Hex.decodeHex(keyedTestVectors[tv][2]), keyedHash)) {
                fail("BLAKE2b mismatch on test vector " + keyedTestVectors[tv][2] + new String(Hex.encodeHexString(keyedHash)));
            }

            offsetTest(blake2bkeyed, input, keyedHash);
        }

        Blake2bDigest blake2bunkeyed = new Blake2bDigest();
        // test unkeyed test vectors:
        for (int i = 0; i < unkeyedTestVectors.length; i++) {

            // blake2bunkeyed.update(
            // unkeyedTestVectors[i][1].getBytes("UTF-8"));
            // test update(byte b)
            byte[] unkeyedInput = toUTF8ByteArray(unkeyedTestVectors[i][1]);
            for (int j = 0; j < unkeyedInput.length; j++) {
                blake2bunkeyed.update(unkeyedInput[j]);
            }

            byte[] unkeyedHash = new byte[64];
            blake2bunkeyed.doFinal(unkeyedHash, 0);
            blake2bunkeyed.reset();

            if (!Arrays.equals(Hex.decodeHex(unkeyedTestVectors[i][0]), unkeyedHash)) {
                fail("BLAKE2b mismatch on test vector " + unkeyedTestVectors[i][0] + new String(Hex.encodeHexString(unkeyedHash)));
            }
        }

        cloneTest();
        resetTest();
        testNullKeyVsUnkeyed();
        testLengthConstruction();
    }

    private void cloneTest() throws DecoderException {
        Blake2bDigest blake2bCloneSource = new Blake2bDigest(Hex.decodeHex(keyedTestVectors[3][1]), 16,
                Hex.decodeHex("000102030405060708090a0b0c0d0e0f"), Hex.decodeHex("101112131415161718191a1b1c1d1e1f"));
        byte[] expected = Hex.decodeHex("b6d48ed5771b17414c4e08bd8d8a3bc4");

        checkClone(blake2bCloneSource, expected);

        // just digest size
        blake2bCloneSource = new Blake2bDigest(160);
        expected = Hex.decodeHex("64202454e538279b21cea0f5a7688be656f8f484");
        checkClone(blake2bCloneSource, expected);

        // null salt and personalisation
        blake2bCloneSource = new Blake2bDigest(Hex.decodeHex(keyedTestVectors[3][1]), 16, null, null);
        expected = Hex.decodeHex("2b4a081fae2d7b488f5eed7e83e42a20");
        checkClone(blake2bCloneSource, expected);

        // null personalisation
        blake2bCloneSource = new Blake2bDigest(Hex.decodeHex(keyedTestVectors[3][1]), 16, Hex.decodeHex("000102030405060708090a0b0c0d0e0f"), null);
        expected = Hex.decodeHex("00c3a2a02fcb9f389857626e19d706f6");
        checkClone(blake2bCloneSource, expected);

        // null salt
        blake2bCloneSource = new Blake2bDigest(Hex.decodeHex(keyedTestVectors[3][1]), 16, null, Hex.decodeHex("101112131415161718191a1b1c1d1e1f"));
        expected = Hex.decodeHex("f445ec9c062a3c724f8fdef824417abb");
        checkClone(blake2bCloneSource, expected);
    }

    private void checkClone(Blake2bDigest blake2bCloneSource, byte[] expected) throws DecoderException {
        byte[] message = Hex.decodeHex(keyedTestVectors[3][0]);

        blake2bCloneSource.update(message, 0, message.length);

        byte[] hash = new byte[blake2bCloneSource.getDigestSize()];

        Blake2bDigest digClone = new Blake2bDigest(blake2bCloneSource);

        blake2bCloneSource.doFinal(hash, 0);
        if (!Arrays.equals(expected, hash)) {
            fail("clone source not correct");
        }

        digClone.doFinal(hash, 0);
        if (!Arrays.equals(expected, hash)) {
            fail("clone not correct");
        }
    }

    private void testLengthConstruction() {
        try {
            new Blake2bDigest(-1);
            fail("no exception");
        } catch (IllegalArgumentException e) {
            assertEquals("BLAKE2b digest bit length must be a multiple of 8 and not greater than 512", e.getMessage());
        }

        try {
            new Blake2bDigest(9);
            fail("no exception");
        } catch (IllegalArgumentException e) {
            assertEquals("BLAKE2b digest bit length must be a multiple of 8 and not greater than 512", e.getMessage());
        }

        try {
            new Blake2bDigest(520);
            fail("no exception");
        } catch (IllegalArgumentException e) {
            assertEquals("BLAKE2b digest bit length must be a multiple of 8 and not greater than 512", e.getMessage());
        }

        try {
            new Blake2bDigest(null, -1, null, null);
            fail("no exception");
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid digest length (required: 1 - 64)", e.getMessage());
        }

        try {
            new Blake2bDigest(null, 65, null, null);
            fail("no exception");
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid digest length (required: 1 - 64)", e.getMessage());
        }
    }

    private void testNullKeyVsUnkeyed() {
        byte[] abc = toByteArray("abc");

        for (int i = 1; i != 64; i++) {
            Blake2bDigest dig1 = new Blake2bDigest(i * 8);
            Blake2bDigest dig2 = new Blake2bDigest(null, i, null, null);

            byte[] out1 = new byte[i];
            byte[] out2 = new byte[i];

            dig1.update(abc, 0, abc.length);
            dig2.update(abc, 0, abc.length);

            dig1.doFinal(out1, 0);
            dig2.doFinal(out2, 0);

            assertTrue(Arrays.equals(out1, out2));
        }
    }

    private void resetTest() {
        // Generate a non-zero key
        byte[] key = new byte[32];
        for (byte i = 0; i < key.length; i++) {
            key[i] = i;
        }
        // Generate some non-zero input longer than the key
        byte[] input = new byte[key.length + 1];
        for (byte i = 0; i < input.length; i++) {
            input[i] = i;
        }
        // Hash the input
        Blake2bDigest digest = new Blake2bDigest(key);
        digest.update(input, 0, input.length);
        byte[] hash = new byte[digest.getDigestSize()];
        digest.doFinal(hash, 0);
        // Using a second instance, hash the input without calling doFinal()
        Blake2bDigest digest1 = new Blake2bDigest(key);
        digest1.update(input, 0, input.length);
        // Reset the second instance and hash the input again
        digest1.reset();
        digest1.update(input, 0, input.length);
        byte[] hash1 = new byte[digest.getDigestSize()];
        digest1.doFinal(hash1, 0);
        // The hashes should be identical
        if (!Arrays.equals(hash, hash1)) {
            fail("state was not reset");
        }
    }

    static byte[] toUTF8ByteArray(String string) {
        return toUTF8ByteArray(string.toCharArray());
    }

    static byte[] toUTF8ByteArray(char[] string) {
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();

        try {
            toUTF8ByteArray(string, bOut);
        } catch (IOException e) {
            throw new IllegalStateException("cannot encode string to byte array!");
        }

        return bOut.toByteArray();
    }

    static void toUTF8ByteArray(char[] string, OutputStream sOut) throws IOException {
        char[] c = string;
        int i = 0;

        while (i < c.length) {
            char ch = c[i];

            if (ch < 0x0080) {
                sOut.write(ch);
            } else if (ch < 0x0800) {
                sOut.write(0xc0 | (ch >> 6));
                sOut.write(0x80 | (ch & 0x3f));
            }
            // surrogate pair
            else if (ch >= 0xD800 && ch <= 0xDFFF) {
                // in error - can only happen, if the Java String class has a
                // bug.
                if (i + 1 >= c.length) {
                    throw new IllegalStateException("invalid UTF-16 codepoint");
                }
                char W1 = ch;
                ch = c[++i];
                char W2 = ch;
                // in error - can only happen, if the Java String class has a
                // bug.
                if (W1 > 0xDBFF) {
                    throw new IllegalStateException("invalid UTF-16 codepoint");
                }
                int codePoint = (((W1 & 0x03FF) << 10) | (W2 & 0x03FF)) + 0x10000;
                sOut.write(0xf0 | (codePoint >> 18));
                sOut.write(0x80 | ((codePoint >> 12) & 0x3F));
                sOut.write(0x80 | ((codePoint >> 6) & 0x3F));
                sOut.write(0x80 | (codePoint & 0x3F));
            } else {
                sOut.write(0xe0 | (ch >> 12));
                sOut.write(0x80 | ((ch >> 6) & 0x3F));
                sOut.write(0x80 | (ch & 0x3F));
            }

            i++;
        }
    }

    static byte[] toByteArray(String string) {
        byte[] bytes = new byte[string.length()];

        for (int i = 0; i != bytes.length; i++) {
            char ch = string.charAt(i);

            bytes[i] = (byte) ch;
        }

        return bytes;
    }
}
