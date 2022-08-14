/* 
 * Copyright (C) 2021 by eliatra Ltd. - All Rights Reserved
 * Unauthorized copying, usage or modification of this file in its source or binary form, 
 * via any medium is strictly prohibited.
 * Proprietary and confidential.
 * 
 * https://eliatra.com
 */
package com.floragunn.searchguard.enterprise.lucene.encryption;

import org.apache.lucene.util.Constants;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

/**
 * This class encapsulates all cryptographic actions. There are two default modes available:
 *
 * <ul>
 *   <li>AES Galois/Counter Mode (GCM)
 *   <li>ChaCha20-Poly1305 (Java 11+ only)
 * </ul>
 */
public abstract class CeffMode {

  protected static final SecureRandom SECURE_RANDOM = new SecureRandom();
  private static final Map<Byte, CeffMode> modes = new HashMap<>();
  /** Ceff AES Galois/Counter Mode (GCM) mode (Java 11+ recommended) */
  public static final CeffMode AES_GCM_MODE = new AesGcmMode();
  /** Ceff ChaCha20 Poly1305 mode (Java 11+ only) */
  public static final CeffMode CHACHA20_POLY1305_MODE = new ChaCha20Poly1305Mode();

  static final CeffMode NULL_MODE = new NullMode();

  static {
    // default modes
    modes.put(NULL_MODE.getModeByte(), NULL_MODE);
    modes.put(AES_GCM_MODE.getModeByte(), AES_GCM_MODE);

    if (CHACHA20_POLY1305_MODE.isSupported()) {
      modes.put(CHACHA20_POLY1305_MODE.getModeByte(), CHACHA20_POLY1305_MODE);
    }
  }

  private final byte mode;

  protected CeffMode(byte mode) {
    this.mode = mode;
  }

  /**
   * Validate key
   *
   * @param key The en/decryption key
   * @throws IllegalArgumentException in case the key is not valid or null
   */
  public void validateKey(byte[] key) {
    if (key == null || key.length != 32) {
      throw new IllegalArgumentException("key must be not null and == 32 bytes in length");
    }
  }

  /**
   * Register a custom encryption mode
   *
   * @param mode Must not be registered before and &gt;= 10
   * @throws IllegalArgumentException when not supported, already registered or mode byte &lt; 10
   */
  public static void registerMode(CeffMode mode) {
    if (!mode.isSupported()) {
      throw new IllegalArgumentException(
          "mode " + mode.getClass().getSimpleName() + " not supported on this platform");
    }

    if (mode.getModeByte() < 10) {
      throw new IllegalArgumentException("mode byte must be >= 10");
    }

    if (modes.containsKey(mode.getModeByte())) {
      throw new IllegalArgumentException(
          "mode already registered as " + modes.get(mode.getModeByte()).getClass().getSimpleName());
    }

    modes.put(mode.getModeByte(), mode);
  }

  /** @return the mode byte */
  public final byte getModeByte() {
    return this.mode;
  }

  /** @return AEAD tag length */
  public abstract int getTagLength();

  /** @return Nonce/IV length */
  public abstract int getNonceLength();

  /** @return a random nonce */
  public abstract byte[] randomNonce();

  /**
   *
   *
   * <ul>
   *   <li>AES Galois/Counter Mode (GCM) is always supported
   *   <li>ChaCha20 Poly1305 is supported on Java 11 onwards
   * </ul>
   *
   * @return true if this mode is supported
   */
  public abstract boolean isSupported();

  /**
   * Encrypt plain text and additional authenticated data
   *
   * @param plainText The plain text
   * @param aad Additional authenticated data
   * @param key The key used for encryption
   * @param nonce The Nonce/IV used for encryption
   * @return The cipher text
   * @throws CeffCryptoException in case there is an encryption issue
   */
  public abstract byte[] encrypt(ByteBuffer plainText, ByteBuffer aad, byte[] key, byte[] nonce)
      throws CeffCryptoException;

  /**
   * Decrypt cipher text and verify additional authenticated data (AAD)
   *
   * @param cipherText The cipher text (including authentication tag)
   * @param aad Additional authenticated data TO VERIFY
   * @param key The key used for decryption
   * @param nonce The Nonce/IV used for decryption
   * @return The plain text
   * @throws CeffCryptoException in case the plain text can not be decrypted or the AAD can not be
   *     verified
   */
  public abstract byte[] decrypt(ByteBuffer cipherText, ByteBuffer aad, byte[] key, byte[] nonce)
      throws CeffCryptoException;

  private static final class AesGcmMode extends CeffMode {

    private static final int IV_LEN = 12; // 12 bytes
    private static final int TAG_LEN = 128; // 128 bits = 16 bytes
    private static final String ALGO = "AES/GCM/NoPadding";

    private AesGcmMode() {
      super((byte) 1);
    }

    @Override
    public int getTagLength() {
      return TAG_LEN / 8;
    }

    @Override
    public int getNonceLength() {
      return IV_LEN;
    }

    @Override
    public byte[] randomNonce() {
      final byte[] iv = new byte[this.getNonceLength()];
      SECURE_RANDOM.nextBytes(iv);
      return iv;
    }

    @Override
    public byte[] encrypt(ByteBuffer plainText, ByteBuffer aad, byte[] key, byte[] nonce)
        throws CeffCryptoException {
      try {
        this.validateKey(key);
        final Cipher encipher = Cipher.getInstance(ALGO);
        encipher.init(
            Cipher.ENCRYPT_MODE,
            new SecretKeySpec(key, "AES"),
            new GCMParameterSpec(TAG_LEN, nonce));
        encipher.updateAAD(aad);
        return encipher.doFinal(CeffUtils.toArray(plainText));
      } catch (final Exception e) {
        throw new CeffCryptoException("encryption failed", e, this);
      }
    }

    @Override
    public byte[] decrypt(ByteBuffer cipherText, ByteBuffer aad, byte[] key, byte[] nonce)
        throws CeffCryptoException {
      try {
        this.validateKey(key);
        final Cipher encipher = Cipher.getInstance(ALGO);
        encipher.init(
            Cipher.DECRYPT_MODE,
            new SecretKeySpec(key, "AES"),
            new GCMParameterSpec(TAG_LEN, nonce));
        encipher.updateAAD(aad);
        return encipher.doFinal(CeffUtils.toArray(cipherText));
      } catch (final Exception e) {
        throw new CeffCryptoException("decryption failed", e, this);
      }
    }

    @Override
    public boolean isSupported() {
      return true;
    }
  }

  private static final class ChaCha20Poly1305Mode extends CeffMode {
    private static final int TAG_LEN = 16; // 16 bytes
    private static final int IV_LEN = 12; // 12 bytes
    static final String ALGO = "ChaCha20-Poly1305";

    private ChaCha20Poly1305Mode() {
      super((byte) 2);
    }

    @Override
    public int getTagLength() {
      return TAG_LEN;
    }

    @Override
    public int getNonceLength() {
      return IV_LEN;
    }

    @Override
    public byte[] randomNonce() {
      final byte[] iv = new byte[this.getNonceLength()];
      SECURE_RANDOM.nextBytes(iv);
      return iv;
    }

    @Override
    public byte[] encrypt(ByteBuffer plainText, ByteBuffer aad, byte[] key, byte[] nonce)
        throws CeffCryptoException {

      if (!Constants.JRE_IS_MINIMUM_JAVA11) {
        throw new CeffCryptoException(ALGO + " only available for Java 11 and above", this);
      }

      try {
        this.validateKey(key);
        final Cipher encipher = Cipher.getInstance(ALGO);
        encipher.init(
            Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "ChaCha20"), new IvParameterSpec(nonce));
        encipher.updateAAD(aad);
        return encipher.doFinal(CeffUtils.toArray(plainText));
      } catch (final Exception e) {
        throw new CeffCryptoException("encryption failed", e, this);
      }
    }

    @Override
    public byte[] decrypt(ByteBuffer cipherText, ByteBuffer aad, byte[] key, byte[] nonce)
        throws CeffCryptoException {

      if (!Constants.JRE_IS_MINIMUM_JAVA11) {
        throw new CeffCryptoException(ALGO + " only available for Java 11 and above", this);
      }

      try {
        this.validateKey(key);
        final Cipher encipher = Cipher.getInstance(ALGO);
        encipher.init(
            Cipher.DECRYPT_MODE, new SecretKeySpec(key, "ChaCha20"), new IvParameterSpec(nonce));
        encipher.updateAAD(aad);
        return encipher.doFinal(CeffUtils.toArray(cipherText));
      } catch (final Exception e) {
        throw new CeffCryptoException("decryption failed", e, this);
      }
    }

    @Override
    public boolean isSupported() {
      return Constants.JRE_IS_MINIMUM_JAVA11;
    }
  }

  private static final class NullMode extends CeffMode {

    private NullMode() {
      super((byte) 0);
    }

    @Override
    public int getTagLength() {
      return 0;
    }

    @Override
    public void validateKey(byte[] key) {}

    @Override
    public int getNonceLength() {
      return 0;
    }

    @Override
    public byte[] randomNonce() {
      return new byte[0];
    }

    @Override
    public byte[] encrypt(ByteBuffer plainText, ByteBuffer aad, byte[] key, byte[] nonce) {
      return CeffUtils.toArray(plainText);
    }

    @Override
    public byte[] decrypt(ByteBuffer cipherText, ByteBuffer aad, byte[] key, byte[] nonce)
        throws CeffCryptoException {
      return CeffUtils.toArray(cipherText);
    }

    @Override
    public boolean isSupported() {
      return true;
    }
  }

  static CeffMode getByModeByte(byte mode) throws CeffCryptoException {
    final CeffMode result = modes.get(mode);
    if (result == null) {
      throw new IllegalArgumentException("Unknown mode: " + mode);
    }
    return result;
  }
}
