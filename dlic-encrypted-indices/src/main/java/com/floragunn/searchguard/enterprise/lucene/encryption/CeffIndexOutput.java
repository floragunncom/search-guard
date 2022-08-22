/* 
 * Copyright (C) 2021 by eliatra Ltd. - All Rights Reserved
 * Unauthorized copying, usage or modification of this file in its source or binary form, 
 * via any medium is strictly prohibited.
 * Proprietary and confidential.
 * 
 * https://eliatra.com
 */
package com.floragunn.searchguard.enterprise.lucene.encryption;

import com.google.common.primitives.Longs;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;
import java.util.zip.CRC32;

/**
 * A {@link IndexOutput} implementations which wraps another IndexOutput and encrypt all write
 * requests.
 */
public final class CeffIndexOutput extends IndexOutput {

  private final IndexOutput delegate;
  private final int chunkLength;
  private final byte[] key;

  private final ByteBuffer buffer;
  private final byte[] singleByteBuffer = new byte[1];
  private final CRC32 crc32 = new CRC32();
  private final ByteBuffer aadBuffer = ByteBuffer.allocate(CeffUtils.AAD_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
  private final ByteBuffer signatureAadBuffer = ByteBuffer.allocate(CeffUtils.AAD_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
  private final MessageDigest sha512md;

  private long filePointer = 0L;
  private long chunk = 0L;

  private final CeffMode mode;

  /**
   * Sole constructor
   *
   * @param delegate The wrapped output
   * @param chunkLength Length of a chunk in bytes. See {@link CeffMode}
   //* @param key The en-/decryption key (the array is cloned)
   * @param mode See {@link CeffMode}
   * @throws IOException if the delegate throws an IOException
   * @throws IllegalArgumentException when chunkSize or key is invalid
   */
  public CeffIndexOutput(IndexOutput delegate, int chunkLength, byte[] key0, CeffMode mode)
          throws IOException, CeffCryptoException {
    super("Ceff " + delegate.toString(), delegate.getName());
    this.delegate = delegate;
    this.chunkLength = chunkLength;
    this.mode = mode;
    this.key = this.mode.randomKey();
    this.mode.validateKey(key0);

    CeffUtils.validateChunkLength(this.chunkLength);

    try {
      this.sha512md = MessageDigest.getInstance(CeffUtils.SHA512_DIGEST_ALGO);
    } catch (final NoSuchAlgorithmException e) {
      // can not happen
      throw new RuntimeException(e);
    }
    this.buffer = ByteBuffer.allocate(this.chunkLength).order(ByteOrder.LITTLE_ENDIAN);

    delegate.writeInt(CeffUtils.CEFF_MAGIC); // write magic bytes
    delegate.writeByte(mode.getModeByte()); // write mode byte

    byte[] nonce = this.mode.randomNonce();
    byte[] cipherKey = this.mode.encrypt(ByteBuffer.wrap(this.key), null, key0, nonce);
    delegate.writeBytes(nonce, nonce.length);
    delegate.writeBytes(cipherKey, cipherKey.length);
  }

  @Override
  public void close() throws IOException {
    try {
      // encrypt last chunk
      this.encryptChunk(true);
    } finally {
      this.delegate.close();
    }
  }

  @Override
  public long getFilePointer() {
    // need to return the plain text oriented file pointer position
    return this.filePointer;
  }

  @Override
  public long getChecksum() throws IOException {
    // need to return the plain text checksum
    return this.crc32.getValue();
  }

  @Override
  public void writeByte(byte b) throws IOException {
    this.singleByteBuffer[0] = b;
    this.writeBytes(this.singleByteBuffer, 0, 1);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {

    if (length == 0) {
      return;
    }

    int written = 0;

    while (written < length) {

      final int maxWrite = Math.min(this.buffer.capacity(), (length - written));

      if (this.buffer.remaining() >= maxWrite) {
        this.buffer.put(b, offset + written, maxWrite);
      } else if (!this.buffer.hasRemaining()) {
        this.encryptChunk(false);
        assert this.buffer.position() == 0;
        assert this.buffer.hasRemaining();
        this.buffer.put(b, offset + written, maxWrite);
      } else {
        final int remaining = this.buffer.remaining();
        this.buffer.put(b, offset + written, remaining);
        this.encryptChunk(false);
        assert this.buffer.position() == 0;
        assert this.buffer.hasRemaining();
        this.buffer.put(b, offset + remaining + written, maxWrite - remaining);
        assert this.buffer.position() == maxWrite - remaining;
      }

      written += maxWrite;
    }

    assert written == length;

    this.crc32.update(b, offset, length);
    this.filePointer += length;
  }

  private void encryptChunk(boolean lastChunk) throws IOException {

    try {
      this.buffer.flip();
      final byte[] cipherText = this.encryptData(lastChunk);
      this.delegate.writeBytes(cipherText, 0, cipherText.length);
      this.buffer.clear();

      this.chunk++;
    } catch (final CeffCryptoException e) {
      throw new IOException(e);
    }
  }

  private byte[] encryptData(boolean lastChunk) throws CeffCryptoException {
    final UUID chunkId = UUID.randomUUID();
    final byte[] nonce = CeffUtils.longToNonce(this.chunk, this.mode.getNonceLength());

    this.aadBuffer.clear();
    this.aadBuffer.putLong(this.chunk);
    this.aadBuffer.putLong(chunkId.getMostSignificantBits());
    this.aadBuffer.putLong(chunkId.getLeastSignificantBits());
    this.aadBuffer.flip();
    this.sha512md.update(this.aadBuffer);
    this.aadBuffer.rewind();

    System.out.println("write this.chunk "+this.chunk);

    final byte[] cipherText = this.mode.encrypt(this.buffer, this.aadBuffer, this.key, nonce);

    this.aadBuffer.rewind();
    if (lastChunk) {

      this.signatureAadBuffer.clear();
      this.signatureAadBuffer.putLong(this.chunkLength);
      this.signatureAadBuffer.putLong(this.chunk);
      this.signatureAadBuffer.putLong(this.filePointer);
      this.signatureAadBuffer.flip();

      final byte[] signatureNonce = CeffUtils.longToNonce((this.chunk+1), this.mode.getNonceLength());
      System.out.println("write this.chunk+1 "+(this.chunk+1));
      final byte[] signature = this.sha512md.digest();
      final byte[] signatureCipherText =
          this.mode.encrypt(
              ByteBuffer.wrap(signature), this.signatureAadBuffer, this.key, signatureNonce);
      this.signatureAadBuffer.rewind();

      return CeffUtils.concatArrays(
          CeffUtils.toArray(this.aadBuffer),
          cipherText,
          CeffUtils.toArray(this.signatureAadBuffer),
          signatureCipherText);
    } else {
      return CeffUtils.concatArrays(CeffUtils.toArray(this.aadBuffer), cipherText);
    }
  }
}
