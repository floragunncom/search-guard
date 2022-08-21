/* 
 * Copyright (C) 2021 by eliatra Ltd. - All Rights Reserved
 * Unauthorized copying, usage or modification of this file in its source or binary form, 
 * via any medium is strictly prohibited.
 * Proprietary and confidential.
 * 
 * https://eliatra.com
 */
package com.floragunn.searchguard.enterprise.lucene.encryption;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * A {@link IndexInput} implementation which wraps an underlying IndexInput and decrypt all read
 * requests.
 */
public final class CeffIndexInput extends IndexInput {

  private static final IndexInput EMPTY_INDEX_INPUT = new EmptyIndexInput();

  private IndexInput delegate;
  /** same value also for slices */
  private IndexInput physicalDelegate;

  private final byte[] key;
  private ByteBuffer buffer;
  private byte[] singleByteBuffer = new byte[1];
  private byte[] readBuffer;
  private ByteBuffer aadBuffer = ByteBuffer.allocate(CeffUtils.AAD_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
  private long absoluteStartChunk = -1L;
  private final long length;
  /** same value also for slices */
  private final long plainFileLength;

  private final long sliceOffset;
  /** same value also for slices */
  private final long absoluteChunkCount;

  private long filePointer = 0L;
  /** same value also for slices */
  private final int chunkLength;

  private final boolean slice;
  private long currentAbsoluteChunkNum = -1L;
  private boolean isClone = false;
  /** same value also for slices */
  private final CeffMode mode;

  /**
   * Sole constructor
   *
   * @param delegate The wrapped input
   * @param key en-/decryption key
   * @throws IOException also in case the file was tampered with
   * @throws IllegalArgumentException when chunkSize or key is invalid
   */
  public CeffIndexInput(IndexInput delegate, byte[] key) throws IOException {
    this(delegate, key, delegate, 0, 0, 0, 0, 0, null, false);
  }

  private CeffIndexInput(
      final IndexInput delegate,
      final byte[] key0,
      final IndexInput physicalDelegate0,
      final long sliceOffset0,
      final long sliceLength0,
      final int chunkLength0,
      final long absoluteChunkCount0,
      final long plainFileLength0,
      final CeffMode mode0,
      final boolean slice0)
      throws IOException {
    super("Ceff " + delegate.toString());
    this.delegate = delegate;
    this.physicalDelegate = physicalDelegate0;
    this.sliceOffset = sliceOffset0;

    if (slice0) {
      // slice (or slice of slice)
      this.isClone = true;
      assert delegate != this.physicalDelegate;
      this.length = sliceLength0;
      this.chunkLength = chunkLength0;
      this.slice = true;
      this.plainFileLength = plainFileLength0;
      this.absoluteChunkCount = absoluteChunkCount0;
      this.mode = mode0;
      this.key = key0;

    } else {
      // physical
      this.isClone = false;
      assert delegate == this.physicalDelegate;
      assert sliceLength0 == 0;
      assert sliceOffset0 == 0;
      assert this.physicalDelegate.length() > 0;

      final int magic = this.physicalDelegate.readInt();

      CeffUtils.validateMagicHeaderBytes(magic);

      final byte ceffmode = this.physicalDelegate.readByte();
      try {
        this.mode = CeffMode.getByModeByte(ceffmode);
        this.mode.validateKey(key0);
        this.slice = false;

        final byte[] nonce = new byte[mode.getNonceLength()];
        final byte[] ekey = new byte[32+mode.getTagLength()];
        this.physicalDelegate.readBytes(nonce,0,mode.getNonceLength());
        this.physicalDelegate.readBytes(ekey,0,32+mode.getTagLength());
        this.key = mode.decrypt(ByteBuffer.wrap(ekey), null, key0, nonce);

        // seek to footer
        this.physicalDelegate.seek(
            this.physicalDelegate.length() - CeffUtils.footerLength(this.mode));


        this.physicalDelegate.readBytes(nonce, 0, nonce.length);

        this.chunkLength = this.castSafe(this.physicalDelegate.readLong());

        CeffUtils.validateChunkLength(this.chunkLength);
        final long lastChunk = this.physicalDelegate.readLong();
        this.plainFileLength = this.physicalDelegate.readLong();
        this.length = this.plainFileLength;
        this.absoluteChunkCount = lastChunk + 1;

        this.aadBuffer.clear();
        this.aadBuffer.putLong(this.chunkLength);
        this.aadBuffer.putLong(lastChunk);
        this.aadBuffer.putLong(this.plainFileLength);
        this.aadBuffer.flip();

        final byte[] sigCipher = new byte[CeffUtils.SIGNATURE_LENGTH + this.mode.getTagLength()];
        this.physicalDelegate.readBytes(
            sigCipher, 0, CeffUtils.SIGNATURE_LENGTH + this.mode.getTagLength());
        // decrypt signature and validate aad
        final byte[] plainTextSignature =
            this.mode.decrypt(ByteBuffer.wrap(sigCipher), this.aadBuffer, this.key, nonce);

        // scan file and validate signature
        MessageDigest sha512md;
        try {
          sha512md = MessageDigest.getInstance(CeffUtils.SHA512_DIGEST_ALGO);
        } catch (final NoSuchAlgorithmException e) {
          // cannot happen
          throw new RuntimeException(e);
        }

        for (long k = 0; k < this.absoluteChunkCount; k++) {
          // seek to the start of the chunk
          this.physicalDelegate.seek(
                  CeffUtils.headerLength(mode)
                  + this.mode.getNonceLength()
                  + (k
                      * (CeffUtils.AAD_LENGTH
                          + this.chunkLength
                          + this.mode.getTagLength()
                          + this.mode.getNonceLength())));
          final long chunk = this.physicalDelegate.readLong();
          final long chunkIdMsb = this.physicalDelegate.readLong();
          final long chunkIdLsb = this.physicalDelegate.readLong();

          this.aadBuffer.clear();
          this.aadBuffer.putLong(chunk);
          this.aadBuffer.putLong(chunkIdMsb);
          this.aadBuffer.putLong(chunkIdLsb);
          this.aadBuffer.flip();
          sha512md.update(this.aadBuffer);

          if (chunk != k) {
            throw new CeffCryptoException("verification failed: chunk number mismatch", this.mode);
          }
        }

        if (!Arrays.equals(plainTextSignature, sha512md.digest())) {
          throw new CeffCryptoException("verification failed: signature mismatch", this.mode);
        }

      } catch (final CeffCryptoException e) {
        throw new IOException(e);
      } catch (final IOException e) {
        throw e;
      }

      assert this.absoluteChunkCount
          == CeffUtils.calculateNumberOfChunks(
              this.physicalDelegate.length(), this.chunkLength, this.mode);
      assert this.plainFileLength
          == CeffUtils.calculatePlainFileLength(
              this.physicalDelegate.length(), this.chunkLength, this.mode);

      // seek to the start of the first chunk
      this.physicalDelegate.seek(CeffUtils.headerLength(mode));
    }

    this.buffer = ByteBuffer.allocate(this.chunkLength).order(ByteOrder.LITTLE_ENDIAN);
    this.readBuffer = new byte[this.chunkLength + CeffUtils.cryptoLength(this.mode)];
    this.decryptChunk(); // decrypt first chunk
  }

  @Override
  public IndexInput clone() {
    final CeffIndexInput clone = (CeffIndexInput) super.clone();
    clone.isClone = true;

    // we need to clone the delegate and deep clone the buffers
    // if this is done correctly the merge thread will throw exceptions

    if (this.delegate == this.physicalDelegate) {
      clone.delegate = this.delegate.clone();
      clone.physicalDelegate = clone.delegate;
    } else {
      clone.delegate = this.delegate.clone();
      clone.physicalDelegate = this.physicalDelegate.clone();
    }

    clone.buffer = CeffUtils.deepClone(this.buffer);
    clone.aadBuffer = CeffUtils.deepClone(this.aadBuffer);
    clone.singleByteBuffer =
        ArrayUtil.copyOfSubArray(this.singleByteBuffer, 0, this.singleByteBuffer.length);
    clone.readBuffer = ArrayUtil.copyOfSubArray(this.readBuffer, 0, this.readBuffer.length);

    return clone;
  }

  @Override
  public void seek(long pos) throws IOException {
    // pos is plaintext oriented
    if (pos < 0) {
      throw new EOFException("invalid position");
    }

    if (pos > this.length()) {
      throw new EOFException("read past EOF");
    }

    if (this.slice) {
      // also slice-of-slice needs to work

      // in which absolute chunk does the seeked position reside?
      final long absoluteChunkNum = (pos + this.sliceOffset) / this.chunkLength;

      assert this.absoluteStartChunk >= 0;

      // because the constructor calls decryptChunk() and sets absoluteStartChunk we know
      // were we are and can calculate a relative chunkNum (relative to the beginning of this slice)
      final long relativeChunkNum = absoluteChunkNum - this.absoluteStartChunk;

      // decrypt the chunk only if not already decrypted (crucial for performance)
      if (absoluteChunkNum != this.currentAbsoluteChunkNum) {
        // because the delegate here is a slice we need to operate with relative chunks
        this.delegate.seek(
            ((relativeChunkNum) * (this.chunkLength + CeffUtils.cryptoLength(this.mode))));
        this.decryptChunk();
      }

      this.filePointer = pos;

      // the chunk where the slice starts (this will be adjusted downward)
      final long offsetChunk = this.sliceOffset / this.chunkLength;

      // the offset from the beginning of the chunk
      final long relativeOffset = this.sliceOffset - (offsetChunk * this.chunkLength);

      // the new buffer offset is the relative offset + the relative pos
      final long bufferOffset = (relativeOffset + pos) - (relativeChunkNum * this.chunkLength);

      assert absoluteChunkNum >= this.absoluteStartChunk;

      // buffer starts always at the beginning of the chunk
      this.buffer.position(this.castSafe(bufferOffset));

    } else {
      // in which chunk does the seeked position reside?
      final long absoluteChunkNum = pos / this.chunkLength;
      assert absoluteChunkNum < this.absoluteChunkCount;

      // decrypt the chunk only if not already decrypted (crucial for performance)
      if (absoluteChunkNum != this.currentAbsoluteChunkNum) {
        // seek physically to the start of the chunk
        this.delegate.seek(
                CeffUtils.headerLength(mode)
                + ((absoluteChunkNum) * (this.chunkLength + CeffUtils.cryptoLength(this.mode))));
        this.decryptChunk();
      } else {
        this.filePointer = absoluteChunkNum * this.chunkLength;
      }

      final int bufferOffset = this.castSafe(((pos) - (absoluteChunkNum * (this.chunkLength))));
      // adjust buffer position because loadChunk()
      // positions the buffer at the beginning
      this.buffer.position(bufferOffset);
      this.filePointer += bufferOffset;
    }
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    // offset and length are plaintext oriented
    if (offset < 0 || length < 0 || offset + length > this.length()) {
      throw new IllegalArgumentException(
          "slice() "
              + sliceDescription
              + " out of bounds: offset="
              + offset
              + ",length="
              + length
              + ",length="
              + this.length()
              + ": "
              + this);
    }

    if (length == 0) {
      return EMPTY_INDEX_INPUT;
    }

    // also slice-of-slice needs to work so we need to keep track of the offsets
    final long effectiveOffset = this.sliceOffset + offset;

    // startChunk and endChunk are
    final long startChunk = effectiveOffset / this.chunkLength;
    final long endChunk = (length + effectiveOffset) / this.chunkLength;
    final long chunkSpan = endChunk - startChunk;

    // locate the offset in the encrypted file. This must be the beginning of a chunk.
    // we need to add the header length and for every chunk the crypto overhead
    final long newOffset =
            CeffUtils.headerLength(mode)
            + ((startChunk) * (this.chunkLength + CeffUtils.cryptoLength(this.mode)));
    long newLength;
    if (endChunk + 1L == this.absoluteChunkCount) {
      // last physical chunk
      // we need to calculate the length of the last chunk
      final long lastChunkLength = this.plainFileLength - ((endChunk * this.chunkLength));
      newLength = lastChunkLength + CeffUtils.cryptoLength(this.mode);
      assert lastChunkLength <= this.chunkLength;
      // add the length of a full sized chunks before the last chunk
      newLength += (chunkSpan) * (this.chunkLength + CeffUtils.cryptoLength(this.mode));
    } else {
      // if the end chunk is not the last chunk we extend the length to match the end of the end
      // chunk
      newLength = (chunkSpan + 1L) * (this.chunkLength + CeffUtils.cryptoLength(this.mode));
    }

    final String sliceDescription0 =
        this.slice ? "[slice-of-slice " + sliceDescription + "]" : sliceDescription;

    return new CeffIndexInput(
        this.physicalDelegate.slice(sliceDescription0, newOffset, newLength),
        this.key,
        this.physicalDelegate,
        effectiveOffset,
        length,
        this.chunkLength,
        this.absoluteChunkCount,
        this.plainFileLength,
        this.mode,
        true);
  }

  @Override
  public void readBytes(byte[] b, int offset, int length) throws IOException {
    // offset and length are plaintext oriented
    if (length == 0) {
      return;
    }

    int read = 0;

    while (read < length) {

      final int maxReadlen = Math.min(this.buffer.capacity(), (length - read));

      if (!this.buffer.hasRemaining()) {
        this.decryptChunk();
        assert this.buffer.position() == 0;
        this.buffer.get(b, offset + read, maxReadlen);
        if (!this.slice) {
          this.filePointer += maxReadlen;
        }
      } else if (this.buffer.remaining() >= maxReadlen) {
        this.buffer.get(b, offset + read, maxReadlen);
        if (!this.slice) {
          this.filePointer += maxReadlen;
        }
      } else {
        final int remaining = this.buffer.remaining();
        this.buffer.get(b, offset + read, remaining);
        assert !this.buffer.hasRemaining();
        this.decryptChunk();
        assert this.buffer.position() == 0;
        this.buffer.get(b, offset + remaining + read, maxReadlen - remaining);
        if (!this.slice) {
          this.filePointer += (maxReadlen - remaining);
        }
      }

      read += maxReadlen;
    }

    if (this.slice) {
      this.filePointer += length;
    }

    if (this.filePointer > this.length) {
      throw new EOFException("read past EOF: " + this.filePointer + " > " + this.length());
    }
  }

  private void decryptChunk() throws IOException {

    this.buffer.clear();
    final long remaining = this.delegate.length() - this.delegate.getFilePointer();

    assert remaining != 0 : "remaining == 0";

    int read = this.readBuffer.length;

    if (this.slice) {
      read = Math.min(this.readBuffer.length, this.castSafe(remaining));
    } else {

      if ((remaining - (CeffUtils.footerLength(this.mode) + CeffUtils.cryptoLength(this.mode)))
          < this.chunkLength) {
        // last chunk, typically less then chunklength in length
        read = this.castSafe(remaining) - CeffUtils.footerLength(this.mode);
      }
    }

    if (read <= 0) {
      throw new EOFException("read past EOF");
    }

    this.delegate.readBytes(this.readBuffer, 0, read);

    this.aadBuffer.clear();
    this.aadBuffer.put(this.readBuffer, this.mode.getNonceLength(), CeffUtils.AAD_LENGTH);
    this.aadBuffer.flip();

    final long chunk = this.aadBuffer.getLong();
    this.currentAbsoluteChunkNum = chunk;
    this.aadBuffer.rewind();

    byte[] plainText = null;
    try {
      plainText =
          this.mode.decrypt(
              ByteBuffer.wrap(
                      this.readBuffer,
                      (this.mode.getNonceLength() + CeffUtils.AAD_LENGTH),
                      read - (this.mode.getNonceLength() + CeffUtils.AAD_LENGTH))
                  .asReadOnlyBuffer(),
              this.aadBuffer,
              this.key,
              ArrayUtil.copyOfSubArray(this.readBuffer, 0, this.mode.getNonceLength()));

      if (this.absoluteStartChunk < 0L) {
        this.absoluteStartChunk = chunk;
      }

      if (!this.slice) {
        // physical mode
        // if chunk == 0 then this means fp=0 which should happen at the beginning of a physical
        // input
        this.filePointer = chunk * this.chunkLength;
      }

      this.buffer.put(plainText);
      this.buffer.flip();

      if (this.slice) {

        if (chunk > this.absoluteStartChunk) {
          this.buffer.position(0);
        }

        if (chunk == this.absoluteStartChunk) {
          this.buffer.position(
              this.castSafe((this.sliceOffset - this.absoluteStartChunk * this.chunkLength)));
        }
      }
    } catch (final CeffCryptoException e) {
      throw new IOException(e);
    }
  }

  @Override
  public byte readByte() throws IOException {
    this.readBytes(this.singleByteBuffer, 0, 1);
    return this.singleByteBuffer[0];
  }

  @Override
  public void close() throws IOException {
    if (!this.isClone) {
      assert this.delegate == this.physicalDelegate;
      this.delegate.close();
    }
  }

  @Override
  public long getFilePointer() {
    // need to return the plain text oriented file pointer position
    return this.filePointer;
  }

  @Override
  public long length() {
    // need to return the plain text length
    return this.length;
  }

  private int castSafe(long num) {
    if (num > Integer.MAX_VALUE || num < Integer.MIN_VALUE) {
      throw new IllegalArgumentException("Cannot cast " + num + " to int");
    }
    return (int) num;
  }

  private static class EmptyIndexInput extends IndexInput {

    protected EmptyIndexInput() {
      super("empty");
    }

    @Override
    public void readBytes(byte[] b, int offset, int length) throws IOException {
      if (length > 0) {
        throw new IOException("empty");
      }
    }

    @Override
    public byte readByte() throws IOException {
      throw new IOException("empty");
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return this;
    }

    @Override
    public void seek(long pos) throws IOException {
      if (pos != 0L) {
        throw new IOException("empty");
      }
    }

    @Override
    public long length() {
      return 0L;
    }

    @Override
    public long getFilePointer() {
      return 0L;
    }

    @Override
    public void close() throws IOException {}
  }
}
