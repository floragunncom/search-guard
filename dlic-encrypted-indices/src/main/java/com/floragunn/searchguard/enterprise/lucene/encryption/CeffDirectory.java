/* 
 * Copyright (C) 2021 by eliatra Ltd. - All Rights Reserved
 * Unauthorized copying, usage or modification of this file in its source or binary form, 
 * via any medium is strictly prohibited.
 * Proprietary and confidential.
 * 
 * https://eliatra.com
 */
package com.floragunn.searchguard.enterprise.lucene.encryption;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Constants;

import java.io.IOException;

/**
 * A Lucene {@link FSDirectory} implementations which wraps another FSDirectory and encrypt and
 * decrypt all read and write requests with a symmetric AEAD encryption scheme (see {@link
 * CeffMode}).
 */
public final class CeffDirectory extends FSDirectory {

  public static final int DEFAULT_CHUNK_LENGTH = 64 * 1024; // 64kb
  private final FSDirectory delegate;
  private final int chunkLength;
  private final byte[] key;
  private final CeffMode mode;

  /**
   * Create a new encrypted directory. Uses a chunks length of 64kb.
   *
   * @param delegate The wrapped implementation, typically {@link MMapDirectory} or {@link
   *     NIOFSDirectory}
   * @param key A secret 256 bit key (the array is cloned)
   * @throws IOException if the delegate throws an IOException or if there where issues with
   *     en-/decryption
   */
  public CeffDirectory(FSDirectory delegate, byte[] key) throws IOException {
    this(
        delegate,
        FSLockFactory.getDefault(),
        key,
        DEFAULT_CHUNK_LENGTH,
        Constants.JRE_IS_MINIMUM_JAVA11 ? CeffMode.CHACHA20_POLY1305_MODE : CeffMode.AES_GCM_MODE);
  }

  /**
   * Create a new encrypted directory.
   *
   * @param delegate The wrapped implementation, typically {@link MMapDirectory} or {@link
   *     NIOFSDirectory}
   * @param key A secret 256 bit key (the array is cloned)
   * @param chunkLength The length (size) of a chunk in bytes. See {@link CeffMode}
   * @param mode See {@link CeffMode}
   * @throws IOException if the delegate throws an IOException or if there were issues with
   *     en-/decryption
   */
  public CeffDirectory(FSDirectory delegate, byte[] key, int chunkLength, CeffMode mode)
      throws IOException {
    this(delegate, FSLockFactory.getDefault(), key, chunkLength, mode);
  }

  /**
   * Create a new encrypted directory.
   *
   * @param delegate The wrapped implementation, typically {@link MMapDirectory} or {@link
   *     NIOFSDirectory}
   * @param lockFactory A {@link LockFactory}
   * @param key A secret 256 bit key (the array is cloned)
   * @param chunkLength The length (size) of a chunk in bytes. See {@link CeffMode}
   * @param mode See {@link CeffMode}
   * @throws IOException if the delegate throws an IOException or if there where issues with
   *     en-/decryption
   */
  public CeffDirectory(
      FSDirectory delegate, LockFactory lockFactory, byte[] key, int chunkLength, CeffMode mode)
      throws IOException {
    super(delegate.getDirectory(), lockFactory);
    this.delegate = delegate;
    this.mode = mode;
    this.mode.validateKey(key);
    this.key = key.clone();
    this.chunkLength = chunkLength;
    CeffUtils.validateChunkLength(this.chunkLength);
  }

  @Override
  public IndexInput openInput(String fileName, IOContext context) throws IOException {
    this.ensureOpen();
    this.ensureCanRead(fileName);
    final IndexInput tmpInput = this.delegate.openInput(fileName, context);

    if (tmpInput.length() < CeffUtils.headerLength(mode)) {
      return tmpInput;
    }

    // read plaintext files
    if (tmpInput.readInt() != CeffUtils.CEFF_MAGIC) {
      tmpInput.seek(0);
      return tmpInput;
    } else {
      tmpInput.seek(0);
    }

    try {
      return new CeffIndexInput(tmpInput, this.key);
    } catch (final IOException e) {
      tmpInput.close();
      throw e;
    }
  }

  @Override
  public IndexOutput createOutput(String fileName, IOContext context) throws IOException {
    final IndexOutput tmpOutput = this.delegate.createOutput(fileName, context);
    try {
      return new CeffIndexOutput(tmpOutput, this.chunkLength, this.key, this.mode);
    } catch (final IOException e) {
      tmpOutput.close();
      throw e;
    } catch (CeffCryptoException e) {
      tmpOutput.close();
      throw new IOException(e);
    }
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      throws IOException {
    final IndexOutput tmpOutput = this.delegate.createTempOutput(prefix, suffix, context);
    try {
      return new CeffIndexOutput(tmpOutput, this.chunkLength, this.key, this.mode);
    } catch (final IOException e) {
      tmpOutput.close();
      throw e;
    } catch (CeffCryptoException e) {
      tmpOutput.close();
      throw new IOException(e);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    this.delegate.close();
    super.close();
  }

  public int getChunkLength() {
    return this.chunkLength;
  }

  /** @return A clone of the byte array */
  public byte[] getKey() {
    return this.key.clone();
  }

  public CeffMode getMode() {
    return this.mode;
  }

  public FSDirectory getDelegate() {
    return this.delegate;
  }
}
