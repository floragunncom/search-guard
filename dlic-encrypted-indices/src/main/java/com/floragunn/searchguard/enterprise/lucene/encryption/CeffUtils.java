/* 
 * Copyright (C) 2021 by eliatra Ltd. - All Rights Reserved
 * Unauthorized copying, usage or modification of this file in its source or binary form, 
 * via any medium is strictly prohibited.
 * Proprietary and confidential.
 * 
 * https://eliatra.com
 */
package com.floragunn.searchguard.enterprise.lucene.encryption;

import org.apache.lucene.util.ArrayUtil;

import java.nio.ByteBuffer;
import java.util.Objects;

/** Constants and static utility methods */
public final class CeffUtils {

  /** The Ceff magic number */
  public static final int CEFF_MAGIC = 846197364;

  /** Maximum size of a chunk (in bytes) */
  public static final int CHUNK_SIZE_MAX = 1_073_741_824;

  /** Minimum size of a chunk (in bytes) */
  public static final int CHUNK_SIZE_MIN = 16 * 1024;

  /** Length of the Ceff magic number in bytes */
  public static final int MAGIC_LENGTH = Integer.BYTES;

  /** Length of the Ceff mode in bytes */
  public static final int MODE_LENGTH = 1;

  /** Length of the additional authentication data in bytes */
  public static final int AAD_LENGTH = 3 * Long.BYTES;

  /** Length of the signature at the end of the file in bytes */
  public static final int SIGNATURE_LENGTH = 64;

  /** Length of a Ceff header in bytes */
  private static final int _HEADER_LENGTH = MAGIC_LENGTH + MODE_LENGTH;

  /** The name of the SHA 512 algorithm */
  public static final String SHA512_DIGEST_ALGO = "SHA-512";

  private CeffUtils() {
    super();
  }

  public static int headerLength(CeffMode mode) {
    return _HEADER_LENGTH+mode.getNonceLength()+32+mode.getTagLength();
  }

  /**
   * @param mode The mode used to encrypt the data.
   * @return The length (in bytes) of the additional crypto data (nonce, auth tag, aad) for a chunk
   */
  public static int cryptoLength(CeffMode mode) {
    return /*mode.getNonceLength() +*/ mode.getTagLength() + AAD_LENGTH;
  }

  /**
   * @param mode The mode used to encrypt the data.
   * @return The length of the footer (in bytes)
   */
  public static int footerLength(CeffMode mode) {
    return cryptoLength(mode) + SIGNATURE_LENGTH;
  }

  /**
   * Validate the chunkLength
   *
   * @param chunkLength Length of a chunk (in bytes)
   * @throws IllegalArgumentException when chunkLength is not between (or equal to) CHUNK_SIZE_MIN
   *     and CHUNK_SIZE_MAX
   */
  public static void validateChunkLength(int chunkLength) {
    if (chunkLength < CHUNK_SIZE_MIN) {
      throw new IllegalArgumentException(
          "chunkLength ("
              + chunkLength
              + ") to small, must be >= "
              + CHUNK_SIZE_MIN
              + " and <= "
              + CHUNK_SIZE_MAX);
    }

    if (chunkLength > CHUNK_SIZE_MAX) {
      throw new IllegalArgumentException(
          "chunkLength ("
              + chunkLength
              + ") to big, must be >= "
              + CHUNK_SIZE_MIN
              + " and <= "
              + CHUNK_SIZE_MAX);
    }
  }

  /**
   * Validate the magic file header
   *
   * @param magic The magic to validate
   * @throws IllegalArgumentException if the header is not valid
   */
  public static void validateMagicHeaderBytes(int magic) {
    if (magic != CEFF_MAGIC) {
      throw new IllegalArgumentException(
          "Invalid magic bytes, is: " + magic + ", expected: " + CEFF_MAGIC);
    }
  }

  /**
   * Concatenate arrays
   *
   * @param first First array (not null)
   * @param second Second array (not null)
   * @param more additional arrays to be concatenated to the first two (some arrays can be null -
   *     they will be ignored)
   * @return The concatenated arrays
   */
  public static byte[] concatArrays(byte[] first, byte[] second, byte[]... more) {
    Objects.requireNonNull(first, "first");
    Objects.requireNonNull(second, "second");

    int newLength = first.length + second.length;

    if (more != null && more.length > 0) {
      for (final byte[] b : more) {
        if (b != null) {
          newLength += b.length;
        }
      }
    }

    final byte[] result = ArrayUtil.growExact(first, newLength);
    System.arraycopy(second, 0, result, first.length, second.length);

    if (more != null && more.length > 0) {
      int offset = first.length + second.length;
      for (final byte[] b : more) {
        if (b != null && b.length > 0) {
          System.arraycopy(b, 0, result, offset, b.length);
          offset += b.length;
        }
      }
    }

    return result;
  }

  /**
   * Transfer the remaining bytes from a ByteBuffer to a new array
   *
   * @param buf The source ByteBuffer. This method does not flip or rewind the buffer.
   * @return The new array containing the remaining bytes of the ByteBuffer.
   */
  public static byte[] toArray(ByteBuffer buf) {
    final byte[] bufBytes = new byte[buf.remaining()];
    buf.get(bufBytes);
    return bufBytes;
  }

  /**
   * Deep clone of a ByteBuffer. The source Buffer will not be modified.
   *
   * @param buf Source buffer to be copied/cloned
   * @return A deep clone of the source Buffer with the same data and same position/limit and byte
   *     order. If the source buffer was a direct buffer the clone will be also a direct buffer.
   */
  public static ByteBuffer deepClone(ByteBuffer buf) {
    final ByteBuffer bufClone =
        buf.isDirect()
            ? ByteBuffer.allocateDirect(buf.capacity())
            : ByteBuffer.allocate(buf.capacity());
    final ByteBuffer bufReadOnly = buf.asReadOnlyBuffer();

    bufReadOnly.rewind();
    bufClone.put(bufReadOnly);
    bufClone.position(buf.position());
    bufClone.limit(buf.limit());
    bufClone.order(buf.order());
    return bufClone;
  }

  /**
   * @param encryptedFileLength Length of encrypted data
   * @param chunkLength Length of a chunk
   * @param mode The mode
   * @return Number of chunks in the file/data
   */
  public static long calculateNumberOfChunks(
      long encryptedFileLength, int chunkLength, CeffMode mode) {
    validateChunkLength(chunkLength);
    if (encryptedFileLength == 0L) {
      return 0L;
    }
    final int additionalBytesLen = CeffUtils.headerLength(mode) + CeffUtils.footerLength(mode);
    return ((encryptedFileLength - additionalBytesLen)
            / (CeffUtils.cryptoLength(mode) + chunkLength))
        + 1;
  }

  /**
   * @param encryptedFileLength Length of encrypted data
   * @param chunkLength Length of a chunk
   * @param mode The mode
   * @return Number of additional bytes added for encryption and signature
   */
  public static long calculateEncryptionOverhead(
      long encryptedFileLength, int chunkLength, CeffMode mode) {
    validateChunkLength(chunkLength);
    if (encryptedFileLength == 0L) {
      return 0L;
    }
    final long chunks = calculateNumberOfChunks(encryptedFileLength, chunkLength, mode);
    return (chunks * cryptoLength(mode)) + CeffUtils.headerLength(mode) + CeffUtils.footerLength(mode);
  }

  /**
   * @param encryptedFileLength Length of encrypted data
   * @param chunkLength Length of a chunk
   * @param mode The mode
   * @return Length of plain text in a chunked file
   */
  public static long calculatePlainFileLength(
      long encryptedFileLength, int chunkLength, CeffMode mode) {
    validateChunkLength(chunkLength);
    return encryptedFileLength
        - calculateEncryptionOverhead(encryptedFileLength, chunkLength, mode);
  }

  public static byte[] longToNonce(long lng, int nonceLength) {
    if(nonceLength < 8) {
      throw new IllegalArgumentException("nonceLength must be >= 8");
    }
    byte[] bytes = new byte[nonceLength];
    bytes[0] = (byte) lng;
    bytes[1] = (byte) (lng >> 8);
    bytes[2] = (byte) (lng >> 16);
    bytes[3] = (byte) (lng >> 24);
    bytes[4] = (byte) (lng >> 32);
    bytes[5] = (byte) (lng >> 40);
    bytes[6] = (byte) (lng >> 48);
    bytes[7] = (byte) (lng >> 56);
    return bytes;
  }
}
