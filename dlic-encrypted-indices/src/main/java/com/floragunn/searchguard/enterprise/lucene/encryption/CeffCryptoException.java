/* 
 * Copyright (C) 2021 by eliatra Ltd. - All Rights Reserved
 * Unauthorized copying, usage or modification of this file in its source or binary form, 
 * via any medium is strictly prohibited.
 * Proprietary and confidential.
 * 
 * https://eliatra.com
 */
package com.floragunn.searchguard.enterprise.lucene.encryption;

public class CeffCryptoException extends Exception {

  /** */
  private static final long serialVersionUID = 1L;

  public CeffCryptoException(String message, Throwable cause, CeffMode mode) {
    super(mode.getClass().getSimpleName() + ":: " + message, cause);
  }

  public CeffCryptoException(String message, CeffMode mode) {
    super(mode.getClass().getSimpleName() + ":: " + message);
  }
}
