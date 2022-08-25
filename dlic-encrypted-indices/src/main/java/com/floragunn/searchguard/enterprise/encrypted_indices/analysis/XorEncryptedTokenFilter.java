/* 
 * Copyright (C) 2021 by eliatra Ltd. - All Rights Reserved
 * Unauthorized copying, usage or modification of this file in its source or binary form, 
 * via any medium is strictly prohibited.
 * Proprietary and confidential.
 * 
 * https://eliatra.com
 */
package com.floragunn.searchguard.enterprise.encrypted_indices.analysis;

import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.CryptoOperations;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;

import java.io.IOException;

public class XorEncryptedTokenFilter extends TokenFilter {

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

  private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);
  private final BytesTermAttribute bytesTermAtt = addAttribute(BytesTermAttribute.class);
  private final TermToBytesRefAttribute termToBytesAtt =
      addAttribute(TermToBytesRefAttribute.class);

  // BytesTermAttribute.java
  // TermToBytesRefAttribute.java
  private final CryptoOperations cryptoOperations;

  private final String fieldName;

  public XorEncryptedTokenFilter(TokenStream in, String fieldName, CryptoOperations cryptoOperations) {
    super(in);
    if(cryptoOperations == null) {
      throw new IllegalArgumentException("cryptoOperations");
    }

    this.cryptoOperations = cryptoOperations;
    this.fieldName = fieldName;

    System.out.println(getClass()+" for "+fieldName);
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      try {
        cryptoOperations.xorAttribute(termAtt, fieldName);
        cryptoOperations.hashAttribute(bytesTermAtt);
        cryptoOperations.hashAttribute(termToBytesAtt);
        cryptoOperations.hashAttribute(keywordAttr);
      } catch (Exception e) {
        throw new IOException(e);
      }
      return true;
    } else {
      return false;
    }
  }
}
