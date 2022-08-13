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
import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.DummyCryptoOperations;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;

import java.io.IOException;

public class EncryptedTokenFilter extends TokenFilter {

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

  private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);
  private final BytesTermAttribute bytesTermAtt = addAttribute(BytesTermAttribute.class);
  private final TermToBytesRefAttribute termToBytesAtt =
      addAttribute(TermToBytesRefAttribute.class);

  // BytesTermAttribute.java
  // TermToBytesRefAttribute.java
  private final CryptoOperations cryptoOperations = new DummyCryptoOperations();

  public EncryptedTokenFilter(TokenStream in) {
    super(in);
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      cryptoOperations.hashAttribute(termAtt);
      cryptoOperations.hashAttribute(bytesTermAtt);
      cryptoOperations.hashAttribute(termToBytesAtt);
      return true;
    } else {
      return false;
    }
  }
}
