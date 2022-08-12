/* 
 * Copyright (C) 2021 by eliatra Ltd. - All Rights Reserved
 * Unauthorized copying, usage or modification of this file in its source or binary form, 
 * via any medium is strictly prohibited.
 * Proprietary and confidential.
 * 
 * https://eliatra.com
 */
package com.floragunn.searchguard.enterprise.encrypted_indices.analysis;

import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.Cryptor;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.TokenStream;

public class EncryptedAnalyzer extends AnalyzerWrapper {

  // 0..n character filters
  // 1 tokenizers
  // 0..n token filters


  private final Analyzer analyzer;
  private final Cryptor cryptor;

  public EncryptedAnalyzer(Analyzer analyzer, Cryptor cryptor) {
    super(analyzer.getReuseStrategy());
    if (analyzer instanceof EncryptedAnalyzer) {
      throw new IllegalArgumentException("Can not wrap an already encrypted analyzer");
    }
    this.analyzer = analyzer;
    this.cryptor = cryptor;
  }

  @Override
  protected Analyzer getWrappedAnalyzer(String fieldName) {
    return this.analyzer;
  }

  @Override
  protected TokenStreamComponents wrapComponents(
      String fieldName, TokenStreamComponents components) {
    final TokenStream delegate = components.getTokenStream();
    TokenStream result;
    if (delegate instanceof EncryptedTokenFilter) {
      result = delegate;
    } else {
      result = new EncryptedTokenFilter(delegate, cryptor);
    }
    return new TokenStreamComponents(components.getSource(), result);
  }
}
