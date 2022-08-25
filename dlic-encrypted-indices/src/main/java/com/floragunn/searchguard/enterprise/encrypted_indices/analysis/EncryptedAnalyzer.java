package com.floragunn.searchguard.enterprise.encrypted_indices.analysis;

import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.CryptoOperations;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class EncryptedAnalyzer extends Analyzer {

    private final CryptoOperations cryptoOperations;

    public EncryptedAnalyzer(CryptoOperations cryptoOperations) {
        super(PER_FIELD_REUSE_STRATEGY);
        this.cryptoOperations = cryptoOperations;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        System.out.println("createComponents '"+fieldName+"'");
        Tokenizer tokenizer = new StandardTokenizer();
        return new TokenStreamComponents(tokenizer, new XorEncryptedTokenFilter(new LowerCaseFilter(tokenizer), fieldName, cryptoOperations));
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        System.out.println("normalize "+fieldName);
        return new XorEncryptedTokenFilter(new LowerCaseFilter(in), fieldName, cryptoOperations);
    }



}
