package com.floragunn.searchguard.enterprise.encrypted_indices.crypto;

import org.opensearch.index.IndexSettings;

public abstract class CryptoOperationsFactory {
    public abstract CryptoOperations createCryptoOperations(IndexSettings indexSettings);
}
