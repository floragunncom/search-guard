package com.floragunn.searchguard.enterprise.encrypted_indices.crypto;

import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;

public class DefaultCryptoOperationsFactory extends CryptoOperationsFactory{
    @Override
    public CryptoOperations createCryptoOperations(IndexSettings indexSettings) {

        Settings settings = indexSettings.getSettings();

        if(settings.getAsBoolean("index.encryption_enabled", false)) {

            final String indexEncryptionKey = settings.get("index.encryption_key");

            if(indexEncryptionKey == null || indexEncryptionKey.isEmpty()) {
                throw new RuntimeException("index.encryption_key can not be empty");
            }

            return new DefaultCryptoOperations(indexEncryptionKey);

        } else {
            return null;
        }
    }
}
