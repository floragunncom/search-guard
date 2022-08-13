package com.floragunn.searchguard.enterprise.encrypted_indices.crypto;

import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.IndexSettings;

import java.util.Objects;

public class DefaultCryptoOperationsFactory extends CryptoOperationsFactory{

    private final Client client;

    private final ThreadContext threadContext;

    public DefaultCryptoOperationsFactory(Client client, ThreadContext threadContext) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.threadContext = Objects.requireNonNull(threadContext, "threadContext must not be null");
    }

    @Override
    public CryptoOperations createCryptoOperations(IndexSettings indexSettings) {

        Settings settings = indexSettings.getSettings();

        if(settings.getAsBoolean("index.encryption_enabled", false)) {

            final String indexEncryptionKey = settings.get("index.encryption_key");

            if(indexEncryptionKey == null || indexEncryptionKey.isEmpty()) {
                throw new RuntimeException("index.encryption_key can not be empty");
            }

            try {
                return new DefaultCryptoOperations(indexSettings.getIndex(), client, threadContext, indexEncryptionKey);
            } catch (Exception e) {
                throw new RuntimeException(e);
                //return null;
            }

        } else {
            return null;
        }
    }
}
