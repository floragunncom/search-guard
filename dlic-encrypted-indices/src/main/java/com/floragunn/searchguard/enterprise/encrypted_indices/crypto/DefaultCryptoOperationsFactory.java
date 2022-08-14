package com.floragunn.searchguard.enterprise.encrypted_indices.crypto;

import com.floragunn.searchguard.enterprise.encrypted_indices.EncryptedIndicesSettings;
import org.apache.lucene.util.Constants;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.IndexSettings;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DefaultCryptoOperationsFactory extends CryptoOperationsFactory {

    private final static Map<Integer, String> supportedAlgos = new HashMap<>();

    static {
        supportedAlgos.put(1, "aes-128");
        supportedAlgos.put(2, "aes-256");

        if(Constants.JRE_IS_MINIMUM_JAVA11) {
            supportedAlgos.put(99,"chacha20-poly1305");
        }

        //supportedAlgos.put(100,"x-chacha-poly1305");

    }

    private final Client client;

    private final ThreadContext threadContext;

    private final ClusterService clusterService;

    public DefaultCryptoOperationsFactory(ClusterService clusterService, Client client, ThreadContext threadContext) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.threadContext = Objects.requireNonNull(threadContext, "threadContext must not be null");
        this.clusterService = Objects.requireNonNull(clusterService, "clusterService must not be null");
    }

    @Override
    public CryptoOperations createCryptoOperations(IndexSettings indexSettings) {

        Settings settings = indexSettings.getSettings();

        if(EncryptedIndicesSettings.INDEX_ENCRYPTION_ENABLED.getFrom(settings)) {

            final String indexPublicKey = EncryptedIndicesSettings.INDEX_ENCRYPTION_KEY.getFrom(settings);

            if(indexPublicKey == null || indexPublicKey.isEmpty()) {
                throw new RuntimeException(EncryptedIndicesSettings.INDEX_ENCRYPTION_KEY.name()+" can not be empty");
            }

            final String algo = EncryptedIndicesSettings.INDEX_ENCRYPTION_ALGO.getFrom(settings);

            CryptoOperations cryptoOperations = getCryptoOperationsForAlgo(algo);

            try {
                return new AesGcmCryptoOperations(clusterService, indexSettings.getIndex(), client, threadContext, indexPublicKey, 16);
            } catch (Exception e) {
                throw new RuntimeException(e);
                //return null;
            }

        } else {
            return null;
        }
    }

    private CryptoOperations getCryptoOperationsForAlgo(String algo) {

        if(algo != null && supportedAlgos.containsValue(algo.toLowerCase())) {

        }

        if(algo == null || algo.equals("auto")) {
            //return supportedAlgos.entrySet().stream().sorted().findFirst().get().getValue();
        }

        return null;
    };
}
