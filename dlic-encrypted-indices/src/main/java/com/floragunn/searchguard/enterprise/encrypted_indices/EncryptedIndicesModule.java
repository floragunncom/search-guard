/*
 * Copyright 2022 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */

package com.floragunn.searchguard.enterprise.encrypted_indices;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.GuiceDependencies;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.enterprise.encrypted_indices.analysis.EncryptedTokenFilter;
import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.CryptoOperations;
import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.CryptoOperationsFactory;
import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.DefaultCryptoOperationsFactory;
import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.DummyCryptoOperations;
import com.floragunn.searchguard.enterprise.encrypted_indices.index.DecryptingDirectoryReaderWrapper;
import com.floragunn.searchguard.enterprise.encrypted_indices.index.EncryptingIndexingOperationListener;
import com.floragunn.searchsupport.StaticSettings;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.DirectoryReader;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.index.shard.IndexingOperationListener;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

public class EncryptedIndicesModule implements SearchGuardModule {

    private final CryptoOperationsFactory cryptoOperationsFactory = new DefaultCryptoOperationsFactory();

    public static final StaticSettings.Attribute<Boolean> INDEX_ENCRYPTION_ENABLED =
            StaticSettings.Attribute
                    .define("index.encryption_enabled")
                    .indexScoped()
            .withDefault(false)
                    .asBoolean();

    public static final StaticSettings.Attribute<String> INDEX_ENCRYPTION_KEY =
            StaticSettings.Attribute
                    .define("index.encryption_key")
                    .indexScoped()
                    .withDefault((String) null)
                    .asString();

    private EncryptedIndicesConfig encryptedIndicesConfig;
    private GuiceDependencies guiceDependencies;

    private Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>> directoryReaderWrapper;


    @Override
    public Collection<Object> createComponents(BaseDependencies baseDependencies) {
        this.encryptedIndicesConfig = new EncryptedIndicesConfig(baseDependencies.getEnvironment(), baseDependencies.getConfigurationRepository());

        guiceDependencies = baseDependencies.getGuiceDependencies();

        baseDependencies.getLicenseRepository().subscribeOnLicenseChange((searchGuardLicense) -> {
            EncryptedIndicesModule.this.encryptedIndicesConfig.onChange(searchGuardLicense);
        });

        this.directoryReaderWrapper = (indexService) -> new DecryptingDirectoryReaderWrapper(indexService, baseDependencies.getAuditLog(), cryptoOperationsFactory);

        return ImmutableList.empty();
    }

    @Override
    public Map<String, IndexStorePlugin.DirectoryFactory> getDirectoryFactories() {
        return SearchGuardModule.super.getDirectoryFactories();
    }

    @Override
    public ImmutableList<IndexingOperationListener> getIndexOperationListeners() {
        return ImmutableList.of(new EncryptingIndexingOperationListener(guiceDependencies, cryptoOperationsFactory));
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver, ScriptService scriptService, Supplier<DiscoveryNodes> nodesInCluster) {
        return SearchGuardModule.super.getRestHandlers(settings, restController, clusterSettings, indexScopedSettings, settingsFilter, indexNameExpressionResolver, scriptService, nodesInCluster);
    }

    @Override
    public StaticSettings.AttributeSet getSettings() {
        return StaticSettings.AttributeSet.of(
                INDEX_ENCRYPTION_ENABLED, INDEX_ENCRYPTION_KEY);
    }

    @Override
    public Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        return Collections.singletonMap("blind_hash", new AnalysisModule.AnalysisProvider<TokenFilterFactory>() {

            @Override
            public TokenFilterFactory get(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException {
                final CryptoOperations cryptoOperations = cryptoOperationsFactory.createCryptoOperations(indexSettings);

                if(cryptoOperations == null) {
                    return null;
                    //throw new IOException("blind_hash can only be used on encrypted indices");
                }

                return new TokenFilterFactory() {
                    @Override
                    public String name() {
                        return name;
                    }

                    @Override
                    public TokenStream create(TokenStream tokenStream) {
                        return new EncryptedTokenFilter(tokenStream, cryptoOperations);
                    }
                };
            }
        });
    }

    @Override
    public ImmutableList<Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>>> getDirectoryReaderWrappersForNormalOperations() {
        return ImmutableList.of(this.directoryReaderWrapper);
    }

    @Override
    public ImmutableSet<String> getCapabilities() {
        return ImmutableSet.of("encrypted_indices");
    }
}
