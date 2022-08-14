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
import com.floragunn.searchguard.enterprise.encrypted_indices.index.DecryptingDirectoryReaderWrapper;
import com.floragunn.searchguard.enterprise.encrypted_indices.index.EncryptingIndexingOperationListener;
import com.floragunn.searchguard.enterprise.lucene.encryption.CeffDirectory;
import com.floragunn.searchguard.enterprise.lucene.encryption.CeffMode;
import com.floragunn.searchsupport.StaticSettings;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.util.Constants;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.index.shard.IndexingOperationListener;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/*
TODO

- check TODO s
- Fix where we have a null key on retrieval to match the desired behaviour
- Check for indexing that we always need the key (owner only or do we allow others to index)
- more tests
- implement api for upload, create encrypted index, infos about keys ...
- do we have dynamic configs for EncryptedIndicesConfig implements LicenseChangeListener ??
- poly1305 chacha
- cryptangular dependency? KeyPairUtil?
- check byte[], byteref, bytesreference conversions for efficiency
- also map to json, json to map
- introduce mode byte to check which enc algo used
- integrate ceff directory as optional possibility, passwd via env var
- index keys public synchronized??
- reuse Cipher cipher = Cipher.getInstance
- mode byte
- check if we can make ceff understand threadcontext an introduce dynamic key
- KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);
*/

public class EncryptedIndicesModule implements SearchGuardModule {

    private CryptoOperationsFactory cryptoOperationsFactory;
    private GuiceDependencies guiceDependencies;

    private Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>> directoryReaderWrapper;


    @Override
    public Collection<Object> createComponents(BaseDependencies baseDependencies) {
        //this.encryptedIndicesConfig = new EncryptedIndicesConfig(baseDependencies.getEnvironment(), baseDependencies.getConfigurationRepository());

        guiceDependencies = baseDependencies.getGuiceDependencies();

        //baseDependencies.getLicenseRepository().subscribeOnLicenseChange((searchGuardLicense) -> {
        //    EncryptedIndicesModule.this.encryptedIndicesConfig.onChange(searchGuardLicense);
        //});

        cryptoOperationsFactory = new DefaultCryptoOperationsFactory(baseDependencies.getClusterService(), baseDependencies.getLocalClient(), baseDependencies.getThreadPool().getThreadContext());

        this.directoryReaderWrapper = (indexService) -> new DecryptingDirectoryReaderWrapper(indexService, baseDependencies.getAuditLog(), cryptoOperationsFactory);

        return ImmutableList.empty();
    }

    @Override
    public Map<String, IndexStorePlugin.DirectoryFactory> getDirectoryFactories() {
        return Collections.singletonMap("ceff", new IndexStorePlugin.DirectoryFactory() {

            final FsDirectoryFactory fsDirectoryFactory = new FsDirectoryFactory();

            @Override
            public Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
                final LockFactory lockFactory = indexSettings.getValue(FsDirectoryFactory.INDEX_LOCK_FACTOR_SETTING);

                final String ceffKeyEnv = System.getenv("CEFF_KEY");
                if(ceffKeyEnv == null || ceffKeyEnv.isEmpty()) {
                    throw new IOException("No CEFF_KEY environment variable set");
                }

                try {
                    final byte[] key = getPasswordBasedKey( 32, ceffKeyEnv.toCharArray());

                    final IndexMetadata newIndexMetadata = new IndexMetadata
                            .Builder(indexSettings.getIndexMetadata())
                            .settings(Settings.builder()
                                    .put(indexSettings.getSettings())
                                    .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.FS.getSettingsKey()))
                            .build();

                    return new CeffDirectory(
                            (FSDirectory) fsDirectoryFactory.newDirectory(new IndexSettings(newIndexMetadata, indexSettings.getNodeSettings(), indexSettings.getScopedSettings()), shardPath),
                            lockFactory,
                            key,
                            CeffDirectory.DEFAULT_CHUNK_LENGTH,
                            Constants.JRE_IS_MINIMUM_JAVA11 ? CeffMode.CHACHA20_POLY1305_MODE : CeffMode.AES_GCM_MODE);
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
        });
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
        return StaticSettings.AttributeSet.of(EncryptedIndicesSettings.attributes);
    }

    @Override
    public Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        return Collections.singletonMap("blind_hash", new AnalysisModule.AnalysisProvider<TokenFilterFactory>() {

            @Override
            public TokenFilterFactory get(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException {
                final CryptoOperations cryptoOperations = cryptoOperationsFactory.createCryptoOperations(indexSettings);

                if(cryptoOperations == null) {
                    //no token filter of type blind_hash required for
                    //unencrypted indices
                    return null;
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

    private static byte[] getPasswordBasedKey(int keySize, char[] password) throws NoSuchAlgorithmException, InvalidKeySpecException {
        final byte[] salt = new byte[100];
        final SecureRandom random = new SecureRandom();
        random.nextBytes(salt);
        final PBEKeySpec pbeKeySpec = new PBEKeySpec(password, salt, 1000, keySize*8);
        final SecretKey pbeKey = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(pbeKeySpec);
        return pbeKey.getEncoded();
    }
}
