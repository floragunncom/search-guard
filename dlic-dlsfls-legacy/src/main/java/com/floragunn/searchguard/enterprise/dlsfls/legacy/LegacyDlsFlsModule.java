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
package com.floragunn.searchguard.enterprise.dlsfls.legacy;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.SearchOperationListener;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.authz.SyncAuthorizationFilter;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.enterprise.dlsfls.legacy.lucene.SearchGuardFlsDlsIndexSearcherWrapper;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import org.elasticsearch.plugins.FieldPredicate;

public class LegacyDlsFlsModule implements SearchGuardModule, ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(LegacyDlsFlsModule.class);

    private DlsFlsValve dlsFlsValve;
    private DlsFlsSearchOperationListener dlsFlsSearchOperationListener;
    private FlsFieldFilter flsFieldFilter;
    private FlsQueryCacheWeightProvider flsQueryCacheWeightProvider;
    private final ComponentState componentState = new ComponentState(1002, null, "dlsfls_legacy", LegacyDlsFlsModule.class)
            .requiresEnterpriseLicense();
    private AtomicReference<DlsFlsProcessedConfig> config = new AtomicReference<>(DlsFlsProcessedConfig.DEFAULT);
    private DlsFlsComplianceConfig complianceConfig;
    private Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>> directoryReaderWrapper;

    @Override
    public Collection<Object> createComponents(BaseDependencies baseDependencies) {
        this.complianceConfig = new DlsFlsComplianceConfig(baseDependencies.getSettings(), baseDependencies.getConfigurationRepository(),
                baseDependencies.getLocalClient());
        baseDependencies.getLicenseRepository().subscribeOnLicenseChange(complianceConfig);

        this.dlsFlsValve = new DlsFlsValve(baseDependencies.getSettings(), baseDependencies.getLocalClient(), baseDependencies.getClusterService(),
                baseDependencies.getIndexNameExpressionResolver(), baseDependencies.getGuiceDependencies(), baseDependencies.getxContentRegistry(),
                baseDependencies.getThreadPool().getThreadContext(), baseDependencies.getConfigurationRepository(), config, complianceConfig);

        this.dlsFlsSearchOperationListener = new DlsFlsSearchOperationListener(baseDependencies.getThreadPool(), this.dlsFlsValve.getDlsQueryParser(),
                config);

        this.flsFieldFilter = new FlsFieldFilter(baseDependencies.getThreadPool(), config);
        this.flsQueryCacheWeightProvider = new FlsQueryCacheWeightProvider(baseDependencies.getThreadPool(), config);
        this.directoryReaderWrapper = (indexService) -> new SearchGuardFlsDlsIndexSearcherWrapper(indexService,
                baseDependencies.getSettings(), baseDependencies.getClusterService(), baseDependencies.getAuditLog(), complianceConfig, config, baseDependencies.getxContentRegistry());

        ConfigurationRepository configurationRepository = baseDependencies.getConfigurationRepository();
        ClusterService clusterService = baseDependencies.getClusterService();

        configurationRepository.subscribeOnChange(new ConfigurationChangeListener() {

            @Override
            public void onChange(ConfigMap configMap) {
                DlsFlsProcessedConfig newConfig = DlsFlsProcessedConfig.createFrom(configMap, componentState,
                        baseDependencies.getIndexNameExpressionResolver(), clusterService); 
                
                DlsFlsProcessedConfig oldConfig = LegacyDlsFlsModule.this.config.get();
                                
                if (oldConfig.isEnabled() != newConfig.isEnabled()) {
                    log.info(newConfig.isEnabled() ? "Legacy DLS/FLS implementation is now ENABLED" : "Legacy DLS/FLS implementation is now DISABLED");
                } 
                LegacyDlsFlsModule.this.config.set(newConfig);

            }
        });

        return ImmutableList.empty();
    }

    @Override
    public ImmutableList<Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>>> getDirectoryReaderWrappersForNormalOperations() {
        return ImmutableList.of(directoryReaderWrapper);
    }

    @Override
    public ImmutableList<SearchOperationListener> getSearchOperationListeners() {
        return ImmutableList.of(dlsFlsSearchOperationListener);
    }

    @Override
    public ImmutableList<SyncAuthorizationFilter> getSyncAuthorizationFilters() {
        return ImmutableList.of(dlsFlsValve);
    }

    @Override
    public ImmutableList<Function<String, FieldPredicate>> getFieldFilters() {
        return ImmutableList.of(flsFieldFilter);
    }

    @Override
    public ImmutableList<QueryCacheWeightProvider> getQueryCacheWeightProviders() {
        return ImmutableList.of(flsQueryCacheWeightProvider);
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
    
    @Override
    public ImmutableSet<String> getCapabilities() {
        return ImmutableSet.of("dls", "fls", "field_masking");
    }

}
