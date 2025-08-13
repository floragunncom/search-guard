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
package com.floragunn.searchguard.enterprise.dlsfls;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.authz.SyncAuthorizationFilter;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.enterprise.dlsfls.lucene.DlsFlsDirectoryReaderWrapper;
import com.floragunn.searchguard.license.SearchGuardLicense;
import com.floragunn.searchguard.license.SearchGuardLicense.Feature;
import com.floragunn.searchsupport.StaticSettings;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.metrics.TimeAggregation;
import com.floragunn.searchsupport.meta.Meta;

public class DlsFlsModule implements SearchGuardModule, ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(DlsFlsModule.class);

    // XXX Hack to trigger early initialization of DlsFlsConfig
    @SuppressWarnings("unused")
    private static final CType<DlsFlsConfig> TYPE = DlsFlsConfig.TYPE;
    
    static final StaticSettings.Attribute<Boolean> PROVIDE_THREAD_CONTEXT_AUTHZ_HASH = StaticSettings.Attribute
            .define("searchguard.dls_fls.provide_thread_context_authz_hash").withDefault(false).asBoolean();

    private final ComponentState componentState = new ComponentState(1000, null, "dlsfls", DlsFlsModule.class).requiresEnterpriseLicense();
    /**
     * DlsFlsDirectoryReaderWrapper is instantiated per index. We however do not want a ComponentState instance per index. Thus, we create it on this level.
     */
    private final ComponentState directoryReaderWrapperComponentState = new ComponentState(10, null, "directory_reader_wrapper",
            DlsFlsDirectoryReaderWrapper.class).initialized();

    private final TimeAggregation directoryReaderWrapperApplyAggregation = new TimeAggregation.Nanoseconds();

    private DlsFlsBaseContext dlsFlsBaseContext;
    private DlsFlsValve dlsFlsValve;
    private DlsFlsSearchOperationListener dlsFlsSearchOperationListener;
    private FlsFieldFilter flsFieldFilter;
    private AtomicReference<DlsFlsProcessedConfig> config = new AtomicReference<>(DlsFlsProcessedConfig.DEFAULT);
    private AtomicReference<DlsFlsLicenseInfo> licenseInfo = new AtomicReference<>(new DlsFlsLicenseInfo(false));
    private FlsQueryCacheWeightProvider flsQueryCacheWeightProvider;
    private ClusterService clusterService;
    private Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>> directoryReaderWrapperFactory;
    private ThreadPool threadPool;

    public DlsFlsModule() {
        this.componentState.addPart(directoryReaderWrapperComponentState);
        this.directoryReaderWrapperComponentState.addMetrics("wrap_reader", directoryReaderWrapperApplyAggregation);
    }

    @Override
    public Collection<Object> createComponents(BaseDependencies baseDependencies) {

        this.clusterService = baseDependencies.getClusterService();

        Supplier<Meta> metaSupplier = () -> Meta.from(baseDependencies.getClusterService());
        this.dlsFlsBaseContext = new DlsFlsBaseContext(baseDependencies.getAuthInfoService(), baseDependencies.getAuthorizationService(),
                baseDependencies.getThreadPool().getThreadContext(), metaSupplier);

        this.dlsFlsValve = new DlsFlsValve(baseDependencies.getLocalClient(), baseDependencies.getClusterService(),
                baseDependencies.getIndexNameExpressionResolver(), baseDependencies.getGuiceDependencies(),
                baseDependencies.getThreadPool().getThreadContext(), config, baseDependencies.getStaticSettings());

        this.dlsFlsSearchOperationListener = new DlsFlsSearchOperationListener(this.dlsFlsBaseContext, config);

        this.flsFieldFilter = new FlsFieldFilter(this.dlsFlsBaseContext, config);

        this.flsQueryCacheWeightProvider = new FlsQueryCacheWeightProvider(this.dlsFlsBaseContext, config);

        this.directoryReaderWrapperFactory = (indexService) -> new DlsFlsDirectoryReaderWrapper(indexService, baseDependencies.getAuditLog(),
                this.dlsFlsBaseContext, config, this.licenseInfo, directoryReaderWrapperComponentState, directoryReaderWrapperApplyAggregation);

        this.componentState.addParts(this.dlsFlsValve.getComponentState(), this.dlsFlsSearchOperationListener.getComponentState(),
                this.flsFieldFilter.getComponentState(), this.flsQueryCacheWeightProvider.getComponentState());

        this.threadPool = baseDependencies.getThreadPool();

        baseDependencies.getConfigurationRepository().subscribeOnChange((ConfigMap configMap) -> {
            DlsFlsProcessedConfig config = DlsFlsProcessedConfig.createFrom(configMap, componentState, Meta.from(clusterService));
            DlsFlsProcessedConfig oldConfig = this.config.getAndSet(config);
            if (oldConfig != null) {
                oldConfig.shutdown();                
            }
        });

        baseDependencies.getLicenseRepository().subscribeOnLicenseChange((SearchGuardLicense license) -> {
            licenseInfo.set(new DlsFlsLicenseInfo(license.hasFeature(Feature.COMPLIANCE)));
        });

        clusterService.addListener(new ClusterStateListener() {

            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                DlsFlsProcessedConfig config = DlsFlsModule.this.config.get();
                config.updateIndicesAsync(clusterService, threadPool);
            }
        });

        return ImmutableList.empty();
    }

    @Override
    public ImmutableList<Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>>> getDirectoryReaderWrappersForNormalOperations() {
        return ImmutableList.of(directoryReaderWrapperFactory);
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

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            ScriptService scriptService, Supplier<DiscoveryNodes> nodesInCluster, Predicate<NodeFeature> clusterSupportsFeature) {
        return ImmutableList.of(DlsFlsConfigApi.REST_API);
    }

    @Override
    public List<ActionHandler> getActions() {
        return DlsFlsConfigApi.ACTION_HANDLERS;
    }
    
    @Override
    public StaticSettings.AttributeSet getSettings() {
        return StaticSettings.AttributeSet.of(PROVIDE_THREAD_CONTEXT_AUTHZ_HASH);
    }
}
