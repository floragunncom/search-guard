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

package com.floragunn.searchguard.enterprise.femt;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.SyncAuthorizationFilter;
import com.floragunn.searchguard.authz.TenantAccessMapper;
import com.floragunn.searchguard.authz.TenantManager;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.configuration.validation.ConfigModificationValidator;
import com.floragunn.searchguard.enterprise.femt.datamigration880.rest.DataMigrationApi;
import com.floragunn.searchguard.enterprise.femt.request.handler.RequestHandlerFactory;
import com.floragunn.searchguard.enterprise.femt.tenants.AvailableTenantService;
import com.floragunn.searchguard.enterprise.femt.tenants.MultitenancyActivationService;
import com.floragunn.searchguard.enterprise.femt.tenants.TenantRepository;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.StaticSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.authc.legacy.LegacySgConfig;
import com.floragunn.searchguard.authz.config.ActionGroup;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;

public class FeMultiTenancyModule implements SearchGuardModule, ComponentStateProvider {

    /**
     * A config option which determines whether the 'single index multi tenancy' implementation
     * is enabled or disabled. It was originally introduced in order to prepare version of the plugin for Elasticsearch 8.8,
     * with multi tenancy support disabled by default.
     * It's undocumented in the user docs.
     */
    private static final StaticSettings.Attribute<Boolean> UNSUPPORTED_SINGLE_INDEX_MT_ENABLED = StaticSettings.Attribute
            .define("searchguard.unsupported.single_index_mt_enabled").withDefault(true).asBoolean();

    private static final Logger log = LogManager.getLogger(FeMultiTenancyModule.class);

    private final ComponentState componentState = new ComponentState(1000, null, "fe_multi_tenancy", FeMultiTenancyModule.class)
            .requiresEnterpriseLicense();
    private volatile boolean enabled;
    private volatile MultiTenancyAuthorizationFilter multiTenancyAuthorizationFilter;
    private volatile FeMultiTenancyConfig config;
    private volatile RoleBasedTenantAuthorization tenantAuthorization;
    private volatile TenantManager tenantManager;
    private volatile FeMultiTenancyTenantAccessMapper feMultiTenancyTenantAccessMapper;

    private volatile ImmutableSet<String> tenantNames = ImmutableSet.empty();
    private ThreadPool threadPool;
    private AdminDNs adminDns;

    private FeMultiTenancyEnabledFlagValidator feMultiTenancyEnabledFlagValidator;

    // XXX Hack to trigger early initialization of FeMultiTenancyConfig
    @SuppressWarnings("unused")
    private static final CType<FeMultiTenancyConfig> TYPE = FeMultiTenancyConfig.TYPE;

    @Override
    public Collection<Object> createComponents(BaseDependencies baseDependencies) {

        FeMultiTenancyConfigurationProvider feMultiTenancyConfigurationProvider = new FeMultiTenancyConfigurationProvider(this);
        this.threadPool = baseDependencies.getThreadPool();
        this.adminDns = new AdminDNs(baseDependencies.getSettings());
        this.feMultiTenancyEnabledFlagValidator = new FeMultiTenancyEnabledFlagValidator(
                feMultiTenancyConfigurationProvider, baseDependencies.getClusterService(),
                baseDependencies.getConfigurationRepository()
        );
        var tenantRepository = new TenantRepository(PrivilegedConfigClient.adapt(baseDependencies.getLocalClient()));
        var activationService = new MultitenancyActivationService(tenantRepository, baseDependencies.getConfigurationRepository(),
            feMultiTenancyConfigurationProvider);

        baseDependencies.getConfigurationRepository().subscribeOnChange((ConfigMap configMap) -> {
            SgDynamicConfiguration<FeMultiTenancyConfig> config = configMap.get(FeMultiTenancyConfig.TYPE);
            SgDynamicConfiguration<LegacySgConfig> legacyConfig = configMap.get(CType.CONFIG);
            FeMultiTenancyConfig feMultiTenancyConfig = null;

            if (config != null && config.getCEntry("default") != null) {
                feMultiTenancyConfig = config.getCEntry("default");
                componentState.setState(State.INITIALIZED, "using_authc_config");
                componentState.setConfigVersion(config.getDocVersion());
            } else if (legacyConfig != null && legacyConfig.getCEntry("sg_config") != null) {
                try {
                    LegacySgConfig sgConfig = legacyConfig.getCEntry("sg_config");
                    feMultiTenancyConfig = FeMultiTenancyConfig.parseLegacySgConfig(sgConfig.getSource(), null);
                    componentState.setState(State.INITIALIZED, "using_legacy_config");
                    componentState.setConfigVersion(legacyConfig.getDocVersion());
                } catch (ConfigValidationException e) {
                    log.warn("Error while parsing legacy MT configuration", e);
                    componentState.setFailed(e);
                    componentState.setConfigVersion(legacyConfig.getDocVersion());
                }
            } else {
                feMultiTenancyConfig = FeMultiTenancyConfig.DEFAULT;
                componentState.setState(State.INITIALIZED, "using_default_config");
                componentState.setConfigVersion(config.getDocVersion());
            }

            this.config = feMultiTenancyConfig;

            SgDynamicConfiguration<Tenant> tenantConfig = configMap.get(CType.TENANTS);

            ImmutableSet<String> tenantNames = ImmutableSet.of(tenantConfig.getCEntries().keySet());

            this.tenantNames = tenantNames;

            SgDynamicConfiguration<Role> roles = configMap.get(CType.ROLES);
            SgDynamicConfiguration<Tenant> tenants = configMap.get(CType.TENANTS);

            ActionGroup.FlattenedIndex actionGroups = configMap.get(CType.ACTIONGROUPS) != null
                    ? new ActionGroup.FlattenedIndex(configMap.get(CType.ACTIONGROUPS))
                    : ActionGroup.FlattenedIndex.EMPTY;

            tenantManager = new TenantManager(tenants.getCEntries().keySet(), feMultiTenancyConfigurationProvider);
            tenantAuthorization = new RoleBasedTenantAuthorization(roles, actionGroups, baseDependencies.getActions(), tenantManager,
                    feMultiTenancyConfig.getMetricsLevel());
            feMultiTenancyTenantAccessMapper = new FeMultiTenancyTenantAccessMapper(tenantManager, tenantAuthorization, baseDependencies.getActions());
            RequestHandlerFactory requestHandlerFactory = new RequestHandlerFactory(baseDependencies.getLocalClient(), baseDependencies.getThreadPool().getThreadContext(), baseDependencies.getClusterService(), baseDependencies.getGuiceDependencies().getIndicesService());

            if (feMultiTenancyConfig.isEnabled()) {
                enabled = true;
                multiTenancyAuthorizationFilter = new MultiTenancyAuthorizationFilter(feMultiTenancyConfig, tenantAuthorization, tenantManager, baseDependencies.getActions(),
                        baseDependencies.getThreadPool().getThreadContext(), baseDependencies.getLocalClient(), requestHandlerFactory);
            } else {
                enabled = false;
                componentState.setState(State.SUSPENDED, "disabled_by_config");
            }

            componentState.setConfigVersion(configMap.getVersionsAsString());
            componentState.replacePart(tenantAuthorization.getComponentState());
            componentState.updateStateFromParts();
            if (log.isDebugEnabled()) {
                log.debug("Using MT config: " + feMultiTenancyConfig + "\nenabled: " + enabled + "\nauthorization filter: " + multiTenancyAuthorizationFilter);
            }
        });
        var availableTenantService = new AvailableTenantService(feMultiTenancyConfigurationProvider,
            baseDependencies.getAuthorizationService(), threadPool, tenantRepository);
        return Arrays.asList(feMultiTenancyConfigurationProvider, tenantAccessMapper, availableTenantService, activationService);
    }

    private final TenantAccessMapper tenantAccessMapper = new TenantAccessMapper() {
        @Override
        public Map<String, Boolean> mapTenantsAccess(User user, Set<String> roles) {
            if (!enabled) {
                return ImmutableMap.empty();
            }
            return feMultiTenancyTenantAccessMapper.mapTenantsAccess(user, roles);
        }
    };

    TenantAccessMapper getTenantAccessMapper() {
        return tenantAccessMapper;
    }

    private final SyncAuthorizationFilter syncAuthorizationFilter = new SyncAuthorizationFilter() {
        
        @Override
        public Result apply(PrivilegesEvaluationContext context, ActionListener<?> listener) {
            MultiTenancyAuthorizationFilter delegate = multiTenancyAuthorizationFilter;
            
            if (enabled && delegate != null) {
                return delegate.apply(context, listener);
            } else {
                return SyncAuthorizationFilter.Result.OK;
            }
        }
    };
    
    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public FeMultiTenancyConfig getConfig() {
        return config;
    }

    ImmutableSet<String> getTenantNames() {
        return tenantNames;
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            ScriptService scriptService, Supplier<DiscoveryNodes> nodesInCluster, Predicate<NodeFeature> clusterSupportsFeature) {
        return ImmutableList.of(FeMultiTenancyConfigApi.REST_API, DataMigrationApi.REST_API);
    }

    @Override
    public ImmutableList<ActionHandler<?, ?>> getActions() {
        return FeMultiTenancyConfigApi.ACTION_HANDLERS
                .with(DataMigrationApi.ACTION_HANDLERS);
    }

    @Override
    public ImmutableSet<String> getCapabilities() {
        return ImmutableSet.of("fe_multi_tenancy");
    }
    
    @Override
    public ImmutableList<SyncAuthorizationFilter> getPrePrivilegeEvaluationSyncAuthorizationFilters() {
        return ImmutableList.of(this.syncAuthorizationFilter);
    }

    @Override
    public StaticSettings.AttributeSet getSettings() {
        return StaticSettings.AttributeSet.of(UNSUPPORTED_SINGLE_INDEX_MT_ENABLED);
    }

    @Override
    public ImmutableList<ConfigModificationValidator<?>> getConfigModificationValidators() {
        return ImmutableList.of(feMultiTenancyEnabledFlagValidator);
    }
}
