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
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.authc.legacy.LegacySgConfig;
import com.floragunn.searchguard.authz.ActionAuthorization;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.privileges.PrivilegesInterceptor;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.google.common.collect.ImmutableList;

public class FeMultiTenancyModule implements SearchGuardModule, ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(FeMultiTenancyModule.class);

    private final ComponentState componentState = new ComponentState(1000, null, "fe_multi_tenancy", FeMultiTenancyModule.class).requiresEnterpriseLicense();
    private volatile boolean enabled;
    private volatile PrivilegesInterceptorImpl interceptorImpl;
    private volatile FeMultiTenancyConfig config;
    private volatile ImmutableSet<String> tenantNames = ImmutableSet.empty();
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private AdminDNs adminDns;

    // XXX Hack to trigger early initialization of FeMultiTenancyConfig
    @SuppressWarnings("unused")
    private static final CType<FeMultiTenancyConfig> TYPE = FeMultiTenancyConfig.TYPE;

    @Override
    public Collection<Object> createComponents(BaseDependencies baseDependencies) {

        this.threadPool = baseDependencies.getThreadPool();
        this.clusterService = baseDependencies.getClusterService();
        this.adminDns = new AdminDNs(baseDependencies.getSettings());

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

            if (feMultiTenancyConfig != null) {
                if (feMultiTenancyConfig.isEnabled()) {
                    enabled = true;
                    interceptorImpl = new PrivilegesInterceptorImpl(feMultiTenancyConfig, tenantNames, baseDependencies.getActions(),
                            clusterService, baseDependencies.getGuiceDependencies().getIndicesService());
                } else {
                    enabled = false;
                    componentState.setState(State.SUSPENDED, "disabled_by_config");
                }
            } else {
                enabled = false;
            }

            if (log.isDebugEnabled()) {
                log.debug("Using MT config: " + feMultiTenancyConfig + "\nenabled: " + enabled + "\ninterceptor: " + interceptorImpl);
            }
        });

        return Arrays.asList(privilegesInterceptor);
    }

    private final PrivilegesInterceptor privilegesInterceptor = new PrivilegesInterceptor() {

        @Override
        public InterceptionResult replaceKibanaIndex(PrivilegesEvaluationContext context, ActionRequest request, Action action,
                                                     ActionAuthorization actionAuthorization, ActionListener<?> listener) throws PrivilegesEvaluationException {
            if (enabled && interceptorImpl != null) {
                return interceptorImpl.replaceKibanaIndex(context, request, action, actionAuthorization, listener);
            } else {
                return InterceptionResult.NORMAL;
            }
        }

        @Override
        public boolean isEnabled() {
            return enabled;
        }

        @Override
        public String getKibanaIndex() {
            if (enabled && interceptorImpl != null) {
                return interceptorImpl.getKibanaIndex();
            } else {
                return ".kibana";
            }
        }

        @Override
        public String getKibanaServerUser() {
            if (enabled && interceptorImpl != null) {
                return interceptorImpl.getKibanaServerUser();
            } else {
                return "kibanaserver";
            }
        }

        @Override
        public Map<String, Boolean> mapTenants(User user, ImmutableSet<String> roles, ActionAuthorization actionAuthorization) {
            if (enabled && interceptorImpl != null) {
                return interceptorImpl.mapTenants(user, roles, actionAuthorization);
            } else {
                return ImmutableMap.empty();
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
            ScriptService scriptService, Supplier<DiscoveryNodes> nodesInCluster) {
        return ImmutableList.of(new TenantInfoAction(settings, restController, this, threadPool, clusterService, adminDns),
                FeMultiTenancyConfigApi.REST_API);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return FeMultiTenancyConfigApi.ACTION_HANDLERS;
    }

    @Override
    public ImmutableSet<String> getCapabilities() {
        return ImmutableSet.of("fe_multi_tenancy");
    }
}
