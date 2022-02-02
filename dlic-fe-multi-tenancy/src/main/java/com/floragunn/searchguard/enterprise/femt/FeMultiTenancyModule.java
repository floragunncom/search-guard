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
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.authc.legacy.LegacySgConfig;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.modules.state.ComponentState.State;
import com.floragunn.searchguard.modules.state.ComponentStateProvider;
import com.floragunn.searchguard.privileges.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.privileges.PrivilegesInterceptor;
import com.floragunn.searchguard.sgconf.SgRoles;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.TenantV7;
import com.floragunn.searchguard.support.ReflectionHelper;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.util.ImmutableSet;
import com.google.common.collect.ImmutableList;

public class FeMultiTenancyModule implements SearchGuardModule, ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(FeMultiTenancyModule.class);

    private final ComponentState componentState = new ComponentState(1000, null, "fe_multi_tenancy", FeMultiTenancyModule.class);
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
                componentState.setState(State.INITIALIZED, "using_authcz_config");
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

            SgDynamicConfiguration<TenantV7> tenantConfig = configMap.get(CType.TENANTS);

            tenantNames = ImmutableSet.of(tenantConfig.getCEntries().keySet());

            if (feMultiTenancyConfig != null) {
                if (feMultiTenancyConfig.isEnabled()) {
                    enabled = true;
                    interceptorImpl = new PrivilegesInterceptorImpl(feMultiTenancyConfig, (t) -> User.USER_TENANT.equals(t) || tenantConfig.getCEntry(t) != null);
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
        
        ReflectionHelper.addLoadedModule(FeMultiTenancyModule.class);

        return Arrays.asList(privilegesInterceptor);
    }

    private final PrivilegesInterceptor privilegesInterceptor = new PrivilegesInterceptor() {

        @Override
        public InterceptionResult replaceKibanaIndex(ActionRequest request, String action, User user, ResolvedIndices requestedResolved,
                SgRoles sgRoles) {
            if (enabled && interceptorImpl != null) {
                return interceptorImpl.replaceKibanaIndex(request, action, user, requestedResolved, sgRoles);
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
        return ImmutableList.of(new TenantInfoAction(settings, restController, this, threadPool, clusterService, adminDns), FeMultiTenancyConfigApi.REST_API);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return FeMultiTenancyConfigApi.ACTION_HANDLERS;
    }
}
