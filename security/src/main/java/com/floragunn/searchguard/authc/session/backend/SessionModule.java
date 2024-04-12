/*
 * Copyright 2021-2022 floragunn GmbH
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.floragunn.searchguard.authc.session.backend;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
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

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.rest.HttpAuthenticationFrontend;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.configuration.variables.ConfigVarService;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.StaticSettings;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;

public class SessionModule implements SearchGuardModule, ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(SessionModule.class);

    private SessionService sessionService;
    private SessionApi.Rest sessionRestAction = new SessionApi.Rest();
    private ConfigVarService configVarService;
    private SessionTokenAuthenticationDomain sessionTokenAuthenticationDomain;
    private final ComponentState componentState = new ComponentState(2, "authc", "session_service", SessionModule.class);

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            ScriptService scriptService, Supplier<DiscoveryNodes> nodesInCluster, Predicate<NodeFeature> clusterSupportsFeature) {
        return Arrays.asList(sessionRestAction, SessionServiceConfigApi.REST_API);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return ImmutableList.of(//
                new ActionHandler<>(SessionApi.GetExtendedInfoAction.INSTANCE, SessionApi.GetExtendedInfoAction.Handler.class),
                new ActionHandler<>(SessionApi.CreateAction.INSTANCE, SessionApi.CreateAction.Handler.class),
                new ActionHandler<>(SessionApi.DeleteAction.INSTANCE, SessionApi.DeleteAction.Handler.class),
                new ActionHandler<>(PushSessionTokenUpdateAction.INSTANCE, PushSessionTokenUpdateAction.TransportAction.class)//
        ).with(SessionServiceConfigApi.ACTION_HANDLERS);
    }

    @Override
    public Collection<Object> createComponents(BaseDependencies baseDependencies) {
        this.configVarService = baseDependencies.getConfigVarService();
        this.configVarService.requestRandomKey(SessionServiceConfig.SIGNING_KEY_SECRET, 512, "authc");

        PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(baseDependencies.getLocalClient());

        sessionService = new SessionService(baseDependencies.getConfigurationRepository(), privilegedConfigClient, baseDependencies.getStaticSettings(),
                baseDependencies.getPrivilegesEvaluator(), baseDependencies.getAuditLog(), baseDependencies.getThreadPool(),
                baseDependencies.getClusterService(), baseDependencies.getProtectedConfigIndexService(), new SessionServiceConfig(),
                baseDependencies.getBlockedIpRegistry(), baseDependencies.getBlockedUserRegistry(), componentState);

        sessionTokenAuthenticationDomain = new SessionTokenAuthenticationDomain(sessionService);

        sessionRestAction.setSessionService(sessionService);

        baseDependencies.getConfigurationRepository().subscribeOnChange(new ConfigurationChangeListener() {

            @Override
            public void onChange(ConfigMap configMap) {
                SgDynamicConfiguration<SessionServiceConfig> config = configMap.get(SessionServiceConfig.TYPE);

                if (config != null && config.getCEntry("default") != null) {
                    sessionService.setConfig(config.getCEntry("default"));
                    componentState.setConfigVersion(config.getDocVersion());
                    componentState.setState(State.INITIALIZED, "using_config");
                } else {
                    try {
                        SessionServiceConfig defaultConfig = SessionServiceConfig.getDefault(configVarService);

                        if (defaultConfig != null) {
                            sessionService.setConfig(defaultConfig);
                            componentState.setState(State.INITIALIZED, "using_default_config");
                        } else {
                            log.trace("Could not yet initialize session service, as the auto-generated key is still not available");
                        }

                    } catch (Exception e) {
                        log.error("Error while getting default config for session service", e);
                        componentState.setFailed(e);
                    }
                }
            }
        });

        return Arrays.asList(sessionService);
    }

    @Override
    public StaticSettings.AttributeSet getSettings() {
        return StaticSettings.AttributeSet.of(SessionService.INDEX_NAME, SessionService.CLEANUP_INTERVAL);
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    @Override
    public List<AuthenticationDomain<HttpAuthenticationFrontend>> getImplicitHttpAuthenticationDomains() {
        return Collections.singletonList(sessionTokenAuthenticationDomain);
    }

    @Override
    public ImmutableSet<String> getPublicCapabilities() {
        return ImmutableSet.of("login_sessions");
    }

}
