/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.session;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;

import com.fasterxml.jackson.core.JsonPointer;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.auth.AuthenticationDomain;
import com.floragunn.searchguard.auth.HTTPAuthenticator;
import com.floragunn.searchguard.configuration.variables.ConfigVarService;
import com.floragunn.searchguard.modules.SearchGuardModule;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.modules.state.ComponentStateProvider;
import com.floragunn.searchguard.session.api.DeleteSessionAction;
import com.floragunn.searchguard.session.api.SessionRestAction;
import com.floragunn.searchguard.sgconf.impl.v7.ConfigV7;
import com.floragunn.searchguard.support.PrivilegedConfigClient;

public class SessionModule implements SearchGuardModule<SessionServiceConfig>, ComponentStateProvider {

    private SessionService sessionService;
    private SessionRestAction sessionRestAction = new SessionRestAction();
    private ConfigVarService secretsStorageService;
    private SessionAuthenticationBackend sessionAuthenticationBackend;
    private SessionAuthTokenHttpJwtAuthenticator sessionHttpAuthenticator;
    private final ComponentState componentState = new ComponentState(1000, null, "session_service", SessionModule.class);

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            ScriptService scriptService, Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(sessionRestAction);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(DeleteSessionAction.INSTANCE, DeleteSessionAction.TransportAction.class),
                new ActionHandler<>(PushSessionTokenUpdateAction.INSTANCE, PushSessionTokenUpdateAction.TransportAction.class));
    }

    @Override
    public Collection<Object> createComponents(BaseDependencies baseDependencies) {
        this.secretsStorageService = baseDependencies.getSecretsService();
        this.secretsStorageService.requestRandomKey(SessionServiceConfig.SIGNING_KEY_SECRET, 512, "authcz");

        baseDependencies.getDynamicConfigFactory().addAuthenticationDomainInjector(this::getAuthenticationDomainsForInjection);

        PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(baseDependencies.getLocalClient());

        sessionService = new SessionService(privilegedConfigClient, baseDependencies.getSettings(), baseDependencies.getThreadPool(),
                baseDependencies.getClusterService(), baseDependencies.getProtectedConfigIndexService(), new SessionServiceConfig(),
                baseDependencies.getBackendRegistry(), componentState);

        sessionAuthenticationBackend = new SessionAuthenticationBackend(sessionService);

        sessionHttpAuthenticator = new SessionAuthTokenHttpJwtAuthenticator(sessionService);

        sessionRestAction.setSessionService(sessionService);

        return Arrays.asList(sessionService, sessionAuthenticationBackend, sessionHttpAuthenticator);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(SessionService.INDEX_NAME, SessionService.CLEANUP_INTERVAL);
    }

    @Override
    public SgConfigMetadata<SessionServiceConfig> getSgConfigMetadata() {
        return new SgConfigMetadata<SessionServiceConfig>(ConfigV7.class, "sg_config", JsonPointer.compile("/dynamic/sessions"),
                (config) -> SessionServiceConfig.parse(config, secretsStorageService), sessionService::setConfig);
    }

    private List<AuthenticationDomain<HTTPAuthenticator>> getAuthenticationDomainsForInjection() {
        if (!this.sessionService.isEnabled()) {
            return Collections.emptyList();
        }

        return Collections.singletonList(new AuthenticationDomain<HTTPAuthenticator>("__internal_session_auth_domain", sessionAuthenticationBackend,
                sessionHttpAuthenticator, false, -999999, Collections.emptyList(), null));
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

}
