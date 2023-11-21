/*
 * Copyright 2020-2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.authtoken;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
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
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.legacy.LegacySgConfig;
import com.floragunn.searchguard.authc.rest.HttpAuthenticationFrontend;
import com.floragunn.searchguard.authtoken.api.AuthTokenInfoAction;
import com.floragunn.searchguard.authtoken.api.AuthTokenInfoRestAction;
import com.floragunn.searchguard.authtoken.api.AuthTokenRestAction;
import com.floragunn.searchguard.authtoken.api.CreateAuthTokenAction;
import com.floragunn.searchguard.authtoken.api.GetAuthTokenAction;
import com.floragunn.searchguard.authtoken.api.RevokeAuthTokenAction;
import com.floragunn.searchguard.authtoken.api.SearchAuthTokenRestAction;
import com.floragunn.searchguard.authtoken.api.SearchAuthTokensAction;
import com.floragunn.searchguard.authtoken.api.TransportAuthTokenInfoAction;
import com.floragunn.searchguard.authtoken.api.TransportCreateAuthTokenAction;
import com.floragunn.searchguard.authtoken.api.TransportGetAuthTokenAction;
import com.floragunn.searchguard.authtoken.api.TransportRevokeAuthTokenAction;
import com.floragunn.searchguard.authtoken.api.TransportSearchAuthTokensAction;
import com.floragunn.searchguard.authtoken.update.PushAuthTokenUpdateAction;
import com.floragunn.searchguard.authtoken.update.TransportPushAuthTokenUpdateAction;
import com.floragunn.searchguard.configuration.variables.ConfigVarService;
import com.floragunn.searchguard.sgconf.history.ConfigHistoryService;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.StaticSettings;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;

public class AuthTokenModule implements SearchGuardModule, ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(AuthTokenModule.class);

    private AuthTokenService authTokenService;
    private ConfigVarService configVarService;
    private final ComponentState componentState = new ComponentState(1000, null, "auth_token_service", AuthTokenModule.class).requiresEnterpriseLicense();
    private AuthTokenAuthenticationDomain authenticationDomain;

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            ScriptService scriptService, Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(new AuthTokenRestAction(), new SearchAuthTokenRestAction(), new AuthTokenInfoRestAction(),
                AuthTokenServiceConfigApi.REST_API);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return ImmutableList
                .of(new ActionHandler<>(CreateAuthTokenAction.INSTANCE, TransportCreateAuthTokenAction.class),
                        new ActionHandler<>(PushAuthTokenUpdateAction.INSTANCE, TransportPushAuthTokenUpdateAction.class),
                        new ActionHandler<>(GetAuthTokenAction.INSTANCE, TransportGetAuthTokenAction.class),
                        new ActionHandler<>(RevokeAuthTokenAction.INSTANCE, TransportRevokeAuthTokenAction.class),
                        new ActionHandler<>(SearchAuthTokensAction.INSTANCE, TransportSearchAuthTokensAction.class),
                        new ActionHandler<>(AuthTokenInfoAction.INSTANCE, TransportAuthTokenInfoAction.class))
                .with(AuthTokenServiceConfigApi.ACTION_HANDLERS);
    }

    @Override
    public Collection<Object> createComponents(BaseDependencies baseDependencies) {
        this.configVarService = baseDependencies.getConfigVarService();

        this.configVarService.requestRandomKey(AuthTokenServiceConfig.SIGNING_KEY_SECRET, 512, "authc");

        PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(baseDependencies.getLocalClient());

        ConfigHistoryService configHistoryService = new ConfigHistoryService(baseDependencies.getConfigurationRepository(),
                baseDependencies.getStaticSgConfig(), privilegedConfigClient, baseDependencies.getProtectedConfigIndexService(),
                baseDependencies.getActions(), baseDependencies.getStaticSettings(), baseDependencies.getPrivilegesEvaluator());

        componentState.addPart(configHistoryService.getComponentState());

        authTokenService = new AuthTokenService(privilegedConfigClient, baseDependencies.getAuthorizationService(),
                baseDependencies.getPrivilegesEvaluator(), configHistoryService, baseDependencies.getStaticSettings(),
                baseDependencies.getThreadPool(), baseDependencies.getClusterService(), baseDependencies.getProtectedConfigIndexService(),
                baseDependencies.getActions(), null, componentState);

        AuthTokenAuthenticationDomain authenticationBackend = new AuthTokenAuthenticationDomain(authTokenService);

        baseDependencies.getSpecialPrivilegesEvaluationContextProviderRegistry().add(authTokenService);

        authenticationDomain = new AuthTokenAuthenticationDomain(authTokenService);

        ConfigurationRepository configurationRepository = baseDependencies.getConfigurationRepository();
        configurationRepository.subscribeOnChange(new ConfigurationChangeListener() {

            @Override
            public void onChange(ConfigMap configMap) {
                SgDynamicConfiguration<AuthTokenServiceConfig> config = configMap.get(AuthTokenServiceConfig.TYPE);
                SgDynamicConfiguration<LegacySgConfig> sgConfig = configMap.get(CType.CONFIG);

                if (config != null && config.getCEntry("default") != null) {
                    authTokenService.setConfig(config.getCEntry("default"));
                    componentState.setConfigVersion(config.getDocVersion());
                    componentState.setState(State.INITIALIZED, "using_config");
                } else if (sgConfig != null && sgConfig.getCEntry("sg_config") != null) {
                    DocNode docNode = sgConfig.getCEntry("sg_config").getSource().getAsNode("dynamic", "auth_token_provider");

                    if (!docNode.isNull()) {
                        try {
                            AuthTokenServiceConfig authTokenServiceConfig = AuthTokenServiceConfig
                                    .parse(docNode, configurationRepository.getParserContext()).get();

                            authTokenService.setConfig(authTokenServiceConfig);
                            componentState.setConfigVersion(sgConfig.getDocVersion());
                            componentState.setState(State.INITIALIZED, "using_legacy_config");
                        } catch (ConfigValidationException e) {
                            log.error("Invalid config for AuthTokenService", e);
                        }
                    }
                } else {
                    componentState.setState(State.SUSPENDED, "not_configured");
                }
            }
        });

        return Arrays.asList(authTokenService, configHistoryService, authenticationBackend);
    }

    @Override
    public StaticSettings.AttributeSet getSettings() {
        return StaticSettings.AttributeSet.of(AuthTokenService.INDEX_NAME, AuthTokenService.CLEANUP_INTERVAL, ConfigHistoryService.CACHE_MAX_SIZE,
                ConfigHistoryService.CACHE_TTL, ConfigHistoryService.INDEX_NAME, ConfigHistoryService.MODEL_CACHE_MAX_SIZE,
                ConfigHistoryService.MODEL_CACHE_TTL);
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    @Override
    public List<AuthenticationDomain<HttpAuthenticationFrontend>> getImplicitHttpAuthenticationDomains() {
        return Collections.singletonList(authenticationDomain);
    }

    @Override
    public ImmutableSet<String> getCapabilities() {
        return ImmutableSet.of("auth_tokens");
    }

}
