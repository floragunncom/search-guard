package com.floragunn.searchguard.authtoken;

import java.util.Arrays;
import java.util.Collection;
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

import com.fasterxml.jackson.core.JsonPointer;
import com.floragunn.searchguard.authtoken.api.AuthTokenRestAction;
import com.floragunn.searchguard.authtoken.api.CreateAuthTokenAction;
import com.floragunn.searchguard.authtoken.api.GetAuthTokenAction;
import com.floragunn.searchguard.authtoken.api.RevokeAuthTokenAction;
import com.floragunn.searchguard.authtoken.api.SearchAuthTokenRestAction;
import com.floragunn.searchguard.authtoken.api.SearchAuthTokensAction;
import com.floragunn.searchguard.authtoken.api.TransportCreateAuthTokenAction;
import com.floragunn.searchguard.authtoken.api.TransportGetAuthTokenAction;
import com.floragunn.searchguard.authtoken.api.TransportRevokeAuthTokenAction;
import com.floragunn.searchguard.authtoken.api.TransportSearchAuthTokensAction;
import com.floragunn.searchguard.authtoken.update.PushAuthTokenUpdateAction;
import com.floragunn.searchguard.authtoken.update.TransportPushAuthTokenUpdateAction;
import com.floragunn.searchguard.modules.SearchGuardModule;
import com.floragunn.searchguard.sgconf.history.ConfigHistoryService;
import com.floragunn.searchguard.sgconf.impl.v7.ConfigV7;
import com.floragunn.searchguard.support.PrivilegedConfigClient;

public class AuthTokenModule implements SearchGuardModule<AuthTokenServiceConfig> {

    private AuthTokenService authTokenService;

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(new AuthTokenRestAction(), new SearchAuthTokenRestAction());
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(CreateAuthTokenAction.INSTANCE, TransportCreateAuthTokenAction.class),
                new ActionHandler<>(PushAuthTokenUpdateAction.INSTANCE, TransportPushAuthTokenUpdateAction.class),
                new ActionHandler<>(GetAuthTokenAction.INSTANCE, TransportGetAuthTokenAction.class),
                new ActionHandler<>(RevokeAuthTokenAction.INSTANCE, TransportRevokeAuthTokenAction.class),
                new ActionHandler<>(SearchAuthTokensAction.INSTANCE, TransportSearchAuthTokensAction.class));
    }

    @Override
    public Collection<Object> createComponents(BaseDependencies baseDependencies) {
        PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(baseDependencies.getLocalClient());

        ConfigHistoryService configHistoryService = new ConfigHistoryService(baseDependencies.getConfigurationRepository(), privilegedConfigClient,
                baseDependencies.getProtectedConfigIndexService(), baseDependencies.getDynamicConfigFactory(), baseDependencies.getSettings());

        authTokenService = new AuthTokenService(privilegedConfigClient, configHistoryService, baseDependencies.getSettings(),
                baseDependencies.getThreadPool(), baseDependencies.getProtectedConfigIndexService(), new AuthTokenServiceConfig());

        AuthTokenAuthenticationBackend authenticationBackend = new AuthTokenAuthenticationBackend(authTokenService);

        baseDependencies.getSpecialPrivilegesEvaluationContextProviderRegistry().add(authTokenService);

        return Arrays.asList(authTokenService, configHistoryService, authenticationBackend);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(AuthTokenService.INDEX_NAME, ConfigHistoryService.CACHE_MAX_SIZE, ConfigHistoryService.CACHE_TTL,
                ConfigHistoryService.INDEX_NAME, ConfigHistoryService.MODEL_CACHE_MAX_SIZE, ConfigHistoryService.MODEL_CACHE_TTL);
    }

    @Override
    public SgConfigMetadata<AuthTokenServiceConfig> getSgConfigMetadata() {
        return new SgConfigMetadata<AuthTokenServiceConfig>(ConfigV7.class, "sg_config", JsonPointer.compile("/dynamic/auth_token_provider"),
                AuthTokenServiceConfig::parse, authTokenService::setConfig);
    }
}
