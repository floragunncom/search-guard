/*
 * Copyright 2016-2020 by floragunn GmbH - All rights reserved
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

package com.floragunn.dlic.auth.http.jwt.keybyoidc;

import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.codova.config.net.ProxyConfig;
import com.floragunn.dlic.util.SettingsBasedSSLConfigurator;
import com.floragunn.dlic.util.SettingsBasedSSLConfigurator.SSLConfigException;
import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.TypedComponent.Factory;
import com.floragunn.searchguard.authc.legacy.LegacyHTTPAuthenticator;
import com.floragunn.searchguard.legacy.LegacyComponentFactory;
import com.floragunn.searchsupport.action.Responses;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.xcontent.ObjectTreeXContent;

@Deprecated
public class HTTPJwtKeyByOpenIdConnectAuthenticator extends AbstractHTTPJwtAuthenticator {

    private final static Logger log = LogManager.getLogger(HTTPJwtKeyByOpenIdConnectAuthenticator.class);

    private ProxyConfig proxyConfig;
    private OpenIdProviderClient openIdProviderClient;

    private final ComponentState componentState = new ComponentState(0, "authentication_frontend", "oidc",
            HTTPJwtKeyByOpenIdConnectAuthenticator.class).initialized().requiresEnterpriseLicense();

    public HTTPJwtKeyByOpenIdConnectAuthenticator(Settings settings, Path configPath) {
        super(settings, configPath);
    }

    protected KeyProvider initKeyProvider(Settings settings, Path configPath) throws Exception {

        this.proxyConfig = ProxyConfig.parse(ObjectTreeXContent.toMap(settings), "proxy");

        try {
            this.openIdProviderClient = new OpenIdProviderClient(URI.create(settings.get("openid_connect_url")), getSSLConfig(settings, configPath),
                    proxyConfig, settings.getAsBoolean("cache_jwks_endpoint", false));
            int idpRequestTimeoutMs = settings.getAsInt("idp_request_timeout_ms", 5000);

            openIdProviderClient.setRequestTimeoutMs(idpRequestTimeoutMs);

        } catch (SSLConfigException e) {
            log.error("Error while initializing openid http authenticator", e);
            throw new RuntimeException("Error while initializing openid http authenticator", e);
        }

        int idpRequestTimeoutMs = settings.getAsInt("idp_request_timeout_ms", 5000);
        int idpQueuedThreadTimeoutMs = settings.getAsInt("idp_queued_thread_timeout_ms", 2500);

        int refreshRateLimitTimeWindowMs = settings.getAsInt("refresh_rate_limit_time_window_ms", 10000);
        int refreshRateLimitCount = settings.getAsInt("refresh_rate_limit_count", 10);

        KeySetRetriever keySetRetriever = new KeySetRetriever(openIdProviderClient);

        SelfRefreshingKeySet selfRefreshingKeySet = new SelfRefreshingKeySet(keySetRetriever);

        selfRefreshingKeySet.setRequestTimeoutMs(idpRequestTimeoutMs);
        selfRefreshingKeySet.setQueuedThreadTimeoutMs(idpQueuedThreadTimeoutMs);
        selfRefreshingKeySet.setRefreshRateLimitTimeWindowMs(refreshRateLimitTimeWindowMs);
        selfRefreshingKeySet.setRefreshRateLimitCount(refreshRateLimitCount);

        return selfRefreshingKeySet;
    }

    private static SettingsBasedSSLConfigurator.SSLConfig getSSLConfig(Settings settings, Path configPath) throws SSLConfigException {
        return new SettingsBasedSSLConfigurator(settings, configPath, "openid_connect_idp").buildSSLConfig();
    }

    @Override
    public boolean handleMetaRequest(RestRequest restRequest, RestChannel restChannel, String generalRequestPathComponent,
            String specificRequestPathComponent, ThreadContext threadContext) {
        try {
            if ("config".equals(specificRequestPathComponent)) {
                OidcProviderConfig oidcProviderConfig = openIdProviderClient.getOidcConfiguration();
                Map<String, Object> oidcProviderConfigMap = new HashMap<String, Object>(oidcProviderConfig.getParsedJson());

                oidcProviderConfigMap.put("token_endpoint_proxy", generalRequestPathComponent + "/token");

                Responses.send(restChannel, RestStatus.OK, oidcProviderConfigMap);
            } else if ("token".equals(specificRequestPathComponent)) {
                ContentType contentType = ContentType.APPLICATION_FORM_URLENCODED;
                byte[] content = BytesReference.toBytes(restRequest.content());

                HttpResponse idpResponse = openIdProviderClient.callTokenEndpoint(content, contentType);
                byte[] idpResponseContent = EntityUtils.toByteArray(idpResponse.getEntity());

                if (idpResponse.getStatusLine().getStatusCode() >= 400) {
                    log.warn("Got error from IDP for token endpoint:\n" + openIdProviderClient.getOidcConfiguration().getTokenEndpoint() + "\n"
                            + new String(content) + "\n" + idpResponse.getStatusLine() + "\n" + Arrays.asList(idpResponse.getAllHeaders()) + "\n"
                            + new String(idpResponseContent));
                }

                restChannel.sendResponse(new BytesRestResponse(RestStatus.fromCode(idpResponse.getStatusLine().getStatusCode()),
                        idpResponse.getEntity().getContentType().getValue(), idpResponseContent));
            } else {
                Responses.sendError(restChannel, RestStatus.NOT_FOUND, "Invalid endpoint: " + restRequest.path());
            }
            return true;
        } catch (Exception e) {
            log.error("Error while handling request", e);
            Responses.sendError(restChannel, RestStatus.INTERNAL_SERVER_ERROR, "Error while handling OpenID request");
            return true;
        }
    }

    @Override
    public String getType() {
        return "openid";
    }

    public static TypedComponent.Info<LegacyHTTPAuthenticator> INFO = new TypedComponent.Info<LegacyHTTPAuthenticator>() {

        @Override
        public Class<LegacyHTTPAuthenticator> getType() {
            return LegacyHTTPAuthenticator.class;
        }

        @Override
        public String getName() {
            return "openid";
        }

        @Override
        public Factory<LegacyHTTPAuthenticator> getFactory() {
            return LegacyComponentFactory.adapt(HTTPJwtKeyByOpenIdConnectAuthenticator::new);
        }
    };

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
}
