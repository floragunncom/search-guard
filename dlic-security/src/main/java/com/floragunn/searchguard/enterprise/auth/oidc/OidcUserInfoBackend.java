/*
 * Copyright 2016-2025 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auth.oidc;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.config.net.ProxyConfig;
import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidatingFunction;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.UserInformationBackend;
import com.floragunn.searchguard.authc.AuthenticationDebugLogger;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchsupport.cstate.metrics.Meter;

/**
 * A {@link UserInformationBackend} that enriches JWT-authenticated credentials with claims
 * fetched from an OIDC userinfo endpoint. This is useful when the JWT token itself does not
 * contain all the information needed for Search Guard role mapping (e.g. when roles are not
 * embedded in the token by the IdP).
 *
 * <p>The backend retrieves the raw JWT string from {@link AuthCredentials#getNativeCredentials()}
 * (stored there by {@code JwtAuthenticator}) and uses it as a Bearer token to call the userinfo
 * endpoint. The userinfo response is made available for role/attribute mapping under the
 * {@code oidc_user_info} attribute key.
 *
 * <p>Optionally, the username can be overridden with a value from the userinfo response via
 * the {@code username_from} configuration option. If the specified field is absent or blank
 * in the userinfo response, the username from the JWT (set by the JWT frontend) is kept.
 *
 * <p>Configuration example in {@code sg_authc.yml}:
 * <pre>
 * auth_domains:
 * - type: jwt
 *   jwt.signing.jwks_from_openid_configuration.url: "https://keycloak.example.com/realms/myrealm/.well-known/openid-configuration"
 *   user_mapping.roles.from: oidc_user_info.roles
 *   additional_user_information:
 *   - type: oidc_userinfo
 *     oidc_userinfo.openid_configuration_url: "https://keycloak.example.com/realms/myrealm/.well-known/openid-configuration"
 *     oidc_userinfo.tls.trusted_cas: "#{file:/etc/elasticsearch/certs/keycloak-ca.pem}"
 *     oidc_userinfo.username_from: display_username
 * </pre>
 */
public class OidcUserInfoBackend implements UserInformationBackend {

    private static final Logger log = LogManager.getLogger(OidcUserInfoBackend.class);

    public static final String TYPE = "oidc_userinfo";

    private final OpenIdProviderClient openIdProviderClient;
    private final String usernameFrom; // TODO - remove

    public OidcUserInfoBackend(Map<String, Object> config, ConfigurationRepository.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(config, validationErrors, context);

        URI openidConfigurationUrl = vNode.get("openid_configuration_url").required().asURI();
        TLSConfig tlsConfig = vNode.get("tls").by((Parser<TLSConfig, Parser.Context>) TLSConfig::parse);
        ProxyConfig proxyConfig = vNode.get("proxy").by((ValidatingFunction<DocNode, ProxyConfig>) ProxyConfig::parse);
        int requestTimeoutMs = vNode.get("request_timeout_ms").withDefault(10000).asInt();
        this.usernameFrom = vNode.get("username_from").asString();

        vNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        this.openIdProviderClient = new OpenIdProviderClient(openidConfigurationUrl, tlsConfig, proxyConfig, true);
        this.openIdProviderClient.setRequestTimeoutMs(requestTimeoutMs);
        log.info("OidcUserInfoBackend created with configuration {}", config);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public CompletableFuture<AuthCredentials> getUserInformation(AuthCredentials authCredentials, Meter meter)
            throws AuthenticatorUnavailableException {
        return getUserInformation(authCredentials, meter, AuthenticationDebugLogger.DISABLED);
    }

    @Override
    public CompletableFuture<AuthCredentials> getUserInformation(AuthCredentials authCredentials, Meter meter, AuthenticationDebugLogger debug)
            throws AuthenticatorUnavailableException {

        log.info("OidcUserInfoBackend getting user information");
        Object nativeCredentials = authCredentials.getNativeCredentials();

        if (!(nativeCredentials instanceof String)) {
            log.warn("oidc_userinfo backend: native credentials are not a JWT string; skipping userinfo call for user {}",
                    authCredentials.getUsername());
            return CompletableFuture.completedFuture(authCredentials);
        }

        String accessToken = (String) nativeCredentials;

        Map<String, Object> userInfo = openIdProviderClient.callUserInfoEndpoint(accessToken, null);

        debug.success(TYPE, "Fetched userinfo from OIDC endpoint", "oidc_user_info", userInfo);

        authCredentials = authCredentials.userMappingAttribute("oidc_user_info", userInfo);
        log.info("Get user information for user '{}' from OIDC userinfo endpoint '{}'. Username from '{}'", authCredentials.getUsername(), userInfo, usernameFrom);
//        if (usernameFrom != null) {
//            Object usernameValue = userInfo.get(usernameFrom);
//            log.info("Retrieved username from OIDC userinfo endpoint '{}'", usernameValue);
//            if (usernameValue instanceof String && !((String) usernameValue).isBlank()) {
//                authCredentials = authCredentials.userName((String) usernameValue);
//            } else {
//                log.warn("oidc_userinfo backend: field '{}' not found or blank in userinfo response for user {}; keeping existing username",
//                        usernameFrom, authCredentials.getUsername());
//            }
//        }

        return CompletableFuture.completedFuture(authCredentials);
    }

    public static TypedComponent.Info<UserInformationBackend> INFO = new TypedComponent.Info<UserInformationBackend>() {

        @Override
        public Class<UserInformationBackend> getType() {
            return UserInformationBackend.class;
        }

        @Override
        public String getName() {
            return TYPE;
        }

        @Override
        public TypedComponent.Factory<UserInformationBackend> getFactory() {
            return OidcUserInfoBackend::new;
        }
    };
}
