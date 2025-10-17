/*
 * Copyright 2016-2022 by floragunn GmbH - All rights reserved
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
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.floragunn.codova.documents.Parser;
import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.config.net.ProxyConfig;
import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidatingFunction;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.TypedComponent.Factory;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.base.AuthcResult;
import com.floragunn.searchguard.authc.session.ActivatedFrontendConfig;
import com.floragunn.searchguard.authc.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.authc.session.GetActivatedFrontendConfigAction;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.enterprise.auth.oidc.OpenIdProviderClient.TokenResponse;
import com.floragunn.searchguard.user.Attributes;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;

public class OidcAuthenticator implements ApiAuthenticationFrontend {
    private final static Logger log = LogManager.getLogger(OidcAuthenticator.class);
    private static final String SSO_CONTEXT_PREFIX_STATE = "oidc_s:";
    private static final String SSO_CONTEXT_PREFIX_CODE_VERIFIER = "oidc_cv:";

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private ProxyConfig proxyConfig;
    private OpenIdProviderClient openIdProviderClient;
    private final String clientId;
    private final String clientSecret;
    private final String scope;
    private JwtVerifier jwtVerifier;
    private JwtVerifier userInfoJwtVerifier;
    private final String logoutUrl;
    private final boolean usePkce;
    private final boolean useUserInfoEndpoint;
    
    private final ComponentState componentState = new ComponentState(0, "authentication_frontend", "oidc", OidcAuthenticator.class).initialized()
            .requiresEnterpriseLicense();

    public OidcAuthenticator(Map<String, Object> config, ConfigurationRepository.Context context) throws ConfigValidationException {

        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(config, validationErrors, context);

        this.clientId = vNode.get("client_id").required().asString();
        this.usePkce = vNode.get("pkce").withDefault(true).asBoolean();
        this.useUserInfoEndpoint = vNode.get("get_user_info").withDefault(false).asBoolean();
        this.clientSecret = vNode.get("client_secret").required(!this.usePkce).asString();

        this.scope = vNode.get("scope").withDefault("openid profile email address phone").asString();
        this.proxyConfig = vNode.get("idp.proxy").by((ValidatingFunction<DocNode, ProxyConfig>) ProxyConfig::parse);

        logoutUrl = vNode.get("logout_url").asString();

        int idpRequestTimeoutMs = vNode.get("idp_request_timeout_ms").withDefault(5000).asInt();
        int idpQueuedThreadTimeoutMs = vNode.get("idp_queued_thread_timeout_ms").withDefault(2500).asInt();

        int refreshRateLimitTimeWindowMs = vNode.get("refresh_rate_limit_time_window_ms").withDefault(10000).asInt();
        int refreshRateLimitCount = vNode.get("refresh_rate_limit_count").withDefault(10).asInt();

        URI openidConnectUrl = vNode.get("idp.openid_configuration_url").required().asURI();
        TLSConfig tlsConfig = vNode.get("idp.tls").by((Parser<TLSConfig, Parser.Context>) TLSConfig::parse);

        boolean cacheJwksEndpoint = vNode.get("cache_jwks_endpoint").withDefault(false).asBoolean();
        String requiredAudience = vNode.get("required_audience").asString();
        String requiredIssuer = vNode.get("required_issuer").asString();
        int maxClockSkewSeconds = vNode.get("max_clock_skew_seconds").withDefault(10).asInt();

        vNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        this.openIdProviderClient = new OpenIdProviderClient(openidConnectUrl, tlsConfig, proxyConfig, cacheJwksEndpoint);

        openIdProviderClient.setRequestTimeoutMs(idpRequestTimeoutMs);

        KeySetRetriever keySetRetriever = new KeySetRetriever(openIdProviderClient);
        SelfRefreshingKeySet selfRefreshingKeySet = new SelfRefreshingKeySet(keySetRetriever);

        selfRefreshingKeySet.setRequestTimeoutMs(idpRequestTimeoutMs);
        selfRefreshingKeySet.setQueuedThreadTimeoutMs(idpQueuedThreadTimeoutMs);
        selfRefreshingKeySet.setRefreshRateLimitTimeWindowMs(refreshRateLimitTimeWindowMs);
        selfRefreshingKeySet.setRefreshRateLimitCount(refreshRateLimitCount);

        jwtVerifier = new JwtVerifier(selfRefreshingKeySet, requiredAudience, requiredIssuer, maxClockSkewSeconds);
        userInfoJwtVerifier = new JwtVerifier(selfRefreshingKeySet, null, requiredIssuer, maxClockSkewSeconds);
    }

    @Override
    public ActivatedFrontendConfig.AuthMethod activateFrontendConfig(ActivatedFrontendConfig.AuthMethod frontendConfig,
            GetActivatedFrontendConfigAction.Request request) throws AuthenticatorUnavailableException {
        try {
            ValidationResult<OidcProviderConfig> oidcProviderConfig = openIdProviderClient.getOidcConfiguration();

            if (oidcProviderConfig.peek() == null || oidcProviderConfig.peek().getAuthorizationEndpoint() == null) {
                throw new AuthenticatorUnavailableException("Invalid OIDC metadata", "authorization_endpoint missing from OIDC metadata")
                        .details("oidc_metadata", oidcProviderConfig.toBasicObject());
            }

            if (request.getFrontendBaseUrl() == null) {
                throw new AuthenticatorUnavailableException("Invalid configuration", "frontend_base_url is required for OIDC authentication")
                        .details("request", request.toBasicObject());
            }

            URI frontendBaseUrl = new URI(request.getFrontendBaseUrl());

            String redirectUri = getLoginPostURI(frontendBaseUrl).toASCIIString();
            String stateToken = createOpaqueToken(24);
            String codeVerifier = null;
            String codeChallenge = null;
            String state;
            String ssoContext = SSO_CONTEXT_PREFIX_STATE + stateToken;

            if (!Strings.isNullOrEmpty(request.getNextURL())) {
                state = stateToken + "|" + request.getNextURL();
            } else {
                state = stateToken;
            }

            URIBuilder ssoLocationBuilder = new URIBuilder(oidcProviderConfig.peek().getAuthorizationEndpoint()).addParameter("client_id", clientId)
                    .addParameter("response_type", "code").addParameter("redirect_uri", redirectUri).addParameter("state", state)
                    .addParameter("scope", scope);

            if (usePkce) {
                codeVerifier = createOpaqueToken(48);
                codeChallenge = BaseEncoding.base64Url().omitPadding()
                        .encode(Hashing.sha256().hashString(codeVerifier, StandardCharsets.US_ASCII).asBytes());
                ssoLocationBuilder.addParameter("code_challenge", codeChallenge);
                ssoLocationBuilder.addParameter("code_challenge_method", "S256");
                ssoContext += ";" + SSO_CONTEXT_PREFIX_CODE_VERIFIER + codeVerifier;
            }

            return frontendConfig.ssoLocation(ssoLocationBuilder.build().toASCIIString()).ssoContext(ssoContext);

        } catch (URISyntaxException e) {
            log.error("Error while activating SAML authenticator", e);
            throw new AuthenticatorUnavailableException("Invalid configuration", "frontend_base_url is not a valid URL", e).details("request",
                    request.toBasicObject());
        }
    }

    @Override
    public AuthCredentials extractCredentials(Map<String, Object> request)
            throws CredentialsException, AuthenticatorUnavailableException, ConfigValidationException {
        Map<String, Object> debugDetails = new HashMap<>();

        String ssoContext = request.containsKey("sso_context") ? String.valueOf(request.get("sso_context")) : null;

        if (ssoContext == null) {
            throw new ConfigValidationException(new MissingAttribute("sso_context"));
        }

        String expectedStateToken = getValueFromSsoContext(SSO_CONTEXT_PREFIX_STATE, ssoContext);

        if (expectedStateToken == null) {
            throw new ConfigValidationException(new InvalidAttributeValue("sso_context", ssoContext, "Must contain " + SSO_CONTEXT_PREFIX_STATE));
        }

        String codeVerifier = getValueFromSsoContext(SSO_CONTEXT_PREFIX_CODE_VERIFIER, ssoContext);

        if (usePkce && codeVerifier == null) {
            throw new ConfigValidationException(
                    new InvalidAttributeValue("sso_context", ssoContext, "Must contain " + SSO_CONTEXT_PREFIX_CODE_VERIFIER));
        }

        String ssoResult = request.containsKey("sso_result") ? String.valueOf(request.get("sso_result")) : null;

        if (ssoResult == null) {
            throw new ConfigValidationException(new MissingAttribute("ssoResult"));
        }

        if (!request.containsKey("frontend_base_url")) {
            throw new ConfigValidationException(new MissingAttribute("frontend_base_url"));
        }

        URI frontendBaseUrl;

        try {
            frontendBaseUrl = new URI(String.valueOf(request.get("frontend_base_url")));
        } catch (URISyntaxException e) {
            throw new ConfigValidationException(new InvalidAttributeValue("frontend_base_url", request.get("frontend_base_url"), "A URL"));
        }

        Map<String, String> ssoResultParams = getUriParams(ssoResult);

        String state = (String) ssoResultParams.get("state");

        if (state == null) {
            throw new ConfigValidationException(new MissingAttribute("ssoResult.state"));
        }

        String actualStateToken;
        String frontendRedirectUri;
        int separator = state.indexOf('|');

        if (separator == -1) {
            actualStateToken = state;
            frontendRedirectUri = null;
        } else {
            actualStateToken = state.substring(0, separator);
            frontendRedirectUri = state.substring(separator + 1);
        }

        String code = (String) ssoResultParams.get("code");

        if (code == null) {
            throw new ConfigValidationException(new MissingAttribute("ssoResult.code"));
        }

        debugDetails.put("sso_context", ssoContext);
        debugDetails.put("sso_result", ssoResult);
        debugDetails.put("code", code);
        debugDetails.put("state", state);

        if (!Objects.equals(expectedStateToken, actualStateToken)) {
            throw new CredentialsException(new AuthcResult.DebugInfo(getType(), false,
                    "Invalid state token: " + expectedStateToken + "/" + actualStateToken, debugDetails));
        }

        String oidcRedirectUri = getLoginPostURI(frontendBaseUrl).toASCIIString();

        Map<String, String> tokenRequest = new LinkedHashMap<>();
        tokenRequest.put("client_id", clientId);
        tokenRequest.put("code", code);
        tokenRequest.put("redirect_uri", oidcRedirectUri);

        if (usePkce) {
            tokenRequest.put("code_verifier", codeVerifier);
        }

        if (clientSecret != null) {
            tokenRequest.put("client_secret", clientSecret);
        }

        TokenResponse tokenResponse = openIdProviderClient.callTokenEndpoint(tokenRequest);

        debugDetails.put("token_response", tokenResponse.asMap());

        String jwtString = tokenResponse.getIdToken();

        JwtToken jwt;

        try {
            jwt = jwtVerifier.getVerifiedJwtToken(jwtString);
        } catch (AuthenticatorUnavailableException e) {
            log.info(e);
            throw e;
        } catch (BadCredentialsException e) {
            log.info("Extracting JWT token from " + jwtString + " failed", e);
            throw new CredentialsException(new AuthcResult.DebugInfo(getType(), false, e.getMessage(), debugDetails), e);
        }

        JwtClaims claims = jwt.getClaims();

        if (log.isTraceEnabled()) {
            log.trace("Claims from JWT: " + claims.asMap());
        }

        debugDetails.put("claims", claims.asMap());
        Map<String, Object> userInfo = Collections.emptyMap();

        if (useUserInfoEndpoint) {
            userInfo = openIdProviderClient.callUserInfoEndpoint(tokenResponse.getAccessToken(), userInfoJwtVerifier);
            String userInfoSubject = String.valueOf(userInfo.get("sub"));

            debugDetails.put("oidc_user_info", userInfo);

            if (!userInfoSubject.equals(jwt.getClaims().getSubject())) {
                throw new CredentialsException(new AuthcResult.DebugInfo(getType(), false,
                        "sub claim from user info does not match sub claim from ID token", debugDetails));
            }

        }

        return AuthCredentials.forUser(claims.getSubject()).userMappingAttribute("oidc_id_token", claims.asMap())
                .userMappingAttribute("jwt", claims.asMap()).userMappingAttribute("oidc_user_info", userInfo).attribute(Attributes.AUTH_TYPE, "oidc")
                .attribute("__oidc_id", jwtString).attribute("__fe_base_url", frontendBaseUrl.toString()).claims(claims.asMap()).complete()
                .redirectUri(frontendRedirectUri).build();
    }

    @Override
    public String getLogoutUrl(User user) throws AuthenticatorUnavailableException {
        try {
            if (logoutUrl != null) {
                return logoutUrl;
            }

            if (user == null) {
                return null;
            }

            if (user.getStructuredAttributes().get("__fe_base_url") == null) {
                return null;
            }

            String frontendBaseUrl = String.valueOf(user.getStructuredAttributes().get("__fe_base_url"));

            ValidationResult<OidcProviderConfig> oidcProviderConfig = openIdProviderClient.getOidcConfiguration();

            URI endSessionEndpoint = oidcProviderConfig.get().getEndSessionEndpoint();

            if (endSessionEndpoint == null) {
                return null;
            }

            String idToken = user.getStructuredAttributes().get("__oidc_id") != null ? user.getStructuredAttributes().get("__oidc_id").toString()
                    : null;
            URI result = new URIBuilder(endSessionEndpoint).addParameter("post_logout_redirect_uri", frontendBaseUrl)
                    .addParameter("id_token_hint", idToken).build();

            return result.toASCIIString();
        } catch (URISyntaxException e) {
            log.error("Error while constructing logout url for " + this, e);
            return null;
        } catch (ConfigValidationException e) {
            log.error("Error while constructing logout url for " + this, e);
            return null;
        }
    }

    private Map<String, String> getUriParams(String uriString) throws ConfigValidationException {
        try {
            URI uri = new URI(uriString);

            List<NameValuePair> nameValuePairs = URLEncodedUtils.parse(uri, Charset.forName("utf-8"));

            HashMap<String, String> result = new HashMap<>(nameValuePairs.size());

            for (NameValuePair nameValuePair : nameValuePairs) {
                result.put(nameValuePair.getName(), nameValuePair.getValue());
            }

            return result;

        } catch (URISyntaxException e) {
            throw new ConfigValidationException(new InvalidAttributeValue("sso_result", uriString, "URI"));
        }
    }

    private URI getLoginPostURI(URI frontendBaseURI) {
        try {
            String path = frontendBaseURI.getPath().endsWith("/") ?
                frontendBaseURI.getPath() + "auth/openid/login" : frontendBaseURI.getPath() + "/auth/openid/login";
            return new URIBuilder(frontendBaseURI).setPath(path).build();
        } catch (URISyntaxException e) {
            log.error("Got URISyntaxException when constructing loginPostURI. This should not happen. frontendBaseURI: " + frontendBaseURI, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getType() {
        return "oidc";
    }

    private String createOpaqueToken(int byteCount) {
        byte[] b = new byte[byteCount];
        SECURE_RANDOM.nextBytes(b);
        return BaseEncoding.base64Url().omitPadding().encode(b);
    }

    private String getValueFromSsoContext(String key, String ssoContext) {
        if (ssoContext == null) {
            return null;
        }

        int keyIndex = ssoContext.indexOf(key);

        if (keyIndex == -1) {
            return null;
        }

        int valueIndex = keyIndex + key.length();
        int endIndex = ssoContext.indexOf(';', valueIndex);
        return ssoContext.substring(valueIndex, endIndex == -1 ? ssoContext.length() : endIndex).trim();
    }

    public static TypedComponent.Info<ApiAuthenticationFrontend> INFO = new TypedComponent.Info<ApiAuthenticationFrontend>() {

        @Override
        public Class<ApiAuthenticationFrontend> getType() {
            return ApiAuthenticationFrontend.class;
        }

        @Override
        public String getName() {
            return "oidc";
        }

        @Override
        public Factory<ApiAuthenticationFrontend> getFactory() {
            return OidcAuthenticator::new;
        }
    };

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
}
