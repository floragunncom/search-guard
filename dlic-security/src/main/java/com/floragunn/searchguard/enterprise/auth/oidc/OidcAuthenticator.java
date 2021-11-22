/*
 * Copyright 2016-2021 by floragunn GmbH - All rights reserved
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
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.config.net.ProxyConfig;
import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.codova.documents.BasicJsonPathDefaultConfiguration;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.dlic.auth.http.jwt.oidc.json.OidcProviderConfig;
import com.floragunn.dlic.util.Roles;
import com.floragunn.searchguard.auth.AuthczResult;
import com.floragunn.searchguard.auth.AuthenticationFrontend;
import com.floragunn.searchguard.auth.AuthenticatorUnavailableException;
import com.floragunn.searchguard.auth.CredentialsException;
import com.floragunn.searchguard.auth.frontend.ActivatedFrontendConfig;
import com.floragunn.searchguard.auth.frontend.GetFrontendConfigAction;
import com.floragunn.searchguard.auth.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.enterprise.auth.oidc.OpenIdProviderClient.TokenResponse;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchguard.user.UserAttributes;
import com.google.common.base.Strings;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;

public class OidcAuthenticator implements ApiAuthenticationFrontend {
    private final static Logger log = LogManager.getLogger(OidcAuthenticator.class);
    private static final String SSO_CONTEXT_PREFIX = "oidc_state:";
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private ProxyConfig proxyConfig;
    private OpenIdProviderClient openIdProviderClient;
    private final String clientId;
    private final String clientSecret;
    private final String scope;
    private JwtVerifier jwtVerifier;
    private final Pattern subjectPattern;
    private final JsonPath jsonSubjectPath;
    private final JsonPath jsonRolesPath;
    private final String logoutUrl;
    private Configuration jsonPathConfig;
    private Map<String, JsonPath> attributeMapping;

    public OidcAuthenticator(Map<String, Object> config, AuthenticationFrontend.Context context) throws ConfigValidationException {

        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(config, validationErrors).expandVariables(context.getConfigVariableProviders());

        this.clientId = vNode.get("client_id").required().asString();
        this.clientSecret = vNode.get("client_secret").required().asString();
        this.scope = vNode.get("scope").withDefault("openid profile email address phone").asString();
        this.proxyConfig = vNode.get("idp.proxy").by(ProxyConfig::parse);

        jsonRolesPath = vNode.get("user_mapping.roles").asJsonPath();
        jsonSubjectPath = vNode.get("user_mapping.subject").asJsonPath();
        subjectPattern = vNode.get("user_mapping.subject_pattern").asPattern();
        logoutUrl = vNode.get("logout_url").asString();

        try {
            attributeMapping = UserAttributes.getAttributeMapping(vNode.get("user_mapping.attrs").asMap());
        } catch (ConfigValidationException e) {
            validationErrors.add("user_mapping.attrs", e);
        }

        int idpRequestTimeoutMs = vNode.get("idp_request_timeout_ms").withDefault(5000).asInt();
        int idpQueuedThreadTimeoutMs = vNode.get("idp_queued_thread_timeout_ms").withDefault(2500).asInt();

        int refreshRateLimitTimeWindowMs = vNode.get("refresh_rate_limit_time_window_ms").withDefault(10000).asInt();
        int refreshRateLimitCount = vNode.get("refresh_rate_limit_count").withDefault(10).asInt();

        URI openidConnectUrl = vNode.get("idp.openid_configuration_url").required().asURI();
        TLSConfig tlsConfig = vNode.get("idp.tls").by(TLSConfig::parse);

        boolean cacheJwksEndpoint = vNode.get("cache_jwks_endpoint").withDefault(false).asBoolean();

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

        jwtVerifier = new JwtVerifier(selfRefreshingKeySet);

        jsonPathConfig = BasicJsonPathDefaultConfiguration.builder().options(Option.ALWAYS_RETURN_LIST).build();
    }

    @Override
    public ActivatedFrontendConfig.AuthMethod activateFrontendConfig(ActivatedFrontendConfig.AuthMethod frontendConfig,
            GetFrontendConfigAction.Request request) throws AuthenticatorUnavailableException {
        try {
            OidcProviderConfig oidcProviderConfig = openIdProviderClient.getOidcConfiguration();

            if (oidcProviderConfig.getAuthorizationEndpoint() == null) {
                throw new AuthenticatorUnavailableException("Invalid OIDC metadata", "authorization_endpoint missing from OIDC metadata").details("oidc_metadata",
                        oidcProviderConfig.getParsedJson());
            }

            if (request.getFrontendBaseUrl() == null) {
                throw new AuthenticatorUnavailableException("Invalid configuration", "frontend_base_url is required for OIDC authentication").details("request",
                        request.toBasicObject());
            }

            URI frontendBaseUrl = new URI(request.getFrontendBaseUrl());

            String redirectUri = getLoginPostURI(frontendBaseUrl).toASCIIString();
            String stateToken = createOpaqueToken();
            String state;
            
            if (!Strings.isNullOrEmpty(request.getNextURL())) {
                state = stateToken + "|" + request.getNextURL(); 
            } else {
                state = stateToken;
            }

            String ssoLocation = new URIBuilder(oidcProviderConfig.getAuthorizationEndpoint()).addParameter("client_id", clientId)
                    .addParameter("response_type", "code").addParameter("redirect_uri", redirectUri).addParameter("state", state)
                    .addParameter("scope", scope).build().toASCIIString();
            String ssoContext = SSO_CONTEXT_PREFIX + stateToken;

            return frontendConfig.ssoLocation(ssoLocation).ssoContext(ssoContext);

        } catch (URISyntaxException e) {
            log.error("Error while activating SAML authenticator", e);
            throw new AuthenticatorUnavailableException("Invalid configuration", "frontend_base_url is not a valid URL", e).details("request", request.toBasicObject());
        }
    }

    @Override
    public AuthCredentials extractCredentials(Map<String, Object> request)
            throws CredentialsException, AuthenticatorUnavailableException, ConfigValidationException {
        Map<String, Object> debugDetails = new HashMap<>();

        String expectedStateToken = null;

        String ssoContext = request.containsKey("sso_context") ? String.valueOf(request.get("sso_context")) : null;

        if (ssoContext != null) {
            if (!ssoContext.startsWith(SSO_CONTEXT_PREFIX)) {
                throw new ConfigValidationException(new InvalidAttributeValue("sso_context", ssoContext, "Must start with " + SSO_CONTEXT_PREFIX));
            }

            expectedStateToken = ssoContext.substring(SSO_CONTEXT_PREFIX.length());
        } else {
            throw new ConfigValidationException(new MissingAttribute("sso_context"));
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
            throw new CredentialsException(new AuthczResult.DebugInfo(getType(), false, "Invalid state token: " + expectedStateToken + "/" + actualStateToken, debugDetails));
        }

        String oidcRedirectUri = getLoginPostURI(frontendBaseUrl).toASCIIString();

        TokenResponse tokenResponse = openIdProviderClient.callTokenEndpoint(clientId, clientSecret, scope, code, oidcRedirectUri);

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
            throw new CredentialsException(new AuthczResult.DebugInfo(getType(), false, e.getMessage(), debugDetails), e);
        }

        JwtClaims claims = jwt.getClaims();

        if (log.isTraceEnabled()) {
            log.trace("Claims from JWT: " + claims.asMap());
        }

        debugDetails.put("claims", claims.asMap());

        String subject = extractSubject(claims);
        List<String> roles = extractRoles(claims, debugDetails);

        if (log.isTraceEnabled()) {
            log.trace("From JWT:\nSubject: " + subject + "\nRoles: " + roles);
        }

        return AuthCredentials.forUser(subject).backendRoles(roles).attributesByJsonPath(attributeMapping, claims)
                .attribute(UserAttributes.AUTH_TYPE, "oidc").attribute("__oidc_id", jwtString).attribute("__fe_base_url", frontendBaseUrl.toString())
                .claims(claims.asMap()).complete().redirectUri(frontendRedirectUri).build();
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

            OidcProviderConfig oidcProviderConfig = openIdProviderClient.getOidcConfiguration();

            URI endSessionEndpoint = oidcProviderConfig.getEndSessionEndpoint();

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
            return new URIBuilder(frontendBaseURI).setPath(frontendBaseURI.getPath() + "auth/openid/login").build();
        } catch (URISyntaxException e) {
            log.error("Got URISyntaxException when constructing loginPostURI. This should not happen. frontendBaseURI: " + frontendBaseURI, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getType() {
        return "oidc";
    }

    protected String extractSubject(JwtClaims claims) throws CredentialsException {
        String subject = claims.getSubject();
        Map<String, Object> debugDetails = new HashMap<>();

        debugDetails.put("claims", claims.asMap());
        debugDetails.put("user_mapping.subject", jsonSubjectPath);
        
        if (jsonSubjectPath != null) {
            try {
                Object subjectObject = JsonPath.using(BasicJsonPathDefaultConfiguration.defaultConfiguration()).parse(claims.asMap()).read(jsonSubjectPath);
                
                if (subjectObject == null) {
                    throw new CredentialsException(new AuthczResult.DebugInfo(getType(), false,
                        "The JWT contains a null subject", debugDetails));
                }
                
                if (subjectObject instanceof Collection) {
                    Collection<?> subjectCollection = (Collection<?>) subjectObject;

                    if (subjectCollection.size() == 0) {
                        throw new CredentialsException(new AuthczResult.DebugInfo(getType(), false,
                                "The subject array is empty", debugDetails));
                    }

                    if (subjectCollection.size() > 1) {
                        throw new CredentialsException(new AuthczResult.DebugInfo(getType(), false,
                                "The subject array contains more than one element.", debugDetails));
                    }

                    subject = String.valueOf(subjectCollection.iterator().next());
                } else {
                    subject = String.valueOf(subjectObject);
                }

                
            } catch (PathNotFoundException e) {
                log.error("The provided JSON path {} could not be found ", jsonSubjectPath.getPath());
                throw new CredentialsException(new AuthczResult.DebugInfo(getType(), false,
                        "The configured JSON Path could not be found in the JWT", debugDetails));
            }
        }

        if (subject != null && subjectPattern != null) {
            Matcher matcher = subjectPattern.matcher(subject);

            if (!matcher.matches()) {
                log.warn("Subject " + subject + " does not match subject_pattern " + subjectPattern);
                throw new CredentialsException(new AuthczResult.DebugInfo(getType(), false,
                        "Subject " + subject + " does not match subject_pattern " + subjectPattern, debugDetails));
            }

            if (matcher.groupCount() == 1) {
                subject = matcher.group(1);
            } else if (matcher.groupCount() > 1) {
                StringBuilder subjectBuilder = new StringBuilder();

                for (int i = 1; i <= matcher.groupCount(); i++) {
                    if (matcher.group(i) != null) {
                        subjectBuilder.append(matcher.group(i));
                    }
                }

                if (subjectBuilder.length() != 0) {
                    subject = subjectBuilder.toString();
                } else {
                    throw new CredentialsException(new AuthczResult.DebugInfo(getType(), false,
                            "subject_pattern " + subjectPattern + " extracted empty subject. Original subject: " + subject, debugDetails));
                }
            }
        }

        return subject;
    }

    protected List<String> extractRoles(JwtClaims claims, Map<String, Object> debugInfo) throws CredentialsException {
        if (jsonRolesPath != null) {
            try {
                return Arrays.asList(Roles.split(JsonPath.using(jsonPathConfig).parse(claims.asMap()).read(jsonRolesPath)));
            } catch (PathNotFoundException e) {
                throw new CredentialsException(new AuthczResult.DebugInfo(getType(), false,
                        "The roles JSON path was not found in the Id token claims: " + jsonRolesPath.getPath(), debugInfo));
            }
        } else {
            return Collections.emptyList();
        }
    }

    private String createOpaqueToken() {
        return RandomStringUtils.random(22, 0, 0, true, true, null, SECURE_RANDOM);
    }

}
