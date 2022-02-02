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

package com.floragunn.searchguard.enterprise.auth.jwt;

import java.net.URI;

import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKeys;
import org.apache.cxf.rs.security.jose.jwk.JwkUtils;
import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;

import com.floragunn.codova.config.net.ProxyConfig;
import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidatingFunction;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.TypedComponent.Factory;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.base.AuthczResult;
import com.floragunn.searchguard.authc.rest.authenticators.HTTPAuthenticator;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.enterprise.auth.oidc.BadCredentialsException;
import com.floragunn.searchguard.enterprise.auth.oidc.JwksProviderClient;
import com.floragunn.searchguard.enterprise.auth.oidc.JwtVerifier;
import com.floragunn.searchguard.enterprise.auth.oidc.KeyProvider;
import com.floragunn.searchguard.enterprise.auth.oidc.OpenIdProviderClient;
import com.floragunn.searchguard.enterprise.auth.oidc.SelfRefreshingKeySet;
import com.floragunn.searchguard.user.Attributes;
import com.floragunn.searchguard.user.AuthCredentials;
import com.google.common.base.Strings;

public class JwtAuthenticator implements HTTPAuthenticator {
    private final static Logger log = LogManager.getLogger(JwtAuthenticator.class);

    private final KeyProvider staticKeySet;
    private final SelfRefreshingKeySet openIdKeySet;
    private final SelfRefreshingKeySet jwksKeySet;
    private final JwtVerifier jwtVerifier;
    private final String jwtHeaderName;
    private final String jwtUrlParameter;

    public JwtAuthenticator(DocNode docNode, ConfigurationRepository.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        this.jwtHeaderName = vNode.get("header").withDefault("Authorization").asString();
        this.jwtUrlParameter = vNode.get("url_parameter").asString();

        JsonWebKeys jwks = vNode.get("jwks").expected("A JWKS document").by((n) -> JwkUtils.readJwkSet(n.toJsonString()));

        if (jwks != null) {
            this.staticKeySet = new KeyProvider() {

                @Override
                public JsonWebKey getKeyAfterRefresh(String kid) throws AuthenticatorUnavailableException, BadCredentialsException {
                    return getKey(kid);
                }

                @Override
                public JsonWebKey getKey(String kid) throws AuthenticatorUnavailableException, BadCredentialsException {
                    if (Strings.isNullOrEmpty(kid)) {
                        return jwks.getKeys().size() != 0 ? jwks.getKeys().get(0) : null;
                    } else {
                        return jwks.getKey(kid);
                    }
                }
            };
        } else {
            this.staticKeySet = null;
        }

        URI openidConnectUrl = vNode.get("keys_from_openid_configuration.url").asURI();

        if (openidConnectUrl != null) {
            boolean cacheJwksEndpoint = vNode.get("keys_from_openid_configuration.cache_jwks_endpoint").withDefault(false).asBoolean();
            TLSConfig tlsConfig = vNode.get("keys_from_openid_configuration.tls").by(TLSConfig::parse);
            ProxyConfig proxyConfig = vNode.get("keys_from_openid_configuration.proxy")
                    .by((ValidatingFunction<DocNode, ProxyConfig>) ProxyConfig::parse);

            OpenIdProviderClient openIdProviderClient = new OpenIdProviderClient(openidConnectUrl, tlsConfig, proxyConfig, cacheJwksEndpoint);
            //openIdProviderClient.setRequestTimeoutMs(idpRequestTimeoutMs);
            this.openIdKeySet = new SelfRefreshingKeySet(() -> openIdProviderClient.getJsonWebKeys());
        } else {
            this.openIdKeySet = null;
        }

        URI jwksUrl = vNode.get("jwks_endpoint.url").asURI();

        if (jwksUrl != null) {
            TLSConfig tlsConfig = vNode.get("jwks_endpoint.tls").by(TLSConfig::parse);
            ProxyConfig proxyConfig = vNode.get("jwks_endpoint.proxy").by((ValidatingFunction<DocNode, ProxyConfig>) ProxyConfig::parse);

            JwksProviderClient jwksProviderClient = new JwksProviderClient(tlsConfig, proxyConfig);
            this.jwksKeySet = new SelfRefreshingKeySet(() -> jwksProviderClient.getJsonWebKeys(jwksUrl));
        } else {
            this.jwksKeySet = null;
        }

        validationErrors.throwExceptionForPresentErrors();

        this.jwtVerifier = new JwtVerifier(KeyProvider.combined(staticKeySet, openIdKeySet, jwksKeySet));
    }

    @Override
    public String getType() {
        return "jwt";
    }

    @Override
    public AuthCredentials extractCredentials(RestRequest request, ThreadContext context)
            throws CredentialsException, AuthenticatorUnavailableException {
        String jwtString = getJwtTokenString(request);
        JwtToken jwt;

        try {
            jwt = jwtVerifier.getVerifiedJwtToken(jwtString);
        } catch (AuthenticatorUnavailableException e) {
            log.info(e);
            throw e;
        } catch (BadCredentialsException e) {
            log.info("Extracting JWT token from " + jwtString + " failed", e);
            throw new CredentialsException(new AuthczResult.DebugInfo(getType(), false, e.getMessage()), e);
        }

        JwtClaims claims = jwt.getClaims();

        if (log.isTraceEnabled()) {
            log.trace("Claims from JWT: " + claims.asMap());
        }

        return AuthCredentials.forUser(claims.getSubject()).attribute(Attributes.AUTH_TYPE, "jwt")
                .userMappingAttribute("jwt", Jose.toBasicObject(claims)).complete().build();
    }

    @Override
    public boolean reRequestAuthentication(RestChannel channel, AuthCredentials credentials) {
        return false;
    }

    private static final String BEARER = "bearer ";

    protected String getJwtTokenString(RestRequest request) {
        String jwtTokenFromHeader = request.header(jwtHeaderName);
        String jwtTokenFromParam = jwtUrlParameter != null ? request.param(jwtUrlParameter) : null;

        if (jwtTokenFromHeader != null && jwtTokenFromHeader.toLowerCase().startsWith(BEARER)) {
            return jwtTokenFromHeader.substring(BEARER.length()).trim();
        }

        if (jwtTokenFromParam != null && jwtTokenFromParam.toLowerCase().startsWith(BEARER)) {
            return jwtTokenFromParam.substring(BEARER.length()).trim();
        }

        return null;
    }

    public static TypedComponent.Info<HTTPAuthenticator> INFO = new TypedComponent.Info<HTTPAuthenticator>() {

        @Override
        public Class<HTTPAuthenticator> getType() {
            return HTTPAuthenticator.class;
        }

        @Override
        public String getName() {
            return "jwt";
        }

        @Override
        public Factory<HTTPAuthenticator> getFactory() {
            return JwtAuthenticator::new;
        }
    };
}
