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

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.net.URI;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.cert.CertificateFactory;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Map;
import java.util.Objects;

import org.apache.cxf.rs.security.jose.jwa.AlgorithmUtils;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKeys;
import org.apache.cxf.rs.security.jose.jwk.JwkUtils;
import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

import com.floragunn.codova.config.net.ProxyConfig;
import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidatingFunction;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.TypedComponent.Factory;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.base.AuthcResult;
import com.floragunn.searchguard.authc.rest.HttpAuthenticationFrontend;
import com.floragunn.searchguard.authc.session.ActivatedFrontendConfig;
import com.floragunn.searchguard.authc.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.authc.session.GetActivatedFrontendConfigAction;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.enterprise.auth.oidc.BadCredentialsException;
import com.floragunn.searchguard.enterprise.auth.oidc.JwksProviderClient;
import com.floragunn.searchguard.enterprise.auth.oidc.JwtVerifier;
import com.floragunn.searchguard.enterprise.auth.oidc.KeyProvider;
import com.floragunn.searchguard.enterprise.auth.oidc.OpenIdProviderClient;
import com.floragunn.searchguard.enterprise.auth.oidc.SelfRefreshingKeySet;
import com.floragunn.searchguard.user.Attributes;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.google.common.base.Strings;

public class JwtAuthenticator implements HttpAuthenticationFrontend, ApiAuthenticationFrontend {
    private final static Logger log = LogManager.getLogger(JwtAuthenticator.class);

    private final KeyProvider staticKeySet;
    private final SelfRefreshingKeySet openIdKeySet;
    private final SelfRefreshingKeySet jwksKeySet;
    private final JwtVerifier jwtVerifier;
    private final String jwtHeaderName;
    private final String jwtUrlParameter;
    private final String requiredAudience;
    private final String requiredIssuer;
    private final boolean challenge;
    private final ComponentState componentState = new ComponentState(0, "authentication_frontend", "jwt", JwtAuthenticator.class).initialized()
            .requiresEnterpriseLicense();

    public JwtAuthenticator(DocNode docNode, ConfigurationRepository.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        this.jwtHeaderName = vNode.get("header").withDefault("Authorization").asString();
        this.jwtUrlParameter = vNode.get("url_parameter").asString();
        this.requiredAudience = vNode.get("required_audience").asString();
        this.requiredIssuer = vNode.get("required_issuer").asString();
        this.challenge = vNode.get("challenge").withDefault(true).asBoolean();
        int maxClockSkewSeconds = vNode.get("max_clock_skew_seconds").withDefault(10).asInt();

        JsonWebKeys jwks = vNode.get("signing.jwks").expected("A JWKS document").by((n) -> JwkUtils.readJwkSet(n.toJsonString()));
        JsonWebKey rsaJwk = vNode.get("signing.rsa").by(JwtAuthenticator::parseRsa);
        JsonWebKey ecJwk = vNode.get("signing.ec").by(JwtAuthenticator::parseEc);

        JsonWebKeys joinedJwks;

        if (rsaJwk != null || ecJwk != null) {
            ImmutableList<JsonWebKey> jwkList = ImmutableList.ofNonNull(rsaJwk, ecJwk);

            if (jwks != null) {
                jwkList = jwkList.with(jwks.getKeys());
            }

            joinedJwks = new JsonWebKeys(jwkList);
        } else {
            joinedJwks = jwks;
        }

        if (joinedJwks != null) {
            this.staticKeySet = new KeyProvider() {

                @Override
                public JsonWebKey getKeyAfterRefresh(String kid) throws AuthenticatorUnavailableException, BadCredentialsException {
                    return getKey(kid);
                }

                @Override
                public JsonWebKey getKey(String kid) throws AuthenticatorUnavailableException, BadCredentialsException {
                    if (Strings.isNullOrEmpty(kid)) {
                        return joinedJwks.getKeys().size() != 0 ? joinedJwks.getKeys().get(0) : null;
                    } else {
                        return joinedJwks.getKey(kid);
                    }
                }
            };
        } else {
            this.staticKeySet = null;
        }

        URI openidConnectUrl = vNode.get("signing.jwks_from_openid_configuration.url").asURI();

        if (openidConnectUrl != null) {
            boolean cacheJwksEndpoint = vNode.get("signing.keys_from_openid_configuration.cache_jwks_endpoint").withDefault(false).asBoolean();
            TLSConfig tlsConfig = vNode.get("signing.keys_from_openid_configuration.tls").by((Parser<TLSConfig, Parser.Context>) TLSConfig::parse);
            ProxyConfig proxyConfig = vNode.get("signing.keys_from_openid_configuration.proxy")
                    .by((ValidatingFunction<DocNode, ProxyConfig>) ProxyConfig::parse);

            OpenIdProviderClient openIdProviderClient = new OpenIdProviderClient(openidConnectUrl, tlsConfig, proxyConfig, cacheJwksEndpoint);
            //openIdProviderClient.setRequestTimeoutMs(idpRequestTimeoutMs);
            this.openIdKeySet = new SelfRefreshingKeySet(() -> openIdProviderClient.getJsonWebKeys());
        } else {
            this.openIdKeySet = null;
        }

        URI jwksUrl = vNode.get("signing.jwks_endpoint.url").asURI();

        if (jwksUrl != null) {
            TLSConfig tlsConfig = vNode.get("signing.jwks_endpoint.tls").by((Parser<TLSConfig, Parser.Context>) TLSConfig::parse);
            ProxyConfig proxyConfig = vNode.get("signing.jwks_endpoint.proxy").by((ValidatingFunction<DocNode, ProxyConfig>) ProxyConfig::parse);

            JwksProviderClient jwksProviderClient = new JwksProviderClient(tlsConfig, proxyConfig);
            this.jwksKeySet = new SelfRefreshingKeySet(() -> jwksProviderClient.getJsonWebKeys(jwksUrl));
        } else {
            this.jwksKeySet = null;
        }

        vNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        this.jwtVerifier = new JwtVerifier(KeyProvider.combined(staticKeySet, openIdKeySet, jwksKeySet), requiredAudience, requiredIssuer, maxClockSkewSeconds);
    }

    @Override
    public String getType() {
        return "jwt";
    }

    @Override
    public AuthCredentials extractCredentials(RequestMetaData<?> request)
            throws CredentialsException, AuthenticatorUnavailableException {
        String jwtString = request.getAuthorizationByScheme(jwtHeaderName, "bearer");
        String jwtTokenFromParam = jwtUrlParameter != null ? request.getParam(jwtUrlParameter) : null;

        if (jwtString == null && jwtTokenFromParam != null) {
            if (jwtTokenFromParam.toLowerCase().startsWith("bearer ")) {
                jwtString = jwtTokenFromParam.substring("bearer ".length()).trim();                
            } else {
                jwtString = jwtTokenFromParam;
            }
        }

        if (jwtString == null) {
            return null;
        }

        return tokenToCredentials(jwtString);
    }

    private AuthCredentials tokenToCredentials(String jwtString) throws AuthenticatorUnavailableException, CredentialsException {
        Objects.requireNonNull(jwtString, "Jwt string is required");
        JwtToken jwt;

        try {
            jwt = jwtVerifier.getVerifiedJwtToken(jwtString);
        } catch (AuthenticatorUnavailableException e) {
            log.info(e);
            throw e;
        } catch (BadCredentialsException e) {
            log.info("Extracting JWT token from " + jwtString + " failed", e);
            throw new CredentialsException(new AuthcResult.DebugInfo(getType(), false, e.getMessage()), e);
        }

        JwtClaims claims = jwt.getClaims();

        if (log.isTraceEnabled()) {
            log.trace("Claims from JWT: " + claims.asMap());
        }

        return AuthCredentials.forUser(claims.getSubject())//
            .nativeCredentials(jwtString)//
            .attribute(Attributes.AUTH_TYPE, "jwt")//
            .userMappingAttribute("jwt", Jose.toBasicObject(claims))//
            .complete()//
            .build();
    }

    @Override
    public String getChallenge(AuthCredentials credentials) {
        if (challenge) {
            return "Bearer realm=\"Search Guard\"";
        } else {
            return null;
        }
    }

    private static JsonWebKey parseRsa(DocNode docNode, Parser.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);
        RSAPublicKey rsaPublicKey = null;

        String certificate = vNode.get("certificate").asString();

        if (certificate != null) {
            try {
                PublicKey publicKey = CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(certificate.getBytes()))
                        .getPublicKey();

                if (publicKey instanceof RSAPublicKey) {
                    rsaPublicKey = (RSAPublicKey) publicKey;
                } else {
                    validationErrors.add(
                            new InvalidAttributeValue("certificate", publicKey.getClass(), "An RSA certificate").message("Not an RSA certificate"));
                }
            } catch (Exception e) {
                validationErrors.add(new ValidationError("certificate", e.getMessage()).cause(e));
            }
        }

        String publicKeyString = vNode.get("public_key").asString();

        if (publicKeyString != null) {
            try {
                PemObject pemObject = new PemReader(new StringReader(publicKeyString)).readPemObject();
                PublicKey publicKey = KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(pemObject.getContent()));
                if (publicKey instanceof RSAPublicKey) {
                    rsaPublicKey = (RSAPublicKey) publicKey;
                } else {
                    validationErrors
                            .add(new InvalidAttributeValue("public_key", publicKey.getClass(), "An RSA public key").message("Not an RSA public key"));
                }
            } catch (Exception e) {
                validationErrors.add(new ValidationError("public_key", e.getMessage()).cause(e));
            }
        }

        String algo = vNode.get("algorithm").validatedBy(AlgorithmUtils::isRsa).asString();
        String kid = vNode.get("kid").asString();

        validationErrors.throwExceptionForPresentErrors();

        if (rsaPublicKey == null) {
            throw new ConfigValidationException(new MissingAttribute("certificate"));
        }

        try {
            return JwkUtils.fromRSAPublicKey(rsaPublicKey, algo, kid);
        } catch (Exception e) {
            throw new ConfigValidationException(new ValidationError(null, e.getMessage()).cause(e));
        }
    }

    private static JsonWebKey parseEc(DocNode docNode, Parser.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);
        ECPublicKey ecPublicKey = null;

        String certificate = vNode.get("certificate").asString();

        if (certificate != null) {
            try {
                PublicKey publicKey = CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(certificate.getBytes()))
                        .getPublicKey();

                if (publicKey instanceof ECPublicKey) {
                    ecPublicKey = (ECPublicKey) publicKey;
                } else {
                    validationErrors.add(
                            new InvalidAttributeValue("certificate", publicKey.getClass(), "An EC certificate").message("Not an EC certificate"));
                }
            } catch (Exception e) {
                validationErrors.add(new ValidationError("certificate", e.getMessage()).cause(e));
            }
        }

        String publicKeyString = vNode.get("public_key").asString();

        if (publicKeyString != null) {
            try {
                PemObject pemObject = new PemReader(new StringReader(publicKeyString)).readPemObject();
                PublicKey publicKey = KeyFactory.getInstance("EC").generatePublic(new X509EncodedKeySpec(pemObject.getContent()));
                if (publicKey instanceof ECPublicKey) {
                    ecPublicKey = (ECPublicKey) publicKey;
                } else {
                    validationErrors
                            .add(new InvalidAttributeValue("public_key", publicKey.getClass(), "An EC public key").message("Not an EC public key"));
                }
            } catch (Exception e) {
                validationErrors.add(new ValidationError("public_key", e.getMessage()).cause(e));
            }
        }

        String curve = vNode.get("curve").validatedBy(AlgorithmUtils::isRsa).asString();
        String kid = vNode.get("kid").asString();

        validationErrors.throwExceptionForPresentErrors();

        if (ecPublicKey == null) {
            throw new ConfigValidationException(new MissingAttribute("certificate"));
        }

        try {
            return JwkUtils.fromECPublicKey(ecPublicKey, curve, kid);
        } catch (Exception e) {
            throw new ConfigValidationException(new ValidationError(null, e.getMessage()).cause(e));
        }
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    public static TypedComponent.Info<HttpAuthenticationFrontend> INFO = new TypedComponent.Info<HttpAuthenticationFrontend>() {

        @Override
        public Class<HttpAuthenticationFrontend> getType() {
            return HttpAuthenticationFrontend.class;
        }

        @Override
        public String getName() {
            return "jwt";
        }

        @Override
        public Factory<HttpAuthenticationFrontend> getFactory() {
            return JwtAuthenticator::new;
        }
    };

    @Override
    public AuthCredentials extractCredentials(Map<String, Object> request)
        throws CredentialsException, ConfigValidationException, AuthenticatorUnavailableException {
        Object jwtToken = request.get("jwt") instanceof String ? request.get("jwt") : null;
        return tokenToCredentials((String) jwtToken);
    }

    @Override
    public ActivatedFrontendConfig.AuthMethod activateFrontendConfig(
        ActivatedFrontendConfig.AuthMethod frontendConfig, GetActivatedFrontendConfigAction.Request request)
        throws AuthenticatorUnavailableException {
        return ApiAuthenticationFrontend.super.activateFrontendConfig(frontendConfig, request);
    }

    @Override
    public String getLogoutUrl(User user) throws AuthenticatorUnavailableException {
        return ApiAuthenticationFrontend.super.getLogoutUrl(user);
    }
}
