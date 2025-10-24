package com.floragunn.searchguard.internalauthtoken;

import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import org.apache.cxf.rs.security.jose.jwa.ContentAlgorithm;
import org.apache.cxf.rs.security.jose.jwe.JweDecryptionOutput;
import org.apache.cxf.rs.security.jose.jwe.JweDecryptionProvider;
import org.apache.cxf.rs.security.jose.jwe.JweUtils;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;
import org.apache.cxf.rs.security.jose.jwk.KeyType;
import org.apache.cxf.rs.security.jose.jwk.PublicKeyUse;
import org.apache.cxf.rs.security.jose.jws.JwsJwtCompactConsumer;
import org.apache.cxf.rs.security.jose.jws.JwsSignatureVerifier;
import org.apache.cxf.rs.security.jose.jws.JwsUtils;
import org.apache.cxf.rs.security.jose.jwt.JoseJwtProducer;
import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtException;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.cxf.rs.security.jose.jwt.JwtUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.ActionAuthorization;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.authz.RoleBasedActionAuthorization;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.user.AuthDomainInfo;
import com.floragunn.searchguard.user.User;

public class InternalAuthTokenProvider {

    public static final String TOKEN_HEADER = ConfigConstants.SG_CONFIG_PREFIX + "internal_auth_token";
    public static final String AUDIENCE_HEADER = ConfigConstants.SG_CONFIG_PREFIX + "internal_auth_token_audience";

    private static final Logger log = LogManager.getLogger(InternalAuthTokenProvider.class);

    private final AuthorizationService authorizationService;
    private final PrivilegesEvaluator privilegesEvaluator;
    private final Actions actions;
    private final ConfigurationRepository configurationRepository;

    private JsonWebKey encryptionKey;
    private JsonWebKey signingKey;
    private JoseJwtProducer jwtProducer;
    private JwsSignatureVerifier jwsSignatureVerifier;
    private JweDecryptionProvider jweDecryptionProvider;
    private volatile SgDynamicConfiguration<Role> roles;

    public InternalAuthTokenProvider(AuthorizationService authorizationService, PrivilegesEvaluator privilegesEvaluator, Actions actions, ConfigurationRepository configurationRepository) {
        this.privilegesEvaluator = privilegesEvaluator;
        this.authorizationService = authorizationService;
        this.actions = actions;
        this.configurationRepository = configurationRepository;

        configurationRepository.subscribeOnChange(new ConfigurationChangeListener() {

            @Override
            public void onChange(ConfigMap configMap) {
                InternalAuthTokenProvider.this.roles = configMap.get(CType.ROLES);
            }
        });
    }

    public String getJwt(User user, String aud) throws IllegalStateException {
        return getJwt(user, aud, null);
    }

    public String getJwt(User user, String aud, TemporalAmount validity) throws IllegalStateException {

        if (jwtProducer == null) {
            throw new IllegalStateException("AuthTokenProvider is not configured");
        }

        JwtClaims jwtClaims = new JwtClaims();
        JwtToken jwt = new JwtToken(jwtClaims);
        Instant now = Instant.now();

        jwtClaims.setNotBefore(now.getEpochSecond() - 30);

        if (validity != null) {
            jwtClaims.setExpiryTime(now.plus(validity).getEpochSecond());
        }

        jwtClaims.setSubject(user.getName());
        jwtClaims.setAudience(aud);
        jwtClaims.setProperty("sg_roles", getSgRolesForUser(user));

        String encodedJwt = this.jwtProducer.processJwt(jwt);

        return encodedJwt;
    }

    public void userAuthFromToken(User user, ThreadContext threadContext, Consumer<SpecialPrivilegesEvaluationContext> onResult,
            Consumer<Exception> onFailure) {
        try {
            onResult.accept(userAuthFromToken(user, threadContext));
        } catch (Exception e) {
            log.error("Error in userAuthFromToken(" + user + ")", e);
            onFailure.accept(e);
        }
    }

    public AuthFromInternalAuthToken userAuthFromToken(User user, ThreadContext threadContext) throws PrivilegesEvaluationException {
        final String authToken = threadContext.getHeader(TOKEN_HEADER);
        final String authTokenAudience = HeaderHelper.getSafeFromHeader(threadContext, AUDIENCE_HEADER);

        if (authToken == null || authTokenAudience == null || authToken.equals("") || authTokenAudience.equals("")) {
            return null;
        }

        return userAuthFromToken(authToken, authTokenAudience);
    }

    public AuthFromInternalAuthToken userAuthFromToken(String authToken, String authTokenAudience) throws PrivilegesEvaluationException {
        try {
            JwtToken verifiedToken = getVerifiedJwtToken(authToken, authTokenAudience);

            Map<String, Object> rolesMap = verifiedToken.getClaims().getMapProperty("sg_roles");

            if (rolesMap == null) {
                throw new JwtException("JWT does not contain claim sg_roles");
            }
            
            SgDynamicConfiguration<Role> rolesConfig = SgDynamicConfiguration.fromMap(rolesMap, CType.ROLES, configurationRepository.getParserContext()).get();
            ImmutableSet<String> roleNames = ImmutableSet.of(rolesConfig.getCEntries().keySet());

            ActionAuthorization actionAuthorization = new RoleBasedActionAuthorization(rolesConfig, privilegesEvaluator.getActionGroups(), actions,
                    null, privilegesEvaluator.getAllConfiguredTenantNames(), null);
            String userName = verifiedToken.getClaims().getSubject();
            User user = User.forUser(userName).authDomainInfo(AuthDomainInfo.STORED_AUTH).searchGuardRoles(roleNames).build();
            AuthFromInternalAuthToken userAuth = new AuthFromInternalAuthToken(user, roleNames, actionAuthorization, rolesConfig);

            return userAuth;
        } catch (Exception e) {
            log.warn("Error while verifying internal auth token: " + authToken + "\n" + authTokenAudience, e);
            throw new PrivilegesEvaluationException("Error while verifying internal auth token", e);
        }
    }

    void initJwtProducer() {
        try {
            this.jwtProducer = new JoseJwtProducer();

            if (signingKey != null) {
                this.jwtProducer.setSignatureProvider(JwsUtils.getSignatureProvider(signingKey));
                this.jwsSignatureVerifier = JwsUtils.getSignatureVerifier(signingKey);
            } else {
                this.jwsSignatureVerifier = null;
            }

            if (this.encryptionKey != null) {
                this.jwtProducer.setEncryptionProvider(JweUtils.createJweEncryptionProvider(encryptionKey, ContentAlgorithm.A256CBC_HS512));
                this.jwtProducer.setJweRequired(true);
                this.jweDecryptionProvider = JweUtils.createJweDecryptionProvider(encryptionKey, ContentAlgorithm.A256CBC_HS512);
            } else {
                this.jweDecryptionProvider = null;
            }

        } catch (Exception e) {
            this.jwtProducer = null;
            log.error("Error while initializing JWT producer in AuthTokenProvider", e);
        }
    }

    private Object getSgRolesForUser(User user) {
        ImmutableSet<String> userRoles = this.authorizationService.getMappedRoles(user, (TransportAddress) null);
        ImmutableMap<String, Role> roles = ImmutableMap.of(this.roles.getCEntries()).intersection(userRoles);

        return Document.toDeepBasicObject(roles);
    }

    private JwtToken getVerifiedJwtToken(String encodedJwt, String authTokenAudience) throws JwtException {
        if (this.jweDecryptionProvider != null) {
            JweDecryptionOutput decOutput = this.jweDecryptionProvider.decrypt(encodedJwt);
            encodedJwt = decOutput.getContentText();
        }

        JwsJwtCompactConsumer jwtConsumer = new JwsJwtCompactConsumer(encodedJwt);
        JwtToken jwt = jwtConsumer.getJwtToken();

        if (this.jwsSignatureVerifier != null) {
            boolean signatureValid = jwtConsumer.verifySignatureWith(jwsSignatureVerifier);

            if (!signatureValid) {
                throw new JwtException("Invalid JWT signature");
            }
        }

        validateClaims(jwt, authTokenAudience);

        return jwt;

    }

    private void validateClaims(JwtToken jwt, String authTokenAudience) throws JwtException {
        JwtClaims claims = jwt.getClaims();

        if (claims == null) {
            throw new JwtException("The JWT does not have any claims");
        }

        JwtUtils.validateJwtExpiry(claims, 0, false);
        JwtUtils.validateJwtNotBefore(claims, 0, false);
        validateAudience(claims, authTokenAudience);

    }

    private void validateAudience(JwtClaims claims, String authTokenAudience) throws JwtException {

        if (authTokenAudience != null) {
            for (String audience : claims.getAudiences()) {
                if (authTokenAudience.equals(audience)) {
                    return;
                }
            }
        }
        throw new JwtException("Internal auth token does not allow audience: " + authTokenAudience + "\nAllowed audiences: " + claims.getAudiences());
    }

    public static class AuthFromInternalAuthToken implements SpecialPrivilegesEvaluationContext {

        private final User user;
        private final ImmutableSet<String> mappedRoles;
        private final ActionAuthorization actionAuthorization;
        private final SgDynamicConfiguration<Role> rolesConfig;

        AuthFromInternalAuthToken(User user, ImmutableSet<String> mappedRoles, ActionAuthorization actionAuthorization, SgDynamicConfiguration<Role> rolesConfig) {
            this.user = user;
            this.mappedRoles = mappedRoles;
            this.actionAuthorization = actionAuthorization;
            this.rolesConfig = rolesConfig;
        }

        public User getUser() {
            return user;
        }

        @Override
        public String toString() {
            return "AuthFromInternalAuthToken [user=" + user + "]";
        }

        @Override
        public ImmutableSet<String> getMappedRoles() {
            return mappedRoles;
        }

        @Override
        public TransportAddress getCaller() {
            return null;
        }

        @Override
        public boolean requiresPrivilegeEvaluationForLocalRequests() {
            return true;
        }

        @Override
        public ActionAuthorization getActionAuthorization() {
            return actionAuthorization;
        }

        @Override
        public SgDynamicConfiguration<Role> getRolesConfig() {
            return rolesConfig;
        }

    }

    public JsonWebKey getSigningKey() {
        return signingKey;
    }

    public void setSigningKey(JsonWebKey signingKey) {
        if (Objects.equals(this.signingKey, signingKey)) {
            return;
        }

        log.info("Updating signing key for " + this);

        this.signingKey = signingKey;
        initJwtProducer();
    }

    public void setSigningKey(String keyString) {
        if (keyString != null && keyString.length() > 0) {

            JsonWebKey jwk = new JsonWebKey();

            jwk.setKeyType(KeyType.OCTET);
            jwk.setAlgorithm("HS512");
            jwk.setPublicKeyUse(PublicKeyUse.SIGN);
            jwk.setProperty("k", keyString);

            setSigningKey(jwk);
        } else {
            setSigningKey((JsonWebKey) null);
        }
    }

    public JsonWebKey getEncryptionKey() {
        return encryptionKey;
    }

    public void setEncryptionKey(JsonWebKey encryptionKey) {
        if (Objects.equals(this.encryptionKey, encryptionKey)) {
            return;
        }

        log.info("Updating encryption key for " + this);

        this.encryptionKey = encryptionKey;
        initJwtProducer();
    }

    public void setEncryptionKey(String keyString) {
        if (keyString != null && keyString.length() > 0) {

            JsonWebKey jwk = new JsonWebKey();

            jwk.setKeyType(KeyType.OCTET);
            jwk.setAlgorithm("A256KW");
            jwk.setPublicKeyUse(PublicKeyUse.ENCRYPT);
            jwk.setProperty("k", keyString);

            setEncryptionKey(jwk);
        } else {
            setEncryptionKey((JsonWebKey) null);
        }
    }
}
