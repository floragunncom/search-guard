package com.floragunn.searchguard.internalauthtoken;

import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

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
import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWEAlgorithm;
import com.nimbusds.jose.JWEDecrypter;
import com.nimbusds.jose.JWEEncrypter;
import com.nimbusds.jose.JWEHeader;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.KeyLengthException;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.AESDecrypter;
import com.nimbusds.jose.crypto.AESEncrypter;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jose.proc.SimpleSecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.BadJWTException;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;

public class InternalAuthTokenProvider {

    public static final String TOKEN_HEADER = ConfigConstants.SG_CONFIG_PREFIX + "internal_auth_token";
    public static final String AUDIENCE_HEADER = ConfigConstants.SG_CONFIG_PREFIX + "internal_auth_token_audience";

    private static final JWSAlgorithm SIGNING_ALGORITHM = JWSAlgorithm.HS512;
    
    private static final Logger log = LogManager.getLogger(InternalAuthTokenProvider.class);

    private final AuthorizationService authorizationService;
    private final PrivilegesEvaluator privilegesEvaluator;
    private final Actions actions;

    private JWSSigner jwsSigner;
    private JWSVerifier jwsVerifier;
    private JWEDecrypter jweDecrypter;
    private JWEEncrypter jweEncrypter;
    private volatile SgDynamicConfiguration<Role> roles;

    public InternalAuthTokenProvider(AuthorizationService authorizationService, PrivilegesEvaluator privilegesEvaluator, Actions actions, ConfigurationRepository configurationRepository) {
        this.privilegesEvaluator = privilegesEvaluator;
        this.authorizationService = authorizationService;
        this.actions = actions;

        configurationRepository.subscribeOnChange(new ConfigurationChangeListener() {

            @Override
            public void onChange(ConfigMap configMap) {
                InternalAuthTokenProvider.this.roles = configMap.get(CType.ROLES);
            }
        });
    }

    public String getJwt(User user, String aud) throws IllegalStateException, JOSEException {
        return getJwt(user, aud, null);
    }

    public String getJwt(User user, String aud, TemporalAmount validity) throws IllegalStateException, JOSEException {

        if (jwsSigner == null) {
            throw new IllegalStateException("AuthTokenProvider is not configured");
        }
        
        JWTClaimsSet.Builder jwtClaims = new JWTClaimsSet.Builder();
        Instant now = Instant.now();

        jwtClaims.notBeforeTime(new java.util.Date(now.getEpochSecond() - 30));

        if (validity != null) {
            jwtClaims.expirationTime(new java.util.Date(now.plus(validity).getEpochSecond()));
        }

        jwtClaims.subject(user.getName());
        jwtClaims.audience(aud);
        jwtClaims.claim("sg_roles", getSgRolesForUser(user));

        SignedJWT signedJWT = new SignedJWT(new JWSHeader(SIGNING_ALGORITHM), jwtClaims.build());
        signedJWT.sign(jwsSigner);

        if (jweEncrypter != null) {
            JWEObject jweObject = new JWEObject(new JWEHeader(JWEAlgorithm.A256KW, EncryptionMethod.A256CBC_HS512), new Payload(signedJWT));
            jweObject.encrypt(jweEncrypter);
            return jweObject.serialize();
        } else {
            return signedJWT.serialize();
        }
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

    public AuthFromInternalAuthToken userAuthFromToken(User user, ThreadContext threadContext) {
        final String authToken = threadContext.getHeader(TOKEN_HEADER);
        final String authTokenAudience = HeaderHelper.getSafeFromHeader(threadContext, AUDIENCE_HEADER);

        if (authToken == null || authTokenAudience == null || authToken.equals("") || authTokenAudience.equals("")) {
            return null;
        }

        return userAuthFromToken(authToken, authTokenAudience);
    }

    public AuthFromInternalAuthToken userAuthFromToken(String authToken, String authTokenAudience) {
        try {
            JWTClaimsSet verifiedToken = getVerifiedJwtToken(authToken, authTokenAudience);

            Map<String, Object> rolesMap = verifiedToken.getJSONObjectClaim("sg_roles");

            if (rolesMap == null) {
                throw new JOSEException("JWT does not contain claim sg_roles");
            }
            
            SgDynamicConfiguration<Role> rolesConfig = SgDynamicConfiguration.fromMap(rolesMap, CType.ROLES, null).get();
            ImmutableSet<String> roleNames = ImmutableSet.of(rolesConfig.getCEntries().keySet());

            ActionAuthorization actionAuthorization = new RoleBasedActionAuthorization(rolesConfig, privilegesEvaluator.getActionGroups(), actions,
                    null, privilegesEvaluator.getAllConfiguredTenantNames(), null);
            String userName = verifiedToken.getSubject();
            User user = User.forUser(userName).authDomainInfo(AuthDomainInfo.STORED_AUTH).searchGuardRoles(roleNames).build();
            AuthFromInternalAuthToken userAuth = new AuthFromInternalAuthToken(user, roleNames, actionAuthorization, rolesConfig);

            return userAuth;
        } catch (Exception e) {
            log.warn("Error while verifying internal auth token: " + authToken + "\n" + authTokenAudience, e);

            return null;
        }
    }

    private Object getSgRolesForUser(User user) {
        ImmutableSet<String> userRoles = this.authorizationService.getMappedRoles(user, (TransportAddress) null);
        ImmutableMap<String, Role> roles = ImmutableMap.of(this.roles.getCEntries()).intersection(userRoles);

        return Document.toDeepBasicObject(roles);
    }

    private JWTClaimsSet getVerifiedJwtToken(String encodedJwt, String authTokenAudience) throws JOSEException, ParseException, BadJWTException {
        if (this.jweDecrypter != null) {
            JWEObject jweObject = JWEObject.parse(encodedJwt);
            jweObject.decrypt(jweDecrypter);
            encodedJwt = jweObject.getPayload().toSignedJWT().serialize();
        }

        SignedJWT signedJwt = SignedJWT.parse(encodedJwt);

        if (this.jwsVerifier != null) {
            boolean signatureValid = signedJwt.verify(jwsVerifier);

            if (!signatureValid) {
                throw new JOSEException("Invalid JWT signature");
            }
        }
        
        validateClaims(signedJwt, authTokenAudience);

        return signedJwt.getJWTClaimsSet();

    }

    private void validateClaims(SignedJWT jwt, String authTokenAudience) throws JOSEException, ParseException, BadJWTException {
        JWTClaimsSet claims = jwt.getJWTClaimsSet();

        if (claims == null) {
            throw new JOSEException("The JWT does not have any claims");
        }

        DefaultJWTClaimsVerifier<SimpleSecurityContext> claimsVerifier = new DefaultJWTClaimsVerifier<>(
                authTokenAudience != null ? Collections.singleton(authTokenAudience) : null,
                null,
                Collections.emptySet(),
                null
            );
            claimsVerifier.verify(claims, null);
        

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

    public void setSigningKey(String keyString) throws JOSEException {
        if (keyString != null && keyString.length() > 0) {
            byte [] keyStringBytes = Base64.getDecoder().decode(keyString);
            this.jwsSigner = new MACSigner(keyStringBytes);
            this.jwsVerifier = new MACVerifier(keyStringBytes);
        } else {
            this.jwsSigner = null;
            this.jwsVerifier = null;
        }
    }

    public void setEncryptionKey(String keyString) throws KeyLengthException {
        if (keyString != null && keyString.length() > 0) {
            byte [] keyStringBytes = Base64.getDecoder().decode(keyString);
            this.jweEncrypter = new AESEncrypter(keyStringBytes);
            this.jweDecrypter = new AESDecrypter(keyStringBytes);
        } else {
            this.jweEncrypter = null;
            this.jweDecrypter = null;
        }
    }
}
