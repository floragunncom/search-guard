package com.floragunn.searchguard.internalauthtoken;

import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.floragunn.searchguard.configuration.ConfigurationRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.ActionAuthorization;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.RoleBasedActionAuthorization;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.config.ActionGroup;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.jwt.JwtVerifier;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.user.AuthDomainInfo;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.PrivilegedCode;
import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWEAlgorithm;
import com.nimbusds.jose.JWEEncrypter;
import com.nimbusds.jose.JWEHeader;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.KeyLengthException;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.AESEncrypter;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.BadJWTException;

public class InternalAuthTokenProvider {

    public static final String TOKEN_HEADER = ConfigConstants.SG_CONFIG_PREFIX + "internal_auth_token";
    public static final String AUDIENCE_HEADER = ConfigConstants.SG_CONFIG_PREFIX + "internal_auth_token_audience";

    private static final JWSAlgorithm SIGNING_ALGORITHM = JWSAlgorithm.HS512;
    
    private static final Logger log = LogManager.getLogger(InternalAuthTokenProvider.class);

    private final Actions actions;
    private final Function<User, ImmutableSet<String>> roleMapper;
    private final Supplier<ActionGroup.FlattenedIndex> actionGroupsSupplier;
    private final Supplier<Set<String>> tenantNameSupplier;
    private final Supplier<SgDynamicConfiguration<Role>> rolesSupplier;
    private final ConfigurationRepository.Context rolesParserContext;

    private JWK encryptionKey;
    private JWK signingKey;
    private JWSSigner jwsSigner;
    private JWEEncrypter jweEncrypter;

    private JwtVerifier jwtVerifier;



    public InternalAuthTokenProvider(Function<User, ImmutableSet<String>> roleMapper, Supplier<ActionGroup.FlattenedIndex> actionGroupsSupplier, Supplier<Set<String>> tenantNameSupplier, Actions actions, Supplier<SgDynamicConfiguration<Role>> rolesSupplier, ConfigurationRepository.Context rolesParserContext) {
        this.actionGroupsSupplier = actionGroupsSupplier;
        this.tenantNameSupplier = tenantNameSupplier;
        this.roleMapper = roleMapper;
        this.actions = actions;
        this.rolesSupplier = rolesSupplier;
        this.rolesParserContext = rolesParserContext;
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
        jwtClaims.claim("sg_i", "n");

        SignedJWT signedJWT = PrivilegedCode.execute(() -> new SignedJWT(new JWSHeader(SIGNING_ALGORITHM), jwtClaims.build()));
        signedJWT.sign(jwsSigner);

        if (jweEncrypter != null) {
            JWEObject jweObject = PrivilegedCode.execute(() -> new JWEObject(new JWEHeader.Builder(JWEAlgorithm.A256KW, EncryptionMethod.A256CBC_HS512)
                    .customParam(JwtVerifier.PRODUCER_CLAIM, JwtVerifier.PRODUCER_CLAIM_NIMBUS).build(), new Payload(signedJWT)));
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
            JWTClaimsSet verifiedToken = getVerifiedJwtToken(authToken, authTokenAudience);

            Map<String, Object> rolesMap = verifiedToken.getJSONObjectClaim("sg_roles");

            if (rolesMap == null) {
                throw new JOSEException("JWT does not contain claim sg_roles");
            }
            
            log.trace("userAuthFromToken({}, {}); verifiedToken: {} {}", authToken, authTokenAudience, verifiedToken, rolesMap);

            SgDynamicConfiguration<Role> rolesConfig = SgDynamicConfiguration.fromMap(rolesMap, CType.ROLES, rolesParserContext).get();
            ImmutableSet<String> roleNames = ImmutableSet.of(rolesConfig.getCEntries().keySet());

            ActionAuthorization actionAuthorization = new RoleBasedActionAuthorization(rolesConfig, this.actionGroupsSupplier.get(), actions,
                    null, this.tenantNameSupplier.get(), null);
            String userName = verifiedToken.getSubject();
            User user = User.forUser(userName).authDomainInfo(AuthDomainInfo.STORED_AUTH).searchGuardRoles(roleNames).build();
            AuthFromInternalAuthToken userAuth = new AuthFromInternalAuthToken(user, roleNames, actionAuthorization, rolesConfig);

            return userAuth;
        } catch (JOSEException | ConfigValidationException | ParseException | BadJWTException e) {
            log.debug("Error while verifying internal auth token: {}", authToken, e);
            throw new PrivilegesEvaluationException("Error while verifying internal auth token", e);
        }
    }

    private Object getSgRolesForUser(User user) {
        ImmutableSet<String> userRoles = this.roleMapper.apply(user);
        ImmutableMap<String, Role> roles = ImmutableMap.of(this.rolesSupplier.get().getCEntries()).intersection(userRoles);

        return Document.toDeepBasicObject(roles);
    }

    private JWTClaimsSet getVerifiedJwtToken(String encodedJwt, String authTokenAudience) throws JOSEException, ParseException, BadJWTException {
        if (this.jwtVerifier == null) {
            throw new RuntimeException("Cannot verify token because signing key is not configured");
        }
        
        JWT jwt = this.jwtVerifier.getVerfiedJwt(encodedJwt, authTokenAudience);
        
        if (jwt != null) {
            return jwt.getJWTClaimsSet();
        } else {
            throw new JOSEException("Invalid JWT");
        }
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

    public synchronized void setSigningKey(String keyString) throws JOSEException {
        if (keyString != null && keyString.length() > 0) {
            byte [] keyStringBytes = Base64.getDecoder().decode(keyString);        
            this.jwsSigner = new MACSigner(keyStringBytes);
            this.signingKey = new OctetSequenceKey.Builder(keyStringBytes)
                    .keyUse(KeyUse.SIGNATURE)  
                    .algorithm(JWSAlgorithm.HS512)     
                    .build();      
            this.updateJwtVerifier();
        } else {
            this.signingKey = null;
            this.jwsSigner = null;
            this.updateJwtVerifier();
        }
    }

    public synchronized void setEncryptionKey(String keyString) throws KeyLengthException {
        if (keyString != null && keyString.length() > 0) {
            byte [] keyStringBytes = Base64.getDecoder().decode(keyString);                      
            this.jweEncrypter = new AESEncrypter(keyStringBytes);
            this.encryptionKey = new OctetSequenceKey.Builder(keyStringBytes)
                    .keyUse(KeyUse.ENCRYPTION)    
                    .algorithm(JWEAlgorithm.A256KW)
                    .build();
            this.updateJwtVerifier();            
        } else {
            this.encryptionKey = null;
            this.jweEncrypter = null;
            this.updateJwtVerifier();
        }
    }
    
    private synchronized void updateJwtVerifier() {
        if (this.signingKey != null) {
            this.jwtVerifier = new JwtVerifier(signingKey, encryptionKey, "");
        } else if (this.jwtVerifier == null) {
            log.warn("Disabling JWT verifier because no signing key is present");
            this.jwtVerifier = null;
        }
    }
}
