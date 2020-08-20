package com.floragunn.searchguard.authtoken;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.cxf.rs.security.jose.jwa.ContentAlgorithm;
import org.apache.cxf.rs.security.jose.jwe.JweDecryptionProvider;
import org.apache.cxf.rs.security.jose.jwe.JweUtils;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;
import org.apache.cxf.rs.security.jose.jwk.KeyType;
import org.apache.cxf.rs.security.jose.jwk.PublicKeyUse;
import org.apache.cxf.rs.security.jose.jws.JwsSignatureVerifier;
import org.apache.cxf.rs.security.jose.jws.JwsUtils;
import org.apache.cxf.rs.security.jose.jwt.JoseJwtProducer;
import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtConstants;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexNotFoundException;

import com.floragunn.searchguard.authtoken.api.CreateAuthTokenRequest;
import com.floragunn.searchguard.compliance.ComplianceIndexingOperationListenerImpl;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProvider;
import com.floragunn.searchguard.sgconf.ConfigModel;
import com.floragunn.searchguard.sgconf.SgRoles;
import com.floragunn.searchguard.sgconf.history.ConfigHistoryService;
import com.floragunn.searchguard.sgconf.history.ConfigSnapshot;
import com.floragunn.searchguard.sgconf.history.UnknownConfigVersionException;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.ValidatingJsonParser;
import com.floragunn.searchsupport.xcontent.ObjectTreeXContent;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;

/**
 * TODO audience claim https://stackoverflow.com/questions/28418360/jwt-json-web-token-audience-aud-versus-client-id-whats-the-difference 
 *
 */
public class AuthTokenService implements SpecialPrivilegesEvaluationContextProvider {

    private static final Logger log = LogManager.getLogger(AuthTokenService.class);

    public static final Setting<String> INDEX_NAME = Setting.simpleString("searchguard.authtokens.index.name", ".searchguard_authtokens",
            Property.NodeScope);

    public static final String USER_TYPE = "auth_token";

    private final String indexName;
    private final PrivilegedConfigClient privilegedConfigClient;
    private final ConfigHistoryService configHistoryService;
    private JoseJwtProducer jwtProducer;
    private String jwtAudience;
    private JsonWebKey encryptionKey;
    private JsonWebKey signingKey;
    private JwsSignatureVerifier jwsSignatureVerifier;
    private JweDecryptionProvider jweDecryptionProvider;

    public AuthTokenService(PrivilegedConfigClient privilegedConfigClient, ConfigHistoryService configHistoryService, Settings settings) {
        this.indexName = INDEX_NAME.get(settings);
        this.privilegedConfigClient = privilegedConfigClient;
        this.configHistoryService = configHistoryService;
    }

    public AuthToken getById(String id) throws NoSuchAuthTokenException {
        try {
            GetResponse getResponse = privilegedConfigClient.get(new GetRequest(indexName, id)).actionGet();

            if (!getResponse.isExists()) {
                throw new NoSuchAuthTokenException(id);
            }

            return AuthToken.parse(id, ValidatingJsonParser.readTree(getResponse.getSourceAsString()));
        } catch (IndexNotFoundException e) {
            throw new NoSuchAuthTokenException(id, e);
        } catch (ConfigValidationException e) {
            throw new RuntimeException("Token " + id + " is not stored in a valid format", e);
        }
    }

    public AuthToken getByClaims(Map<String, Object> claims) throws NoSuchAuthTokenException, InvalidTokenException {
        String id = Objects.toString(claims.get(JwtConstants.CLAIM_JWT_ID), null);
        Set<String> audience = getClaimAsSet(claims, JwtConstants.CLAIM_AUDIENCE);

        if (!audience.contains(this.jwtAudience)) {
            throw new InvalidTokenException("Invalid JWT audience claim. Supplied: " + audience + "; Expected: " + this.jwtAudience);
        }

        if (id == null) {
            throw new InvalidTokenException("Supplied auth token does not have an id claim");
        }

        return getById(id);

    }

    public AuthToken create(User user, CreateAuthTokenRequest request) throws TokenCreationException {
        String id = getRandomId();

        ConfigSnapshot configSnapshot = configHistoryService.getCurrentConfigSnapshot(CType.ROLES, CType.ROLESMAPPING, CType.ACTIONGROUPS,
                CType.TENANTS);

        AuthTokenPrivilegeBase base = new AuthTokenPrivilegeBase(restrictRoles(request, user.getRoles()),
                restrictRoles(request, user.getSearchGuardRoles()), user.getCustomAttributesMap(), configSnapshot.getConfigVersions());

        if (base.getBackendRoles().size() == 0 && base.getSearchGuardRoles().size() == 0) {
            throw new TokenCreationException(
                    "Cannot create token. The resulting token would have no privileges as the specified roles do not intersect with the user's roles. Specified: "
                            + request.getRequestedPrivileges().getRoles() + " User: " + user.getRoles() + " + " + user.getSearchGuardRoles());
        }

        AuthToken authToken = new AuthToken(id, user.getName(), request.getTokenName(), request.getRequestedPrivileges(), base);

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            authToken.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);

            privilegedConfigClient.index(new IndexRequest(indexName).id(id).source(xContentBuilder)).actionGet();
        } catch (IOException e) {
            throw new TokenCreationException("Error while creating token", e);
        }

        return authToken;
    }

    public String createJwt(User user, CreateAuthTokenRequest request) throws TokenCreationException {

        if (jwtProducer == null) {
            throw new TokenCreationException("AuthTokenProvider is not configured");
        }

        AuthToken authToken = create(user, request);

        JwtClaims jwtClaims = new JwtClaims();
        JwtToken jwt = new JwtToken(jwtClaims);
        Instant now = Instant.now();

        jwtClaims.setNotBefore(now.getEpochSecond() - 30);

        if (request.getExpiresAfter() != null) {
            jwtClaims.setExpiryTime(now.plus(request.getExpiresAfter()).getEpochSecond());
        }

        jwtClaims.setSubject(user.getName());
        jwtClaims.setTokenId(authToken.getId());
        jwtClaims.setAudience(jwtAudience);
        jwtClaims.setProperty("requested", ObjectTreeXContent.toObjectTree(authToken.getRequestedPrivilges()));
        jwtClaims.setProperty("base", ObjectTreeXContent.toObjectTree(authToken.getBase()));

        String encodedJwt = this.jwtProducer.processJwt(jwt);

        return encodedJwt;
    }

    private String getRandomId() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());

        return BaseEncoding.base64Url().encode(byteBuffer.array());
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

    private Set<String> getClaimAsSet(Map<String, Object> claims, String claimName) {
        Object claim = claims.get(claimName);

        if (claim == null) {
            return Collections.emptySet();
        } else if (claim instanceof Collection) {
            return ((Collection<?>) claim).stream().map((e) -> String.valueOf(e)).collect(Collectors.toSet());
        } else {
            return Collections.singleton(String.valueOf(claim));
        }
    }

    private Set<String> restrictRoles(CreateAuthTokenRequest request, Set<String> roles) {
        if (request.getRequestedPrivileges().getRoles() != null) {
            return Sets.intersection(new HashSet<>(request.getRequestedPrivileges().getRoles()), roles);
        } else {
            return roles;
        }
    }

    @Override
    public SpecialPrivilegesEvaluationContext apply(User user, ThreadContext threadContext) {
        if (!(user.getSpecialAuthzConfig() instanceof AuthToken)) {
            return null;
        }

        AuthToken authToken = (AuthToken) user.getSpecialAuthzConfig();

        try {
            ConfigModel configModelSnapshot = configHistoryService.getConfigSnapshotAsModel(authToken.getBase().getConfigVersions());

            User userWithRoles = user.copy().backendRoles(authToken.getBase().getBackendRoles())
                    .searchGuardRoles(authToken.getBase().getSearchGuardRoles()).build();
            TransportAddress callerTransportAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
            Set<String> mappedBaseRoles = configModelSnapshot.mapSgRoles(userWithRoles, callerTransportAddress);
            SgRoles filteredBaseSgRoles = configModelSnapshot.getSgRoles().filter(mappedBaseRoles);

            RestrictedSgRoles restrictedSgRoles = new RestrictedSgRoles(filteredBaseSgRoles, authToken.getRequestedPrivilges(),
                    configModelSnapshot.getActionGroupResolver());

            return new SpecialPrivilegesEvaluationContextImpl(userWithRoles, mappedBaseRoles, restrictedSgRoles);

        } catch (UnknownConfigVersionException e) {
            throw new ElasticsearchSecurityException("Invalid auth token " + authToken, e);
        }
    }

    static class SpecialPrivilegesEvaluationContextImpl implements SpecialPrivilegesEvaluationContext {

        private final User user;
        private final Set<String> mappedRoles;
        private final SgRoles sgRoles;

        SpecialPrivilegesEvaluationContextImpl(User user, Set<String> mappedRoles, SgRoles sgRoles) {
            this.user = user;
            this.mappedRoles = mappedRoles;
            this.sgRoles = sgRoles;
        }

        @Override
        public User getUser() {
            return user;
        }

        @Override
        public Set<String> getMappedRoles() {
            return mappedRoles;
        }

        @Override
        public SgRoles getSgRoles() {
            return sgRoles;
        }

    }

}
