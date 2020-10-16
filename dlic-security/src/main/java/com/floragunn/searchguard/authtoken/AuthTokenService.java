package com.floragunn.searchguard.authtoken;

import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.cxf.rs.security.jose.jwa.ContentAlgorithm;
import org.apache.cxf.rs.security.jose.jwe.JweDecryptionOutput;
import org.apache.cxf.rs.security.jose.jwe.JweDecryptionProvider;
import org.apache.cxf.rs.security.jose.jwe.JweUtils;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;
import org.apache.cxf.rs.security.jose.jws.JwsJwtCompactConsumer;
import org.apache.cxf.rs.security.jose.jws.JwsSignatureVerifier;
import org.apache.cxf.rs.security.jose.jws.JwsUtils;
import org.apache.cxf.rs.security.jose.jwt.JoseJwtProducer;
import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtConstants;
import org.apache.cxf.rs.security.jose.jwt.JwtException;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.cxf.rs.security.jose.jwt.JwtUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.authtoken.api.CreateAuthTokenRequest;
import com.floragunn.searchguard.authtoken.api.CreateAuthTokenResponse;
import com.floragunn.searchguard.authtoken.update.PushAuthTokenUpdateAction;
import com.floragunn.searchguard.authtoken.update.PushAuthTokenUpdateRequest;
import com.floragunn.searchguard.authtoken.update.PushAuthTokenUpdateResponse;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService.ConfigIndex;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProvider;
import com.floragunn.searchguard.sgconf.ConfigModel;
import com.floragunn.searchguard.sgconf.SgRoles;
import com.floragunn.searchguard.sgconf.history.ConfigHistoryService;
import com.floragunn.searchguard.sgconf.history.ConfigSnapshot;
import com.floragunn.searchguard.sgconf.history.ConfigVersionSet;
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
 * TODO clean up expired tokens
 */
public class AuthTokenService implements SpecialPrivilegesEvaluationContextProvider {

    private static final Logger log = LogManager.getLogger(AuthTokenService.class);

    public static final Setting<String> INDEX_NAME = Setting.simpleString("searchguard.authtokens.index.name", ".searchguard_authtokens",
            Property.NodeScope);

    public static final String USER_TYPE = "sg_auth_token";

    private final String indexName;
    private final PrivilegedConfigClient privilegedConfigClient;
    private final ConfigHistoryService configHistoryService;
    private final ThreadPool threadPool;
    private volatile Map<String, AuthToken> idToAuthTokenMap = new ConcurrentHashMap<>();
    private JoseJwtProducer jwtProducer;
    private String jwtAudience;
    private JsonWebKey encryptionKey;
    private JsonWebKey signingKey;
    private JwsSignatureVerifier jwsSignatureVerifier;
    private JweDecryptionProvider jweDecryptionProvider;
    private AuthTokenServiceConfig config;
    private Set<AuthToken> unpushedTokens = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private boolean sendTokenUpdates = true;
    private boolean initialized = false;

    public AuthTokenService(PrivilegedConfigClient privilegedConfigClient, ConfigHistoryService configHistoryService, Settings settings,
            ThreadPool threadPool, ProtectedConfigIndexService protectedConfigIndexService, AuthTokenServiceConfig config) {
        this.indexName = INDEX_NAME.get(settings);
        this.privilegedConfigClient = privilegedConfigClient;
        this.configHistoryService = configHistoryService;
        this.threadPool = threadPool;

        this.setConfig(config);

        protectedConfigIndexService
                .createIndex(new ConfigIndex(indexName).dependsOnIndices(configHistoryService.getIndexName()).onIndexReady(this::init));
    }

    public AuthToken getById(String id) throws NoSuchAuthTokenException {
        AuthToken result = idToAuthTokenMap.get(id);

        if (result != null) {
            return result;
        } else {
            throw new NoSuchAuthTokenException(id);
        }
    }

    public AuthToken getByIdFromIndex(String id) throws NoSuchAuthTokenException {
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

        // TODO new attributes

        AuthTokenPrivilegeBase base = new AuthTokenPrivilegeBase(restrictRoles(request, user.getRoles()),
                restrictRoles(request, user.getSearchGuardRoles()), user.getCustomAttributesMap(), configSnapshot.getConfigVersions());

        if (log.isDebugEnabled()) {
            log.debug("base for auth token " + request + ": " + base);
        }

        base.setConfigSnapshot(configSnapshot);

        if (base.getBackendRoles().size() == 0 && base.getSearchGuardRoles().size() == 0) {
            throw new TokenCreationException(
                    "Cannot create token. The resulting token would have no privileges as the specified roles do not intersect with the user's roles. Specified: "
                            + request.getRequestedPrivileges().getRoles() + " User: " + user.getRoles() + " + " + user.getSearchGuardRoles());
        }

        OffsetDateTime now = OffsetDateTime.now();

        OffsetDateTime expiresAt = getExpiryTime(now, request);

        RequestedPrivileges requestedPrivilegesWithDefaultExclusions = request.getRequestedPrivileges()
                .excludeClusterPermissions(config.getExcludeClusterPermissions()).excludeIndexPermissions(config.getExcludeIndexPermissions());

        AuthToken authToken = new AuthToken(id, user.getName(), request.getTokenName(), requestedPrivilegesWithDefaultExclusions, base,
                now.toEpochSecond(), expiresAt != null ? expiresAt.toEpochSecond() : Long.MAX_VALUE, null);

        this.idToAuthTokenMap.put(id, authToken);

        try {
            updateAuthToken(authToken);
        } catch (Exception e) {
            throw new TokenCreationException("Error while creating token", e);
        }

        return authToken;
    }

    public CreateAuthTokenResponse createJwt(User user, CreateAuthTokenRequest request) throws TokenCreationException {

        if (jwtProducer == null) {
            throw new TokenCreationException("AuthTokenProvider is not configured");
        }

        AuthToken authToken = create(user, request);

        JwtClaims jwtClaims = new JwtClaims();
        JwtToken jwt = new JwtToken(jwtClaims);

        jwtClaims.setNotBefore(authToken.getCreationTime());

        if (authToken.getExpiryTime() != Long.MAX_VALUE) {
            jwtClaims.setExpiryTime(authToken.getExpiryTime());
        }

        jwtClaims.setSubject(user.getName());
        jwtClaims.setTokenId(authToken.getId());
        jwtClaims.setAudience(config.getJwtAud());
        jwtClaims.setProperty("requested", ObjectTreeXContent.toObjectTree(authToken.getRequestedPrivilges()));
        jwtClaims.setProperty("base", ObjectTreeXContent.toObjectTree(authToken.getBase(), AuthTokenPrivilegeBase.COMPACT));

        String encodedJwt = this.jwtProducer.processJwt(jwt);

        return new CreateAuthTokenResponse(authToken, encodedJwt);
    }

    public JwtToken getVerifiedJwtToken(String encodedJwt) throws JwtException {
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

        validateClaims(jwt);

        return jwt;
    }

    public String revoke(User user, String id) throws NoSuchAuthTokenException, TokenUpdateException {
        AuthToken authToken = getById(id);

        if (authToken.getRevokedAt() != null) {
            return "Auth token was already revoked";
        }

        String updateStatus = updateAuthToken(authToken.getRevokedInstance());

        if (updateStatus != null) {
            return updateStatus;
        }

        return "Auth token has been revoked";
    }

    public void setConfig(AuthTokenServiceConfig config) {
        if (config == null) {
            // Expected when SG is not initialized yet
            return;
        }

        this.config = config;
        this.jwtAudience = config.getJwtAud();

        setKeys(config.getJwtSigningKey(), config.getJwtEncryptionKey());
    }

    private void init(ProtectedConfigIndexService.FailureListener failureListener) {
        reloadAuthTokensFromIndex(failureListener);
    }

    private void reloadAuthTokensFromIndex(ProtectedConfigIndexService.FailureListener failureListener) {
        if (log.isDebugEnabled()) {
            log.debug("Reloading auth tokens");
        }

        long now = System.currentTimeMillis();

        this.privilegedConfigClient.search(new SearchRequest(indexName)
                .source(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("expires_at").gte(now)).size(1000)).scroll(new TimeValue(10000)),
                new ActionListener<SearchResponse>() {

                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        Map<String, AuthToken> idToAuthTokenMap = new ConcurrentHashMap<>();
                        SearchHits searchHits = searchResponse.getHits();

                        while (searchHits.getTotalHits().value != 0) {
                            for (SearchHit hit : searchHits.getHits()) {
                                try {
                                    String id = hit.getId();
                                    AuthToken authToken = AuthToken.parse(id, ValidatingJsonParser.readTree(hit.getSourceAsString()));

                                    idToAuthTokenMap.put(id, authToken);
                                } catch (ConfigValidationException e) {
                                    log.error("Invalid auth token in index at " + hit + ":\n" + e.getValidationErrors(), e);
                                }
                            }

                            searchResponse = privilegedConfigClient.prepareSearchScroll(searchResponse.getScrollId()).setScroll(new TimeValue(10000))
                                    .execute().actionGet();
                            searchHits = searchResponse.getHits();
                        }

                        if (log.isDebugEnabled()) {
                            log.debug("Loaded " + idToAuthTokenMap.size() + " auth tokens");
                        }

                        initConfigSnapshots(idToAuthTokenMap);

                        AuthTokenService.this.idToAuthTokenMap = idToAuthTokenMap;

                        initComplete();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (failureListener != null) {
                            failureListener.onFailure(e);
                        } else {
                            log.error("Error while loading auth tokens", e);
                        }
                    }
                });
    }

    private void initConfigSnapshots(Map<String, AuthToken> idToAuthTokenMap) {
        Set<ConfigVersionSet> configVersionSets = new HashSet<>(idToAuthTokenMap.size());

        for (AuthToken authToken : idToAuthTokenMap.values()) {
            configVersionSets.add(authToken.getBase().getConfigVersions());
        }

        Map<ConfigVersionSet, ConfigSnapshot> configSnapshotMap = configHistoryService.getConfigSnapshots(configVersionSets);

        if (log.isDebugEnabled()) {
            log.debug("Loaded " + configSnapshotMap.size() + " config snapshots");
        }

        for (AuthToken authToken : idToAuthTokenMap.values()) {
            ConfigSnapshot configSnapshot = configSnapshotMap.get(authToken.getBase().getConfigVersions());

            if (configSnapshot != null) {
                authToken.getBase().setConfigSnapshot(configSnapshot);
            } else {
                log.warn("Could not find config snapshot for " + authToken);
            }
        }

    }

    private void validateClaims(JwtToken jwt) throws JwtException {
        JwtClaims claims = jwt.getClaims();

        if (claims == null) {
            throw new JwtException("The JWT does not have any claims");
        }

        JwtUtils.validateJwtExpiry(claims, 0, false);
        JwtUtils.validateJwtNotBefore(claims, 0, false);
        validateAudience(claims);

    }

    private void validateAudience(JwtClaims claims) throws JwtException {

        if (jwtAudience != null) {
            for (String audience : claims.getAudiences()) {
                if (jwtAudience.equals(audience)) {
                    return;
                }
            }
        }
        throw new JwtException("Invalid audience: " + claims.getAudiences() + "\nExpected audience: " + jwtAudience);
    }

    private synchronized void initComplete() {
        this.initialized = true;
        notifyAll();
    }

    public synchronized void waitForInitComplete(long timeoutMillis) {
        if (this.initialized) {
            return;
        }

        try {
            wait(timeoutMillis);
        } catch (InterruptedException e) {
        }

        if (!this.initialized) {
            throw new RuntimeException(this + " did not initialize after " + timeoutMillis);
        }
    }

    public String pushAuthTokenUpdate(PushAuthTokenUpdateRequest request) {
        if (log.isDebugEnabled()) {
            log.debug("got auth token update: " + request);
        }

        // TODO sanity checks

        int configCacheHits = 0;
        int existing = 0;
        int existingUpdated = 0;
        int processed = 0;

        List<AuthToken> pendingAuthTokens = new ArrayList<>();

        for (AuthToken authToken : request.getUpdatedTokens()) {
            AuthToken existingAuthToken = this.idToAuthTokenMap.get(authToken.getId());

            if (existingAuthToken != null) {
                existing++;

                if (authToken.equals(existingAuthToken)) {
                    continue;
                } else {
                    existingUpdated++;
                }
            }

            processed++;

            ConfigVersionSet configVersionSet = authToken.getBase().getConfigVersions();
            ConfigSnapshot configSnapshot = configHistoryService.peekConfigSnapshotFromCache(configVersionSet);

            if (!configSnapshot.hasMissingConfigVersions()) {
                authToken.getBase().setConfigSnapshot(configSnapshot);
                configCacheHits++;
                this.idToAuthTokenMap.put(authToken.getId(), authToken);
            } else {
                pendingAuthTokens.add(authToken);
            }
        }

        if (pendingAuthTokens.size() > 0) {
            this.threadPool.generic().submit(() -> {
                for (AuthToken authToken : pendingAuthTokens) {
                    try {
                        ConfigVersionSet configVersionSet = authToken.getBase().getConfigVersions();

                        ConfigSnapshot configSnapshot = configHistoryService.peekConfigSnapshot(configVersionSet);

                        if (log.isDebugEnabled()) {
                            log.debug("Loaded configSnapshot for " + authToken + ": " + configSnapshot);
                        }

                        if (configSnapshot.hasMissingConfigVersions()) {
                            log.error("Could not get complete config snapshot for " + authToken + ": " + configSnapshot);
                            continue;
                        }

                        authToken.getBase().setConfigSnapshot(configSnapshot);
                        this.idToAuthTokenMap.put(authToken.getId(), authToken);
                    } catch (Exception e) {
                        log.error("Error while loading config snapshot for " + authToken, e);
                    }
                }
            });

        }

        String status = "config cache hits: " + configCacheHits + "/" + processed + "; existing updated: " + existingUpdated + "/" + existing;

        if (log.isDebugEnabled()) {
            log.debug(status);
        }

        return status;
    }

    private String updateAuthToken(AuthToken authToken) throws TokenUpdateException {
        AuthToken oldToken = this.idToAuthTokenMap.put(authToken.getId(), authToken);

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            authToken.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);

            IndexResponse indexResponse = privilegedConfigClient
                    .index(new IndexRequest(indexName).id(authToken.getId()).source(xContentBuilder).setRefreshPolicy(RefreshPolicy.IMMEDIATE))
                    .actionGet();

            if (log.isDebugEnabled()) {
                log.debug("Token stored: " + indexResponse);
            }

        } catch (Exception e) {
            this.idToAuthTokenMap.put(oldToken.getId(), oldToken);
            log.warn("Error while storing token " + authToken, e);
            throw new TokenUpdateException(e);
        }

        if (!sendTokenUpdates) {
            return "Update disabled";
        }

        try {
            PushAuthTokenUpdateResponse pushAuthTokenUpdateResponse = privilegedConfigClient
                    .execute(PushAuthTokenUpdateAction.INSTANCE, new PushAuthTokenUpdateRequest(authToken)).actionGet();

            if (log.isDebugEnabled()) {
                log.debug("Token update pushed: " + pushAuthTokenUpdateResponse);
            }

            if (pushAuthTokenUpdateResponse.hasFailures()) {
                unpushedTokens.add(authToken);
                return "Update partially failed: " + pushAuthTokenUpdateResponse.failures();
            }

        } catch (Exception e) {
            log.warn("Token update push failed: " + authToken, e);
            unpushedTokens.add(authToken);
            return "Update partially failed: " + e;
        }

        return null;
    }

    private OffsetDateTime getExpiryTime(OffsetDateTime now, CreateAuthTokenRequest request) {
        OffsetDateTime expiresAfter = null;
        OffsetDateTime expiresAfterMax = null;

        if (request.getExpiresAfter() != null) {
            expiresAfter = now.plus(request.getExpiresAfter());
        }

        if (config.getMaxValidity() != null) {
            expiresAfterMax = now.plus(config.getMaxValidity());
        }

        if (expiresAfter == null) {
            expiresAfter = expiresAfterMax;
        } else if (expiresAfter != null && expiresAfterMax != null && expiresAfterMax.isBefore(expiresAfter)) {
            expiresAfter = expiresAfterMax;
        }

        return expiresAfter;
    }

    private String getRandomId() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());

        return BaseEncoding.base64Url().encode(byteBuffer.array()).replace("=", "");
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

    public void setKeys(JsonWebKey signingKey, JsonWebKey encryptionKey) {
        if (Objects.equals(this.signingKey, signingKey) && Objects.equals(this.encryptionKey, encryptionKey)) {
            return;
        }

        log.info("Updating keys for " + this);

        this.signingKey = signingKey;
        this.encryptionKey = encryptionKey;
        initJwtProducer();
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
        if (user == null || !(USER_TYPE.equals(user.getType()))) {
            return null;
        }

        String authTokenId = (String) user.getSpecialAuthzConfig();

        if (log.isTraceEnabled()) {
            log.trace("AuthTokenService.apply(" + user.getName() + ") on " + authTokenId);
        }

        try {
            AuthToken authToken = getById(authTokenId);

            if (authToken.getBase().getConfigSnapshot().hasMissingConfigVersions()) {
                throw new RuntimeException("Stored config snapshot is not complete: " + authToken);
            }

            ConfigModel configModelSnapshot = configHistoryService.getConfigModelForSnapshot(authToken.getBase().getConfigSnapshot());

            User userWithRoles = user.copy().backendRoles(authToken.getBase().getBackendRoles())
                    .searchGuardRoles(authToken.getBase().getSearchGuardRoles()).build();
            TransportAddress callerTransportAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
            Set<String> mappedBaseRoles = configModelSnapshot.mapSgRoles(userWithRoles, callerTransportAddress);
            SgRoles filteredBaseSgRoles = configModelSnapshot.getSgRoles().filter(mappedBaseRoles);

            if (log.isTraceEnabled()) {
                log.trace("ConfigSnapshot: " + authToken.getBase().getConfigSnapshot());
                log.trace("mappedBaseRoles: " + mappedBaseRoles + "; userWithRoles: " + userWithRoles);
                log.trace("configModelSnapshot.roles" + configModelSnapshot.getSgRoles());
                log.trace("filteredBaseSgRoles: " + filteredBaseSgRoles);
            }

            RestrictedSgRoles restrictedSgRoles = new RestrictedSgRoles(filteredBaseSgRoles, authToken.getRequestedPrivilges(),
                    configModelSnapshot.getActionGroupResolver());

            return new SpecialPrivilegesEvaluationContextImpl(userWithRoles, mappedBaseRoles, restrictedSgRoles);
        } catch (NoSuchAuthTokenException e) {
            throw new ElasticsearchSecurityException("Cannot authenticate user due to invalid auth token " + authTokenId, e);
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

    public String getIndexName() {
        return indexName;
    }

    boolean isSendTokenUpdates() {
        return sendTokenUpdates;
    }

    void setSendTokenUpdates(boolean sendTokenUpdates) {
        this.sendTokenUpdates = sendTokenUpdates;
    }

}
