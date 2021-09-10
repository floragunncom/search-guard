/*
 * Copyright 2021 floragunn GmbH
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.floragunn.searchguard.session;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
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
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.threadpool.ThreadPool;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.auth.AuthczResult;
import com.floragunn.searchguard.auth.BackendRegistry;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService.ConfigIndex;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.session.PushSessionTokenUpdateAction.Request.UpdateType;
import com.floragunn.searchguard.session.api.StartSessionResponse;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cleanup.IndexCleanupAgent;
import com.floragunn.searchsupport.config.validation.ValidatingJsonParser;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.io.BaseEncoding;

public class SessionService {

    private static final Logger log = LogManager.getLogger(SessionService.class);

    private static final String JWT_AUDIENCE = "sg_session";

    public static final Setting<String> INDEX_NAME = Setting.simpleString("searchguard.sessions.index.name", ".searchguard_sessions",
            Property.NodeScope);
    public static final Setting<TimeValue> CLEANUP_INTERVAL = Setting.timeSetting("searchguard.sessions.cleanup_interval",
            TimeValue.timeValueHours(1), TimeValue.timeValueSeconds(1), Property.NodeScope, Property.Filtered);

    public static final String USER_TYPE = "session";

    private final PrivilegedConfigClient privilegedConfigClient;
    private final Cache<String, SessionToken> idToAuthTokenMap = CacheBuilder.newBuilder().expireAfterWrite(60, TimeUnit.MINUTES).build();
    private final BackendRegistry backendRegistry;
    private final ThreadPool threadPool;
    private final String indexName;
    private final ComponentState componentState;
    private final ComponentState configComponentState;
    private SessionActivityTracker activityTracker;
    private IndexCleanupAgent indexCleanupAgent;
    private long maxTokensPerUser = 100;

    private SessionServiceConfig config;
    private JoseJwtProducer jwtProducer;
    private JsonWebKey encryptionKey;
    private JsonWebKey signingKey;
    private JwsSignatureVerifier jwsSignatureVerifier;
    private JweDecryptionProvider jweDecryptionProvider;
    private boolean initialized = false;

    public SessionService(PrivilegedConfigClient privilegedConfigClient, Settings settings, ThreadPool threadPool, ClusterService clusterService,
            ProtectedConfigIndexService protectedConfigIndexService, SessionServiceConfig config, BackendRegistry backendRegistry,
            ComponentState componentState) {
        this.indexName = INDEX_NAME.get(settings);
        this.privilegedConfigClient = privilegedConfigClient;
        this.backendRegistry = backendRegistry;
        this.threadPool = threadPool;
        this.componentState = componentState;
        this.configComponentState = componentState.getOrCreatePart("config", "sg_config");

        activityTracker = new SessionActivityTracker(config.getInactivityTimeout() != null ? config.getInactivityTimeout() : Duration.ofHours(1),
                this, indexName, privilegedConfigClient, threadPool);

        this.setConfig(config);

        ConfigIndex configIndex = new ConfigIndex(indexName).mapping(SessionToken.INDEX_MAPPING).onIndexReady(this::init);

        componentState.addPart(protectedConfigIndexService.createIndex(configIndex));

        if (activityTracker != null) {
            this.indexCleanupAgent = new IndexCleanupAgent(indexName,
                    () -> QueryBuilders.boolQuery().should(QueryBuilders.rangeQuery(SessionToken.DYNAMIC_EXPIRES_AT).lt(System.currentTimeMillis()))
                            .should(QueryBuilders.rangeQuery(SessionToken.EXPIRES_AT).lt(System.currentTimeMillis())),
                    CLEANUP_INTERVAL.get(settings), privilegedConfigClient, clusterService, threadPool);
        } else {
            this.indexCleanupAgent = new IndexCleanupAgent(indexName, SessionToken.EXPIRES_AT, CLEANUP_INTERVAL.get(settings), privilegedConfigClient,
                    clusterService, threadPool);
        }
    }

    public void createSession(Map<String, Object> request, RestRequest restRequest, Consumer<StartSessionResponse> onResult,
            Consumer<AuthczResult> onAuthFailure, Consumer<Exception> onFailure) {

        if (this.config == null) {
            onFailure.accept(new SessionCreationException("SessionService is not configured", RestStatus.INTERNAL_SERVER_ERROR));
            return;
        }
        
        ThreadContext threadContext = threadPool.getThreadContext();

        backendRegistry.authenticateApi(request, restRequest, config.getRequiredLoginPrivileges(), threadContext, (authczResult) -> {
            if (authczResult.getStatus() == AuthczResult.Status.PASS) {
                threadPool.generic().submit(() -> {
                    try {
                        StartSessionResponse response = createLightweightJwt(authczResult.getUser());
                        onResult.accept(response);
                    } catch (SessionCreationException e) {
                        log.info("Creating token failed", e);
                        onAuthFailure.accept(AuthczResult.stop(e.getRestStatus(), e.getMessage()));
                    } catch (Exception e) {
                        onFailure.accept(e);
                    }
                });
            } else {
                onAuthFailure.accept(authczResult);
            }

        }, onFailure);
    }

    private StartSessionResponse createLightweightJwt(User user) throws SessionCreationException {

        if (jwtProducer == null) {
            throw new SessionCreationException("SessionService is not configured", RestStatus.INTERNAL_SERVER_ERROR);
        }

        SessionToken authToken = create(user);

        JwtClaims jwtClaims = new JwtClaims();
        JwtToken jwt = new JwtToken(jwtClaims);

        jwtClaims.setNotBefore(authToken.getCreationTime().getEpochSecond());

        if (authToken.getExpiryTime() != null) {
            jwtClaims.setExpiryTime(authToken.getExpiryTime().getEpochSecond());
        }

        jwtClaims.setSubject(user.getName());
        jwtClaims.setTokenId(authToken.getId());
        jwtClaims.setAudience(JWT_AUDIENCE);

        String encodedJwt;

        try {
            encodedJwt = this.jwtProducer.processJwt(jwt);
        } catch (Exception e) {
            log.error("Error while creating JWT. Possibly the key configuration is not valid.", e);
            throw new SessionCreationException("Error while creating JWT. Possibly the key configuration is not valid.",
                    RestStatus.INTERNAL_SERVER_ERROR, e);
        }
        return new StartSessionResponse(encodedJwt);
    }

    public SessionToken getById(String id) throws NoSuchSessionException {
        SessionToken result = idToAuthTokenMap.getIfPresent(id);

        if (result != null) {
            return result;
        } else {
            return getByIdFromIndex(id);
        }
    }

    public void getById(String id, Consumer<SessionToken> onResult, Consumer<NoSuchSessionException> onNoSuchAuthToken,
            Consumer<Exception> onFailure) {

        SessionToken result = idToAuthTokenMap.getIfPresent(id);

        if (result != null) {
            onResult.accept(result);
        } else {
            getByIdFromIndex(id, onResult, onNoSuchAuthToken, onFailure);
        }
    }

    public SessionToken getByIdFromIndex(String id) throws NoSuchSessionException {

        CompletableFuture<SessionToken> completableFuture = new CompletableFuture<>();

        getByIdFromIndex(id, completableFuture::complete, completableFuture::completeExceptionally, completableFuture::completeExceptionally);

        try {
            return completableFuture.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NoSuchSessionException) {
                throw (NoSuchSessionException) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }
    }

    public void getByIdFromIndex(String id, Consumer<SessionToken> onResult, Consumer<NoSuchSessionException> onNoSuchSession,
            Consumer<Exception> onFailure) {

        privilegedConfigClient.get(new GetRequest(indexName, id), new ActionListener<GetResponse>() {

            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists()) {

                    try {
                        SessionToken authToken = SessionToken.parse(id, ValidatingJsonParser.readTree(getResponse.getSourceAsString()));

                        idToAuthTokenMap.put(id, authToken);

                        onResult.accept(authToken);
                    } catch (ConfigValidationException e) {
                        onFailure.accept(new RuntimeException("Token " + id + " is not stored in a valid format", e));
                    } catch (Exception e) {
                        log.error(e);
                        onFailure.accept(e);
                    }

                } else {
                    onNoSuchSession.accept(new NoSuchSessionException(id));
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof IndexNotFoundException) {
                    onNoSuchSession.accept(new NoSuchSessionException(id));
                } else {
                    onFailure.accept(e);
                }
            }
        });

    }

    public String delete(User user, SessionToken authToken) throws NoSuchSessionException, SessionUpdateException {
        if (log.isTraceEnabled()) {
            log.trace("revoke(" + user + ", " + authToken.getId() + ")");
        }

        authToken = getById(authToken.getId());

        if (authToken.getRevokedAt() != null) {
            log.info("Auth token " + authToken + " was already revoked");
            return "Auth token was already revoked";
        }

        String updateStatus = updateSessionToken(authToken.getRevokedInstance(), UpdateType.REVOKED);

        if (updateStatus != null) {
            return updateStatus;
        }

        return "Sesion has been deleted";
    }

    public SessionToken getByClaims(Map<String, Object> claims) throws NoSuchSessionException, InvalidTokenException {
        CompletableFuture<SessionToken> completableFuture = new CompletableFuture<>();

        getByClaims(claims, completableFuture::complete, completableFuture::completeExceptionally, completableFuture::completeExceptionally);

        try {
            return completableFuture.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NoSuchSessionException) {
                throw (NoSuchSessionException) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }
    }

    public void getByClaims(Map<String, Object> claims, Consumer<SessionToken> onResult, Consumer<NoSuchSessionException> onNoSuchAuthToken,
            Consumer<Exception> onFailure) throws InvalidTokenException {
        String id = Objects.toString(claims.get(JwtConstants.CLAIM_JWT_ID), null);
        Set<String> audience = getClaimAsSet(claims, JwtConstants.CLAIM_AUDIENCE);

        if (!audience.contains(JWT_AUDIENCE)) {
            throw new InvalidTokenException("Invalid JWT audience claim. Supplied: " + audience + "; Expected: " + JWT_AUDIENCE);
        }

        if (id == null) {
            throw new InvalidTokenException("Supplied auth token does not have an id claim");
        }

        getById(id, onResult, onNoSuchAuthToken, onFailure);
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

    public SessionToken create(User user) throws SessionCreationException {
        if (config == null || !config.isEnabled()) {
            throw new SessionCreationException("Auth token handling is not enabled", RestStatus.INTERNAL_SERVER_ERROR);
        }

        if (log.isDebugEnabled()) {
            log.debug("create(user: " + user + ")");
        }

        Set<String> baseBackendRoles = user.getRoles();
        Set<String> baseSearchGuardRoles = user.getSearchGuardRoles();
        Map<String, Object> baseAttributes = user.getStructuredAttributes();

        String id = getRandomId();

        SessionPrivileges base = new SessionPrivileges(baseBackendRoles, baseSearchGuardRoles, baseAttributes);

        if (maxTokensPerUser == 0) {
            throw new SessionCreationException("Cannot create token. max_tokens_per_user is set to 0", RestStatus.FORBIDDEN);
        } else if (maxTokensPerUser > 0) {
            long existingTokenCount = countSessionsOfUser(user);

            if (existingTokenCount + 1 > maxTokensPerUser) {
                throw new SessionCreationException(
                        "Cannot create session. Session limit per user exceeded. Max number of allowed sessions is " + maxTokensPerUser,
                        RestStatus.FORBIDDEN);
            }
        }

        OffsetDateTime now = OffsetDateTime.now().withNano(0);

        OffsetDateTime expiresAt = getExpiryTime(now);

        OffsetDateTime dynamicExpiresAt = null;

        if (activityTracker != null) {
            dynamicExpiresAt = now.plus(activityTracker.getInactivityTimeout());
        }

        SessionToken sessionToken = new SessionToken(id, user.getName(), base, now.toInstant(), expiresAt != null ? expiresAt.toInstant() : null,
                dynamicExpiresAt != null ? dynamicExpiresAt.toInstant() : null, null);

        try {
            updateSessionToken(sessionToken, UpdateType.NEW);
        } catch (Exception e) {
            componentState.addLastException("create", e);
            throw new SessionCreationException("Error while creating token", RestStatus.INTERNAL_SERVER_ERROR, e);
        }

        return sessionToken;
    }

    private String getRandomId() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());

        String result = BaseEncoding.base64Url().encode(byteBuffer.array()).replace("=", "");

        return result;
    }

    private long countSessionsOfUser(User user) {

        SearchRequest searchRequest = new SearchRequest(indexName)
                .source(new SearchSourceBuilder().query(QueryBuilders.termQuery("user_name", user.getName())).size(0));

        SearchResponse searchResponse = privilegedConfigClient.search(searchRequest).actionGet();

        return searchResponse.getHits().getTotalHits().value;
    }

    private OffsetDateTime getExpiryTime(OffsetDateTime now) {
        OffsetDateTime expiresAfter = null;

        if (config.getMaxValidity() != null) {
            expiresAfter = now.plus(config.getMaxValidity());
        }

        return expiresAfter;
    }

    private String updateSessionToken(SessionToken authToken, UpdateType updateType) throws SessionUpdateException {
        SessionToken oldToken = null;

        try {
            oldToken = getById(authToken.getId());
        } catch (NoSuchSessionException e) {
            oldToken = null;
        }

        if (updateType == UpdateType.NEW && oldToken != null) {
            throw new SessionUpdateException("Token ID already exists: " + authToken.getId());
        }

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            authToken.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);

            IndexResponse indexResponse = privilegedConfigClient
                    .index(new IndexRequest(indexName).id(authToken.getId()).source(xContentBuilder).setRefreshPolicy(RefreshPolicy.IMMEDIATE))
                    .actionGet();

            if (log.isDebugEnabled()) {
                log.debug("Token stored: " + indexResponse);
            }

        } catch (Exception e) {
            if (oldToken != null) {
                this.idToAuthTokenMap.put(oldToken.getId(), oldToken);
            } else {
                this.idToAuthTokenMap.invalidate(authToken.getId());
            }
            log.warn("Error while storing token " + authToken, e);
            throw new SessionUpdateException(e);
        }

        try {
            PushSessionTokenUpdateAction.Response pushAuthTokenUpdateResponse = privilegedConfigClient
                    .execute(PushSessionTokenUpdateAction.INSTANCE, new PushSessionTokenUpdateAction.Request(authToken, updateType, 0)).actionGet();

            if (log.isDebugEnabled()) {
                log.debug("Token update pushed: " + pushAuthTokenUpdateResponse);
            }

            if (pushAuthTokenUpdateResponse.hasFailures()) {
                return "Update partially failed: " + pushAuthTokenUpdateResponse.failures();
            }

        } catch (Exception e) {
            log.warn("Token update push failed: " + authToken, e);
            return "Update partially failed: " + e;
        }

        return null;
    }

    public JwtToken getVerifiedJwtToken(String encodedJwt) throws JwtException {

        if (this.jweDecryptionProvider != null) {
            JweDecryptionOutput decOutput = this.jweDecryptionProvider.decrypt(encodedJwt);
            encodedJwt = decOutput.getContentText();
        }

        JwsJwtCompactConsumer jwtConsumer = new JwsJwtCompactConsumer(encodedJwt);
        JwtToken jwt = jwtConsumer.getJwtToken();

        if (!validateAudience(jwt.getClaims())) {
            if (log.isTraceEnabled()) {
                log.trace("Not checking this token because it has a different audience: " + jwt.getClaims().getAudience());
            }
            return null;
        }

        if (this.jwsSignatureVerifier != null) {
            boolean signatureValid = jwtConsumer.verifySignatureWith(jwsSignatureVerifier);

            if (!signatureValid) {
                throw new JwtException("Invalid JWT signature for token " + jwt.getClaims().asMap());
            }
        }

        validateClaims(jwt);

        return jwt;
    }

    private void validateClaims(JwtToken jwt) throws JwtException {
        JwtClaims claims = jwt.getClaims();

        if (claims == null) {
            throw new JwtException("The JWT does not have any claims");
        }

        JwtUtils.validateJwtExpiry(claims, 0, false);
        JwtUtils.validateJwtNotBefore(claims, 0, false);

    }

    private boolean validateAudience(JwtClaims claims) throws JwtException {

        for (String audience : claims.getAudiences()) {
            if (JWT_AUDIENCE.equals(audience)) {
                return true;
            }
        }

        return false;
    }

    public void checkExpiryAndTrackAccess(SessionToken authToken, Consumer<Boolean> onResult, Consumer<Exception> onFailure) {
        activityTracker.checkExpiryAndTrackAccess(authToken, onResult, onFailure);
    }

    public void setConfig(SessionServiceConfig config) {
        if (config == null) {
            // Expected when SG is not initialized yet
            return;
        }

        this.config = config;
        this.maxTokensPerUser = config.getMaxSessionsPerUser();

        setKeys(config.getJwtSigningKey(), config.getJwtEncryptionKey());

        activityTracker.setInactivityTimeout(config.getInactivityTimeout() != null ? config.getInactivityTimeout() : Duration.ofHours(1));
    }

    public void shutdown() {
        this.indexCleanupAgent.shutdown();
    }

    public String pushAuthTokenUpdate(PushSessionTokenUpdateAction.Request request) {
        if (log.isDebugEnabled()) {
            log.debug("got auth token update: " + request);
        }

        SessionToken updatedSessionToken = request.getUpdatedToken();
        SessionToken existingSessionToken = this.idToAuthTokenMap.getIfPresent(updatedSessionToken.getId());

        if (existingSessionToken == null) {
            return "Session token is not cached";
        } else {
            this.idToAuthTokenMap.put(updatedSessionToken.getId(), updatedSessionToken);
            return "Session token updated";
        }
    }

    public boolean isEnabled() {
        return config.isEnabled();
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
            this.configComponentState.setFailed(e);
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

    private void init(ProtectedConfigIndexService.FailureListener failureListener) {
        initComplete();
        failureListener.onSuccess();
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
}
