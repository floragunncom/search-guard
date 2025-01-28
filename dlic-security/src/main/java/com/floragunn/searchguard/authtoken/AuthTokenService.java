/*
 * Copyright 2020-2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.authtoken;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.cxf.rs.security.jose.jwt.JwtConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authtoken.AuthTokenServiceConfig.FreezePrivileges;
import com.floragunn.searchguard.authtoken.api.CreateAuthTokenRequest;
import com.floragunn.searchguard.authtoken.api.CreateAuthTokenResponse;
import com.floragunn.searchguard.authtoken.update.PushAuthTokenUpdateAction;
import com.floragunn.searchguard.authtoken.update.PushAuthTokenUpdateRequest;
import com.floragunn.searchguard.authtoken.update.PushAuthTokenUpdateRequest.UpdateType;
import com.floragunn.searchguard.authtoken.update.PushAuthTokenUpdateResponse;
import com.floragunn.searchguard.authz.ActionAuthorization;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService.ConfigIndex;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.jwt.JwtVerifier;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProvider;
import com.floragunn.searchguard.sgconf.history.ConfigHistoryService;
import com.floragunn.searchguard.sgconf.history.ConfigModel;
import com.floragunn.searchguard.sgconf.history.ConfigSnapshot;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.StaticSettings;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.ExceptionRecord;
import com.floragunn.searchsupport.indices.IndexCleanupAgent;
import com.floragunn.searchsupport.xcontent.ObjectTreeXContent;
import com.google.common.cache.Cache;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWEAlgorithm;
import com.nimbusds.jose.JWEEncrypter;
import com.nimbusds.jose.JWEHeader;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.DirectEncrypter;
import com.nimbusds.jose.crypto.ECDHEncrypter;
import com.nimbusds.jose.crypto.RSAEncrypter;
import com.nimbusds.jose.crypto.factories.DefaultJWSSignerFactory;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.BadJWTException;

public class AuthTokenService implements SpecialPrivilegesEvaluationContextProvider {

    private static final Logger log = LogManager.getLogger(AuthTokenService.class);

    public static final StaticSettings.Attribute<String> INDEX_NAME = StaticSettings.Attribute.define("searchguard.authtokens.index.name")
            .withDefault(".searchguard_authtokens").asString();
    public static final StaticSettings.Attribute<TimeValue> CLEANUP_INTERVAL = StaticSettings.Attribute
            .define("searchguard.authtokens.cleanup_interval").withDefault(TimeValue.timeValueHours(1)).asTimeValue();

    public static final String USER_TYPE = "sg_auth_token";
    public static final String USER_TYPE_FULL_CURRENT_PERMISSIONS = "sg_auth_token_full_current_permissions";

    private static final EncryptionMethod CONTENT_ENCRYPTION_METHOD = EncryptionMethod.A256CBC_HS512;

    private final String indexName;
    private final PrivilegedConfigClient privilegedConfigClient;
    private final ConfigHistoryService configHistoryService;
    private final ComponentState componentState;
    private final AuthorizationService authorizationService;
    private final PrivilegesEvaluator privilegesEvaluator;
    private final Actions actions;

    private Cache<String, AuthToken> idToAuthTokenMap;
    private JWSSigner jwsSigner;
    private JWEEncrypter jweEncrypter;
    private String jwtAudience;
    private JWK encryptionKey;
    private JWK signingKey;
    private JwtVerifier jwtVerifier;
    private AuthTokenServiceConfig config;
    private boolean sendTokenUpdates = true;
    private boolean initialized = false;
    private IndexCleanupAgent indexCleanupAgent;
    private long maxTokensPerUser = 100;

    public AuthTokenService(PrivilegedConfigClient privilegedConfigClient, AuthorizationService authorizationService,
            PrivilegesEvaluator privilegesEvaluator, ConfigHistoryService configHistoryService, StaticSettings settings, ThreadPool threadPool,
            ClusterService clusterService, ProtectedConfigIndexService protectedConfigIndexService, Actions actions, AuthTokenServiceConfig config,
            ComponentState componentState) {
        this.indexName = settings.get(INDEX_NAME);
        this.privilegedConfigClient = privilegedConfigClient;
        this.configHistoryService = configHistoryService;
        this.componentState = componentState;
        this.authorizationService = authorizationService;
        this.privilegesEvaluator = privilegesEvaluator;
        this.actions = actions;

        this.idToAuthTokenMap = AuthTokenServiceConfig.DEFAULT_TOKEN_CACHE_CONFIG.build();

        this.setConfig(config);

        ConfigIndex configIndex = new ConfigIndex(indexName).mapping(AuthToken.INDEX_MAPPING).onIndexReady(this::init);

        if (configHistoryService != null) {
            configIndex.dependsOnIndices(configHistoryService.getIndexName());
        }

        componentState.addPart(protectedConfigIndexService.createIndex(configIndex));

        this.indexCleanupAgent = new IndexCleanupAgent(indexName, AuthToken.EXPIRES_AT, settings.get(CLEANUP_INTERVAL), privilegedConfigClient,
                clusterService, threadPool);
    }

    public AuthTokenService(PrivilegedConfigClient privilegedConfigClient, AuthorizationService authorizationService,
            PrivilegesEvaluator privilegesEvaluator, ConfigHistoryService configHistoryService, StaticSettings settings, ThreadPool threadPool,
            ClusterService clusterService, ProtectedConfigIndexService protectedConfigIndexService, Actions actions, AuthTokenServiceConfig config) {
        this(privilegedConfigClient, authorizationService, privilegesEvaluator, configHistoryService, settings, threadPool, clusterService,
                protectedConfigIndexService, actions, config, new ComponentState(1000, null, "auth_token_service"));
    }

    public AuthToken getById(String id) throws NoSuchAuthTokenException {
        Optional<AuthToken> result = getTokenFromCache(id);

        if (result.isPresent()) {
            return result.get();
        } else {
            return getByIdFromIndex(id);
        }
    }

    public void getById(String id, Consumer<AuthToken> onResult, Consumer<NoSuchAuthTokenException> onNoSuchAuthToken,
            Consumer<Exception> onFailure) {

        Optional<AuthToken> result = getTokenFromCache(id);

        if (result.isPresent()) {
            onResult.accept(result.get());
        } else {
            getByIdFromIndex(id, onResult, onNoSuchAuthToken, onFailure);
        }
    }

    public AuthToken getByIdFromIndex(String id) throws NoSuchAuthTokenException {

        CompletableFuture<AuthToken> completableFuture = new CompletableFuture<>();

        getByIdFromIndex(id, completableFuture::complete, completableFuture::completeExceptionally, completableFuture::completeExceptionally);

        try {
            return completableFuture.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NoSuchAuthTokenException) {
                throw (NoSuchAuthTokenException) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }
    }

    public void getByIdFromIndex(String id, Consumer<AuthToken> onResult, Consumer<NoSuchAuthTokenException> onNoSuchAuthToken,
            Consumer<Exception> onFailure) {

        privilegedConfigClient.get(new GetRequest(indexName, id), new ActionListener<GetResponse>() {

            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists()) {

                    try {
                        AuthToken authToken = AuthToken.parse(id, DocNode.parse(Format.JSON).from(getResponse.getSourceAsString()));

                        addTokenToCache(id, authToken);

                        onResult.accept(authToken);
                    } catch (ConfigValidationException e) {
                        onFailure.accept(new RuntimeException("Token " + id + " is not stored in a valid format", e));
                    } catch (Exception e) {
                        log.error(e);
                        onFailure.accept(e);
                    }

                } else {
                    onNoSuchAuthToken.accept(new NoSuchAuthTokenException(id));
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof IndexNotFoundException) {
                    onNoSuchAuthToken.accept(new NoSuchAuthTokenException(id));
                } else {
                    onFailure.accept(e);
                }
            }
        });

    }

    public AuthToken getByClaims(Map<String, Object> claims) throws NoSuchAuthTokenException, InvalidTokenException {

        CompletableFuture<AuthToken> completableFuture = new CompletableFuture<>();

        getByClaims(claims, completableFuture::complete, completableFuture::completeExceptionally, completableFuture::completeExceptionally);

        try {
            return completableFuture.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NoSuchAuthTokenException) {
                throw (NoSuchAuthTokenException) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }
    }

    public void getByClaims(Map<String, Object> claims, Consumer<AuthToken> onResult, Consumer<NoSuchAuthTokenException> onNoSuchAuthToken,
            Consumer<Exception> onFailure) throws InvalidTokenException {
        String id = Objects.toString(claims.get(JwtConstants.CLAIM_JWT_ID), null);
        Set<String> audience = getClaimAsSet(claims, JwtConstants.CLAIM_AUDIENCE);

        if (!audience.contains(this.jwtAudience)) {
            throw new InvalidTokenException("Invalid JWT audience claim. Supplied: " + audience + "; Expected: " + this.jwtAudience);
        }

        if (id == null) {
            throw new InvalidTokenException("Supplied auth token does not have an id claim");
        }

        getById(id, onResult, onNoSuchAuthToken, onFailure);
    }

    public void getByIdWithConfigSnapshot(String id, Consumer<AuthToken> onResult, Consumer<NoSuchAuthTokenException> onNoSuchAuthToken,
            Consumer<Exception> onFailure) {

        getById(id, (authToken) -> {
            if (authToken.getBase().getConfigVersions() == null || authToken.getBase().peekConfigSnapshot() != null) {
                onResult.accept(authToken);
            } else {
                configHistoryService.getConfigSnapshot(authToken.getBase().getConfigVersions(), (configSnapshot) -> {
                    authToken.getBase().setConfigSnapshot(configSnapshot);
                    onResult.accept(authToken);
                }, onFailure);
            }
        }, onNoSuchAuthToken, onFailure);

    }

    public AuthToken getByIdWithConfigSnapshot(String id) throws NoSuchAuthTokenException {

        CompletableFuture<AuthToken> completableFuture = new CompletableFuture<>();

        getByIdWithConfigSnapshot(id, completableFuture::complete, completableFuture::completeExceptionally,
                completableFuture::completeExceptionally);

        try {
            return completableFuture.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NoSuchAuthTokenException) {
                throw (NoSuchAuthTokenException) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }
    }

    public AuthToken create(User user, CreateAuthTokenRequest request) throws TokenCreationException {
        if (config == null || !config.isEnabled()) {
            throw new TokenCreationException("Auth token handling is not enabled", RestStatus.INTERNAL_SERVER_ERROR);
        }

        if (log.isDebugEnabled()) {
            log.debug("create(user: " + user + ", request: " + request + ")");
        }

        Set<String> baseBackendRoles;
        Set<String> baseSearchGuardRoles;
        Map<String, Object> baseAttributes;
        ConfigSnapshot configSnapshot;

        if (USER_TYPE.equals(user.getType())) {
            log.debug("User is based on an auth token. Resulting auth token will be based on the original one");
            String authTokenId = (String) user.getSpecialAuthzConfig();

            try {
                AuthToken existingAuthToken = getByIdWithConfigSnapshot(authTokenId);
                configSnapshot = existingAuthToken.getBase().getConfigSnapshot();
                baseBackendRoles = new HashSet<>(existingAuthToken.getBase().getBackendRoles());
                baseSearchGuardRoles = new HashSet<>(existingAuthToken.getBase().getSearchGuardRoles());
                baseAttributes = existingAuthToken.getBase().getAttributes();
            } catch (NoSuchAuthTokenException e) {
                componentState.addLastException("create", e);
                throw new TokenCreationException("Error while creating auth token: Could not find base token " + authTokenId,
                        RestStatus.INTERNAL_SERVER_ERROR, e);
            }
        } else {
            if ((request.isFreezePrivileges() && config.getFreezePrivileges() == FreezePrivileges.USER_CHOOSES)
                    || config.getFreezePrivileges() == FreezePrivileges.ALWAYS) {
                configSnapshot = configHistoryService.getCurrentConfigSnapshot(CType.ROLES, CType.ROLESMAPPING, CType.ACTIONGROUPS, CType.TENANTS);
            } else {
                configSnapshot = null;
            }

            baseBackendRoles = user.getRoles();
            baseSearchGuardRoles = user.getSearchGuardRoles();
            baseAttributes = user.getStructuredAttributes();
        }

        String id = getRandomId();

        AuthTokenPrivilegeBase base = new AuthTokenPrivilegeBase(restrictRoles(request, baseBackendRoles),
                restrictRoles(request, baseSearchGuardRoles), baseAttributes, configSnapshot != null ? configSnapshot.getConfigVersions() : null);

        if (log.isDebugEnabled()) {
            log.debug("base for auth token " + request + ": " + base);
        }

        base.setConfigSnapshot(configSnapshot);

        if (base.getBackendRoles().size() == 0 && base.getSearchGuardRoles().size() == 0) {
            throw new TokenCreationException(
                    "Cannot create token. The resulting token would have no privileges as the specified roles do not intersect with the user's roles. Specified: "
                            + request.getRequestedPrivileges().getRoles() + " User: " + baseBackendRoles + " + " + baseSearchGuardRoles,
                    RestStatus.BAD_REQUEST);
        }

        if (maxTokensPerUser == 0) {
            throw new TokenCreationException("Cannot create token. max_tokens_per_user is set to 0", RestStatus.FORBIDDEN);
        } else if (maxTokensPerUser > 0) {
            long existingTokenCount = countAuthTokensOfUser(user);

            if (existingTokenCount + 1 > maxTokensPerUser) {
                throw new TokenCreationException(
                        "Cannot create token. Token limit per user exceeded. Max number of allowed tokens is " + maxTokensPerUser,
                        RestStatus.FORBIDDEN);
            }
        }

        OffsetDateTime now = OffsetDateTime.now().withNano(0);

        OffsetDateTime expiresAt = getExpiryTime(now, request);

        RequestedPrivileges requestedPrivilegesWithDefaultExclusions = request.getRequestedPrivileges()
                .excludeClusterPermissions(config.getExcludeClusterPermissions());

        AuthToken authToken = new AuthToken(id, user.getName(), request.getTokenName(), requestedPrivilegesWithDefaultExclusions, base,
                now.toInstant(), expiresAt != null ? expiresAt.toInstant() : null, null);

        try {
            updateAuthToken(authToken, UpdateType.NEW);
        } catch (Exception e) {
            componentState.addLastException("create", e);
            throw new TokenCreationException("Error while creating token", RestStatus.INTERNAL_SERVER_ERROR, e);
        }

        return authToken;
    }

    public CreateAuthTokenResponse createJwt(User user, CreateAuthTokenRequest request) throws TokenCreationException {

        if (jwsSigner == null) {
            throw new TokenCreationException("AuthTokenProvider is not configured", RestStatus.INTERNAL_SERVER_ERROR);
        }

        AuthToken authToken = create(user, request);

        JWTClaimsSet.Builder jwtClaims = new JWTClaimsSet.Builder();

        jwtClaims.notBeforeTime(java.util.Date.from(authToken.getCreationTime()));

        if (authToken.getExpiryTime() != null) {
            jwtClaims.expirationTime(java.util.Date.from(authToken.getExpiryTime()));
        }

        jwtClaims.subject(user.getName());
        jwtClaims.jwtID(authToken.getId());
        jwtClaims.audience(config.getJwtAud());
        jwtClaims.claim("requested", ObjectTreeXContent.toObjectTree(authToken.getRequestedPrivileges()));
        jwtClaims.claim("base", ObjectTreeXContent.toObjectTree(authToken.getBase(), AuthTokenPrivilegeBase.COMPACT));

        String encodedJwt;

        try {
            SignedJWT signedJwt = new SignedJWT(new JWSHeader((JWSAlgorithm) signingKey.getAlgorithm()), jwtClaims.build());
            signedJwt.sign(jwsSigner);

            if (jweEncrypter != null) {
                JWEObject encryptedJwt = new JWEObject(new JWEHeader((JWEAlgorithm) encryptionKey.getAlgorithm(), CONTENT_ENCRYPTION_METHOD),
                        new Payload(signedJwt));
                encryptedJwt.encrypt(jweEncrypter);
                encodedJwt = encryptedJwt.serialize();
            } else {
                encodedJwt = signedJwt.serialize();
            }
        } catch (Exception e) {
            componentState.addLastException("createJwt",
                    new ExceptionRecord(e, "Error while creating JWT. Possibly the key configuration is not valid."));
            log.error("Error while creating JWT. Possibly the key configuration is not valid.", e);
            throw new TokenCreationException("Error while creating JWT. Possibly the key configuration is not valid.",
                    RestStatus.INTERNAL_SERVER_ERROR, e);
        }
        return new CreateAuthTokenResponse(authToken, encodedJwt);
    }

    public CreateAuthTokenResponse createLightweightJwt(User user, CreateAuthTokenRequest request) throws TokenCreationException {

        if (jwsSigner == null) {
            throw new TokenCreationException("AuthTokenProvider is not configured", RestStatus.INTERNAL_SERVER_ERROR);
        }

        AuthToken authToken = create(user, request);

        JWTClaimsSet.Builder jwtClaims = new JWTClaimsSet.Builder();

        jwtClaims.notBeforeTime(java.util.Date.from(authToken.getCreationTime()));

        if (authToken.getExpiryTime() != null) {
            jwtClaims.expirationTime(java.util.Date.from(authToken.getExpiryTime()));
        }

        jwtClaims.subject(user.getName());
        jwtClaims.jwtID(authToken.getId());
        jwtClaims.audience(config.getJwtAud());

        String encodedJwt;

        try {
            SignedJWT signedJwt = new SignedJWT(new JWSHeader((JWSAlgorithm) signingKey.getAlgorithm()), jwtClaims.build());
            signedJwt.sign(jwsSigner);

            if (jweEncrypter != null) {
                JWEObject encryptedJwt = new JWEObject(new JWEHeader((JWEAlgorithm) encryptionKey.getAlgorithm(), CONTENT_ENCRYPTION_METHOD),
                        new Payload(signedJwt));
                encryptedJwt.encrypt(jweEncrypter);
                encodedJwt = encryptedJwt.serialize();
            } else {
                encodedJwt = signedJwt.serialize();
            }
        } catch (Exception e) {
            log.error("Error while creating JWT. Possibly the key configuration is not valid.", e);
            throw new TokenCreationException("Error while creating JWT. Possibly the key configuration is not valid.",
                    RestStatus.INTERNAL_SERVER_ERROR, e);
        }
        return new CreateAuthTokenResponse(authToken, encodedJwt);
    }

    public JWT getVerifiedJwtToken(String encodedJwt) throws BadJWTException, ParseException, JOSEException {
        return jwtVerifier.getVerfiedJwt(encodedJwt);     
    }

    public String revoke(User user, String id) throws NoSuchAuthTokenException, TokenUpdateException {
        if (log.isTraceEnabled()) {
            log.trace("revoke(" + user + ", " + id + ")");
        }

        AuthToken authToken = getById(id);

        if (authToken.getRevokedAt() != null) {
            log.info("Auth token " + authToken + " was already revoked");
            return "Auth token was already revoked";
        }

        String updateStatus = updateAuthToken(authToken.getRevokedInstance(), UpdateType.REVOKED);

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

        this.idToAuthTokenMap = Optional.ofNullable(config.getCacheConfig()).orElse(AuthTokenServiceConfig.DEFAULT_TOKEN_CACHE_CONFIG).build();
        this.config = config;
        this.jwtAudience = config.getJwtAud();
        this.maxTokensPerUser = config.getMaxTokensPerUser();

        setKeys(config.getJwtSigningKey(), config.getJwtEncryptionKey());
    }

    private void init(ProtectedConfigIndexService.FailureListener failureListener) {
        initComplete();
        failureListener.onSuccess();
        this.componentState.updateStateFromParts();
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

        AuthToken updatedAuthToken = request.getUpdatedToken();
        Optional<AuthToken> existingAuthToken = getTokenFromCache(updatedAuthToken.getId());

        if (!existingAuthToken.isPresent()) {
            return "Auth token is not cached";
        } else {
            addTokenToCache(updatedAuthToken.getId(), updatedAuthToken);
            return "Auth token updated";
        }
    }

    private String updateAuthToken(AuthToken authToken, UpdateType updateType) throws TokenUpdateException {
        AuthToken oldToken = null;

        try {
            oldToken = getById(authToken.getId());
        } catch (NoSuchAuthTokenException e) {
            oldToken = null;
        }

        if (updateType == UpdateType.NEW && oldToken != null) {
            throw new TokenUpdateException("Token ID already exists: " + authToken.getId());
        }

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            authToken.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);

            DocWriteResponse indexResponse = privilegedConfigClient
                    .index(new IndexRequest(indexName).id(authToken.getId()).source(xContentBuilder).setRefreshPolicy(RefreshPolicy.IMMEDIATE))
                    .actionGet();

            if (log.isDebugEnabled()) {
                log.debug("Token stored: " + indexResponse);
            }

        } catch (Exception e) {
            if (oldToken != null) {
                addTokenToCache(oldToken.getId(), oldToken);
            } else {
                removeTokenFromCache(authToken.getId());
            }
            log.warn("Error while storing token " + authToken, e);
            throw new TokenUpdateException(e);
        }

        if (!sendTokenUpdates) {
            return "Update disabled";
        }

        try {
            PushAuthTokenUpdateResponse pushAuthTokenUpdateResponse = privilegedConfigClient
                    .execute(PushAuthTokenUpdateAction.INSTANCE, new PushAuthTokenUpdateRequest(authToken, updateType, 0)).actionGet();

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

    private long countAuthTokensOfUser(User user) {

        SearchRequest searchRequest = new SearchRequest(getIndexName())
                .source(new SearchSourceBuilder().query(QueryBuilders.termQuery("user_name", user.getName())).size(0));

        SearchResponse searchResponse = privilegedConfigClient.search(searchRequest).actionGet();
        try {
            return searchResponse.getHits().getTotalHits().value;
        } finally {
            searchResponse.decRef();
        }
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

        String result = BaseEncoding.base64Url().encode(byteBuffer.array()).replace("=", "");

        return result;
    }

    void initJwtProducer() {
        try {
            if (signingKey != null) {
                this.jwsSigner = new DefaultJWSSignerFactory().createJWSSigner(signingKey);
                this.jwtVerifier = new JwtVerifier(signingKey, encryptionKey, jwtAudience);
            } else {
                this.jwsSigner = null;
                this.jwtVerifier = null;
            }

            if (this.encryptionKey != null) {
                this.jweEncrypter = createJweEncrypter(encryptionKey);
            } else {
                this.jweEncrypter = null;
            }

        } catch (Exception e) {
            this.componentState.setFailed(e);
            this.componentState.setSubState("jwt_producer_not_initialized");
            this.jwsSigner = null;
            this.jweEncrypter = null;
            log.error("Error while initializing JWT producer in AuthTokenProvider", e);
        }
    }

    public JWK getSigningKey() {
        return signingKey;
    }

    public void setSigningKey(JWK signingKey) {
        if (Objects.equals(this.signingKey, signingKey)) {
            return;
        }

        log.info("Updating signing key for " + this);

        this.signingKey = signingKey;
        initJwtProducer();
    }

    public JWK getEncryptionKey() {
        return encryptionKey;
    }

    public void setEncryptionKey(JWK encryptionKey) {
        if (Objects.equals(this.encryptionKey, encryptionKey)) {
            return;
        }

        log.info("Updating encryption key for " + this);

        this.encryptionKey = encryptionKey;
        initJwtProducer();
    }

    public void setKeys(JWK signingKey, JWK encryptionKey) {
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
    public void provide(User user, ThreadContext threadContext, Consumer<SpecialPrivilegesEvaluationContext> onResult,
            Consumer<Exception> onFailure) {

        if (config == null || !config.isEnabled()) {
            onResult.accept(null);
            return;
        }

        if (user == null || !(USER_TYPE.equals(user.getType()))) {
            onResult.accept(null);
            return;
        }

        String authTokenId = (String) user.getSpecialAuthzConfig();

        if (log.isDebugEnabled()) {
            log.debug("AuthTokenService.provide(" + user.getName() + ") on " + authTokenId);
        }

        // We need to drop the thread context incl user information. This is necessary to avoid running in an infinite loop
        // because we do transport action requests further down, which will hit again the SearchGuardFilter, which might branch
        // to here again if it finds a user authenticated by an auth token

        Supplier<StoredContext> restorableCtx = threadContext.newRestorableContext(true);

        try (StoredContext ctx = threadContext.stashContext()) {
            getByIdWithConfigSnapshot(authTokenId, (authToken) -> {

                try {
                    if (log.isTraceEnabled()) {
                        log.trace("Got token: " + authToken);
                    }

                    if (authToken.isRevoked()) {
                        log.info("Using revoked auth token: " + authToken);
                        try (StoredContext restoredCtx = restorableCtx.get()) {
                            onResult.accept(null);
                            return;
                        }
                    }

                    ConfigModel configModelSnapshot;

                    if (authToken.getBase().getConfigSnapshot() == null) {
                        configModelSnapshot = getCurrentConfigModel();
                    } else {
                        if (authToken.getBase().getConfigSnapshot().hasMissingConfigVersions()) {
                            throw new RuntimeException("Stored config snapshot is not complete: " + authToken);
                        }

                        configModelSnapshot = configHistoryService.getConfigModelForSnapshot(authToken.getBase().getConfigSnapshot());
                    }

                    User userWithRoles = user.copy().backendRoles(authToken.getBase().getBackendRoles())
                            .searchGuardRoles(authToken.getBase().getSearchGuardRoles()).build();
                    TransportAddress callerTransportAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
                    Set<String> mappedBaseRoles = configModelSnapshot.getRoleMapping().evaluate(userWithRoles, callerTransportAddress,
                            privilegesEvaluator.getRolesMappingResolution());

                    if (log.isDebugEnabled()) {
                        log.debug("AuthTokenService.provide returns SpecialPrivilegesEvaluationContext for " + user + "\nuserWithRoles: "
                                + userWithRoles + "\nmappedBaseRoles: " + mappedBaseRoles);
                    }

                    RestrictedActionAuthorization restrictedSgRoles = new RestrictedActionAuthorization(configModelSnapshot.getActionAuthorization(),
                            authToken.getRequestedPrivileges(), configModelSnapshot.getActionGroups(), actions, null,
                            privilegesEvaluator.getAllConfiguredTenantNames(), null);

                    try (StoredContext restoredCtx = restorableCtx.get()) {
                        onResult.accept(new SpecialPrivilegesEvaluationContextImpl(userWithRoles, mappedBaseRoles, restrictedSgRoles,
                                configModelSnapshot.getRolesConfig(), authToken.getRequestedPrivileges()));
                    }
                } catch (Exception e) {
                    log.error("Error in provide(" + user + "); authTokenId: " + authTokenId, e);
                    try (StoredContext restoredCtx = restorableCtx.get()) {
                        onFailure.accept(e);
                    }
                }
            }, (noSuchAuthTokenException) -> {
                try (StoredContext restoredCtx = restorableCtx.get()) {
                    onFailure.accept(new ElasticsearchSecurityException("Cannot authenticate user due to invalid auth token " + authTokenId,
                            noSuchAuthTokenException));
                }
            }, onFailure);
        }
    }

    public void shutdown() {
        this.indexCleanupAgent.shutdown();
    }

    private ConfigModel getCurrentConfigModel() {
        return new ConfigModel(privilegesEvaluator.getActionAuthorization(), authorizationService.getRoleMapping(),
                privilegesEvaluator.getActionGroups());
    }
    
    private static JWEEncrypter createJweEncrypter(JWK encryptionKey) throws JOSEException {
        if (encryptionKey instanceof RSAKey) {
            return new RSAEncrypter((RSAKey) encryptionKey);
        } else if (encryptionKey instanceof OctetSequenceKey) {
            return new DirectEncrypter((OctetSequenceKey) encryptionKey);
        } else if (encryptionKey instanceof ECKey) {
            return new ECDHEncrypter((ECKey) encryptionKey);
        } else {
            throw new IllegalArgumentException("Unsupported key type for encryption: " + encryptionKey.getKeyType());
        }
    }

    static class SpecialPrivilegesEvaluationContextImpl implements SpecialPrivilegesEvaluationContext {

        private final User user;
        private final ImmutableSet<String> mappedRoles;
        private final ActionAuthorization actionAuthorization;
        private final SgDynamicConfiguration<Role> rolesConfig;
        private final RequestedPrivileges requestedPrivileges;

        SpecialPrivilegesEvaluationContextImpl(User user, Set<String> mappedRoles, ActionAuthorization actionAuthorization,
                SgDynamicConfiguration<Role> rolesConfig, RequestedPrivileges requestedPrivileges) {
            this.user = user;
            this.mappedRoles = ImmutableSet.of(mappedRoles);
            this.actionAuthorization = actionAuthorization;
            this.requestedPrivileges = requestedPrivileges;
            this.rolesConfig = rolesConfig;
        }

        @Override
        public User getUser() {
            return user;
        }

        @Override
        public ImmutableSet<String> getMappedRoles() {
            return mappedRoles;
        }

        @Override
        public ActionAuthorization getActionAuthorization() {
            return actionAuthorization;
        }

        @Override
        public boolean isSgConfigRestApiAllowed() {
            // This is kind of a hack in order to allow the creation of tokens which don't have the privilege to use the rest API
            return (requestedPrivileges.getClusterPermissions().contains("*")
                    || requestedPrivileges.getClusterPermissions().contains("cluster:admin:searchguard:configrestapi"))
                    && !requestedPrivileges.getExcludedClusterPermissions().contains("cluster:admin:searchguard:configrestapi");
        }

        @Override
        public SgDynamicConfiguration<Role> getRolesConfig() {
            return rolesConfig;
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

    public AuthTokenServiceConfig getConfig() {
        return config;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public ComponentState getComponentState() {
        componentState.updateStateFromParts();
        return componentState;
    }

    public boolean isEnabled() {
        return config != null && config.isEnabled();
    }

    private Optional<AuthToken> getTokenFromCache(String id) {
        return Optional.ofNullable(idToAuthTokenMap).map(cache -> cache.getIfPresent(id));
    }

    private void addTokenToCache(String id, AuthToken token) {
        Optional.ofNullable(idToAuthTokenMap).ifPresent(cache -> cache.put(id, token));
    }

    private void removeTokenFromCache(String id) {
        Optional.ofNullable(idToAuthTokenMap).ifPresent(cache -> cache.invalidate(id));
    }
}
