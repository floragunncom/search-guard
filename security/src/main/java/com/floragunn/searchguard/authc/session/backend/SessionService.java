/*
 * Copyright 2021-2022 floragunn GmbH
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

package com.floragunn.searchguard.authc.session.backend;

import static java.util.stream.Collectors.toList;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import com.floragunn.codova.config.templates.AttributeSource;
import com.floragunn.codova.config.templates.ExpressionEvaluationException;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.AuthFailureListener;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.base.AuthcResult;
import com.floragunn.searchguard.authc.blocking.BlockedIpRegistry;
import com.floragunn.searchguard.authc.blocking.BlockedUserRegistry;
import com.floragunn.searchguard.authc.rest.ClientAddressAscertainer;
import com.floragunn.searchguard.authc.rest.ClientAddressAscertainer.ClientIpInfo;
import com.floragunn.searchguard.authc.rest.RestAuthcConfig;
import com.floragunn.searchguard.authc.rest.RestRequestMetaData;
import com.floragunn.searchguard.authc.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.authc.session.ApiAuthenticationProcessor;
import com.floragunn.searchguard.authc.session.FrontendAuthcConfig;
import com.floragunn.searchguard.authc.session.backend.PushSessionTokenUpdateAction.Request.UpdateType;
import com.floragunn.searchguard.authc.session.backend.SessionApi.StartSessionResponse;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService.ConfigIndex;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.user.Attributes;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.StaticSettings;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.cstate.metrics.TimeAggregation;
import com.floragunn.searchsupport.indices.IndexCleanupAgent;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.io.BaseEncoding;

import inet.ipaddr.IPAddress;

public class SessionService {

    private static final Logger log = LogManager.getLogger(SessionService.class);

    public static final StaticSettings.Attribute<String> INDEX_NAME = StaticSettings.Attribute.define("searchguard.sessions.index.name").withDefault(".searchguard_sessions").asString();
    public static final StaticSettings.Attribute<TimeValue> CLEANUP_INTERVAL = StaticSettings.Attribute.define("searchguard.sessions.cleanup_interval").withDefault(TimeValue.timeValueHours(1)).asTimeValue();

    public static final String USER_TYPE = "session";

    private final PrivilegedConfigClient privilegedConfigClient;
    private final Cache<String, SessionToken> idToAuthTokenMap = CacheBuilder.newBuilder().expireAfterWrite(60, TimeUnit.MINUTES).build();
    private final ThreadPool threadPool;
    private final ThreadContext threadContext;
    private final String indexName;
    private final AdminDNs adminDns;
    private final PrivilegesEvaluator privilegesEvaluator;
    private final AuditLog auditLog;
    private final BlockedIpRegistry blockedIpRegistry;
    private final BlockedUserRegistry blockedUserRegistry;
    private final ClusterService clusterService;

    private final ComponentState componentState;
    private final ComponentState configComponentState;

    private final TimeAggregation startAuthenticatedSessionMetrics = new TimeAggregation.Milliseconds();
    private final TimeAggregation startSessionMetrics = new TimeAggregation.Milliseconds();

    private final TimeAggregation deleteMetrics = new TimeAggregation.Milliseconds();

    private SessionActivityTracker activityTracker;
    private IndexCleanupAgent indexCleanupAgent;
    private long maxTokensPerUser = 100;

    private SessionServiceConfig config;
    private JoseJwtProducer jwtProducer;
    private JsonWebKey encryptionKey;
    private JsonWebKey signingKey;
    private JwsSignatureVerifier jwsSignatureVerifier;
    private JweDecryptionProvider jweDecryptionProvider;
    private String jwtAudience;

    private boolean initialized = false;

    private volatile MergedAuthcConfig authcConfig;
    private volatile ClientAddressAscertainer clientAddressAscertainer;
    private List<AuthFailureListener> ipAuthFailureListeners = ImmutableList.empty();

    public SessionService(ConfigurationRepository configurationRepository, PrivilegedConfigClient privilegedConfigClient, StaticSettings settings,
            PrivilegesEvaluator privilegesEvaluator, AuditLog auditLog, ThreadPool threadPool, ClusterService clusterService,
            ProtectedConfigIndexService protectedConfigIndexService, SessionServiceConfig config, BlockedIpRegistry blockedIpRegistry,
            BlockedUserRegistry blockedUserRegistry, ComponentState componentState) {
        this.indexName = settings.get(INDEX_NAME);
        this.privilegedConfigClient = privilegedConfigClient;
        this.threadPool = threadPool;
        this.componentState = componentState;
        this.configComponentState = componentState.getOrCreatePart("config", "sg_config");
        this.threadContext = threadPool.getThreadContext();
        this.adminDns = new AdminDNs(settings.getPlatformSettings());
        this.privilegesEvaluator = privilegesEvaluator;
        this.auditLog = auditLog;
        this.blockedIpRegistry = blockedIpRegistry;
        this.blockedUserRegistry = blockedUserRegistry;
        this.clusterService = clusterService;

        activityTracker = new SessionActivityTracker(config.getInactivityTimeout() != null ? config.getInactivityTimeout() : Duration.ofHours(1),
                this, indexName, privilegedConfigClient, threadPool);

        this.componentState.addPart(activityTracker.getComponentState());
        this.componentState.addMetrics("start_session_with_authentication", startAuthenticatedSessionMetrics);
        this.componentState.addMetrics("start_session_with_external_authentication", startSessionMetrics);
        this.componentState.addMetrics("delete_session", deleteMetrics);

        this.setConfig(config);

        ConfigIndex configIndex = new ConfigIndex(indexName).mapping(SessionToken.INDEX_MAPPING).onIndexReady(this::init);

        componentState.addPart(protectedConfigIndexService.createIndex(configIndex));

        if (activityTracker != null) {
            this.indexCleanupAgent = new IndexCleanupAgent(indexName,
                    () -> QueryBuilders.boolQuery().should(QueryBuilders.rangeQuery(SessionToken.DYNAMIC_EXPIRES_AT).lt(System.currentTimeMillis()))
                            .should(QueryBuilders.rangeQuery(SessionToken.EXPIRES_AT).lt(System.currentTimeMillis())),
                    settings.get(CLEANUP_INTERVAL), privilegedConfigClient, clusterService, threadPool);
        } else {
            this.indexCleanupAgent = new IndexCleanupAgent(indexName, SessionToken.EXPIRES_AT, settings.get(CLEANUP_INTERVAL), privilegedConfigClient,
                    clusterService, threadPool);
        }
        
        componentState.addPart(this.indexCleanupAgent.getComponentState());        
        
        configurationRepository.subscribeOnChange(new ConfigurationChangeListener() {

            @Override
            public void onChange(ConfigMap configMap) {
                SgDynamicConfiguration<FrontendAuthcConfig> frontendConfig = configMap.get(CType.FRONTEND_AUTHC);
                SgDynamicConfiguration<RestAuthcConfig> config = configMap.get(CType.AUTHC);
                RestAuthcConfig restAuthcConfig = null;

                if (config != null && config.getCEntry("default") != null) {
                    restAuthcConfig = config.getCEntry("default");
                    componentState.replacePartsWithType("config", config.getComponentState());
                } else {
                    componentState.setState(State.SUSPENDED, "no_configuration");
                }

                if (frontendConfig != null) {
                    componentState.addPart(frontendConfig.getComponentState());
                }

                if (log.isDebugEnabled()) {
                    log.debug("New configuration:\nFrontendConfig: " + frontendConfig + "\nRestAuthcConfig: " + restAuthcConfig);
                }

                MergedAuthcConfig authcConfig = new MergedAuthcConfig(frontendConfig, restAuthcConfig);
                SessionService.this.authcConfig = authcConfig;
                componentState.replacePartsWithType("auth_domain",
                        authcConfig.getAuthenticationDomains().stream().map((d) -> d.getComponentState()).collect(Collectors.toList()));

                clientAddressAscertainer = ClientAddressAscertainer.create(restAuthcConfig != null ? restAuthcConfig.getNetwork() : null);
                configComponentState.initialized();
                componentState.updateStateFromParts();
            }
        });
    }

    public void authenticateAndCreateSession(Map<String, Object> request, RestRequest restRequest, Consumer<StartSessionResponse> onResult,
            Consumer<AuthcResult> onAuthFailure, Consumer<Exception> onFailure) {

        if (this.config == null) {
            onFailure.accept(new SessionCreationException("SessionService is not configured", RestStatus.INTERNAL_SERVER_ERROR));
            return;
        }

        Meter meter = Meter.basic(getMetricsLevel(), startAuthenticatedSessionMetrics);

        authenticate(request, restRequest, config.getRequiredLoginPrivileges(), (authcResult) -> {
            if (authcResult.getStatus() == AuthcResult.Status.PASS) {
                threadPool.generic().submit(() -> {
                    try {
                        StartSessionResponse response = createLightweightJwt(authcResult.getUser(), authcResult.getRedirectUri(), meter);
                        meter.close();
                        auditLog.logSucceededKibanaLogin(authcResult.getUser());
                        onResult.accept(response);
                    } catch (SessionCreationException e) {
                        meter.close();
                        log.info("Creating token failed", e);
                        onAuthFailure.accept(AuthcResult.stop(e.getRestStatus(), e.getMessage()));
                    } catch (Exception e) {
                        meter.close();
                        onFailure.accept(e);
                    }
                });
            } else {
                meter.close();
                onAuthFailure.accept(authcResult);
            }

        }, meter.consumer(onFailure), meter);
    }

    public CompletableFuture<StartSessionResponse> createSession(User user) {
        CompletableFuture<StartSessionResponse> result = new CompletableFuture<>();

        Meter meter = Meter.basic(getMetricsLevel(), startSessionMetrics);

        threadPool.generic().submit(() -> {
            try {
                StartSessionResponse response = createLightweightJwt(user, null, meter);
                auditLog.logSucceededKibanaLogin(user);
                result.complete(response);
            } catch (Exception e) {
                log.error("Creating token failed", e);
                result.completeExceptionally(e);
            } finally {
                meter.close();
            }
        });

        return result;
    }

    public String getSsoLogoutUrl(User user) {

        String authType = user.getAttributeAsString(Attributes.AUTH_TYPE);

        if (authType == null) {
            // Handle legacy implementations
            return (String) threadPool.getThreadContext().getTransient(ConfigConstants.SSO_LOGOUT_URL);
        }

        String frontendConfigId = user.getAttributeAsString(Attributes.FRONTEND_CONFIG_ID);

        if (frontendConfigId == null) {
            frontendConfigId = "default";
        }

        List<AuthenticationDomain<ApiAuthenticationFrontend>> apiAuthenticationDomains = this.authcConfig.get(frontendConfigId);

        if (apiAuthenticationDomains == null) {
            log.error("Cannot determine logoutUrl because frontend config " + frontendConfigId + " is not known: " + user);
            return null;
        }

        apiAuthenticationDomains = apiAuthenticationDomains.stream().filter((d) -> authType.equals(d.getFrontend().getType())).collect(toList());

        for (AuthenticationDomain<ApiAuthenticationFrontend> domain : apiAuthenticationDomains) {
            try {
                String logoutUrl = domain.getFrontend().getLogoutUrl(user);

                if (logoutUrl != null) {
                    return logoutUrl;
                }
            } catch (Exception e) {
                log.error("Error while determining logoutUrl via " + domain, e);
            }
        }

        return null;
    }

    private void authenticate(Map<String, Object> request, RestRequest restRequest, List<String> requiredLoginPrivileges,
            Consumer<AuthcResult> onResult, Consumer<Exception> onFailure, Meter meter) {

        Meter subMeter = meter.basic("authenticate");

        ClientIpInfo clientInfo = null;
        try {
            clientInfo = clientAddressAscertainer.getActualRemoteAddress(restRequest);
        } catch (ElasticsearchStatusException e) {
            onFailure.accept(e);
            return;
        }
        IPAddress remoteIpAddress = clientInfo.getOriginatingIpAddress();

        if (log.isTraceEnabled()) {
            log.trace("Rest authentication request from {} [original: {}]", remoteIpAddress, restRequest.getHttpChannel().getRemoteAddress());
        }

        if (clientInfo.isTrustedProxy()) {
            threadContext.putTransient(ConfigConstants.SG_XFF_DONE, Boolean.TRUE);
        }

        threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, clientInfo.getOriginatingTransportAddress());

        if (blockedIpRegistry.isIpBlocked(remoteIpAddress)) {
            if (log.isDebugEnabled()) {
                log.debug("Rejecting REST request because of blocked address: " + remoteIpAddress);
            }
            subMeter.close();
            auditLog.logBlockedIp(restRequest, clientInfo.getOriginatingTransportAddress().address());
            onResult.accept(new AuthcResult(AuthcResult.Status.STOP, RestStatus.UNAUTHORIZED, ConfigConstants.UNAUTHORIZED));
            return;
        }

        String configId = request.get("config_id") != null ? request.get("config_id").toString() : "default";

        List<AuthenticationDomain<ApiAuthenticationFrontend>> apiAuthenticationDomains = this.authcConfig.get(configId);

        if (apiAuthenticationDomains == null) {
            log.error("Invalid config_id: " + configId + "; available: " + this.authcConfig);
            subMeter.close();
            onResult.accept(new AuthcResult(AuthcResult.Status.STOP, RestStatus.BAD_REQUEST, "Invalid config_id"));
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Using auth domains from frontend config " + configId + ": " + apiAuthenticationDomains);
        }

        String mode = request.get("mode") != null ? request.get("mode").toString() : null;

        if (mode != null) {
            apiAuthenticationDomains = apiAuthenticationDomains.stream().filter((d) -> mode.equals(d.getFrontend().getType())).collect(toList());
        }

        String id = request.get("id") != null ? request.get("id").toString() : null;

        if (id != null) {
            apiAuthenticationDomains = apiAuthenticationDomains.stream().filter((d) -> id.equals(d.getId())).collect(toList());
        }

        if (log.isDebugEnabled()) {
            log.debug("Auth domains after filtering by mode " + mode + " and id " + id + ": " + apiAuthenticationDomains);
        }

        RequestMetaData<RestRequest> requestMetaData = new RestRequestMetaData(restRequest, remoteIpAddress, null);

        new ApiAuthenticationProcessor(request, requestMetaData, apiAuthenticationDomains, adminDns, privilegesEvaluator, auditLog,
                blockedUserRegistry, ipAuthFailureListeners, requiredLoginPrivileges, authcConfig.isDebugEnabled(configId))
                        .authenticate(subMeter.consumer(onResult), subMeter.consumer(onFailure));
    }

    private StartSessionResponse createLightweightJwt(User user, String redirectUri, Meter meter) throws SessionCreationException {

        if (jwtProducer == null) {
            throw new SessionCreationException("SessionService is not configured", RestStatus.INTERNAL_SERVER_ERROR);
        }

        SessionToken sessionToken = create(user, meter);

        JwtClaims jwtClaims = new JwtClaims();
        JwtToken jwt = new JwtToken(jwtClaims);

        jwtClaims.setNotBefore(sessionToken.getCreationTime().getEpochSecond());

        if (sessionToken.getExpiryTime() != null) {
            jwtClaims.setExpiryTime(sessionToken.getExpiryTime().getEpochSecond());
        }

        jwtClaims.setSubject(user.getName());
        jwtClaims.setTokenId(sessionToken.getId());
        jwtClaims.setAudience(jwtAudience);

        String encodedJwt;

        try {
            encodedJwt = this.jwtProducer.processJwt(jwt);
        } catch (Exception e) {
            log.error("Error while creating JWT. Possibly the key configuration is not valid.", e);
            throw new SessionCreationException("Error while creating JWT. Possibly the key configuration is not valid.",
                    RestStatus.INTERNAL_SERVER_ERROR, e);
        }
        return new StartSessionResponse(encodedJwt, redirectUri);
    }

    public SessionToken getById(String id, Meter meter) throws NoSuchSessionException {
        SessionToken result = idToAuthTokenMap.getIfPresent(id);

        if (result != null) {
            return result;
        } else {
            return getByIdFromIndex(id, meter);
        }
    }

    public void getById(String id, Consumer<SessionToken> onResult, Consumer<NoSuchSessionException> onNoSuchAuthToken, Consumer<Exception> onFailure,
            Meter meter) {

        SessionToken result = idToAuthTokenMap.getIfPresent(id);

        if (result != null) {
            onResult.accept(result);
        } else {
            getByIdFromIndex(id, onResult, onNoSuchAuthToken, onFailure, meter);
        }
    }

    public SessionToken getByIdFromIndex(String id, Meter meter) throws NoSuchSessionException {

        CompletableFuture<SessionToken> completableFuture = new CompletableFuture<>();

        getByIdFromIndex(id, completableFuture::complete, completableFuture::completeExceptionally, completableFuture::completeExceptionally, meter);

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
            Consumer<Exception> onFailure, Meter meter) {

        Meter subMeter = meter.basic("index_read_by_id");

        privilegedConfigClient.get(new GetRequest(indexName, id), new ActionListener<GetResponse>() {

            @Override
            public void onResponse(GetResponse getResponse) {
                if (log.isTraceEnabled()) {
                    log.trace("SessionService.getByIdFromIndex(" + id + ") =>\n" + Strings.toString(getResponse));
                }

                if (getResponse.isExists()) {

                    try {
                        SessionToken sessionToken = SessionToken.parse(id, DocNode.parse(Format.JSON).from(getResponse.getSourceAsString()));

                        idToAuthTokenMap.put(id, sessionToken);

                        subMeter.close();
                        onResult.accept(sessionToken);
                    } catch (ConfigValidationException e) {
                        subMeter.close();
                        onFailure.accept(new RuntimeException("Token " + id + " is not stored in a valid format", e));
                    } catch (Exception e) {
                        subMeter.close();
                        log.error(e);
                        onFailure.accept(e);
                    }

                } else {
                    subMeter.close();
                    onNoSuchSession.accept(new NoSuchSessionException(id));
                }
            }

            @Override
            public void onFailure(Exception e) {
                subMeter.close();

                if (e instanceof IndexNotFoundException) {
                    onNoSuchSession.accept(new NoSuchSessionException(id));
                } else {
                    onFailure.accept(e);
                }
            }
        });

    }

    public String delete(User user, SessionToken sessionToken) throws NoSuchSessionException, SessionUpdateException {
        try (Meter meter = Meter.basic(getMetricsLevel(), deleteMetrics)) {
            if (log.isTraceEnabled()) {
                log.trace("delete(" + user + ", " + sessionToken.getId() + ")");
            }

            sessionToken = getById(sessionToken.getId(), meter);

            if (sessionToken.getRevokedAt() != null) {
                log.info("Session token " + sessionToken + " was already revoked");
                return "Session token was already revoked";
            }

            String updateStatus = updateSessionToken(sessionToken.getRevokedInstance(), UpdateType.REVOKED, meter);

            auditLog.logSucceededKibanaLogout(user);

            if (updateStatus != null) {
                return updateStatus;
            }

            return "Sesion has been deleted";
        }
    }

    public SessionToken getByClaims(Map<String, Object> claims, Meter meter) throws NoSuchSessionException, InvalidTokenException {
        CompletableFuture<SessionToken> completableFuture = new CompletableFuture<>();

        getByClaims(claims, completableFuture::complete, completableFuture::completeExceptionally, completableFuture::completeExceptionally, meter);

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
            Consumer<Exception> onFailure, Meter meter) throws InvalidTokenException {
        String id = Objects.toString(claims.get(JwtConstants.CLAIM_JWT_ID), null);
        Set<String> audience = getClaimAsSet(claims, JwtConstants.CLAIM_AUDIENCE);

        if (!audience.contains(jwtAudience)) {
            throw new InvalidTokenException("Invalid JWT audience claim. Supplied: " + audience + "; Expected: " + jwtAudience);
        }

        if (id == null) {
            throw new InvalidTokenException("Supplied auth token does not have an id claim");
        }

        getById(id, onResult, onNoSuchAuthToken, onFailure, meter);
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

    private SessionToken create(User user, Meter meter) throws SessionCreationException {
        if (config == null || !config.isEnabled()) {
            throw new SessionCreationException("Session token handling is not enabled", RestStatus.INTERNAL_SERVER_ERROR);
        }

        if (log.isDebugEnabled()) {
            log.debug("create(user: " + user.toStringWithAttributes() + ")");
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
            updateSessionToken(sessionToken, UpdateType.NEW, meter);
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
        try {
            return searchResponse.getHits().getTotalHits().value;
        } finally {
            searchResponse.decRef();
        }
    }

    private OffsetDateTime getExpiryTime(OffsetDateTime now) {
        OffsetDateTime expiresAfter = null;

        if (config.getMaxValidity() != null) {
            expiresAfter = now.plus(config.getMaxValidity());
        }

        return expiresAfter;
    }

    private String updateSessionToken(SessionToken sessionToken, UpdateType updateType, Meter meter) throws SessionUpdateException {
        SessionToken oldToken = null;

        try {
            oldToken = getById(sessionToken.getId(), meter);
        } catch (NoSuchSessionException e) {
            oldToken = null;
        }

        if (updateType == UpdateType.NEW && oldToken != null) {
            throw new SessionUpdateException("Token ID already exists: " + sessionToken.getId());
        }

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder(); Meter subMeter = meter.basic("index_update")) {
            sessionToken.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);

            DocWriteResponse indexResponse = privilegedConfigClient
                    .index(new IndexRequest(indexName).id(sessionToken.getId()).source(xContentBuilder).setRefreshPolicy(RefreshPolicy.IMMEDIATE))
                    .actionGet();

            if (log.isDebugEnabled()) {
                log.debug("Token stored: " + indexResponse);
            }

        } catch (Exception e) {
            if (oldToken != null) {
                this.idToAuthTokenMap.put(oldToken.getId(), oldToken);
            } else {
                this.idToAuthTokenMap.invalidate(sessionToken.getId());
            }
            log.warn("Error while storing token " + sessionToken, e);
            throw new SessionUpdateException(e);
        }

        try (Meter subMeter = meter.basic("cluster_update")) {
            PushSessionTokenUpdateAction.Response pushAuthTokenUpdateResponse = privilegedConfigClient
                    .execute(PushSessionTokenUpdateAction.INSTANCE, new PushSessionTokenUpdateAction.Request(sessionToken, updateType, 0))
                    .actionGet();

            if (log.isDebugEnabled()) {
                log.debug("Token update pushed: " + pushAuthTokenUpdateResponse);
            }

            if (pushAuthTokenUpdateResponse.hasFailures()) {
                return "Update partially failed: " + pushAuthTokenUpdateResponse.failures();
            }

        } catch (Exception e) {
            log.warn("Token update push failed: " + sessionToken, e);
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
            if (jwtAudience.equals(audience)) {
                return true;
            }
        }

        return false;
    }

    void checkExpiryAndTrackAccess(SessionToken sessionToken, Consumer<Boolean> onResult, Consumer<Exception> onFailure, Meter meter) {
        activityTracker.checkExpiryAndTrackAccess(sessionToken, onResult, onFailure, meter);
    }

    public void setConfig(SessionServiceConfig config) {
        if (config == null) {
            // Expected when SG is not initialized yet
            return;
        }

        this.config = config;
        this.maxTokensPerUser = config.getMaxSessionsPerUser();
        if (config.getJwtAudience() != null) {
            try {
                this.jwtAudience = config.getJwtAudience().render(AttributeSource.of("cluster.name", this.clusterService.getClusterName()));
            } catch (ExpressionEvaluationException e) {
                log.error("Invalid configuration for jwt_audience: " + config.getJwtAudience(), e);
                this.jwtAudience = "sg_session_" + this.clusterService.getClusterName();
                componentState.addLastException("jwt_audience", e);
            }
        } else {
            this.jwtAudience = "sg_session_" + this.clusterService.getClusterName();
        }

        setKeys(config.getJwtSigningKey(), config.getJwtEncryptionKey());

        activityTracker.setInactivityTimeout(config.getInactivityTimeout() != null ? config.getInactivityTimeout() : Duration.ofHours(1));
        activityTracker.setIndexRefreshPolicy(config.isRefreshSessionActivityIndex() ? RefreshPolicy.IMMEDIATE : RefreshPolicy.NONE);
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

    MetricsLevel getMetricsLevel() {
        return config.getMetricsLevel();
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
}
