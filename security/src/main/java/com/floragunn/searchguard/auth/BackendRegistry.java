/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard.auth;

import static java.util.stream.Collectors.toList;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

import org.apache.commons.collections.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.auth.api.AuthenticationBackend;
import com.floragunn.searchguard.auth.api.AuthenticationBackend.UserCachingPolicy;
import com.floragunn.searchguard.auth.api.SyncAuthenticationBackend;
import com.floragunn.searchguard.auth.api.SyncAuthorizationBackend;
import com.floragunn.searchguard.auth.blocking.ClientBlockRegistry;
import com.floragunn.searchguard.auth.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.auth.session.ApiAuthenticationProcessor;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.http.XFFResolver;
import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.sgconf.ConfigModel;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory.DCFListener;
import com.floragunn.searchguard.sgconf.DynamicConfigModel;
import com.floragunn.searchguard.sgconf.InternalUsersModel;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.FrontendConfig;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HTTPHelper;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.AuthDomainInfo;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchguard.user.UserAttributes;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressNetwork.IPAddressGenerator;

public class BackendRegistry implements DCFListener {
    private static final String BLOCKED_USERS = "BLOCKED_USERS";
    protected final Logger log = LogManager.getLogger(this.getClass());
    private final IPAddressGenerator ipAddressGenerator = new IPAddressGenerator();
    private SortedSet<AuthenticationDomain<HTTPAuthenticator>> restAuthenticationDomains;
    private SortedSet<AuthenticationDomain<HTTPAuthenticator>> transportAuthenticationDomains;
    private Set<AuthorizationDomain> restAuthorizationDomains;
    private Set<AuthorizationDomain> transportAuthorizationDomains;
    private Map<String, List<AuthenticationDomain<ApiAuthenticationFrontend>>> apiAuthenticationDomainMap;

    private List<AuthFailureListener> ipAuthFailureListeners;
    private Multimap<String, AuthFailureListener> authBackendFailureListeners;
    private List<ClientBlockRegistry<InetAddress>> ipClientBlockRegistries;
    private Multimap<String, ClientBlockRegistry<String>> authBackendClientBlockRegistries;
    private List<ClientBlockRegistry<IPAddress>> blockedNetmasks;

    private SgDynamicConfiguration<FrontendConfig> frontendConfig;
    
    private volatile boolean initialized;
    private final AdminDNs adminDns;
    private final XFFResolver xffResolver;
    private volatile boolean anonymousAuthEnabled = false;
    private final Settings esSettings;

    private final AuditLog auditLog;
    private final ThreadPool threadPool;
    private final PrivilegesEvaluator privilegesEvaluator;
    private final int ttlInMin;
    private Cache<AuthCredentials, User> userCache; //rest standard
    private Cache<String, User> restImpersonationCache; //used for rest impersonation
    private Cache<String, User> userCacheTransport; //transport no creds, possibly impersonated
    private Cache<AuthCredentials, User> authenticatedUserCacheTransport; //transport creds, no impersonation

    private Cache<User, Set<String>> transportRoleCache; //
    private Cache<User, Set<String>> restRoleCache; //
    private Cache<String, User> transportImpersonationCache; //used for transport impersonation

    private volatile String transportUsernameAttribute = null;
    private boolean debug;

    private void createCaches() {
        userCache = CacheBuilder.newBuilder().expireAfterWrite(ttlInMin, TimeUnit.MINUTES)
                .removalListener((RemovalListener<AuthCredentials, User>) notification -> log.debug("Clear user cache for {} due to {}",
                        notification.getKey().getUsername(), notification.getCause()))
                .build();

        userCacheTransport = CacheBuilder.newBuilder().expireAfterWrite(ttlInMin, TimeUnit.MINUTES)
                .removalListener((RemovalListener<String, User>) notification -> log.debug("Clear user cache for {} due to {}", notification.getKey(),
                        notification.getCause()))
                .build();

        authenticatedUserCacheTransport = CacheBuilder.newBuilder().expireAfterWrite(ttlInMin, TimeUnit.MINUTES)
                .removalListener((RemovalListener<AuthCredentials, User>) notification -> log.debug("Clear user cache for {} due to {}",
                        notification.getKey().getUsername(), notification.getCause()))
                .build();

        restImpersonationCache = CacheBuilder.newBuilder().expireAfterWrite(ttlInMin, TimeUnit.MINUTES)
                .removalListener((RemovalListener<String, User>) notification -> log.debug("Clear user cache for {} due to {}", notification.getKey(),
                        notification.getCause()))
                .build();

        transportRoleCache = CacheBuilder.newBuilder().expireAfterWrite(ttlInMin, TimeUnit.MINUTES)
                .removalListener((RemovalListener<User, Set<String>>) notification -> log.debug("Clear user cache for {} due to {}",
                        notification.getKey(), notification.getCause()))
                .build();

        restRoleCache = CacheBuilder.newBuilder().expireAfterWrite(ttlInMin, TimeUnit.MINUTES)
                .removalListener((RemovalListener<User, Set<String>>) notification -> log.debug("Clear user cache for {} due to {}",
                        notification.getKey(), notification.getCause()))
                .build();

        transportImpersonationCache = CacheBuilder.newBuilder().expireAfterWrite(ttlInMin, TimeUnit.MINUTES)
                .removalListener((RemovalListener<String, User>) notification -> log.debug("Clear user cache for {} due to {}", notification.getKey(),
                        notification.getCause()))
                .build();

    }

    public BackendRegistry(final Settings settings, final AdminDNs adminDns, final XFFResolver xffResolver, final AuditLog auditLog,
            PrivilegesEvaluator privilegesEvaluator, final ThreadPool threadPool) {
        this.adminDns = adminDns;
        this.esSettings = settings;
        this.xffResolver = xffResolver;
        this.auditLog = auditLog;
        this.threadPool = threadPool;
        this.privilegesEvaluator = privilegesEvaluator;

        this.ttlInMin = settings.getAsInt(ConfigConstants.SEARCHGUARD_CACHE_TTL_MINUTES, 60);

        createCaches();
    }

    public boolean isInitialized() {
        return initialized;
    }

    public void invalidateCache() {
        userCache.invalidateAll();
        userCacheTransport.invalidateAll();
        authenticatedUserCacheTransport.invalidateAll();
        restImpersonationCache.invalidateAll();
        restRoleCache.invalidateAll();
        transportRoleCache.invalidateAll();
        transportImpersonationCache.invalidateAll();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onChanged(ConfigModel cm, DynamicConfigModel dcm, InternalUsersModel ium) {
        invalidateCache();

        transportUsernameAttribute = dcm.getTransportUsernameAttribute();// config.dynamic.transport_userrname_attribute;
        anonymousAuthEnabled = dcm.isAnonymousAuthenticationEnabled()//config.dynamic.http.anonymous_auth_enabled
                && !esSettings.getAsBoolean(ConfigConstants.SEARCHGUARD_COMPLIANCE_DISABLE_ANONYMOUS_AUTHENTICATION, false);

        restAuthenticationDomains = Collections.unmodifiableSortedSet(dcm.getRestAuthenticationDomains());
        transportAuthenticationDomains = Collections.unmodifiableSortedSet(dcm.getTransportAuthenticationDomains());
        restAuthorizationDomains = Collections.unmodifiableSet(dcm.getRestAuthorizationDomains());
        transportAuthorizationDomains = Collections.unmodifiableSet(dcm.getTransportAuthorizationDomains());
        apiAuthenticationDomainMap = dcm.getApiAuthenticationDomainMap();

        ipAuthFailureListeners = dcm.getIpAuthFailureListeners();
        authBackendFailureListeners = dcm.getAuthBackendFailureListeners();
        ipClientBlockRegistries = dcm.getIpClientBlockRegistries();
        authBackendClientBlockRegistries = dcm.getAuthBackendClientBlockRegistries();
        debug = dcm.isAuthDebugEnabled();

        if (cm.getBlockIpAddresses() != null) {
            if (ipClientBlockRegistries == null) {
                ipClientBlockRegistries = Collections.emptyList();
            }
            ipClientBlockRegistries = Collections.unmodifiableList(ListUtils.union(cm.getBlockIpAddresses(), ipClientBlockRegistries));
        }

        if (cm.getBlockedUsers() != null) {
            if (authBackendClientBlockRegistries == null) {
                authBackendClientBlockRegistries = ArrayListMultimap.create();
            }
            ArrayListMultimap<String, ClientBlockRegistry<String>> registry = ArrayListMultimap.create();
            registry.putAll(authBackendClientBlockRegistries);
            registry.putAll(BLOCKED_USERS, cm.getBlockedUsers());
            authBackendClientBlockRegistries = Multimaps.unmodifiableMultimap(registry);
        }

        if (cm.getBlockedNetmasks() != null) {
            blockedNetmasks = cm.getBlockedNetmasks();
        }
        
        frontendConfig = dcm.getFrontendConfig();

        //SG6 no default authc
        initialized = !restAuthenticationDomains.isEmpty() || anonymousAuthEnabled;
    }

    public User authenticate(final TransportRequest request, final String sslPrincipal, final Task task, final String action) {
        if (log.isDebugEnabled() && request.remoteAddress() != null) {
            log.debug("Transport authentication request from {}", request.remoteAddress());
        }

        User origPKIUser = new User(sslPrincipal, AuthDomainInfo.TLS_CERT);

        if (adminDns.isAdmin(origPKIUser)) {
            auditLog.logSucceededLogin(origPKIUser, true, null, request, action, task);
            return origPKIUser;
        }

        IPAddress remoteIpAddress = null;

        if (request.remoteAddress() != null) {
            remoteIpAddress = ipAddressGenerator.from(request.remoteAddress().address().getAddress());
        }

        if (isIpBlocked(remoteIpAddress)) {
            if (log.isDebugEnabled()) {
                log.debug("Rejecting transport request because of blocked address: " + request.remoteAddress());
            }
            auditLog.logBlockedIp(request, action, request.remoteAddress(), task);
            return null;
        }

        if (!isInitialized()) {
            log.error("Not yet initialized (you may need to run sgadmin)");
            return null;
        }

        final String authorizationHeader = threadPool.getThreadContext().getHeader("Authorization");
        //Use either impersonation OR credentials authentication
        //if both is supplied credentials authentication win
        AuthCredentials.Builder credentialBuilder = HTTPHelper.extractCredentials(authorizationHeader, log);
        AuthCredentials creds = credentialBuilder != null ? credentialBuilder.authenticatorType("transport_basic").build() : null;
        
        User impersonatedTransportUser = null;

        if (creds != null) {
            if (log.isDebugEnabled()) {
                log.debug("User {} submitted also basic credentials: {}", origPKIUser.getName(), creds);
            }
        }

        //loop over all transport auth domains
        for (final AuthenticationDomain<HTTPAuthenticator> authenticationDomain : transportAuthenticationDomains) {
            if (log.isDebugEnabled()) {
                log.debug("Check transport authdomain {}/{} or {} in total", authenticationDomain.getBackend().getType(),
                        authenticationDomain.getOrder(), transportAuthenticationDomains.size());
            }

            User authenticatedUser;

            if (creds == null) {
                //no credentials submitted
                //impersonation possible
                impersonatedTransportUser = impersonate(origPKIUser);
                origPKIUser = resolveTransportUsernameAttribute(origPKIUser);
                authenticatedUser = checkExistsAndAuthz(userCacheTransport,
                        impersonatedTransportUser == null ? origPKIUser : impersonatedTransportUser, authenticationDomain.getBackend(),
                        transportAuthorizationDomains);
            } else {
                //auth credentials submitted
                //impersonation not possible, if requested it will be ignored
                authenticatedUser = authcz(authenticatedUserCacheTransport, transportRoleCache, creds, (SyncAuthenticationBackend) authenticationDomain.getBackend(),
                        transportAuthorizationDomains);
            }

            if (authenticatedUser == null) {
                for (AuthFailureListener authFailureListener : authBackendFailureListeners
                        .get(authenticationDomain.getBackend().getClass().getName())) {
                    authFailureListener.onAuthFailure(request.remoteAddress() != null ? request.remoteAddress().address().getAddress() : null, creds,
                            request);
                }

                if (log.isDebugEnabled()) {
                    log.debug("Cannot authenticate transport user {} (or add roles) with authdomain {}/{} of {}, try next",
                            creds == null ? (impersonatedTransportUser == null ? origPKIUser.getName() : impersonatedTransportUser.getName())
                                    : creds.getUsername(),
                            authenticationDomain.getBackend().getType(), authenticationDomain.getOrder(), transportAuthenticationDomains.size());
                }
                continue;
            }

            if (adminDns.isAdmin(authenticatedUser)) {
                log.error("Cannot authenticate transport user because admin user is not permitted to login");
                auditLog.logFailedLogin(authenticatedUser, true, null, request, task);
                return null;
            }

            if (isUserBlocked(authenticationDomain.getBackend().getClass().getName(), authenticatedUser.getName())) {
                if (log.isDebugEnabled()) {
                    log.debug("Rejecting TRANSPORT request because of blocked user: " + authenticatedUser.getName() + "; authDomain: "
                            + authenticationDomain);
                }
                auditLog.logBlockedUser(authenticatedUser, false, origPKIUser, request, task);
                continue;
            }

            if (log.isDebugEnabled()) {
                log.debug("Transport user '{}' is authenticated", authenticatedUser);
            }

            auditLog.logSucceededLogin(authenticatedUser, false, impersonatedTransportUser == null ? null : origPKIUser, request,
                    action, task);

            return authenticatedUser;
        } //end looping auth domains

        //auditlog
        if (creds == null) {
            auditLog.logFailedLogin(impersonatedTransportUser == null ? origPKIUser : impersonatedTransportUser, false,
                    impersonatedTransportUser == null ? null : origPKIUser, request, task);
        } else {
            auditLog.logFailedLogin(creds, false, null, request, task);
        }

        log.warn("Transport authentication finally failed for {} from {}",
                creds == null ? impersonatedTransportUser == null ? origPKIUser.getName() : impersonatedTransportUser.getName() : creds.getUsername(),
                request.remoteAddress());

        notifyIpAuthFailureListeners(request.remoteAddress() != null ? request.remoteAddress().address().getAddress() : null, creds, request);

        return null;
    }

    public void authenticate(final RestHandler restHandler, final RestRequest request, final RestChannel channel, final ThreadContext threadContext,
            Consumer<AuthczResult> onResult, Consumer<Exception> onFailure) {
        final String sslPrincipal = threadPool.getThreadContext().getTransient(ConfigConstants.SG_SSL_PRINCIPAL);

        if (adminDns.isAdminDN(sslPrincipal)) {
            //PKI authenticated REST call
            User user = new User(sslPrincipal, AuthDomainInfo.TLS_CERT);
            threadPool.getThreadContext().putTransient(ConfigConstants.SG_USER, user);
            auditLog.logSucceededLogin(user, true, null, request);
            onResult.accept(new AuthczResult(user, AuthczResult.Status.PASS));
            return;
        }

        final TransportAddress remoteAddress = xffResolver.resolve(request);

        if (log.isTraceEnabled()) {
            log.trace("Rest authentication request from {} [original: {}]", remoteAddress, request.getHttpChannel().getRemoteAddress());
        }
        
        IPAddress remoteIpAddress = null;

        if (remoteAddress != null) {
            remoteIpAddress = ipAddressGenerator.from(remoteAddress.address().getAddress());
        }

        if (isIpBlocked(remoteIpAddress)) {
            if (log.isDebugEnabled()) {
                log.debug("Rejecting REST request because of blocked address: " + remoteAddress);
            }
            auditLog.logBlockedIp(request, remoteAddress.address());
            channel.sendResponse(new BytesRestResponse(RestStatus.UNAUTHORIZED, "Authentication finally failed"));
            onResult.accept(new AuthczResult(AuthczResult.Status.STOP));
            return;
        }

        threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);
        
        if (!isInitialized()) {
            log.error("Not yet initialized (you may need to run sgadmin)");
            channel.sendResponse(new BytesRestResponse(RestStatus.SERVICE_UNAVAILABLE,
                    "Search Guard not initialized (SG11). See https://docs.search-guard.com/latest/sgadmin"));
            onResult.accept(new AuthczResult(AuthczResult.Status.STOP));
            return;
        }

        new HTTPAuthenticationProcessor(restHandler, request, channel, remoteIpAddress, threadContext, restAuthenticationDomains,
                restAuthorizationDomains, adminDns, privilegesEvaluator, authenticatedUserCacheTransport, restRoleCache, restImpersonationCache,
                auditLog, authBackendFailureListeners, authBackendClientBlockRegistries, ipAuthFailureListeners, Collections.emptyList(),
                anonymousAuthEnabled, debug).authenticate(onResult, onFailure);

    }

    public void authenticateApi(Map<String, Object> request, RestRequest restRequest, List<String> requiredLoginPrivileges,
            ThreadContext threadContext, Consumer<AuthczResult> onResult, Consumer<Exception> onFailure) {

        final TransportAddress remoteAddress = xffResolver.resolve(restRequest);

        if (log.isTraceEnabled()) {
            log.trace("Rest authentication request from " + remoteAddress + "\n" + request);
        }

        IPAddress remoteIpAddress = null;

        if (remoteAddress != null) {
            remoteIpAddress = ipAddressGenerator.from(remoteAddress.address().getAddress());
        }

        if (isIpBlocked(remoteIpAddress)) {
            if (log.isDebugEnabled()) {
                log.debug("Rejecting REST request because of blocked address: " + remoteAddress);
            }
            auditLog.logBlockedIp(restRequest, remoteAddress.address());
            onResult.accept(new AuthczResult(AuthczResult.Status.STOP, RestStatus.UNAUTHORIZED, "Authentication finally failed"));
            return;
        }

        if (!isInitialized()) {
            log.error("Not yet initialized (you may need to run sgadmin)");
            onResult.accept(new AuthczResult(AuthczResult.Status.STOP, RestStatus.SERVICE_UNAVAILABLE,
                    "Search Guard not initialized (SG11). See https://docs.search-guard.com/latest/sgadmin"));
            return;
        }
        
        String configId = request.get("config_id") != null ? request.get("config_id").toString() : "default";        
                        
        List<AuthenticationDomain<ApiAuthenticationFrontend>> apiAuthenticationDomains = this.apiAuthenticationDomainMap.get(configId);

        if (apiAuthenticationDomains == null) {
            log.error("Invalid config_id: " + configId +  "; available: " + this.apiAuthenticationDomainMap.keySet());
            onResult.accept(new AuthczResult(AuthczResult.Status.STOP, RestStatus.BAD_REQUEST, "Invalid config_id"));
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Using auth domains from frontend config " + configId + ": " + apiAuthenticationDomains);
        }
        
        String mode = request.get("mode") != null ? request.get("mode").toString() : null;

        if (mode != null) {
            apiAuthenticationDomains = apiAuthenticationDomains.stream().filter((d) -> mode.equals(d.getHttpAuthenticator().getType()))
                    .collect(toList());
        }

        String id = request.get("id") != null ? request.get("id").toString() : null;

        if (id != null) {
            apiAuthenticationDomains = apiAuthenticationDomains.stream().filter((d) -> id.equals(d.getId())).collect(toList());
        }
        
        if (log.isDebugEnabled()) {
            log.debug("Auth domains after filtering by mode " + mode + " and id " + id + ": " + apiAuthenticationDomains);
        }
        
        new ApiAuthenticationProcessor(request, restRequest, remoteIpAddress, threadContext, apiAuthenticationDomains, restAuthorizationDomains,
                adminDns, privilegesEvaluator, auditLog, authBackendFailureListeners, authBackendClientBlockRegistries, ipAuthFailureListeners,
                requiredLoginPrivileges, anonymousAuthEnabled, debug).authenticate(onResult, onFailure);

    }

    public String getSsoLogoutUrl(User user) {
        String authType = user.getAttributeAsString(UserAttributes.AUTH_TYPE);

        if (authType == null) {
            // Handle legacy implementations
            return (String) threadPool.getThreadContext().getTransient(ConfigConstants.SSO_LOGOUT_URL);
        }
        
        String frontendConfigId = user.getAttributeAsString(UserAttributes.FRONTEND_CONFIG_ID);
        
        if (frontendConfigId == null) {
            frontendConfigId = "default";
        }
        
        List<AuthenticationDomain<ApiAuthenticationFrontend>> apiAuthenticationDomains = this.apiAuthenticationDomainMap.get(frontendConfigId);
        
        if (apiAuthenticationDomains == null) {
            log.error("Cannot determine logoutUrl because frontend config " + frontendConfigId + " is not known: " + user);
            return null;
        }
        
        apiAuthenticationDomains = apiAuthenticationDomains.stream()
                .filter((d) -> authType.equals(d.getHttpAuthenticator().getType())).collect(toList());

        for (AuthenticationDomain<ApiAuthenticationFrontend> domain : apiAuthenticationDomains) {
            try {
                String logoutUrl = domain.getHttpAuthenticator().getLogoutUrl(user);

                if (logoutUrl != null) {
                    return logoutUrl;
                }
            } catch (Exception e) {
                log.error("Error while determining logoutUrl via " + domain, e);
            }
        }

        return null;
    }

    private void notifyIpAuthFailureListeners(InetAddress remoteAddress, AuthCredentials authCredentials, Object request) {
        for (AuthFailureListener authFailureListener : this.ipAuthFailureListeners) {
            authFailureListener.onAuthFailure(remoteAddress, authCredentials, request);
        }
    }

    private User checkExistsAndAuthz(final Cache<String, User> cache, final User user, final AuthenticationBackend authenticationBackend,
            final Set<AuthorizationDomain> authorizationDomains) {
        if (user == null) {
            return null;
        }

        try {
            //no cache miss in case of noop
            return cache.get(user.getName(), () -> {
                if (log.isTraceEnabled()) {
                    log.trace("Credentials for user " + user.getName() + " not cached, return from " + authenticationBackend.getType()
                            + " backend directly");
                }
                if (authenticationBackend.exists(user)) {
                    authz(user, null, authorizationDomains); //no role cache because no miss here in case of noop
                    return user;
                }

                if (log.isDebugEnabled()) {
                    log.debug("User " + user.getName() + " does not exist in " + authenticationBackend.getType());
                }
                return null;
            });
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Can not check and authorize " + user.getName() + " due to " + e.toString(), e);
            }
            return null;
        }
    }

    private void authz(User authenticatedUser, Cache<User, Set<String>> roleCache, Set<AuthorizationDomain> authorizationDomains) {
        if (authenticatedUser == null) {
            return;
        }

        if (roleCache != null) {
            final Set<String> cachedBackendRoles = roleCache.getIfPresent(authenticatedUser);

            if (cachedBackendRoles != null) {
                authenticatedUser.addRoles(new HashSet<>(cachedBackendRoles));
                return;
            }
        }

        if (authorizationDomains == null || authorizationDomains.isEmpty()) {
            return;
        }

        for (AuthorizationDomain authorizationDomain : authorizationDomains) {            
            List<String> skippedUsers = authorizationDomain.getSkippedUsers();            

            if (!skippedUsers.isEmpty() && authenticatedUser.getName() != null
                    && WildcardMatcher.matchAny(skippedUsers, authenticatedUser.getName())) {
                if (log.isDebugEnabled()) {
                    log.debug("Skipped authorization of user {}", authenticatedUser.getName());
                }
                continue;
            }

            if (!(authorizationDomain.getAuthorizationBackend() instanceof SyncAuthorizationBackend)) {
                continue;
            }
            
            SyncAuthorizationBackend ab = (SyncAuthorizationBackend) authorizationDomain.getAuthorizationBackend();
            try {
                if (log.isTraceEnabled()) {
                    log.trace("Backend roles for " + authenticatedUser.getName() + " not cached, return from " + ab.getType() + " backend directly");
                }
                ab.fillRoles(authenticatedUser, AuthCredentials.forUser(authenticatedUser.getName()).build());
            } catch (Exception e) {
                log.error("Cannot retrieve roles for {} from {} due to {}", authenticatedUser, ab.getType(), e.toString(), e);
            }
        }
        if (roleCache != null) {
            roleCache.put(authenticatedUser, new HashSet<>(authenticatedUser.getRoles()));
        }
    }

    private User authcz(final Cache<AuthCredentials, User> cache, Cache<User, Set<String>> roleCache, final AuthCredentials ac,
            final SyncAuthenticationBackend authBackend, Set<AuthorizationDomain> authorizationDomains) {
        if (ac == null) {
            return null;
        }

        try {            
            AuthenticationBackend.UserCachingPolicy cachingPolicy = authBackend.userCachingPolicy();

            if (cachingPolicy == UserCachingPolicy.NEVER) {
                User authenticatedUser = authBackend.authenticate(ac);

                if (!ac.isAuthzComplete() && !authenticatedUser.isAuthzComplete()) {
                    authz(authenticatedUser, roleCache, authorizationDomains);
                }

                return authenticatedUser;
            } else if (cachingPolicy == UserCachingPolicy.ONLY_IF_AUTHZ_SEPARATE && authorizationDomains.isEmpty()) {
                // noop backend 
                // that means authc and authz was completely done via HTTP (like JWT or PKI)

                return authBackend.authenticate(ac);
            }

            return cache.get(ac, () -> {
                if (log.isTraceEnabled()) {
                    log.trace("Credentials for user " + ac.getUsername() + " not cached, return from " + authBackend.getType() + " backend directly");
                }
                final User authenticatedUser = authBackend.authenticate(ac);

                if (!ac.isAuthzComplete() && !authenticatedUser.isAuthzComplete()) {
                    authz(authenticatedUser, roleCache, authorizationDomains);
                }

                return authenticatedUser;
            });
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Can not authenticate " + ac.getUsername() + " due to " + e.toString(), e);
            }
            return null;
        } finally {
            ac.clearSecrets();
        }
    }

    private User impersonate(final User origPKIuser) throws ElasticsearchSecurityException {
        final String impersonatedUser = threadPool.getThreadContext().getHeader("sg_impersonate_as");

        if (Strings.isNullOrEmpty(impersonatedUser)) {
            return null; //nothing to do
        }

        if (!isInitialized()) {
            throw new ElasticsearchSecurityException("Could not check for impersonation because Search Guard is not yet initialized");
        }

        if (origPKIuser == null) {
            throw new ElasticsearchSecurityException("no original PKI user found");
        }

        if (adminDns.isAdminDN(impersonatedUser)) {
            throw new ElasticsearchSecurityException(
                    "'" + origPKIuser.getName() + "' is not allowed to impersonate as an adminuser  '" + impersonatedUser + "'");
        }

        try {
            if (!adminDns.isTransportImpersonationAllowed(new LdapName(origPKIuser.getName()), impersonatedUser)) {
                throw new ElasticsearchSecurityException(
                        "'" + origPKIuser.getName() + "' is not allowed to impersonate as transport user '" + impersonatedUser + "'");
            } else {
                for (final AuthenticationDomain<HTTPAuthenticator> authenticationDomain : transportAuthenticationDomains) {
                    final AuthenticationBackend authenticationBackend = authenticationDomain.getBackend();
                    final User impersonatedUserObject = checkExistsAndAuthz(transportImpersonationCache,
                            new User(impersonatedUser, AuthDomainInfo.IMPERSONATION_TLS), authenticationBackend, transportAuthorizationDomains);

                    if (impersonatedUserObject == null) {
                        log.debug(
                                "Unable to impersonate transport user from '{}' to '{}' because the impersonated user does not exists in {}, try next ...",
                                origPKIuser.getName(), impersonatedUser, authenticationBackend.getType());
                        continue;
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Impersonate transport user from '{}' to '{}'", origPKIuser.getName(), impersonatedUser);
                    }
                    return impersonatedUserObject;
                }

                log.debug("Unable to impersonate transport user from '{}' to '{}' because the impersonated user does not exists",
                        origPKIuser.getName(), impersonatedUser);
                throw new ElasticsearchSecurityException("No such transport user: " + impersonatedUser, RestStatus.FORBIDDEN);
            }
        } catch (final InvalidNameException e1) {
            throw new ElasticsearchSecurityException("PKI does not have a valid name ('" + origPKIuser.getName() + "'), should never happen", e1);
        }
    }

    private User resolveTransportUsernameAttribute(User pkiUser) {
        //#547
        if (transportUsernameAttribute != null && !transportUsernameAttribute.isEmpty()) {
            try {
                final LdapName sslPrincipalAsLdapName = new LdapName(pkiUser.getName());
                for (final Rdn rdn : sslPrincipalAsLdapName.getRdns()) {
                    if (rdn.getType().equals(transportUsernameAttribute)) {
                        return new User((String) rdn.getValue(), AuthDomainInfo.from(pkiUser));
                    }
                }
            } catch (InvalidNameException e) {
                //cannot happen
            }
        }

        return pkiUser;
    }

    private boolean isIpBlocked(IPAddress address) {
        if (address == null) {
            log.warn("isIpBlocked(null)");
            return false;
        }

        if ((this.ipClientBlockRegistries == null || this.ipClientBlockRegistries.isEmpty())
                && (this.blockedNetmasks == null || this.blockedNetmasks.isEmpty())) {
            return false;
        }

        InetAddress inetAddress = address.toInetAddress();

        if (ipClientBlockRegistries != null) {
            for (ClientBlockRegistry<InetAddress> clientBlockRegistry : ipClientBlockRegistries) {
                if (clientBlockRegistry.isBlocked(inetAddress)) {
                    return true;
                }
            }
        }

        if (blockedNetmasks != null) {
            for (ClientBlockRegistry<IPAddress> registry : blockedNetmasks) {
                if (registry.isBlocked(address)) {
                    return true;
                }
            }
        }

        return false;
    }

    private boolean isUserBlocked(String authBackend, String userName) {
        if (this.authBackendClientBlockRegistries == null) {
            return false;
        }

        Collection<ClientBlockRegistry<String>> blockedUsers = authBackendClientBlockRegistries.get(BLOCKED_USERS);

        if (blockedUsers != null) {
            for (ClientBlockRegistry<String> registry : blockedUsers) {
                if (registry.isBlocked(userName)) {
                    return true;
                }
            }
        }

        Collection<ClientBlockRegistry<String>> clientBlockRegistries = this.authBackendClientBlockRegistries.get(authBackend);

        if (clientBlockRegistries.isEmpty()) {
            return false;
        }

        for (ClientBlockRegistry<String> clientBlockRegistry : clientBlockRegistries) {
            if (clientBlockRegistry.isBlocked(userName)) {
                return true;
            }
        }

        return false;
    }

    public SgDynamicConfiguration<FrontendConfig> getFrontendConfig() {
        return frontendConfig;
    }

    public boolean isDebugEnabled() {
        return debug;
    }

}
