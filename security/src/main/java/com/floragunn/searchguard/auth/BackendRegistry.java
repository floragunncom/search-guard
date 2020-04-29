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


import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.auth.blocking.ClientBlockRegistry;
import com.floragunn.searchguard.auth.internal.NoOpAuthenticationBackend;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.filter.TenantAwareRestHandler;
import com.floragunn.searchguard.http.XFFResolver;
import com.floragunn.searchguard.sgconf.ConfigModel;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory.DCFListener;
import com.floragunn.searchguard.sgconf.DynamicConfigModel;
import com.floragunn.searchguard.sgconf.InternalUsersModel;
import com.floragunn.searchguard.ssl.util.Utils;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HTTPHelper;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import inet.ipaddr.IPAddressString;
import org.apache.commons.collections.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.*;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class BackendRegistry implements DCFListener {
    private static final String BLOCKED_USERS = "BLOCKED_USERS";
    protected final Logger log = LogManager.getLogger(this.getClass());
    private SortedSet<AuthenticationDomain> restAuthenticationDomains;
    private SortedSet<AuthenticationDomain> transportAuthenticationDomains;
    private Set<AuthorizationDomain> restAuthorizationDomains;
    private Set<AuthorizationDomain> transportAuthorizationDomains;

    private List<AuthFailureListener> ipAuthFailureListeners;
    private Multimap<String, AuthFailureListener> authBackendFailureListeners;
    private List<ClientBlockRegistry<InetAddress>> ipClientBlockRegistries;
    private Multimap<String, ClientBlockRegistry<String>> authBackendClientBlockRegistries;
    private List<ClientBlockRegistry<IPAddressString>> blockedNetmasks;

    private volatile boolean initialized;
    private final AdminDNs adminDns;
    private final XFFResolver xffResolver;
    private volatile boolean anonymousAuthEnabled = false;
    private final Settings esSettings;

    private final AuditLog auditLog;
    private final ThreadPool threadPool;
    private final UserInjector userInjector;
    private final int ttlInMin;
    private Cache<AuthCredentials, User> userCache; //rest standard
    private Cache<String, User> restImpersonationCache; //used for rest impersonation
    private Cache<String, User> userCacheTransport; //transport no creds, possibly impersonated
    private Cache<AuthCredentials, User> authenticatedUserCacheTransport; //transport creds, no impersonation

    private Cache<User, Set<String>> transportRoleCache; //
    private Cache<User, Set<String>> restRoleCache; //
    private Cache<String, User> transportImpersonationCache; //used for transport impersonation

    private volatile String transportUsernameAttribute = null;

    private void createCaches() {
        userCache = CacheBuilder.newBuilder().expireAfterWrite(ttlInMin, TimeUnit.MINUTES)
                .removalListener((RemovalListener<AuthCredentials, User>) notification ->
                        log.debug("Clear user cache for {} due to {}", notification.getKey().getUsername(), notification.getCause())).build();

        userCacheTransport = CacheBuilder.newBuilder().expireAfterWrite(ttlInMin, TimeUnit.MINUTES)
                .removalListener((RemovalListener<String, User>) notification ->
                        log.debug("Clear user cache for {} due to {}", notification.getKey(), notification.getCause())).build();

        authenticatedUserCacheTransport = CacheBuilder.newBuilder().expireAfterWrite(ttlInMin, TimeUnit.MINUTES)
                .removalListener((RemovalListener<AuthCredentials, User>) notification ->
                        log.debug("Clear user cache for {} due to {}", notification.getKey().getUsername(), notification.getCause())).build();

        restImpersonationCache = CacheBuilder.newBuilder().expireAfterWrite(ttlInMin, TimeUnit.MINUTES)
                .removalListener((RemovalListener<String, User>) notification ->
                        log.debug("Clear user cache for {} due to {}", notification.getKey(), notification.getCause())).build();

        transportRoleCache = CacheBuilder.newBuilder().expireAfterWrite(ttlInMin, TimeUnit.MINUTES)
                .removalListener((RemovalListener<User, Set<String>>) notification ->
                        log.debug("Clear user cache for {} due to {}", notification.getKey(), notification.getCause())).build();

        restRoleCache = CacheBuilder.newBuilder().expireAfterWrite(ttlInMin, TimeUnit.MINUTES)
                .removalListener((RemovalListener<User, Set<String>>) notification ->
                        log.debug("Clear user cache for {} due to {}", notification.getKey(), notification.getCause())).build();

        transportImpersonationCache = CacheBuilder.newBuilder().expireAfterWrite(ttlInMin, TimeUnit.MINUTES)
                .removalListener((RemovalListener<String, User>) notification ->
                        log.debug("Clear user cache for {} due to {}", notification.getKey(), notification.getCause())).build();

    }

    public BackendRegistry(final Settings settings, final AdminDNs adminDns,
                           final XFFResolver xffResolver, final AuditLog auditLog, final ThreadPool threadPool) {
        this.adminDns = adminDns;
        this.esSettings = settings;
        this.xffResolver = xffResolver;
        this.auditLog = auditLog;
        this.threadPool = threadPool;
        this.userInjector = new UserInjector(settings, threadPool, auditLog, xffResolver);

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

        ipAuthFailureListeners = dcm.getIpAuthFailureListeners();
        authBackendFailureListeners = dcm.getAuthBackendFailureListeners();
        ipClientBlockRegistries = dcm.getIpClientBlockRegistries();
        authBackendClientBlockRegistries = dcm.getAuthBackendClientBlockRegistries();

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

        //SG6 no default authc
        initialized = !restAuthenticationDomains.isEmpty() || anonymousAuthEnabled;
    }

    public User authenticate(final TransportRequest request, final String sslPrincipal, final Task task, final String action) {
        if (log.isDebugEnabled() && request.remoteAddress() != null) {
            log.debug("Transport authentication request from {}", request.remoteAddress());
        }

        User origPKIUser = new User(sslPrincipal);

        if (adminDns.isAdmin(origPKIUser)) {
            auditLog.logSucceededLogin(origPKIUser.getName(), true, null, request, action, task);
            return origPKIUser;
        }

        if (request.remoteAddress() != null && isIpBlocked(request.remoteAddress().address().getAddress())) {
            if (log.isDebugEnabled()) {
                log.debug("Rejecting transport request because of blocked address: " + request.remoteAddress());
            }
            return null;
        }

        if (!isInitialized()) {
            log.error("Not yet initialized (you may need to run sgadmin)");
            return null;
        }

        final String authorizationHeader = threadPool.getThreadContext().getHeader("Authorization");
        //Use either impersonation OR credentials authentication
        //if both is supplied credentials authentication win
        final AuthCredentials creds = HTTPHelper.extractCredentials(authorizationHeader, log);

        User impersonatedTransportUser = null;

        if (creds != null) {
            if (log.isDebugEnabled()) {
                log.debug("User {} submitted also basic credentials: {}", origPKIUser.getName(), creds);
            }
        }

        //loop over all transport auth domains
        for (final AuthenticationDomain authenticationDomain : transportAuthenticationDomains) {
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
                authenticatedUser = authcz(authenticatedUserCacheTransport, transportRoleCache, creds, authenticationDomain.getBackend(), transportAuthorizationDomains);
            }

            if (authenticatedUser == null) {
                for (AuthFailureListener authFailureListener : authBackendFailureListeners.get(authenticationDomain.getBackend().getClass().getName())) {
                    authFailureListener.onAuthFailure(request.remoteAddress() != null ? request.remoteAddress().address().getAddress() : null, creds,
                            request);
                }

                if (log.isDebugEnabled()) {
                    log.debug("Cannot authenticate transport user {} (or add roles) with authdomain {}/{} of {}, try next",
                            creds == null ? (impersonatedTransportUser == null ? origPKIUser.getName() : impersonatedTransportUser.getName()) : creds.getUsername(),
                            authenticationDomain.getBackend().getType(), authenticationDomain.getOrder(), transportAuthenticationDomains.size());
                }
                continue;
            }

            if (adminDns.isAdmin(authenticatedUser)) {
                log.error("Cannot authenticate transport user because admin user is not permitted to login");
                auditLog.logFailedLogin(authenticatedUser.getName(), true, null, request, task);
                return null;
            }

            if (log.isDebugEnabled()) {
                log.debug("Transport user '{}' is authenticated", authenticatedUser);
            }

            auditLog.logSucceededLogin(authenticatedUser.getName(), false, impersonatedTransportUser == null ? null : origPKIUser.getName(), request,
                    action, task);

            return authenticatedUser;
        } //end looping auth domains

        //auditlog
        if (creds == null) {
            auditLog.logFailedLogin(impersonatedTransportUser == null ? origPKIUser.getName() : impersonatedTransportUser.getName(), false,
                    impersonatedTransportUser == null ? null : origPKIUser.getName(), request, task);
        } else {
            auditLog.logFailedLogin(creds.getUsername(), false, null, request, task);
        }

        log.warn("Transport authentication finally failed for {} from {}",
                creds == null ? impersonatedTransportUser == null ? origPKIUser.getName() : impersonatedTransportUser.getName() : creds.getUsername(),
                request.remoteAddress());

        notifyIpAuthFailureListeners(request.remoteAddress() != null ? request.remoteAddress().address().getAddress() : null, creds, request);

        return null;
    }

    public boolean authenticate(final RestHandler restHandler, final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
        final String sslPrincipal = threadPool.getThreadContext().getTransient(ConfigConstants.SG_SSL_PRINCIPAL);

        if (adminDns.isAdminDN(sslPrincipal)) {
            //PKI authenticated REST call
            threadPool.getThreadContext().putTransient(ConfigConstants.SG_USER, new User(sslPrincipal));
            auditLog.logSucceededLogin(sslPrincipal, true, null, request);
            return true;
        }

        if (request.getHttpChannel().getRemoteAddress() != null && isIpBlocked(request.getHttpChannel().getRemoteAddress().getAddress())) {
            if (log.isDebugEnabled()) {
                log.debug("Rejecting REST request because of blocked address: " + request.getHttpChannel().getRemoteAddress());
            }

            channel.sendResponse(new BytesRestResponse(RestStatus.UNAUTHORIZED, "Authentication finally failed"));
            return false;
        }

        if (userInjector.injectUser(request)) {
            // ThreadContext injected user
            return true;
        }

        if (!isInitialized()) {
            log.error("Not yet initialized (you may need to run sgadmin)");
            channel.sendResponse(new BytesRestResponse(RestStatus.SERVICE_UNAVAILABLE,
                    "Search Guard not initialized (SG11). See https://docs.search-guard.com/latest/sgadmin"));
            return false;
        }

        final TransportAddress remoteAddress = xffResolver.resolve(request);

        if (log.isTraceEnabled()) {
            log.trace("Rest authentication request from {} [original: {}]", remoteAddress, request.getHttpChannel().getRemoteAddress());
        }

        threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);

        boolean authenticated = false;

        User authenticatedUser = null;

        AuthCredentials authCredenetials = null;

        HTTPAuthenticator firstChallengingHttpAuthenticator = null;

        //loop over all http/rest auth domains
        for (final AuthenticationDomain authenticationDomain : restAuthenticationDomains) {
            if (log.isDebugEnabled()) {
                log.debug("Check authdomain for rest {}/{} or {} in total", authenticationDomain.getBackend().getType(),
                        authenticationDomain.getOrder(), restAuthenticationDomains.size());
            }

            final HTTPAuthenticator httpAuthenticator = authenticationDomain.getHttpAuthenticator();

            if (authenticationDomain.isChallenge() && firstChallengingHttpAuthenticator == null) {
                firstChallengingHttpAuthenticator = httpAuthenticator;
            }

            if (log.isTraceEnabled()) {
                log.trace("Try to extract auth creds from {} http authenticator", httpAuthenticator.getType());
            }
            final AuthCredentials ac;
            try {
                ac = httpAuthenticator.extractCredentials(request, threadContext);
            } catch (Exception e1) {
                if (log.isDebugEnabled()) {
                    log.debug("'{}' extracting credentials from {} http authenticator", e1.toString(), httpAuthenticator.getType(), e1);
                }
                continue;
            }

            if (ac != null && isUserBlocked(authenticationDomain.getBackend().getClass().getName(), ac.getUsername())) {
                if (log.isDebugEnabled()) {
                    log.debug("Rejecting REST request because of blocked user: " + ac.getUsername() + "; authDomain: " + authenticationDomain);
                }

                continue;
            }

            authCredenetials = ac;

            if (ac == null) {
                //no credentials found in request
                if (anonymousAuthEnabled) {
                    continue;
                }

                if (authenticationDomain.isChallenge() && httpAuthenticator.reRequestAuthentication(channel, null)) {
                    auditLog.logFailedLogin("<NONE>", false, null, request);
                    log.trace("No 'Authorization' header, send 401 and 'WWW-Authenticate Basic'");
                    return false;
                } else {
                    //no reRequest possible
                    log.trace("No 'Authorization' header, send 403");
                    continue;
                }
            } else {
                List<String> skippedUsers = authenticationDomain.getSkippedUsers();

                if (!skippedUsers.isEmpty() && (WildcardMatcher.matchAny(skippedUsers, ac.getUsername()))) {
                    if (log.isDebugEnabled()) {
                        log.debug("Skipped authentication of user {}", ac.getUsername());
                    }
                    continue;
                }

                org.apache.logging.log4j.ThreadContext.put("user", ac.getUsername());
                if (!ac.isComplete()) {
                    //credentials found in request but we need another client challenge
                    if (httpAuthenticator.reRequestAuthentication(channel, ac)) {
                        //auditLog.logFailedLogin(ac.getUsername()+" <incomplete>", request); --noauditlog
                        return false;
                    } else {
                        //no reRequest possible
                        continue;
                    }
                }
            }

            //http completed
            authenticatedUser = authcz(userCache, restRoleCache, ac, authenticationDomain.getBackend(), restAuthorizationDomains);

            if (authenticatedUser == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Cannot authenticate rest user {} (or add roles) with authdomain {}/{} of {}, try next",
                            ac.getUsername(), authenticationDomain.getBackend().getType(), authenticationDomain.getOrder(), restAuthenticationDomains);
                }

                for (AuthFailureListener authFailureListener : this.authBackendFailureListeners.get(authenticationDomain.getBackend().getClass().getName())) {
                    authFailureListener.onAuthFailure(
                            (request.getHttpChannel().getRemoteAddress() != null) ? request.getHttpChannel().getRemoteAddress().getAddress()
                                    : null,
                            ac, request);
                }
                continue;
            }

            if (adminDns.isAdmin(authenticatedUser)) {
                log.error("Cannot authenticate rest user because admin user is not permitted to login via HTTP");
                auditLog.logFailedLogin(authenticatedUser.getName(), true, null, request);
                channel.sendResponse(new BytesRestResponse(RestStatus.FORBIDDEN,
                        "Cannot authenticate user because admin user is not permitted to login via HTTP"));
                return false;
            }

            final String tenant = getRequestedTenant(restHandler, request);

            if (log.isDebugEnabled()) {
                log.debug("Rest user '{}' is authenticated", authenticatedUser);
                log.debug("sgtenant '{}'", tenant);
            }

            authenticatedUser.setRequestedTenant(tenant);
            authenticated = true;
            break;
        } //end looping auth domains

        if (authenticated) {
            final User impersonatedUser = impersonate(request, authenticatedUser);
            threadContext.putTransient(ConfigConstants.SG_USER, impersonatedUser == null ? authenticatedUser : impersonatedUser);
            auditLog.logSucceededLogin((impersonatedUser == null ? authenticatedUser : impersonatedUser).getName(), false,
                    authenticatedUser.getName(), request);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("User still not authenticated after checking {} auth domains", restAuthenticationDomains.size());
            }

            if (authCredenetials == null && anonymousAuthEnabled) {
                threadContext.putTransient(ConfigConstants.SG_USER, User.ANONYMOUS);
                auditLog.logSucceededLogin(User.ANONYMOUS.getName(), false, null, request);
                if (log.isDebugEnabled()) {
                    log.debug("Anonymous User is authenticated");
                }
                return true;
            }

            if (firstChallengingHttpAuthenticator != null) {

                if (log.isDebugEnabled()) {
                    log.debug("Rerequest with {}", firstChallengingHttpAuthenticator.getClass());
                }

                if (firstChallengingHttpAuthenticator.reRequestAuthentication(channel, null)) {
                    if (log.isDebugEnabled()) {
                        log.debug("Rerequest {} failed", firstChallengingHttpAuthenticator.getClass());
                    }

                    log.warn("Authentication finally failed for {} from {}", authCredenetials == null ? null : authCredenetials.getUsername(),
                            remoteAddress);
                    auditLog.logFailedLogin(authCredenetials == null ? null : authCredenetials.getUsername(), false, null, request);

                    notifyIpAuthFailureListeners(request, authCredenetials);

                    return false;
                }
            }

            log.warn("Authentication finally failed for {} from {}", authCredenetials == null ? null : authCredenetials.getUsername(), remoteAddress);
            auditLog.logFailedLogin(authCredenetials == null ? null : authCredenetials.getUsername(), false, null, request);

            notifyIpAuthFailureListeners(request, authCredenetials);

            channel.sendResponse(new BytesRestResponse(RestStatus.UNAUTHORIZED, "Authentication finally failed"));
            return false;
        }

        return authenticated;
    }

    private String getRequestedTenant(RestHandler restHandler, RestRequest request) {
        if (restHandler instanceof TenantAwareRestHandler) {
            return ((TenantAwareRestHandler) restHandler).getTenantName(request);
        } else {
            return Utils.coalesce(request.header("sgtenant"), request.header("sg_tenant"));
        }
    }

    private void notifyIpAuthFailureListeners(RestRequest request, AuthCredentials authCredentials) {
        notifyIpAuthFailureListeners(
                (request.getHttpChannel().getRemoteAddress() != null) ? request.getHttpChannel().getRemoteAddress().getAddress() : null,
                authCredentials, request);
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

            if (!skippedUsers.isEmpty() && authenticatedUser.getName() != null && WildcardMatcher.matchAny(skippedUsers, authenticatedUser.getName())) {
                if (log.isDebugEnabled()) {
                    log.debug("Skipped authorization of user {}", authenticatedUser.getName());
                }
                continue;
            }

            AuthorizationBackend ab = authorizationDomain.getAuthorizationBackend();
            try {
                if (log.isTraceEnabled()) {
                    log.trace("Backend roles for " + authenticatedUser.getName() + " not cached, return from " + ab.getType() + " backend directly");
                }
                ab.fillRoles(authenticatedUser, new AuthCredentials(authenticatedUser.getName()));
            } catch (Exception e) {
                log.error("Cannot retrieve roles for {} from {} due to {}", authenticatedUser, ab.getType(), e.toString(), e);
            }
        }
        if (roleCache != null) {
            roleCache.put(authenticatedUser, new HashSet<>(authenticatedUser.getRoles()));
        }
    }

    private User authcz(final Cache<AuthCredentials, User> cache, Cache<User, Set<String>> roleCache, final AuthCredentials ac,
                        final AuthenticationBackend authBackend, Set<AuthorizationDomain> authorizationDomains) {
        if (ac == null) {
            return null;
        }
        try {

            //noop backend configured and no authorizers
            //that mean authc and authz was completely done via HTTP (like JWT or PKI)
            if (authBackend.getClass() == NoOpAuthenticationBackend.class && authorizationDomains.isEmpty()) {
                //no cache
                return authBackend.authenticate(ac);
            }

            return cache.get(ac, () -> {
                if (log.isTraceEnabled()) {
                    log.trace("Credentials for user " + ac.getUsername() + " not cached, return from " + authBackend.getType()
                            + " backend directly");
                }
                final User authenticatedUser = authBackend.authenticate(ac);
                authz(authenticatedUser, roleCache, authorizationDomains);

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
                for (final AuthenticationDomain authenticationDomain : transportAuthenticationDomains) {
                    final AuthenticationBackend authenticationBackend = authenticationDomain.getBackend();
                    final User impersonatedUserObject = checkExistsAndAuthz(transportImpersonationCache, new User(impersonatedUser),
                            authenticationBackend, transportAuthorizationDomains);

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

    private User impersonate(final RestRequest request, final User originalUser) throws ElasticsearchSecurityException {
        final String impersonatedUserHeader = request.header("sg_impersonate_as");

        if (Strings.isNullOrEmpty(impersonatedUserHeader) || originalUser == null) {
            return null;
        }

        if (!isInitialized()) {
            throw new ElasticsearchSecurityException("Could not check for impersonation because Search Guard is not yet initialized");
        }

        if (adminDns.isAdminDN(impersonatedUserHeader)) {
            throw new ElasticsearchSecurityException("It is not allowed to impersonate as an adminuser  '" + impersonatedUserHeader + "'",
                    RestStatus.FORBIDDEN);
        }

        if (!adminDns.isRestImpersonationAllowed(originalUser.getName(), impersonatedUserHeader)) {
            throw new ElasticsearchSecurityException(
                    "'" + originalUser.getName() + "' is not allowed to impersonate as '" + impersonatedUserHeader + "'", RestStatus.FORBIDDEN);
        } else {
            //loop over all http/rest auth domains
            for (final AuthenticationDomain authenticationDomain : restAuthenticationDomains) {
                final AuthenticationBackend authenticationBackend = authenticationDomain.getBackend();
                final User impersonatedUser = checkExistsAndAuthz(restImpersonationCache, new User(impersonatedUserHeader), authenticationBackend,
                        restAuthorizationDomains);

                if (impersonatedUser == null) {
                    log.debug("Unable to impersonate rest user from '{}' to '{}' because the impersonated user does not exists in {}, try next ...",
                            originalUser.getName(), impersonatedUserHeader, authenticationBackend.getType());
                    continue;
                }

                if (log.isDebugEnabled()) {
                    log.debug("Impersonate rest user from '{}' to '{}'", originalUser.toStringWithAttributes(), impersonatedUser.toStringWithAttributes());
                }

                impersonatedUser.setRequestedTenant(originalUser.getRequestedTenant());
                return impersonatedUser;
            }

            log.debug("Unable to impersonate rest user from '{}' to '{}' because the impersonated user does not exists", originalUser.getName(),
                    impersonatedUserHeader);
            throw new ElasticsearchSecurityException("No such user:" + impersonatedUserHeader, RestStatus.FORBIDDEN);
        }
    }

    private User resolveTransportUsernameAttribute(User pkiUser) {
        //#547
        if (transportUsernameAttribute != null && !transportUsernameAttribute.isEmpty()) {
            try {
                final LdapName sslPrincipalAsLdapName = new LdapName(pkiUser.getName());
                for (final Rdn rdn : sslPrincipalAsLdapName.getRdns()) {
                    if (rdn.getType().equals(transportUsernameAttribute)) {
                        return new User((String) rdn.getValue());
                    }
                }
            } catch (InvalidNameException e) {
                //cannot happen
            }
        }

        return pkiUser;
    }

    private boolean isIpBlocked(InetAddress address) {
        if ((this.ipClientBlockRegistries == null || this.ipClientBlockRegistries.isEmpty()) &&
                (this.blockedNetmasks == null || this.blockedNetmasks.isEmpty())) {
            return false;
        }

        if (ipClientBlockRegistries != null) {
            for (ClientBlockRegistry<InetAddress> clientBlockRegistry : ipClientBlockRegistries) {
                if (clientBlockRegistry.isBlocked(address)) {
                    return true;
                }
            }
        }

        if (blockedNetmasks != null) {
            for (ClientBlockRegistry<IPAddressString> registry : blockedNetmasks) {
                if (registry.isBlocked(new IPAddressString(address.getHostAddress()))) {
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

}
