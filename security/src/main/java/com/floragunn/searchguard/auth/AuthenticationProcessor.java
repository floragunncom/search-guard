/*
 * Copyright 2015-2021 floragunn GmbH
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.auth.api.AuthenticationBackend;
import com.floragunn.searchguard.auth.api.AuthenticationBackend.UserCachingPolicy;
import com.floragunn.searchguard.auth.blocking.ClientBlockRegistry;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import inet.ipaddr.IPAddress;

public abstract class AuthenticationProcessor<AuthenticatorType extends AuthenticationFrontend> {
    private static final Logger log = LogManager.getLogger(AuthenticationProcessor.class);

    protected final IPAddress remoteIpAddress;
    protected final ThreadContext threadContext;
    protected final AuditLog auditLog;
    protected final RestRequest restRequest;
    protected final boolean anonymousAuthEnabled;

    private final Collection<AuthenticationDomain<AuthenticatorType>> authenticationDomains;
    private final Iterator<AuthenticationDomain<AuthenticatorType>> authenticationDomainIter;
    private final Set<AuthorizationDomain> authorizationDomains;
    private final Multimap<String, AuthFailureListener> authBackendFailureListeners;
    private final List<AuthFailureListener> ipAuthFailureListeners;
    private final Multimap<String, ClientBlockRegistry<String>> authBackendClientBlockRegistries;
    private final AdminDNs adminDns;
    private final Cache<AuthCredentials, User> userCache;
    private final Cache<User, Set<String>> roleCache;
    private final Cache<String, User> impersonationCache;
    private final PrivilegesEvaluator privilegesEvaluator;
    private final List<AuthczResult.DebugInfo> debugInfoList = new ArrayList<>();
    private final boolean debug;
    private final List<String> requiredLoginPrivileges;

    private boolean cacheResult = true;

    protected AuthCredentials authCredenetials = null;

    public AuthenticationProcessor(RestRequest restRequest, IPAddress remoteIpAddress, ThreadContext threadContext,
            Collection<AuthenticationDomain<AuthenticatorType>> authenticationDomains, Set<AuthorizationDomain> authorizationDomains,
            AdminDNs adminDns, PrivilegesEvaluator privilegesEvaluator, Cache<AuthCredentials, User> userCache, Cache<User, Set<String>> roleCache,
            Cache<String, User> impersonationCache, AuditLog auditLog, Multimap<String, AuthFailureListener> authBackendFailureListeners,
            Multimap<String, ClientBlockRegistry<String>> authBackendClientBlockRegistries, List<AuthFailureListener> ipAuthFailureListeners,
            List<String> requiredLoginPrivileges, boolean anonymousAuthEnabled, boolean debug) {

        this.remoteIpAddress = remoteIpAddress;
        this.restRequest = restRequest;
        this.threadContext = threadContext;
        this.authenticationDomains = authenticationDomains;
        this.authenticationDomainIter = authenticationDomains.iterator();
        this.authorizationDomains = authorizationDomains;
        this.authBackendFailureListeners = authBackendFailureListeners;
        this.ipAuthFailureListeners = ipAuthFailureListeners;
        this.authBackendClientBlockRegistries = authBackendClientBlockRegistries;
        this.adminDns = adminDns;
        this.userCache = userCache;
        this.roleCache = roleCache;
        this.impersonationCache = impersonationCache;
        this.auditLog = auditLog;
        this.anonymousAuthEnabled = anonymousAuthEnabled;
        this.privilegesEvaluator = privilegesEvaluator;
        this.debug = debug;
        this.requiredLoginPrivileges = requiredLoginPrivileges;
    }

    public void authenticate(Consumer<AuthczResult> onResult, Consumer<Exception> onFailure) {
        if (authenticationDomains.isEmpty()) {
            log.warn("Cannot authenticated request because no authentication domains are configured: " + this);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Authenticating request using: " + authenticationDomains);
            }
        }
        checkNextAuthenticationDomains(onResult, onFailure);
    }

    protected abstract AuthDomainState handleCurrentAuthenticationDomain(AuthenticationDomain<AuthenticatorType> authenticationDomain,
            Consumer<AuthczResult> onResult, Consumer<Exception> onFailure);

    protected AuthczResult handleChallenge() {
        return null;
    }

    protected String getImpersonationUser() {
        return null;
    }

    protected String getRequestedTenant() {
        return null;
    }

    protected void decorateAuthenticatedUser(User authenticatedUser) {
    }

    protected boolean checkLoginPrivileges(User user) {
        if (requiredLoginPrivileges == null || requiredLoginPrivileges.isEmpty()) {
            return true;
        }

        return privilegesEvaluator.hasClusterPermissions(user, requiredLoginPrivileges, null);
    }

    protected boolean userHasRoles(User user) {
       return user.getRoles().size() != 0 || user.getSearchGuardRoles().size() != 0;
    }
    
    protected void notifyIpAuthFailureListeners(AuthCredentials authCredentials) {
        for (AuthFailureListener authFailureListener : this.ipAuthFailureListeners) {
            authFailureListener.onAuthFailure(remoteIpAddress != null ? remoteIpAddress.toInetAddress() : null, authCredentials, restRequest);
        }
    }

    private void checkNextAuthenticationDomains(Consumer<AuthczResult> onResult, Consumer<Exception> onFailure) {

        while (authenticationDomainIter.hasNext()) {
            AuthenticationDomain<AuthenticatorType> authenticationDomain = authenticationDomainIter.next();

            AuthDomainState state = checkCurrentAuthenticationDomain(authenticationDomain, onResult, onFailure);

            if (state == AuthDomainState.PENDING) {
                // will be continued via onSuccess callback
                return;
            } else if (state == AuthDomainState.STOP) {
                onResult.accept(AuthczResult.STOP);
                return;
            }

        }

        if (authCredenetials == null && anonymousAuthEnabled) {
            threadContext.putTransient(ConfigConstants.SG_USER, User.ANONYMOUS);
            auditLog.logSucceededLogin(User.ANONYMOUS, false, null, restRequest);
            if (log.isDebugEnabled()) {
                log.debug("Anonymous User is authenticated");
            }

            onResult.accept(AuthczResult.PASS_ANONYMOUS);
        } else {
            onResult.accept(handleFinalAuthFailure());
        }
    }

    private AuthDomainState checkCurrentAuthenticationDomain(AuthenticationDomain<AuthenticatorType> authenticationDomain,
            Consumer<AuthczResult> onResult, Consumer<Exception> onFailure) {

        try {
            if (log.isDebugEnabled()) {
                log.debug("Checking authdomain " + authenticationDomain + " (total: " + this.authenticationDomains.size() + ")");
            }

            if (authenticationDomain.getEnabledOnlyForIps() != null && !authenticationDomain.getEnabledOnlyForIps().contains(remoteIpAddress)) {
                if (log.isDebugEnabled()) {
                    log.debug("Skipping " + authenticationDomain + " because it is disabled for " + remoteIpAddress + ": "
                            + authenticationDomain.getEnabledOnlyForIps());
                }

                return AuthDomainState.SKIP;
            }

            return handleCurrentAuthenticationDomain(authenticationDomain, onResult, onFailure);
        } catch (Exception e) {
            log.error("Error while handling auth domain " + authenticationDomain, e);
            return AuthDomainState.SKIP;
        }
    }

    protected AuthDomainState proceed(AuthCredentials ac, AuthenticationDomain<AuthenticatorType> authenticationDomain,
            Consumer<AuthczResult> onResult, Consumer<Exception> onFailure) {
        callAuthczBackends(ac, authenticationDomain.getBackend(), (authenticatedUser) -> {
            try {

                ac.clearSecrets();

                if (authenticatedUser != null) {
                    if (adminDns.isAdmin(authenticatedUser)) {
                        log.error("Cannot authenticate rest user because admin user is not permitted to login via HTTP");
                        auditLog.logFailedLogin(authenticatedUser, true, null, restRequest);
                        addDebugInfo(new AuthczResult.DebugInfo(authenticationDomain.getId(), false, "User name is associated with an administrator. These are only allowed to login via certificate."));
                        onResult.accept(AuthczResult.stop(RestStatus.FORBIDDEN,
                                "Cannot authenticate user because admin user is not permitted to login via HTTP", debugInfoList));
                        return;
                    }
                    
                    if (!userHasRoles(authenticatedUser)) {
                        addDebugInfo(new AuthczResult.DebugInfo(authenticationDomain.getId(), false,
                                "User does not have any roles. Please verify the configuration of the authentication frontend and backend.",
                                ImmutableMap.of("claims", ac.getClaims() != null ? ac.getClaims() : Collections.emptyMap())));                      
                    }

                    if (!checkLoginPrivileges(authenticatedUser)) {
                        log.error("Cannot authenticate rest user because user does not have the necessary login privileges: "
                                + requiredLoginPrivileges + "; " + authenticatedUser + "; backend roles: " + authenticatedUser.getRoles()
                                + "; sg roles: " + authenticatedUser.getSearchGuardRoles());
                        addDebugInfo(new AuthczResult.DebugInfo(authenticationDomain.getId(), false,
                                "User does not have the necessary login privileges: " + requiredLoginPrivileges,
                                ImmutableMap.of("backend_roles", authenticatedUser.getRoles(), "sg_roles", authenticatedUser.getSearchGuardRoles())));

                        onResult.accept(AuthczResult.stop(RestStatus.FORBIDDEN, "The user '" + ac.getName() + "' is not allowed to log in.", debugInfoList));
                        return;
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Authcz successful for " + authenticatedUser + " on " + authenticationDomain);
                    }

                    decorateAuthenticatedUser(authenticatedUser);

                    authenticatedUser.setRequestedTenant(getRequestedTenant());

                    if (isImpersonationRequested()) {
                        new RestImpersonationProcessor<AuthenticatorType>(authenticatedUser, getImpersonationUser(), authenticationDomains,
                                authorizationDomains, adminDns, impersonationCache).impersonate((result) -> {
                                    if (result.getUser() != null) {
                                        threadContext.putTransient(ConfigConstants.SG_USER, result.getUser());
                                        auditLog.logSucceededLogin(result.getUser(), false, authenticatedUser, restRequest);
                                    }
                                    onResult.accept(result);
                                }, onFailure);
                    } else {
                        // This is the happy case :-)

                        threadContext.putTransient(ConfigConstants.SG_USER, authenticatedUser);
                        auditLog.logSucceededLogin(authenticatedUser, false, authenticatedUser, restRequest);
                        addDebugInfo(new AuthczResult.DebugInfo(authenticationDomain.getId(), true, "User " + ac.getUsername() + " is logged in"));
                        onResult.accept(AuthczResult.pass(authenticatedUser));
                    }

                } else {
                    addDebugInfo(new AuthczResult.DebugInfo(authenticationDomain.getId(), false,
                            "User " + ac.getUsername() + " could not be authenticated by auth backend"));
                    handleAuthFailure(ac, authenticationDomain, null);
                    checkNextAuthenticationDomains(onResult, onFailure);
                }
            } catch (Exception e) {
                addDebugInfo(new AuthczResult.DebugInfo(authenticationDomain.getId(), false,
                        "Exception while authenticating " + ac.getUsername() + ": " + e));
                log.error(e);
                onFailure.accept(e);
            }
        }, (e) -> {
            addDebugInfo(
                    new AuthczResult.DebugInfo(authenticationDomain.getId(), false, "Exception while authenticating " + ac.getUsername() + ": " + e));

            if (e instanceof OpenSearchSecurityException) {
                handleAuthFailure(ac, authenticationDomain, e);
            } else {
                log.error("Error while authenticating " + ac, e);
            }

            ac.clearSecrets();
            checkNextAuthenticationDomains(onResult, onFailure);
        });

        return AuthDomainState.PENDING;
    }

    protected AuthczResult handleFinalAuthFailure() {

        try {
            log.warn("Authentication finally failed for {} from {}", authCredenetials == null ? null : authCredenetials.getUsername(),
                    remoteIpAddress);

            auditLog.logFailedLogin(authCredenetials, false, null, restRequest);
            notifyIpAuthFailureListeners(authCredenetials);

            AuthczResult challengeHandled = handleChallenge();

            if (challengeHandled != null) {
                return challengeHandled;
            }

            return AuthczResult.stop(RestStatus.UNAUTHORIZED, "Authentication failed", debugInfoList);
        } catch (Exception e) {
            log.error("Error while handling auth failure", e);
            return AuthczResult.stop(RestStatus.UNAUTHORIZED, "Authentication failed", debugInfoList);
        }
    }

    private void callAuthczBackends(AuthCredentials ac, AuthenticationBackend authBackend, Consumer<User> onSuccess, Consumer<Exception> onFailure) {
        // TODO Optimization: If we have SyncAuthenticationBackend use the guarantees of Cache to avoid redundant concurrent loads

        try {
            AuthenticationBackend.UserCachingPolicy cachingPolicy = authBackend.userCachingPolicy();

            if (userCache == null || cachingPolicy == UserCachingPolicy.NEVER) {
                authBackend.authenticate(ac, (authenticatedUser) -> {                    
                    if (ac.isAuthzComplete() || authenticatedUser.isAuthzComplete()) {
                        addDebugInfo(new AuthczResult.DebugInfo(authBackend.getType(), true,
                                "User has been successfully authenticated by auth backend. Authorization information is complete."));
                        
                        onSuccess.accept(authenticatedUser);
                    } else {
                        addDebugInfo(new AuthczResult.DebugInfo(authBackend.getType(), true,
                                "User has been successfully authenticated by auth backend."));

                        authz(authenticatedUser, onSuccess, onFailure);
                    }

                }, onFailure);
            } else if (cachingPolicy == UserCachingPolicy.ONLY_IF_AUTHZ_SEPARATE && authorizationDomains.isEmpty()) {
                // noop backend 
                // that means authc and authz was completely done via HTTP (like JWT or PKI)

                authBackend.authenticate(ac, onSuccess, onFailure);
            } else {
                User user = userCache.getIfPresent(ac);

                if (user != null) {
                    addDebugInfo(new AuthczResult.DebugInfo(authBackend.getType(), true,
                            "User has been successfully authenticated by user cache"));
                    
                    onSuccess.accept(user);
                } else {
                    authBackend.authenticate(ac, (authenticatedUser) -> authzAndCache(ac, authBackend, authenticatedUser, onSuccess, onFailure),
                            onFailure);
                }
            }

        } catch (Exception e) {
            ac.clearSecrets();
            onFailure.accept(e);
        }
    }

    private void authzAndCache(AuthCredentials ac, AuthenticationBackend authBackend, User authenticatedUser, Consumer<User> onSuccess,
            Consumer<Exception> onFailure) {
        try {
            if (log.isTraceEnabled()) {
                log.trace("Auth backend " + authBackend + " returned " + authenticatedUser);
            }

            if (ac.isAuthzComplete() || authenticatedUser.isAuthzComplete()) {
                addDebugInfo(new AuthczResult.DebugInfo(authBackend.getType(), true,
                        "User has been successfully authenticated by auth backend. Authorization information is complete."));

                if (cacheResult && userCache != null) {
                    userCache.put(ac, authenticatedUser);
                }

                onSuccess.accept(authenticatedUser);
            } else {
                addDebugInfo(new AuthczResult.DebugInfo(authBackend.getType(), true,
                        "User has been successfully authenticated by auth backend."));

                authz(authenticatedUser, (authenticatedUser2) -> {
                    if (cacheResult && userCache != null) {
                        userCache.put(ac, authenticatedUser2);
                    }

                    onSuccess.accept(authenticatedUser2);
                }, onFailure);
            }
        } catch (Exception e) {
            log.error(e);
            onFailure.accept(e);
        }
    }

    private void authz(User authenticatedUser, Consumer<User> onSuccess, Consumer<Exception> onFailure) {
        new AuthorizationProcessor(authorizationDomains, roleCache).authz(authenticatedUser, onSuccess, onFailure);
    }

    private void handleAuthFailure(AuthCredentials ac, AuthenticationDomain<AuthenticatorType> authenticationDomain, Exception e) {
        if (log.isDebugEnabled()) {
            log.debug("Cannot authenticate user {} with authdomain {} ({}/{})", ac.getUsername(), authenticationDomain.getBackend().getType(),
                    authenticationDomain.getOrder(), authenticationDomains.size(), e);
        }

        for (AuthFailureListener authFailureListener : this.authBackendFailureListeners.get(authenticationDomain.getBackend().getClass().getName())) {
            authFailureListener.onAuthFailure(remoteIpAddress != null ? remoteIpAddress.toInetAddress() : null, ac, restRequest);
        }

    }

    private boolean isImpersonationRequested() {
        return !Strings.isNullOrEmpty(getImpersonationUser());
    }

    protected boolean isUserBlocked(String authBackend, String userName) {
        if (this.authBackendClientBlockRegistries == null) {
            return false;
        }

        Collection<ClientBlockRegistry<String>> blockedUsers = authBackendClientBlockRegistries.get("BLOCKED_USERS");

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

    protected void addDebugInfo(AuthczResult.DebugInfo debugInfo) {
        if (debug) {
            this.debugInfoList.add(debugInfo);
        }
    }

    protected static enum AuthDomainState {
        PENDING, SKIP, PASS, STOP
    }

    @Override
    public String toString() {
        return "AuthenticationProcessor [remoteIpAddress=" + remoteIpAddress + ", restRequest=" + restRequest + ", anonymousAuthEnabled="
                + anonymousAuthEnabled + ", authenticationDomains=" + authenticationDomains + ", authorizationDomains=" + authorizationDomains
                + ", ipAuthFailureListeners=" + ipAuthFailureListeners + ", debug=" + debug + ", cacheResult=" + cacheResult + "]";
    }

}
