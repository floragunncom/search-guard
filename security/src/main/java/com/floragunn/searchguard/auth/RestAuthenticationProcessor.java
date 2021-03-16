/*
 * Copyright 2015-2020 floragunn GmbH
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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.auth.api.AuthenticationBackend;
import com.floragunn.searchguard.auth.api.AuthenticationBackend.UserCachingPolicy;
import com.floragunn.searchguard.auth.blocking.ClientBlockRegistry;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.filter.TenantAwareRestHandler;
import com.floragunn.searchguard.ssl.util.Utils;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchguard.user.UserInformation;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.collect.Multimap;

import inet.ipaddr.IPAddress;

public class RestAuthenticationProcessor {
    private static final Logger log = LogManager.getLogger(RestAuthenticationProcessor.class);

    private final RestHandler restHandler;
    private final RestRequest restRequest;
    private final RestChannel restChannel;
    private final IPAddress remoteIpAddress;
    private final ThreadContext threadContext;
    private final Collection<AuthenticationDomain> authenticationDomains;
    private final Iterator<AuthenticationDomain> authenticationDomainIter;
    private final Set<AuthorizationDomain> authorizationDomains;
    private final Multimap<String, AuthFailureListener> authBackendFailureListeners;
    private final List<AuthFailureListener> ipAuthFailureListeners;
    private final Multimap<String, ClientBlockRegistry<String>> authBackendClientBlockRegistries;
    private final AdminDNs adminDns;
    private final MetaRequestInfo authDomainMetaRequest;
    private final boolean isAuthDomainMetaRequest;
    private final boolean anonymousAuthEnabled;
    private final Cache<AuthCredentials, User> userCache;
    private final Cache<User, Set<String>> roleCache;
    private final Cache<String, User> impersonationCache;
    private final AuditLog auditLog;

    private boolean cacheResult = true;

    private AuthCredentials authCredenetials = null;

    private HTTPAuthenticator firstChallengingHttpAuthenticator = null;

    public RestAuthenticationProcessor(RestHandler restHandler, RestRequest restRequest, RestChannel restChannel, IPAddress remoteIpAddress,
            ThreadContext threadContext, Collection<AuthenticationDomain> authenticationDomains, Set<AuthorizationDomain> authorizationDomains,
            AdminDNs adminDns, Cache<AuthCredentials, User> userCache, Cache<User, Set<String>> roleCache, Cache<String, User> impersonationCache,
            AuditLog auditLog, Multimap<String, AuthFailureListener> authBackendFailureListeners,
            Multimap<String, ClientBlockRegistry<String>> authBackendClientBlockRegistries, List<AuthFailureListener> ipAuthFailureListeners,
            boolean anonymousAuthEnabled) {
        this.restHandler = restHandler;
        this.restRequest = restRequest;
        this.restChannel = restChannel;
        this.remoteIpAddress = remoteIpAddress;
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
        this.authDomainMetaRequest = checkAuthDomainMetaRequest(restRequest);
        this.isAuthDomainMetaRequest = authDomainMetaRequest != null;
    }

    public void authenticate(Consumer<AuthczResult> onResult, Consumer<Exception> onFailure) {
        checkNextAuthenticationDomains(onResult, onFailure);
    }

    private void checkNextAuthenticationDomains(Consumer<AuthczResult> onResult, Consumer<Exception> onFailure) {

        while (authenticationDomainIter.hasNext()) {
            AuthenticationDomain authenticationDomain = authenticationDomainIter.next();

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

    private AuthDomainState checkCurrentAuthenticationDomain(AuthenticationDomain authenticationDomain, Consumer<AuthczResult> onResult,
            Consumer<Exception> onFailure) {

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

            final HTTPAuthenticator httpAuthenticator = authenticationDomain.getHttpAuthenticator();

            if (authenticationDomain.isChallenge() && firstChallengingHttpAuthenticator == null) {
                firstChallengingHttpAuthenticator = httpAuthenticator;
            }

            if (isAuthDomainMetaRequest && authDomainMetaRequest.authDomainType.equals(httpAuthenticator.getType())
                    && ("_first".equals(authDomainMetaRequest.authDomainId)
                            || authenticationDomain.getId().equals(authDomainMetaRequest.authDomainId))) {

                if (httpAuthenticator.handleMetaRequest(restRequest, restChannel, authDomainMetaRequest.authDomainPath,
                        authDomainMetaRequest.remainingPath, threadContext)) {
                    return AuthDomainState.STOP;
                }
            }

            if (log.isTraceEnabled()) {
                log.trace("Try to extract auth creds from {} http authenticator", httpAuthenticator.getType());
            }

            final AuthCredentials ac;
            try {
                ac = httpAuthenticator.extractCredentials(restRequest, threadContext);
            } catch (Exception e1) {
                if (log.isDebugEnabled()) {
                    log.debug("'{}' extracting credentials from {} http authenticator", e1.toString(), httpAuthenticator.getType(), e1);
                }
                return AuthDomainState.SKIP;
            }

            if (ac != null && isUserBlocked(authenticationDomain.getBackend().getClass().getName(), ac.getUsername())) {
                if (log.isDebugEnabled()) {
                    log.debug("Rejecting REST request because of blocked user: " + ac.getUsername() + "; authDomain: " + authenticationDomain);
                }
                auditLog.logBlockedUser(ac, false, ac, restRequest);
                return AuthDomainState.SKIP;
            }

            authCredenetials = ac;

            if (ac == null) {
                //no credentials found in request
                if (anonymousAuthEnabled) {
                    return AuthDomainState.SKIP;
                }

                if (authenticationDomain.isChallenge() && httpAuthenticator.reRequestAuthentication(restChannel, null)) {
                    auditLog.logFailedLogin(UserInformation.NONE, false, null, restRequest);
                    log.trace("No 'Authorization' header, send 401 and 'WWW-Authenticate Basic'");
                    return AuthDomainState.STOP;
                } else {
                    //no reRequest possible
                    log.trace("No 'Authorization' header, send 403");
                    return AuthDomainState.SKIP;
                }
            } else {
                List<String> skippedUsers = authenticationDomain.getSkippedUsers();

                if (!skippedUsers.isEmpty() && (WildcardMatcher.matchAny(skippedUsers, ac.getUsername()))) {
                    if (log.isDebugEnabled()) {
                        log.debug("Skipped authentication of user {}", ac.getUsername());
                    }

                    ac.clearSecrets();

                    return AuthDomainState.SKIP;
                }

                org.apache.logging.log4j.ThreadContext.put("user", ac.getUsername());
                if (!ac.isComplete()) {
                    //credentials found in request but we need anot)her client challenge
                    if (httpAuthenticator.reRequestAuthentication(restChannel, ac)) {
                        ac.clearSecrets();
                        return AuthDomainState.STOP;
                    } else {
                        ac.clearSecrets();
                        //no reRequest possible
                        return AuthDomainState.SKIP;
                    }
                }
            }

            callAuthczBackends(ac, authenticationDomain.getBackend(), (authenticatedUser) -> {
                try {

                    ac.clearSecrets();

                    if (authenticatedUser != null) {
                        if (adminDns.isAdmin(authenticatedUser)) {
                            log.error("Cannot authenticate rest user because admin user is not permitted to login via HTTP");
                            auditLog.logFailedLogin(authenticatedUser, true, null, restRequest);
                            onResult.accept(AuthczResult.stop(RestStatus.FORBIDDEN,
                                    "Cannot authenticate user because admin user is not permitted to login via HTTP"));
                            return;
                        }

                        if (log.isDebugEnabled()) {
                            log.debug("Authcz successful for " + authenticatedUser + " on " + authenticationDomain);
                        }

                        authenticatedUser.setRequestedTenant(getRequestedTenant(restHandler, restRequest));

                        if (isImpersonationRequested()) {
                            new RestImpersonationProcessor(authenticatedUser, restRequest.header("sg_impersonate_as"), authenticationDomains,
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
                            onResult.accept(AuthczResult.pass(authenticatedUser));
                        }

                    } else {
                        handleAuthFailure(ac, authenticationDomain, null);
                        checkNextAuthenticationDomains(onResult, onFailure);
                    }
                } catch (Exception e) {
                    log.error(e);
                    onFailure.accept(e);
                }
            }, (e) -> {
                if (e instanceof ElasticsearchSecurityException) {
                    handleAuthFailure(ac, authenticationDomain, e);
                } else {
                    log.error("Error while authenticating " + ac, e);
                }

                ac.clearSecrets();
                checkNextAuthenticationDomains(onResult, onFailure);
            });

            return AuthDomainState.PENDING;
        } catch (Exception e) {
            log.error("Error while handling auth domain " + authenticationDomain, e);
            return AuthDomainState.SKIP;
        }
    }

    private void callAuthczBackends(AuthCredentials ac, AuthenticationBackend authBackend, Consumer<User> onSuccess, Consumer<Exception> onFailure) {
        // TODO Optimization: If we have SyncAuthenticationBackend use the guarantees of Cache to avoid redundant concurrent loads

    
        try {
            AuthenticationBackend.UserCachingPolicy cachingPolicy = authBackend.userCachingPolicy();

            if (cachingPolicy == UserCachingPolicy.NEVER) {
                authBackend.authenticate(ac, (authenticatedUser) -> {
                    if (ac.isAuthzComplete() || authenticatedUser.isAuthzComplete()) {
                        onSuccess.accept(authenticatedUser);
                    } else {
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

                if (cacheResult) {
                    userCache.put(ac, authenticatedUser);
                }

                onSuccess.accept(authenticatedUser);
            } else {
                authz(authenticatedUser, (authenticatedUser2) -> {
                    if (cacheResult) {
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

    private void handleAuthFailure(AuthCredentials ac, AuthenticationDomain authenticationDomain, Exception e) {
        if (log.isDebugEnabled()) {
            log.debug("Cannot authenticate user {} with authdomain {} ({}/{})", ac.getUsername(), authenticationDomain.getBackend().getType(),
                    authenticationDomain.getOrder(), authenticationDomains.size(), e);
        }

        for (AuthFailureListener authFailureListener : this.authBackendFailureListeners.get(authenticationDomain.getBackend().getClass().getName())) {
            authFailureListener.onAuthFailure(remoteIpAddress != null ? remoteIpAddress.toInetAddress() : null, ac, restRequest);
        }

    }

    private void notifyIpAuthFailureListeners(AuthCredentials authCredentials) {
        for (AuthFailureListener authFailureListener : this.ipAuthFailureListeners) {
            authFailureListener.onAuthFailure(remoteIpAddress != null ? remoteIpAddress.toInetAddress() : null, authCredentials, restRequest);
        }
    }

    private boolean isImpersonationRequested() {
        return !Strings.isNullOrEmpty(restRequest.header("sg_impersonate_as"));
    }

    private AuthczResult handleFinalAuthFailure() {

        try {
            log.warn("Authentication finally failed for {} from {}", authCredenetials == null ? null : authCredenetials.getUsername(),
                    remoteIpAddress);

            if (firstChallengingHttpAuthenticator != null) {

                if (log.isDebugEnabled()) {
                    log.debug("Rerequest with {}", firstChallengingHttpAuthenticator.getClass());
                }

                if (firstChallengingHttpAuthenticator.reRequestAuthentication(restChannel, null)) {
                    if (log.isDebugEnabled()) {
                        log.debug("Rerequest {} failed", firstChallengingHttpAuthenticator.getClass());
                    }

                    log.warn("Authentication finally failed for {} from {}", authCredenetials == null ? null : authCredenetials.getUsername(),
                            remoteIpAddress);
                    auditLog.logFailedLogin(authCredenetials, false, null, restRequest);

                    notifyIpAuthFailureListeners(authCredenetials);

                    return AuthczResult.STOP;
                }
            }

            auditLog.logFailedLogin(authCredenetials, false, null, restRequest);
            notifyIpAuthFailureListeners(authCredenetials);
            return AuthczResult.stop(RestStatus.UNAUTHORIZED, "Authentication finally failed");
        } catch (Exception e) {
            log.error("Error while handling auth failure", e);
            return AuthczResult.stop(RestStatus.UNAUTHORIZED, "Authentication finally failed");
        }
    }

    private MetaRequestInfo checkAuthDomainMetaRequest(RestRequest restRequest) {
        String prefix = "/_searchguard/auth_domain/";
        String path = restRequest.path();

        if (!path.startsWith(prefix)) {
            return null;
        }

        int nextSlash = path.indexOf('/', prefix.length());

        if (nextSlash <= 0) {
            return null;
        }

        String authDomainId = path.substring(prefix.length(), nextSlash);

        int nextNextSlash = path.indexOf('/', nextSlash + 1);

        String authDomainType = null;
        String authDomainPath = null;
        String remainingPath = "";

        if (nextNextSlash > 0) {
            authDomainPath = path.substring(0, nextNextSlash);
            authDomainType = path.substring(nextSlash + 1, nextNextSlash);
            remainingPath = path.substring(nextNextSlash + 1);
        } else {
            authDomainPath = path;
            authDomainType = path.substring(nextSlash + 1);
        }

        return new MetaRequestInfo(authDomainId, authDomainType, authDomainPath, remainingPath);
    }

    private String getRequestedTenant(RestHandler restHandler, RestRequest request) {
        if (restHandler instanceof TenantAwareRestHandler) {
            return ((TenantAwareRestHandler) restHandler).getTenantName(request);
        } else {
            return Utils.coalesce(request.header("sgtenant"), request.header("sg_tenant"));
        }
    }

    private boolean isUserBlocked(String authBackend, String userName) {
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

    private static class MetaRequestInfo {

        final String authDomainId;
        final String authDomainType;
        final String authDomainPath;
        final String remainingPath;

        public MetaRequestInfo(String authDomainId, String authDomainType, String authDomainPath, String remainingPath) {
            this.authDomainId = authDomainId;
            this.authDomainType = authDomainType;
            this.authDomainPath = authDomainPath;
            this.remainingPath = remainingPath;
        }
    }

    private static enum AuthDomainState {
        PENDING, SKIP, PASS, STOP
    }

}
