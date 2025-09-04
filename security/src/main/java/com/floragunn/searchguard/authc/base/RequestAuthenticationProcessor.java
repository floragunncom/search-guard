/*
 * Copyright 2015-2022 floragunn GmbH
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
package com.floragunn.searchguard.authc.base;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import com.floragunn.searchguard.support.ConfigConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.AuthFailureListener;
import com.floragunn.searchguard.authc.AuthenticationDebugLogger;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.AuthenticationFrontend;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.blocking.BlockedUserRegistry;
import com.floragunn.searchguard.authc.rest.RestImpersonationProcessor;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;

public abstract class RequestAuthenticationProcessor<AuthenticatorType extends AuthenticationFrontend> {
    private static final Logger log = LogManager.getLogger(RequestAuthenticationProcessor.class);

    protected final RequestMetaData<?> request;
    protected final AuditLog auditLog;
    private final Collection<AuthenticationDomain<AuthenticatorType>> authenticationDomains;
    private final Iterator<AuthenticationDomain<AuthenticatorType>> authenticationDomainIter;
    private final List<AuthFailureListener> ipAuthFailureListeners;
    private final BlockedUserRegistry blockedUserRegistry;
    private final AdminDNs adminDns;
    private final Cache<AuthCredentials, User> userCache;
    private final Cache<String, User> impersonationCache;
    private final PrivilegesEvaluator privilegesEvaluator;
    protected final AuthenticationDebugLogger debug;
    private final List<String> requiredLoginPrivileges;

    private boolean cacheResult = true;

    protected AuthCredentials authCredentials = null;

    public RequestAuthenticationProcessor(RequestMetaData<?> request,
            Collection<AuthenticationDomain<AuthenticatorType>> authenticationDomains, AdminDNs adminDns, PrivilegesEvaluator privilegesEvaluator,
            Cache<AuthCredentials, User> userCache, Cache<String, User> impersonationCache, AuditLog auditLog,
            BlockedUserRegistry blockedUserRegistry, List<AuthFailureListener> ipAuthFailureListeners, List<String> requiredLoginPrivileges,
            boolean debug) {

        this.request = request;
        this.authenticationDomains = authenticationDomains;
        this.authenticationDomainIter = authenticationDomains.iterator();
        this.ipAuthFailureListeners = ipAuthFailureListeners;
        this.blockedUserRegistry = blockedUserRegistry;
        this.adminDns = adminDns;
        this.userCache = userCache;
        this.impersonationCache = impersonationCache;
        this.auditLog = auditLog;
        this.privilegesEvaluator = privilegesEvaluator;
        this.requiredLoginPrivileges = requiredLoginPrivileges;
        this.debug = AuthenticationDebugLogger.create(debug);
    }

    public void authenticate(Consumer<AuthcResult> onResult, Consumer<Exception> onFailure) {
        if (authenticationDomains.isEmpty()) {
            log.warn("Cannot authenticate request because no authentication domains are configured: " + this);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Authenticating request using: " + authenticationDomains);
            }
        }
        checkNextAuthenticationDomains(onResult, onFailure);
    }

    protected abstract AuthDomainState handleCurrentAuthenticationDomain(AuthenticationDomain<AuthenticatorType> authenticationDomain,
            Consumer<AuthcResult> onResult, Consumer<Exception> onFailure);

    protected AuthcResult handleChallenge() {
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

    protected boolean checkLoginPrivileges(User user) throws PrivilegesEvaluationException {
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
            authFailureListener.onAuthFailure(request.getOriginatingIpAddress() != null ? request.getOriginatingIpAddress().toInetAddress() : null,
                    authCredentials, request.getRequest());
        }
    }

    private void checkNextAuthenticationDomains(Consumer<AuthcResult> onResult, Consumer<Exception> onFailure) {

        while (authenticationDomainIter.hasNext()) {
            AuthenticationDomain<AuthenticatorType> authenticationDomain = authenticationDomainIter.next();

            AuthDomainState state = checkCurrentAuthenticationDomain(authenticationDomain, onResult, onFailure);

            if (state == AuthDomainState.PENDING) {
                // will be continued via onSuccess callback
                return;
            } else if (state == AuthDomainState.STOP) {
                onResult.accept(AuthcResult.STOP);
                return;
            }

        }

        onResult.accept(handleFinalAuthFailure());
    }

    private AuthDomainState checkCurrentAuthenticationDomain(AuthenticationDomain<AuthenticatorType> authenticationDomain,
            Consumer<AuthcResult> onResult, Consumer<Exception> onFailure) {

        try {
            if (!authenticationDomain.isEnabled()) {
                return AuthDomainState.SKIP;
            }

            if (log.isTraceEnabled()) {
                log.trace("Checking authdomain " + authenticationDomain + " (total: " + this.authenticationDomains.size() + ")");
            }

            if (!authenticationDomain.accept(request)) {
                if (log.isDebugEnabled()) {
                    log.debug("Skipping " + authenticationDomain + " because it is disabled by acceptance rules: " + request.getDirectIpAddress()
                            + "/" + request.getOriginatingIpAddress());
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
            Consumer<AuthcResult> onResult, Consumer<Exception> onFailure) {
        authCredentials = ac;

        try {
            ac = authenticationDomain.getCredentialsMapper().mapCredentials(ac);
        } catch (CredentialsException e) {
            log.warn("Error while mapping auth credentials for " + authenticationDomain, e);
            debug.add(authenticationDomain.getType(), e.getDebugInfo());
            ac.clearSecrets();

            return AuthDomainState.SKIP;
        }

        authCredentials = ac;

        if (!authenticationDomain.accept(ac)) {
            if (log.isDebugEnabled()) {
                log.debug("Skipped authentication of user {}", ac.getUsername());
            }
            debug.failure(authenticationDomain.getType(), "User was skipped because of access/skip settings of auth domain", "user_name",
                    ac.getUsername());
            ac.clearSecrets();

            return AuthDomainState.SKIP;
        }

        log.trace("Calling {} backends", authenticationDomain.getType());

        final AuthCredentials pendingCredentials = ac;

        callAuthcBackends(ac, authenticationDomain, (authenticatedUser) -> {
            try {

                pendingCredentials.clearSecrets();

                if (authenticatedUser != null) {
                    if (adminDns.isAdmin(authenticatedUser)) {
                        log.error("Cannot authenticate rest user because admin user is not permitted to login via HTTP");
                        // TODO ES 9.1.x restore auditlog call
//                        auditLog.logFailedLogin(authenticatedUser, true, null, request.getRequest());
                        debug.failure(authenticationDomain.getType(),
                                "User name is associated with an administrator. These are only allowed to login via certificate.");
                        onResult.accept(AuthcResult.stop(RestStatus.FORBIDDEN,
                                "Cannot authenticate user because admin user is not permitted to login via HTTP", debug.get()));
                        return;
                    }

                    if (!userHasRoles(authenticatedUser)) {
                        debug.failure(authenticationDomain.getType(),
                                "User does not have any roles. Please verify the configuration of the authentication frontend and backend.",
                                "user_mapping_attributes", pendingCredentials.getAttributesForUserMapping());
                    }

                    if (!checkLoginPrivileges(authenticatedUser)) {
                        log.error("Cannot authenticate rest user because user does not have the necessary login privileges: "
                                + requiredLoginPrivileges + "; " + authenticatedUser + "; backend roles: " + authenticatedUser.getRoles()
                                + "; sg roles: " + authenticatedUser.getSearchGuardRoles() + "; source attributes: "
                                + pendingCredentials.getAttributesForUserMapping());
                        debug.failure(authenticationDomain.getType(), "User does not have the necessary login privileges: " + requiredLoginPrivileges,
                                "backend_roles", authenticatedUser.getRoles(), "sg_roles", authenticatedUser.getSearchGuardRoles(),
                                "source_attributes", pendingCredentials.getAttributesForUserMapping());

                        onResult.accept(AuthcResult.stop(RestStatus.FORBIDDEN,
                                "The user '" + pendingCredentials.getName() + "' is not allowed to log in.", debug.get()));
                        return;
                    }

                    String requestedTenant = getRequestedTenant();

                    if (log.isDebugEnabled()) {
                        log.debug("Authentication successful for " + authenticatedUser.toStringWithAttributes() + " on " + authenticationDomain
                                + " using " + this + "\nrequestedTenant: " + requestedTenant);
                    }

                    decorateAuthenticatedUser(authenticatedUser);

                    authenticatedUser.setRequestedTenant(requestedTenant);

                    if (isImpersonationRequested()) {
                        new RestImpersonationProcessor<AuthenticatorType>(authenticatedUser, getImpersonationUser(), authenticationDomains, adminDns,
                                impersonationCache).impersonate((result) -> {
                                    if (result.getUser() != null) {
                                        // TODO ES 9.1.x restore auditlog call
//                                        auditLog.logSucceededLogin(result.getUser(), false, authenticatedUser, request.getRequest());
                                    }
                                    onResult.accept(result);
                                }, onFailure);
                    } else {
                        // This is the happy case :-)

                        // TODO ES 9.1.x restore auditlog call
//                        auditLog.logSucceededLogin(authenticatedUser, false, authenticatedUser, request.getRequest());
                        if (debug.isEnabled()) {
                            debug.success(authenticationDomain.getType(), "User is logged in", "user",
                                    ImmutableMap.of("name", authenticatedUser.getName(), "roles", authenticatedUser.getRoles(), "search_guard_roles",
                                            authenticatedUser.getSearchGuardRoles(), "attributes", authenticatedUser.getStructuredAttributes()));
                        }
                        onResult.accept(AuthcResult.pass(authenticatedUser, pendingCredentials.getRedirectUri(), debug.get()));
                    }

                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("Could not authenticate user with " + authenticationDomain.getType());
                    }

                    debug.failure(authenticationDomain.getType(),
                            "User " + pendingCredentials.getUsername() + " could not be authenticated by auth backend");
                    handleAuthFailure(pendingCredentials, authenticationDomain, null);
                    checkNextAuthenticationDomains(onResult, onFailure);
                }
            } catch (Exception e) {
                debug.failure(authenticationDomain.getType(), "Exception while authenticating " + pendingCredentials.getUsername() + ": " + e);
                log.error(e);
                onFailure.accept(e);
            }
        }, (e) -> {

            if (e instanceof ElasticsearchSecurityException) {
                debug.failure(authenticationDomain.getType(), e.getMessage());

                handleAuthFailure(pendingCredentials, authenticationDomain, e);
            } else if (e instanceof CredentialsException) {
                if (((CredentialsException) e).getDebugInfo() != null) {
                    debug.add(((CredentialsException) e).getDebugInfo());
                } else {
                    debug.failure(authenticationDomain.getType(), e.getMessage());
                }
                handleAuthFailure(pendingCredentials, authenticationDomain, e);
            } else if (e instanceof AuthenticatorUnavailableException) {
                debug.failure(authenticationDomain.getType(), "Authenticator unavailable: " + e.getMessage(),
                        ((AuthenticatorUnavailableException) e).getDetails());

                log.error("Error while authenticating " + pendingCredentials + "\n" + ((AuthenticatorUnavailableException) e).getDetails(), e);
            } else {
                debug.failure(authenticationDomain.getType(), "Exception while authenticating " + pendingCredentials.getUsername() + ": " + e);
                log.error("Error while authenticating " + pendingCredentials, e);
            }

            pendingCredentials.clearSecrets();
            checkNextAuthenticationDomains(onResult, onFailure);
        });

        return AuthDomainState.PENDING;
    }

    protected AuthcResult handleFinalAuthFailure() {

        try {
            log.warn("Authentication failed for {} from {}", authCredentials == null ? null : authCredentials.getUsername(), request);

            // TODO ES 9.1.x restore auditlog call
//            auditLog.logFailedLogin(authCredentials != null ? authCredentials : AuthCredentials.NONE, false, null, request.getRequest());
            notifyIpAuthFailureListeners(authCredentials);

            AuthcResult challengeHandled = handleChallenge();

            if (challengeHandled != null) {
                return challengeHandled;
            }

            return AuthcResult.stop(RestStatus.UNAUTHORIZED, ConfigConstants.UNAUTHORIZED, debug.get());
        } catch (Exception e) {
            log.error("Error while handling auth failure", e);
            return AuthcResult.stop(RestStatus.UNAUTHORIZED, ConfigConstants.UNAUTHORIZED, debug.get());
        }
    }

    private void callAuthcBackends(AuthCredentials ac, AuthenticationDomain<AuthenticatorType> authenticationDomain, Consumer<User> onSuccess,
            Consumer<Exception> onFailure) {
        // TODO Optimization: If we have SyncAuthenticationBackend use the guarantees of Cache to avoid redundant concurrent loads

        try {

            debug.success(authenticationDomain.getType(), "Extracted credentials", "user_name", ac.getUsername(), "user_mapping_attributes",
                    ac.getAttributesForUserMapping());

            if (userCache == null || !authenticationDomain.cacheUser()) {
                authenticationDomain.authenticate(ac, debug).whenComplete((authenticatedUser, e) -> {
                    if (e != null) {
                        onFailure.accept((Exception) e);
                    } else if (authenticatedUser != null) {
                        debug.success(authenticationDomain.getType(), "User has been successfully authenticated by auth backend.");

                        onSuccess.accept(authenticatedUser);
                    } else {
                        onFailure.accept(new Exception("User not authenticated"));
                    }
                });

            } else {
                User user = userCache.getIfPresent(ac);

                if (user != null) {
                    debug.success(authenticationDomain.getType(), "User has been successfully authenticated by user cache");

                    onSuccess.accept(user);
                } else {
                    authenticationDomain.authenticate(ac, debug).whenComplete((authenticatedUser, e) -> {
                        if (e != null) {
                            onFailure.accept((Exception) e);
                        } else if (authenticatedUser != null) {
                            authzAndCache(ac, authenticationDomain, authenticatedUser, onSuccess, onFailure);
                        } else {
                            onFailure.accept(new CredentialsException("User not authenticated"));
                        }
                    });
                }
            }

        } catch (Exception e) {
            ac.clearSecrets();
            onFailure.accept(e);
        }
    }

    private void authzAndCache(AuthCredentials ac, AuthenticationDomain<AuthenticatorType> authenticationDomain, User authenticatedUser,
            Consumer<User> onSuccess, Consumer<Exception> onFailure) {
        try {
            if (cacheResult && userCache != null) {
                userCache.put(ac, authenticatedUser);
            }

            onSuccess.accept(authenticatedUser);
        } catch (Exception e) {
            log.error(e);
            onFailure.accept(e);
        }
    }

    private void handleAuthFailure(AuthCredentials ac, AuthenticationDomain<AuthenticatorType> authenticationDomain, Exception e) {
        /* TODO
        if (log.isDebugEnabled()) {
            log.debug("Cannot authenticate user {} with authdomain {} ({})", ac.getUsername(), authenticationDomain.getBackend().getType(),
                    authenticationDomains.size(), e);
        }
        
        if (this.authBackendFailureListeners != null) {
            for (AuthFailureListener authFailureListener : this.authBackendFailureListeners
                    .get(authenticationDomain.getBackend().getClass().getName())) {
                authFailureListener.onAuthFailure(
                        request.getOriginatingIpAddress() != null ? request.getOriginatingIpAddress().toInetAddress() : null, ac,
                        request.getRequest());
            }
        }
        */
    }

    private boolean isImpersonationRequested() {
        return !Strings.isNullOrEmpty(getImpersonationUser());
    }

    protected boolean isUserBlocked(String authBackend, String userName) {
        return blockedUserRegistry.isUserBlocked(userName);
    }

    protected static enum AuthDomainState {
        PENDING, SKIP, PASS, STOP
    }

}
