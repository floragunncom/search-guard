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

package com.floragunn.searchguard.auth;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.auth.blocking.ClientBlockRegistry;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchguard.user.UserInformation;
import com.google.common.cache.Cache;
import com.google.common.collect.Multimap;

import inet.ipaddr.IPAddress;

public class HTTPAuthenticationProcessor extends AuthenticationProcessor<HTTPAuthenticator> {
    private static final Logger log = LogManager.getLogger(HTTPAuthenticationProcessor.class);

    private final MetaRequestInfo authDomainMetaRequest;
    private final boolean isAuthDomainMetaRequest;
    private final RestHandler restHandler;
    private final RestRequest restRequest;
    private final RestChannel restChannel;

    private HTTPAuthenticator firstChallengingHttpAuthenticator = null;

    public HTTPAuthenticationProcessor(RestHandler restHandler, RestRequest restRequest, RestChannel restChannel, IPAddress remoteIpAddress,
            ThreadContext threadContext, Collection<AuthenticationDomain<HTTPAuthenticator>> authenticationDomains,
            Set<AuthorizationDomain> authorizationDomains, AdminDNs adminDns, PrivilegesEvaluator privilegesEvaluator,
            Cache<AuthCredentials, User> userCache, Cache<User, Set<String>> roleCache, Cache<String, User> impersonationCache, AuditLog auditLog,
            Multimap<String, AuthFailureListener> authBackendFailureListeners,
            Multimap<String, ClientBlockRegistry<String>> authBackendClientBlockRegistries, List<AuthFailureListener> ipAuthFailureListeners,
            List<String> requiredLoginPrivileges, boolean anonymousAuthEnabled, boolean debug) {
        super(restRequest, remoteIpAddress, threadContext, authenticationDomains, authorizationDomains, adminDns, privilegesEvaluator, userCache,
                roleCache, impersonationCache, auditLog, authBackendFailureListeners, authBackendClientBlockRegistries, ipAuthFailureListeners,
                requiredLoginPrivileges, anonymousAuthEnabled, debug);

        this.restHandler = restHandler;
        this.restRequest = restRequest;
        this.restChannel = restChannel;
        this.authDomainMetaRequest = checkAuthDomainMetaRequest(restRequest);
        this.isAuthDomainMetaRequest = authDomainMetaRequest != null;
    }

    @Override
    protected AuthDomainState handleCurrentAuthenticationDomain(AuthenticationDomain<HTTPAuthenticator> authenticationDomain, Consumer<AuthczResult> onResult,
            Consumer<Exception> onFailure) {
        final HTTPAuthenticator httpAuthenticator = authenticationDomain.getHttpAuthenticator();

        if (authenticationDomain.isChallenge() && firstChallengingHttpAuthenticator == null) {
            firstChallengingHttpAuthenticator = httpAuthenticator;
        }

        if (isAuthDomainMetaRequest && authDomainMetaRequest.authDomainType.equals(httpAuthenticator.getType())
                && ("_first".equals(authDomainMetaRequest.authDomainId) || authenticationDomain.getId().equals(authDomainMetaRequest.authDomainId))) {

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
        
       return proceed(ac, authenticationDomain, onResult, onFailure);
        
    }

    @Override
    protected AuthczResult handleChallenge() {

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

                return AuthczResult.STOP;
            }
        }

        return null;
    }
    
    @Override
    protected String getImpersonationUser() {
        return restRequest.header("sg_impersonate_as");
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
}
