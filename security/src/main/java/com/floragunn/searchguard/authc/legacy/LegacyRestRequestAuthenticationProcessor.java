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

package com.floragunn.searchguard.authc.legacy;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.AuthFailureListener;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.base.AuthczResult;
import com.floragunn.searchguard.authc.base.RequestAuthenticationProcessor;
import com.floragunn.searchguard.authc.blocking.BlockedUserRegistry;
import com.floragunn.searchguard.authc.rest.authenticators.HTTPAuthenticator;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.filter.TenantAwareRestHandler;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.google.common.cache.Cache;

public class LegacyRestRequestAuthenticationProcessor extends RequestAuthenticationProcessor<HTTPAuthenticator> {
    private static final Logger log = LogManager.getLogger(LegacyRestRequestAuthenticationProcessor.class);

    private final MetaRequestInfo authDomainMetaRequest;
    private final boolean isAuthDomainMetaRequest;
    private final RestHandler restHandler;
    private final RestRequest restRequest;
    private final RestChannel restChannel;

    private LinkedHashSet<String> challenges = new LinkedHashSet<>(2);

    public LegacyRestRequestAuthenticationProcessor(RestHandler restHandler, RequestMetaData<RestRequest> request, RestChannel restChannel,
            ThreadContext threadContext, Collection<AuthenticationDomain<HTTPAuthenticator>> authenticationDomains, AdminDNs adminDns,
            PrivilegesEvaluator privilegesEvaluator, Cache<AuthCredentials, User> userCache, Cache<String, User> impersonationCache,
            AuditLog auditLog, BlockedUserRegistry blockedUserRegistry, List<AuthFailureListener> ipAuthFailureListeners,
            List<String> requiredLoginPrivileges, boolean debug) {
        super(request, threadContext, authenticationDomains, adminDns, privilegesEvaluator, userCache, impersonationCache, auditLog,
                blockedUserRegistry, ipAuthFailureListeners, requiredLoginPrivileges, debug);

        this.restHandler = restHandler;
        this.restRequest = request.getRequest();
        this.restChannel = restChannel;
        this.authDomainMetaRequest = checkAuthDomainMetaRequest(restRequest);
        this.isAuthDomainMetaRequest = authDomainMetaRequest != null;
    }

    @Override
    protected AuthDomainState handleCurrentAuthenticationDomain(AuthenticationDomain<HTTPAuthenticator> authenticationDomain,
            Consumer<AuthczResult> onResult, Consumer<Exception> onFailure) {
        HTTPAuthenticator httpAuthenticator = authenticationDomain.getFrontend();

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

        AuthCredentials ac;
        try {
            ac = httpAuthenticator.extractCredentials(restRequest, threadContext);
        } catch (Exception e1) {
            log.warn("'{}' extracting credentials from {} http authenticator", e1.toString(), httpAuthenticator.getType(), e1);
            return AuthDomainState.SKIP;
        }

        if (ac != null && isUserBlocked(authenticationDomain.getType(), ac.getUsername())) {
            if (log.isDebugEnabled()) {
                log.debug("Rejecting REST request because of blocked user: " + ac.getUsername() + "; authDomain: " + authenticationDomain);
            }
            auditLog.logBlockedUser(ac, false, ac, restRequest);
            return AuthDomainState.SKIP;
        }

        if (ac == null) {
            log.debug("no {} credentials found in request", authenticationDomain.getFrontend().getType());

            String challenge = httpAuthenticator.getChallenge(ac);

            if (challenge != null) {
                challenges.add(challenge);
            }

            return AuthDomainState.SKIP;
        } else {
            org.apache.logging.log4j.ThreadContext.put("user", ac.getUsername());
            if (!ac.isComplete()) {
                //credentials found in request but we need another client challenge
                String challenge = httpAuthenticator.getChallenge(ac);

                if (challenge != null) {
                    challenges.add(challenge);
                    ac.clearSecrets();
                    return AuthDomainState.STOP;
                }
            }

            ac = ac.attributesForUserMapping(ImmutableMap.of("request", ImmutableMap.of("headers", restRequest.getHeaders(), "direct_ip_address",
                    String.valueOf(request.getDirectIpAddress()), "originating_ip_address", String.valueOf(request.getOriginatingIpAddress()))));

            return proceed(ac, authenticationDomain, onResult, onFailure);
        }

    }

    @Override
    protected AuthczResult handleChallenge() {

        if (challenges.size() == 0) {
            return null;
        }

        if (log.isDebugEnabled()) {
            log.debug("Sending WWW-Authenticate: " + String.join(", ", challenges));
        }

        BytesRestResponse wwwAuthenticateResponse = new BytesRestResponse(RestStatus.UNAUTHORIZED, "Unauthorized");
        wwwAuthenticateResponse.addHeader("WWW-Authenticate", String.join(", ", challenges));

        restChannel.sendResponse(wwwAuthenticateResponse);

        return AuthczResult.STOP;
    }

    @Override
    protected String getRequestedTenant() {
        if (restHandler instanceof TenantAwareRestHandler) {
            return ((TenantAwareRestHandler) restHandler).getTenantName(restRequest);
        } else {
            return restRequest.header("sgtenant") != null ? restRequest.header("sgtenant") : restRequest.header("sg_tenant");
        }
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
