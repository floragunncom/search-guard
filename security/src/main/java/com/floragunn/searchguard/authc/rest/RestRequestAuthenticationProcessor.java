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

package com.floragunn.searchguard.authc.rest;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.AuthFailureListener;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
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

public class RestRequestAuthenticationProcessor extends RequestAuthenticationProcessor<HTTPAuthenticator> {
    private static final Logger log = LogManager.getLogger(RestRequestAuthenticationProcessor.class);

    private final RestHandler restHandler;
    private final RestRequest restRequest;

    private LinkedHashSet<String> challenges = new LinkedHashSet<>(2);

    public RestRequestAuthenticationProcessor(RestHandler restHandler, RequestMetaData<RestRequest> request, RestChannel restChannel,
            ThreadContext threadContext, Collection<AuthenticationDomain<HTTPAuthenticator>> authenticationDomains, AdminDNs adminDns,
            PrivilegesEvaluator privilegesEvaluator, Cache<AuthCredentials, User> userCache, Cache<String, User> impersonationCache,
            AuditLog auditLog, BlockedUserRegistry blockedUserRegistry, List<AuthFailureListener> ipAuthFailureListeners,
            List<String> requiredLoginPrivileges, boolean debug) {
        super(request, threadContext, authenticationDomains, adminDns, privilegesEvaluator, userCache, impersonationCache, auditLog,
                blockedUserRegistry, ipAuthFailureListeners, requiredLoginPrivileges, debug);

        this.restHandler = restHandler;
        this.restRequest = request.getRequest();
    }

    @Override
    protected AuthDomainState handleCurrentAuthenticationDomain(AuthenticationDomain<HTTPAuthenticator> authenticationDomain,
            Consumer<AuthczResult> onResult, Consumer<Exception> onFailure) {
        HTTPAuthenticator authenticationFrontend = authenticationDomain.getFrontend();

        if (log.isTraceEnabled()) {
            log.trace("Try to extract auth creds from {} http authenticator", authenticationFrontend.getType());
        }

        AuthCredentials ac;
        try {
            ac = authenticationFrontend.extractCredentials(restRequest, threadContext);
        } catch (CredentialsException e) {
            if (log.isDebugEnabled()) {
                log.debug("'{}' extracting credentials from {} authentication frontend", e.toString(), authenticationFrontend.getType(), e);
            }

            debug.add(e.getDebugInfo());
            return AuthDomainState.SKIP;
        } catch (AuthenticatorUnavailableException e) {
            log.warn("'{}' extracting credentials from {} authentication frontend", e.toString(), authenticationFrontend.getType(), e);

            debug.failure(authenticationFrontend.getType(), e.getMessage());
            return AuthDomainState.SKIP;
        } catch (Exception e) {
            log.error("'{}' extracting credentials from {} authentication frontend", e.toString(), authenticationFrontend.getType(), e);

            debug.failure(authenticationFrontend.getType(), e.toString());
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

            String challenge = authenticationFrontend.getChallenge(ac);

            if (challenge != null) {
                challenges.add(challenge);
                debug.failure(authenticationFrontend.getType(), "No credentials extracted. Sending challenge", "challenge", challenge);
            } else {
                debug.failure(authenticationFrontend.getType(), "No credentials extracted");
            }

            return AuthDomainState.SKIP;
        } else {
            org.apache.logging.log4j.ThreadContext.put("user", ac.getUsername());
            if (!ac.isComplete()) {
                //credentials found in request but we need anot)her client challenge

                String challenge = authenticationFrontend.getChallenge(ac);

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

        return AuthczResult.stop(RestStatus.UNAUTHORIZED, "Unauthorized", ImmutableMap.of("WWW-Authenticate", ImmutableList.of(challenges)),
                debug.get());
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

}
