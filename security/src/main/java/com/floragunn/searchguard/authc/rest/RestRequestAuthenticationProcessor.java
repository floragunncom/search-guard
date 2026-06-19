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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.floragunn.searchguard.SignalsTenantParamResolver;
import com.floragunn.searchguard.support.ConfigConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import com.floragunn.searchguard.authc.base.AuthcResult;
import com.floragunn.searchguard.authc.base.RequestAuthenticationProcessor;
import com.floragunn.searchguard.authc.blocking.BlockedUserRegistry;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.google.common.cache.Cache;

public class RestRequestAuthenticationProcessor extends RequestAuthenticationProcessor<HttpAuthenticationFrontend> {
    private static final Logger log = LogManager.getLogger(RestRequestAuthenticationProcessor.class);

    private final RequestMetaData<RestRequest> request;

    private LinkedHashSet<String> challenges = new LinkedHashSet<>(2);

    public RestRequestAuthenticationProcessor(RequestMetaData<RestRequest> request,
             Collection<AuthenticationDomain<HttpAuthenticationFrontend>> authenticationDomains, AdminDNs adminDns,
            PrivilegesEvaluator privilegesEvaluator, Cache<AuthCredentials, User> userCache, Cache<String, User> impersonationCache,
            AuditLog auditLog, BlockedUserRegistry blockedUserRegistry, List<AuthFailureListener> ipAuthFailureListeners,
            List<String> requiredLoginPrivileges, boolean debug) {
        super(request, authenticationDomains, adminDns, privilegesEvaluator, userCache, impersonationCache, auditLog,
                blockedUserRegistry, ipAuthFailureListeners, requiredLoginPrivileges, debug);

        this.request = request;
    }

    @Override
    protected AuthDomainState handleCurrentAuthenticationDomain(AuthenticationDomain<HttpAuthenticationFrontend> authenticationDomain,
            Consumer<AuthcResult> onResult, Consumer<Exception> onFailure) {
        HttpAuthenticationFrontend authenticationFrontend = authenticationDomain.getFrontend();

        if (log.isTraceEnabled()) {
            log.trace("Try to extract auth creds from {} http authenticator", authenticationFrontend.getType());
        }

        AuthCredentials ac;
        try {
            ac = authenticationFrontend.extractCredentials(request);
        } catch (CredentialsException e) {
            if (log.isTraceEnabled()) {
                log.trace("'{}' extracting credentials from {} authentication frontend", e.toString(), authenticationFrontend.getType(), e);
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
            auditLog.logBlockedUser(ac, false, ac, request.getRequest());
            return AuthDomainState.SKIP;
        }

        if (ac == null) {
            log.trace("no {} credentials found in request", authenticationDomain.getFrontend().getType());

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

            // Two problems require attention here:
            //
            // 1. Serialization: request.getHeaders() is backed by Netty's HttpHeadersMap whose per-key value lists are
            //    non-serializable internal Netty types (HeadersUtils$1). When these values reach User.structuredAttributes
            //    via UserMapping, cross-node transport fails with NotSerializableException. List.copyOf() produces a
            //    plain, serializable list, so it is applied to every value.
            //
            // 2. Null slots in ImmutableMap: Netty's HttpHeadersMap.size() counts every header entry individually,
            //    including duplicates (e.g. two "x-proxy-roles" lines = size 2). Its entrySet(), however, follows
            //    httpHeaders.names() which yields only unique keys and merges duplicates into a single list entry.
            //    ImmutableMap.of(source) allocates its internal array using source.size() but populates it from
            //    source.entrySet(), leaving trailing null slots for any duplicate headers. Calling mapValues() on such
            //    a map passes those null slots to List.copyOf(), causing NullPointerException. Copying into a HashMap
            //    first normalises the map so that size() == entrySet().size() before ImmutableMap wraps it.
            ImmutableMap<String, List<String>> headers = ImmutableMap.of(new HashMap<>(request.getHeaders())).mapValues(List::copyOf);
            ac = ac.userMappingAttributes(ImmutableMap.of("request", ImmutableMap.of("headers", ImmutableMap.of(headers), "direct_ip_address",
                    String.valueOf(request.getDirectIpAddress()), "originating_ip_address", String.valueOf(request.getOriginatingIpAddress()))));
            return proceed(ac, authenticationDomain, onResult, onFailure);
        }

    }

    @Override
    protected AuthcResult handleChallenge(RestRequest restRequest) {

        if (challenges.size() == 0) {
            return null;
        }

        if (log.isDebugEnabled()) {
            log.debug("Sending WWW-Authenticate: " + String.join(", ", challenges));
        }

        return AuthcResult.stop(RestStatus.UNAUTHORIZED, ConfigConstants.UNAUTHORIZED, ImmutableMap.of("WWW-Authenticate", ImmutableList.of(challenges)),
                debug.get());
    }

    @Override
    protected String getRequestedTenant() {
        return SignalsTenantParamResolver.getRequestedTenant(request.getRequest());
    }

    /*@Override
    protected String getRequestedTenant() {
        if (restHandler instanceof TenantAwareRestHandler) {
            return ((TenantAwareRestHandler) restHandler).getTenantName(request);
        } else {
            return request.getHeader("sgtenant") != null ? request.getHeader("sgtenant") : request.getHeader("sg_tenant");
        }
    }*/

    @Override
    protected String getImpersonationUser() {
        return request.getHeader("sg_impersonate_as");
    }

}
