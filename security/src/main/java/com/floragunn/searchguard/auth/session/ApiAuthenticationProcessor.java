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

package com.floragunn.searchguard.auth.session;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.auth.AuthFailureListener;
import com.floragunn.searchguard.auth.AuthczResult;
import com.floragunn.searchguard.auth.AuthenticationDomain;
import com.floragunn.searchguard.auth.AuthenticationProcessor;
import com.floragunn.searchguard.auth.AuthorizationDomain;
import com.floragunn.searchguard.auth.CredentialsException;
import com.floragunn.searchguard.auth.blocking.ClientBlockRegistry;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchguard.user.UserAttributes;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import inet.ipaddr.IPAddress;

public class ApiAuthenticationProcessor extends AuthenticationProcessor<ApiAuthenticationFrontend> {
    private static final Logger log = LogManager.getLogger(ApiAuthenticationProcessor.class);

    private final Map<String, Object> request;
    private final String frontendConfigId;

    public ApiAuthenticationProcessor(Map<String, Object> request, RestRequest restRequest, IPAddress remoteIpAddress, ThreadContext threadContext,
            Collection<AuthenticationDomain<ApiAuthenticationFrontend>> authenticationDomains, Set<AuthorizationDomain> authorizationDomains,
            AdminDNs adminDns, PrivilegesEvaluator privilegesEvaluator, AuditLog auditLog,
            Multimap<String, AuthFailureListener> authBackendFailureListeners,
            Multimap<String, ClientBlockRegistry<String>> authBackendClientBlockRegistries, List<AuthFailureListener> ipAuthFailureListeners,
            List<String> requiredLoginPrivileges, boolean anonymousAuthEnabled, boolean debug) {
        super(restRequest, remoteIpAddress, threadContext, authenticationDomains, authorizationDomains, adminDns, privilegesEvaluator, null, null,
                null, auditLog, authBackendFailureListeners, authBackendClientBlockRegistries, ipAuthFailureListeners, requiredLoginPrivileges,
                anonymousAuthEnabled, debug);

        this.request = request;
        this.frontendConfigId = request.get("config_id") != null ? request.get("config_id").toString() : "default";
    }

    @Override
    protected AuthDomainState handleCurrentAuthenticationDomain(AuthenticationDomain<ApiAuthenticationFrontend> authenticationDomain,
            Consumer<AuthczResult> onResult, Consumer<Exception> onFailure) {
        ApiAuthenticationFrontend authenticationFrontend = authenticationDomain.getHttpAuthenticator();

        if (log.isTraceEnabled()) {
            log.trace("Try to extract auth creds from {} authentication frontend", authenticationFrontend.getType());
        }

        final AuthCredentials ac;
        try {
            ac = authenticationFrontend.extractCredentials(request);

        } catch (CredentialsException e) {
            if (log.isDebugEnabled()) {
                log.debug("'{}' extracting credentials from {} authentication frontend", e.toString(), authenticationFrontend.getType(), e);
            }

            addDebugInfo(e.getDebugInfo());
            return AuthDomainState.SKIP;

        } catch (ConfigValidationException e) {
            if (log.isDebugEnabled()) {
                log.debug("'{}' extracting credentials from {} authentication frontend", e.toString(), authenticationFrontend.getType(), e);
            }

            addDebugInfo(new AuthczResult.DebugInfo(authenticationFrontend.getType(), false, "Bad API request",
                    ImmutableMap.of("validation_errors", e.getValidationErrors())));
            return AuthDomainState.SKIP;

        } catch (Exception e1) {
            if (log.isDebugEnabled()) {
                log.debug("'{}' extracting credentials from {} authentication frontend", e1.toString(), authenticationFrontend.getType(), e1);
            }

            addDebugInfo(new AuthczResult.DebugInfo(authenticationFrontend.getType(), false, e1.toString()));
            return AuthDomainState.SKIP;
        }

        if (ac != null && isUserBlocked(authenticationDomain.getBackend().getClass().getName(), ac.getUsername())) {
            if (log.isDebugEnabled()) {
                log.debug("Rejecting REST request because of blocked user: " + ac.getUsername() + "; authDomain: " + authenticationDomain);
            }
            auditLog.logBlockedUser(ac, false, ac, restRequest);
            addDebugInfo(new AuthczResult.DebugInfo(authenticationFrontend.getType(), false, "User " + ac.getUsername() + " is blocked"));

            return AuthDomainState.SKIP;
        }

        authCredenetials = ac;

        if (ac == null) {
            addDebugInfo(new AuthczResult.DebugInfo(authenticationFrontend.getType(), false, "No credentials extracted"));

            return AuthDomainState.SKIP;
        } else {
            addDebugInfo(
                    new AuthczResult.DebugInfo(authenticationFrontend.getType(), true, "User has been identified by auth frontend: " + ac.getName(),
                            ImmutableMap.of("username", ac.getUsername(), "roles", ac.getBackendRoles(), "attributes", ac.getStructuredAttributes(),
                                    "claims", ac.getClaims() != null ? ac.getClaims() : Collections.emptyMap())));

            List<String> skippedUsers = authenticationDomain.getSkippedUsers();

            if (!skippedUsers.isEmpty() && (WildcardMatcher.matchAny(skippedUsers, ac.getUsername()))) {
                if (log.isDebugEnabled()) {
                    log.debug("Skipped authentication of user {}", ac.getUsername());
                }

                addDebugInfo(new AuthczResult.DebugInfo(authenticationFrontend.getType(), false,
                        "User " + ac.getUsername() + " is skipped according to auth domain settings"));

                ac.clearSecrets();

                return AuthDomainState.SKIP;
            }
        }

        return proceed(ac, authenticationDomain, onResult, onFailure);
    }

    @Override
    protected void decorateAuthenticatedUser(User authenticatedUser) {
        if (this.frontendConfigId != null && !this.frontendConfigId.equals("default")) {
            authenticatedUser.addStructuredAttribute(UserAttributes.FRONTEND_CONFIG_ID, frontendConfigId);
        }
    }
}
