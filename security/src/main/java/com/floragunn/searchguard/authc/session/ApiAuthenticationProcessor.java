/*
 * Copyright 2021-2022 floragunn GmbH
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

package com.floragunn.searchguard.authc.session;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.rest.RestRequest;

import com.floragunn.codova.validation.ConfigValidationException;
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
import com.floragunn.searchguard.user.Attributes;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

public class ApiAuthenticationProcessor extends RequestAuthenticationProcessor<ApiAuthenticationFrontend> {
    private static final Logger log = LogManager.getLogger(ApiAuthenticationProcessor.class);

    private final Map<String, Object> request;
    private final String frontendConfigId;

    public ApiAuthenticationProcessor(Map<String, Object> request, RequestMetaData<RestRequest> requestMetaData, 
            Collection<AuthenticationDomain<ApiAuthenticationFrontend>> authenticationDomains, AdminDNs adminDns,
            PrivilegesEvaluator privilegesEvaluator, AuditLog auditLog, BlockedUserRegistry blockedUserRegistry,
            List<AuthFailureListener> ipAuthFailureListeners, List<String> requiredLoginPrivileges, boolean debug) {
        super(requestMetaData, authenticationDomains, adminDns, privilegesEvaluator, null, null, auditLog, blockedUserRegistry,
                ipAuthFailureListeners, requiredLoginPrivileges, debug);

        this.request = request;
        this.frontendConfigId = request.get("config_id") != null ? request.get("config_id").toString() : "default";
    }

    @Override
    protected AuthDomainState handleCurrentAuthenticationDomain(AuthenticationDomain<ApiAuthenticationFrontend> authenticationDomain,
            Consumer<AuthcResult> onResult, Consumer<Exception> onFailure) {
        ApiAuthenticationFrontend authenticationFrontend = authenticationDomain.getFrontend();

        if (log.isTraceEnabled()) {
            log.trace("Try to extract auth creds from {} authentication frontend", authenticationFrontend.getType());
        }

        final AuthCredentials ac;
        try {
            ac = authenticationFrontend.extractCredentials(request);
        } catch (CredentialsException e) {
            if (log.isTraceEnabled()) {
                log.trace("'{}' extracting credentials from {} authentication frontend", e.toString(), authenticationFrontend.getType(), e);
            }

            debug.add(e.getDebugInfo());
            return AuthDomainState.SKIP;
        } catch (ConfigValidationException e) {
            log.error("'{}' extracting credentials from {} authentication frontend", e.toString(), authenticationFrontend.getType(), e);

            debug.failure(authenticationFrontend.getType(), "Bad API request", "validation_errors", e.getValidationErrors());
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
            // TODO ES 9.1.x restore auditlog call
//            auditLog.logBlockedUser(ac, false, ac, super.request.getRequest());
            debug.failure(authenticationFrontend.getType(), "User " + ac.getUsername() + " is blocked");

            return AuthDomainState.SKIP;
        }

        authCredentials = ac;

        if (ac == null) {
            debug.failure(authenticationFrontend.getType(), "No credentials extracted");

            return AuthDomainState.SKIP;
        } else {
            debug.success(authenticationFrontend.getType(), "User has been identified by auth frontend", "username", ac.getUsername(), "roles",
                    ac.getBackendRoles(), "attributes", ac.getStructuredAttributes(), "claims",
                    ac.getClaims() != null ? ac.getClaims() : Collections.emptyMap());

            if (!authenticationDomain.accept(ac)) {
                if (log.isDebugEnabled()) {
                    log.debug("Skipped authentication of user {}", ac.getUsername());
                }

                debug.failure(authenticationFrontend.getType(), "User " + ac.getUsername() + " is skipped according to auth domain settings");

                ac.clearSecrets();

                return AuthDomainState.SKIP;
            }
        }

        return proceed(ac, authenticationDomain, onResult, onFailure);
    }

    @Override
    protected void decorateAuthenticatedUser(User authenticatedUser) {
        if (this.frontendConfigId != null && !this.frontendConfigId.equals("default")) {
            authenticatedUser.addStructuredAttribute(Attributes.FRONTEND_CONFIG_ID, frontendConfigId);
        }
    }
}
