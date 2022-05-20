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
package com.floragunn.searchguard.authc.rest;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.rest.RestStatus;

import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.AuthenticationFrontend;
import com.floragunn.searchguard.authc.base.AuthcResult;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;

public class RestImpersonationProcessor<AuthenticatorType extends AuthenticationFrontend> {
    private static final Logger log = LogManager.getLogger(RestImpersonationProcessor.class);

    private final User originalUser;

    private final Collection<AuthenticationDomain<AuthenticatorType>> authenticationDomains;
    private final Iterator<AuthenticationDomain<AuthenticatorType>> authenticationDomainIter;
    private final AdminDNs adminDns;
    private final Cache<String, User> impersonationCache;
    private final String impersonatedUserHeader;
    private boolean cacheResult = true;

    public RestImpersonationProcessor(User originalUser, String impersonatedUserHeader,
            Collection<AuthenticationDomain<AuthenticatorType>> authenticationDomains, AdminDNs adminDns, Cache<String, User> impersonationCache) {

        this.originalUser = originalUser;
        this.authenticationDomains = authenticationDomains;
        this.authenticationDomainIter = authenticationDomains.iterator();
        this.adminDns = adminDns;
        this.impersonationCache = impersonationCache;

        this.impersonatedUserHeader = impersonatedUserHeader; // restRequest.header("sg_impersonate_as");

        if (Strings.isNullOrEmpty(impersonatedUserHeader) || originalUser == null) {
            throw new IllegalStateException("impersonate() called with " + impersonatedUserHeader + "; " + originalUser);
        }
    }

    public void impersonate(Consumer<AuthcResult> onResult, Consumer<Exception> onFailure) {

        try {
            if (adminDns.isAdminDN(impersonatedUserHeader)) {
                throw new OpenSearchSecurityException("It is not allowed to impersonate as an adminuser  '" + impersonatedUserHeader + "'",
                        RestStatus.FORBIDDEN);
            }

            if (!adminDns.isRestImpersonationAllowed(originalUser.getName(), impersonatedUserHeader)) {
                throw new OpenSearchSecurityException(
                        "'" + originalUser.getName() + "' is not allowed to impersonate as '" + impersonatedUserHeader + "'", RestStatus.FORBIDDEN);
            }

            if (impersonationCache != null) {
                User impersonatedUser = impersonationCache.getIfPresent(impersonatedUserHeader);

                if (impersonatedUser != null) {
                    impersonatedUser.setRequestedTenant(originalUser.getRequestedTenant());
                    onResult.accept(AuthcResult.pass(impersonatedUser));
                    return;
                }
            }

            checkNextAuthenticationDomains(onResult, onFailure);
        } catch (Exception e) {
            onFailure.accept(e);
        }
    }

    private void checkNextAuthenticationDomains(Consumer<AuthcResult> onResult, Consumer<Exception> onFailure) {

        try {
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
            log.debug("Unable to impersonate rest user from '{}' to '{}' because the impersonated user does not exist", originalUser.getName(),
                    impersonatedUserHeader);

            throw new OpenSearchSecurityException("No such user:" + impersonatedUserHeader, RestStatus.FORBIDDEN);
        } catch (Exception e) {
            onFailure.accept(e);
        }
    }

    private AuthDomainState checkCurrentAuthenticationDomain(AuthenticationDomain<AuthenticatorType> authenticationDomain,
            Consumer<AuthcResult> onResult, Consumer<Exception> onFailure) {

        try {
            if (log.isDebugEnabled()) {
                log.debug("Checking authdomain " + authenticationDomain + " (total: " + this.authenticationDomains.size() + ")");
            }
        
            AuthCredentials impersonatedUser = AuthCredentials.forUser(this.impersonatedUserHeader)
                    .build();

            authenticationDomain.impersonate(this.originalUser, impersonatedUser).whenComplete((completedUser, e) -> {
                if (e != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Impersonation on " + authenticationDomain + " failed", e);
                    }
                    onFailure.accept((Exception) e);
                } else if (completedUser != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Impersonation on " + authenticationDomain + " successful: " + completedUser);
                    }

                    completedUser.setRequestedTenant(originalUser.getRequestedTenant());

                    if (cacheResult && impersonationCache != null) {
                        impersonationCache.put(impersonatedUserHeader, completedUser);
                    }

                    onResult.accept(AuthcResult.pass(completedUser));
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Impersonation on " + authenticationDomain + " did not find user information.");
                    }

                    checkNextAuthenticationDomains(onResult, onFailure);
                }
            });

            return AuthDomainState.PENDING;

        } catch (Exception e) {
            log.error("Error while handling auth domain " + authenticationDomain, e);
            return AuthDomainState.SKIP;
        }
    }

    private static enum AuthDomainState {
        PENDING, SKIP, PASS, STOP
    }

}
