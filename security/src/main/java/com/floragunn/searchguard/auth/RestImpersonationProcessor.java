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
import java.util.Set;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.rest.RestStatus;

import com.floragunn.searchguard.auth.api.AuthenticationBackend;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.user.AuthDomainInfo;
import com.floragunn.searchguard.user.User;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;

public class RestImpersonationProcessor<AuthenticatorType extends AuthenticationFrontend> {
    private static final Logger log = LogManager.getLogger(RestImpersonationProcessor.class);

    private final User originalUser;

    private final Collection<AuthenticationDomain<AuthenticatorType>> authenticationDomains;
    private final Iterator<AuthenticationDomain<AuthenticatorType>> authenticationDomainIter;
    private final Set<AuthorizationDomain> authorizationDomains;
    private final AdminDNs adminDns;
    private final Cache<String, User> impersonationCache;
    private final String impersonatedUserHeader;
    private boolean cacheResult = true;

    public RestImpersonationProcessor(User originalUser, String impersonatedUserHeader,
            Collection<AuthenticationDomain<AuthenticatorType>> authenticationDomains, Set<AuthorizationDomain> authorizationDomains,
            AdminDNs adminDns, Cache<String, User> impersonationCache) {

        this.originalUser = originalUser;
        this.authenticationDomains = authenticationDomains;
        this.authenticationDomainIter = authenticationDomains.iterator();
        this.authorizationDomains = authorizationDomains;
        this.adminDns = adminDns;
        this.impersonationCache = impersonationCache;

        this.impersonatedUserHeader = impersonatedUserHeader; // restRequest.header("sg_impersonate_as");

        if (Strings.isNullOrEmpty(impersonatedUserHeader) || originalUser == null) {
            throw new IllegalStateException("impersonate() called with " + impersonatedUserHeader + "; " + originalUser);
        }
    }

    public void impersonate(Consumer<AuthczResult> onResult, Consumer<Exception> onFailure) {

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
                    onResult.accept(AuthczResult.pass(impersonatedUser));
                    return;
                }
            }

            checkNextAuthenticationDomains(onResult, onFailure);
        } catch (Exception e) {
            onFailure.accept(e);
        }
    }

    private void checkNextAuthenticationDomains(Consumer<AuthczResult> onResult, Consumer<Exception> onFailure) {

        try {
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
            log.debug("Unable to impersonate rest user from '{}' to '{}' because the impersonated user does not exists", originalUser.getName(),
                    impersonatedUserHeader);

            throw new OpenSearchSecurityException("No such user:" + impersonatedUserHeader, RestStatus.FORBIDDEN);
        } catch (Exception e) {
            onFailure.accept(e);
        }
    }

    private AuthDomainState checkCurrentAuthenticationDomain(AuthenticationDomain<AuthenticatorType> authenticationDomain,
            Consumer<AuthczResult> onResult, Consumer<Exception> onFailure) {

        try {
            if (log.isDebugEnabled()) {
                log.debug("Checking authdomain " + authenticationDomain + " (total: " + this.authenticationDomains.size() + ")");
            }

            AuthenticationBackend authenticationBackend = authenticationDomain.getBackend();
            User impersonatedUser = new User(this.impersonatedUserHeader,
                    AuthDomainInfo.from(this.originalUser).addAuthBackend(authenticationBackend.getType() + "+impersonation"));

            if (authenticationBackend.exists(impersonatedUser)) {
                authz(impersonatedUser, (user) -> {
                    impersonatedUser.setRequestedTenant(originalUser.getRequestedTenant());

                    if (cacheResult && impersonationCache != null) {
                        impersonationCache.put(impersonatedUserHeader, impersonatedUser);
                    }

                    onResult.accept(AuthczResult.pass(impersonatedUser));
                }, (e) -> {
                    log.error("Error while impersonating " + impersonatedUser, e);
                    cacheResult = false;
                    checkNextAuthenticationDomains(onResult, onFailure);
                });
                return AuthDomainState.PENDING;
            } else {
                return AuthDomainState.SKIP;
            }

        } catch (Exception e) {
            log.error("Error while handling auth domain " + authenticationDomain, e);
            return AuthDomainState.SKIP;
        }
    }

    private void authz(User authenticatedUser, Consumer<User> onSuccess, Consumer<Exception> onFailure) {
        new AuthorizationProcessor(authorizationDomains, null).authz(authenticatedUser, onSuccess, onFailure);
    }

    private static enum AuthDomainState {
        PENDING, SKIP, PASS, STOP
    }

}
