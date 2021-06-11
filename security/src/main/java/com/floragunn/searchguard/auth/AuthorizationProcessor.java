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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.searchguard.auth.api.AuthorizationBackend;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.google.common.cache.Cache;

public class AuthorizationProcessor {
    private static final Logger log = LogManager.getLogger(AuthorizationProcessor.class);

    private final Set<AuthorizationDomain> authorizationDomains;
    private final Iterator<AuthorizationDomain> authorizationDomainIter;
    private final Cache<User, Set<String>> roleCache;

    private boolean cacheResult = true;


    public AuthorizationProcessor(Set<AuthorizationDomain> authorizationDomains, Cache<User, Set<String>> roleCache) {

        this.authorizationDomains = authorizationDomains;
        this.authorizationDomainIter = authorizationDomains.iterator();

        this.roleCache = roleCache;

    }
   
    public void authz(User authenticatedUser, Consumer<User> onSuccess, Consumer<Exception> onFailure) {
        //        if (authenticatedUser == null) {
        //          return;
        //     }

        if (roleCache != null) {
            final Set<String> cachedBackendRoles = roleCache.getIfPresent(authenticatedUser);

            if (cachedBackendRoles != null) {
                authenticatedUser.addRoles(new HashSet<>(cachedBackendRoles));
                onSuccess.accept(authenticatedUser);
                return;
            }
        }

        if (authorizationDomains == null || authorizationDomains.isEmpty()) {
            onSuccess.accept(authenticatedUser);
            return;
        }

        checkNextAuthzDomain(authenticatedUser, onSuccess, onFailure);
    }

    private void checkNextAuthzDomain(User authenticatedUser, Consumer<User> onSuccess, Consumer<Exception> onFailure) {
        AuthorizationDomain authorizationDomain = nextAuthorizationDomain(authenticatedUser);

        if (authorizationDomain == null) {
            if (roleCache != null && cacheResult) {
                roleCache.put(authenticatedUser, new HashSet<>(authenticatedUser.getRoles()));
            }

            onSuccess.accept(authenticatedUser);
            return;
        }

        AuthorizationBackend authorizationBackend = authorizationDomain.getAuthorizationBackend();
        try {
            if (log.isTraceEnabled()) {
                log.trace("Backend roles for " + authenticatedUser.getName() + " not cached, return from " + authorizationBackend.getType()
                        + " backend directly");
            }

            authorizationBackend.retrieveRoles(authenticatedUser, AuthCredentials.forUser(authenticatedUser.getName()).build(), (roles) -> {
                authenticatedUser.addRoles(roles);
                checkNextAuthzDomain(authenticatedUser, onSuccess, onFailure);
            }, (e) -> {
                log.error("Cannot retrieve roles for {} from {} due to {}", authenticatedUser, authorizationBackend.getType(), e.toString(), e);
                cacheResult = false;
                checkNextAuthzDomain(authenticatedUser, onSuccess, onFailure);
            });

        } catch (Exception e) {
            log.error("Cannot retrieve roles for {} from {} due to {}", authenticatedUser, authorizationBackend.getType(), e.toString(), e);
            cacheResult = false;
            checkNextAuthzDomain(authenticatedUser, onSuccess, onFailure);
        }
    }

    private AuthorizationDomain nextAuthorizationDomain(User authenticatedUser) {
        while (authorizationDomainIter.hasNext()) {
            AuthorizationDomain authorizationDomain = authorizationDomainIter.next();

            List<String> skippedUsers = authorizationDomain.getSkippedUsers();

            if (!skippedUsers.isEmpty() && authenticatedUser.getName() != null
                    && WildcardMatcher.matchAny(skippedUsers, authenticatedUser.getName())) {
                if (log.isDebugEnabled()) {
                    log.debug("Skipped authorization of user {}", authenticatedUser.getName());
                }
                continue;
            }

            return authorizationDomain;
        }

        return null;
    }

 
}
