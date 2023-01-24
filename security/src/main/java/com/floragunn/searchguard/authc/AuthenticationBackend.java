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
package com.floragunn.searchguard.authc;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import java.util.concurrent.CompletableFuture;

public interface AuthenticationBackend extends ComponentStateProvider {

    /**
     * The type (name) of the authenticator.
     * @return the type
     */
    String getType();

    /**
     * Validate credentials and return an authenticated user
     *
     * @param The credentials to be validated, never null
     * @return the authenticated User, or null if the user does not exist
     * @throws AuthenticatorUnavailableException if the authentication backend is not available right now.
     */
    CompletableFuture<AuthCredentials> authenticate(AuthCredentials authCredentials, Meter meter)
            throws AuthenticatorUnavailableException, CredentialsException;

    /**
     * Are users produced by this authentication backend allowed to be cached in a node-local heap-based cache?
     *
     * In most cases, ALWAYS can be returned here. Return false if the user objects are not suitable for caching AND retrieval of users is fast.
     */
    default UserCachingPolicy userCachingPolicy() {
        return UserCachingPolicy.ALWAYS;
    }

    default ImmutableMap<String, String> describeAvailableUserMappingAttributes() {
        return ImmutableMap.empty();
    }

    enum UserCachingPolicy {
        ALWAYS, ONLY_IF_AUTHZ_SEPARATE, NEVER
    }

    @FunctionalInterface
    interface UserMapper {
        User map(AuthCredentials authCredentials) throws CredentialsException;

        final static UserMapper DIRECT = (authCredentials) -> User.forUser(authCredentials.getName()).with(authCredentials).build();

    }

    final static AuthenticationBackend NOOP = new AuthenticationBackend() {

        private final ComponentState componentState = new ComponentState(0, "authentication_backend", "noop").initialized();

        @Override
        public String getType() {
            return "noop";
        }

        @Override
        public CompletableFuture<AuthCredentials> authenticate(AuthCredentials authCredentials, Meter meter)
                throws AuthenticatorUnavailableException, CredentialsException {
            return CompletableFuture.completedFuture(authCredentials);
        }

        @Override
        public UserCachingPolicy userCachingPolicy() {
            return UserCachingPolicy.ONLY_IF_AUTHZ_SEPARATE;
        }

        @Override
        public ComponentState getComponentState() {
            return componentState;
        }
    };
}
