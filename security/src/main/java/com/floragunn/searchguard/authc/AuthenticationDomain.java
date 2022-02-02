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

import java.util.concurrent.CompletableFuture;

import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

public interface AuthenticationDomain<AuthenticatorType extends AuthenticationFrontend> {

    AuthenticatorType getFrontend();

    default CredentialsMapper getCredentialsMapper() {
        return (authCredentials) -> authCredentials;
    }

    String getId();

    String getType();

    boolean accept(RequestMetaData<?> request);

    boolean accept(AuthCredentials authCredentials);

    boolean isEnabled();

    CompletableFuture<User> authenticate(AuthCredentials authCredentials, AuthenticationDebugLogger debugInfoConsumer)
            throws AuthenticatorUnavailableException, CredentialsException;

    CompletableFuture<User> impersonate(User originalUser, AuthCredentials authCredentials)
            throws AuthenticatorUnavailableException, CredentialsException;

    boolean cacheUser();

    @FunctionalInterface
    interface CredentialsMapper {
        AuthCredentials mapCredentials(AuthCredentials authCredentials) throws CredentialsException;

        static final CredentialsMapper DIRECT = (authCredentials) -> authCredentials;
    }

}