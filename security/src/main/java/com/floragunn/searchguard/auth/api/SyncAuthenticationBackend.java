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
package com.floragunn.searchguard.auth.api;

import java.util.function.Consumer;

import org.opensearch.OpenSearchSecurityException;

import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

public interface SyncAuthenticationBackend extends AuthenticationBackend {

    /**
     * Validate credentials and return an authenticated user (or throw an OpenSearchSecurityException)
     * <p/>
     * Results of this method are normally cached so that we not need to query the backend for every authentication attempt.
     * <p/> 
     * @param The credentials to be validated, never null
     * @return the authenticated User, never null
     * @throws OpenSearchSecurityException in case an authentication failure 
     * (when credentials are incorrect, the user does not exist or the backend is not reachable)
     */
    User authenticate(AuthCredentials credentials) throws OpenSearchSecurityException;

    /**
     * Validate credentials and return an authenticated user (or throw an OpenSearchSecurityException)
     * <p/>
     * Results of this method are normally cached so that we not need to query the backend for every authentication attempt.
     * <p/> 
     * @param The credentials to be validated, never null
     * @return the authenticated User, never null
     * @throws OpenSearchSecurityException in case an authentication failure 
     * (when credentials are incorrect, the user does not exist or the backend is not reachable)
     */
    default void authenticate(AuthCredentials credentials, Consumer<User> onSuccess, Consumer<Exception> onFailure) {
        try {
            User user = this.authenticate(credentials);
            onSuccess.accept(user);
        } catch (Exception e) {
            onFailure.accept(e);
        }
    }
}
