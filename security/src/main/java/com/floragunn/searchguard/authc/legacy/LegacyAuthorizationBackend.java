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
package com.floragunn.searchguard.authc.legacy;

import java.util.concurrent.CompletableFuture;

import org.opensearch.OpenSearchSecurityException;

import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.UserInformationBackend;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

public interface LegacyAuthorizationBackend extends UserInformationBackend {
    /**
     * Populate a {@link User} with backend roles. This method will not be called for cached users.
     * <p/>
     * Add them by calling either {@code user.addRole()} or {@code user.addRoles()}
     * </P>
     * @param user The authenticated user to populate with backend roles, never null
     * @param credentials Credentials to authenticate to the authorization backend, maybe null.
     * <em>This parameter is for future usage, currently always empty credentials are passed!</em> 
     * @throws OpenSearchSecurityException in case when the authorization backend cannot be reached 
     * or the {@code credentials} are insufficient to authenticate to the authorization backend.
     */
    void fillRoles(User user, AuthCredentials credentials) throws AuthenticatorUnavailableException;

    default CompletableFuture<AuthCredentials> getUserInformation(AuthCredentials userInformation) throws AuthenticatorUnavailableException {
        User tempUser = User.forUser(userInformation.getName()).with(userInformation).build();

        fillRoles(tempUser, userInformation);

        return CompletableFuture.completedFuture(AuthCredentials.forUser(userInformation.getName()).backendRoles(tempUser.getRoles())
                .searchGuardRoles(tempUser.getSearchGuardRoles()).build());
    }
}
