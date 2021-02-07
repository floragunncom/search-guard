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

package com.floragunn.searchguard.session;

import java.util.function.Consumer;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.searchguard.auth.api.AuthenticationBackend;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

public class SessionAuthenticationBackend implements AuthenticationBackend {

    private SessionService sessionService;

    public SessionAuthenticationBackend(SessionService sessionService) {
        this.sessionService = sessionService;
    }

    @Override
    public String getType() {
        return "session";
    }

    @Override
    public void authenticate(AuthCredentials credentials, Consumer<User> onSuccess, Consumer<Exception> onFailure) {
        try {
            sessionService.getByClaims(credentials.getClaims(), (sessionToken) -> {
                if (sessionToken.isRevoked()) {
                    onFailure.accept(new ElasticsearchSecurityException("Session " + sessionToken.getId() + " has been expired or deleted",
                            RestStatus.UNAUTHORIZED));
                } else {
                    sessionService.checkExpiryAndTrackAccess(sessionToken, (ok) -> {
                        if (ok) {
                            onSuccess.accept(User.forUser(sessionToken.getUserName()).type(SessionService.USER_TYPE)
                                    .backendRoles(sessionToken.getBase().getBackendRoles()).searchGuardRoles(sessionToken.getBase().getSearchGuardRoles())
                                    .specialAuthzConfig(sessionToken.getId()).attributes(sessionToken.getBase().getAttributes()).authzComplete().build());
                        } else {
                            onFailure.accept(new ElasticsearchSecurityException("Session " + sessionToken.getId() + " has been expired",
                                    RestStatus.UNAUTHORIZED));
                        }
                    }, onFailure);
                }

            }, (noSuchAuthTokenException) -> {
                onFailure.accept(
                        new ElasticsearchSecurityException(noSuchAuthTokenException.getMessage(), RestStatus.UNAUTHORIZED, noSuchAuthTokenException));
            }, (e) -> {
                onFailure.accept(e);
            });

        } catch (InvalidTokenException e) {
            onFailure.accept(new ElasticsearchSecurityException(e.getMessage(), e));
        } catch (Exception e) {
            onFailure.accept(e);
        }
    }

    @Override
    public boolean exists(User user) {
        // This is only related to impersonation. Sessions don't support impersonation.
        return false;
    }

    @Override
    public UserCachingPolicy userCachingPolicy() {
        return UserCachingPolicy.NEVER;
    }

}
