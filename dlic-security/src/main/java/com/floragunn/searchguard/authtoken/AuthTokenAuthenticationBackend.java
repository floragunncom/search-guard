/*
 * Copyright 2020 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.searchguard.authtoken;

import java.util.function.Consumer;

import org.elasticsearch.ElasticsearchSecurityException;

import com.floragunn.searchguard.auth.api.AuthenticationBackend;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

public class AuthTokenAuthenticationBackend implements AuthenticationBackend {

    private AuthTokenService authTokenService;

    public AuthTokenAuthenticationBackend(AuthTokenService authTokenService) {
        this.authTokenService = authTokenService;
    }

    @Override
    public String getType() {
        return "sg_auth_token";
    }

    @Override
    public void authenticate(AuthCredentials credentials, Consumer<User> onSuccess, Consumer<Exception> onFailure) {
        try {
            authTokenService.getByClaims(credentials.getClaims(), (authToken) -> {
                
                if (authToken.isRevoked()) {
                    onFailure.accept(new ElasticsearchSecurityException("Auth token " + authToken.getId() + " has been revoked"));
                }

                if (authToken.getBase().getConfigVersions() == null && authToken.getRequestedPrivileges().isTotalWildcard()) {
                    // This auth token has no restrictions and no snapshotted base. We can use the current roles. Thus, we can completely initialize the user

                    onSuccess.accept(User.forUser(authToken.getUserName()).type(AuthTokenService.USER_TYPE_FULL_CURRENT_PERMISSIONS)
                            .authDomainInfo(credentials.getAuthDomainInfo().authBackendType(getType()))
                            .backendRoles(authToken.getBase().getBackendRoles()).searchGuardRoles(authToken.getBase().getSearchGuardRoles())
                            .specialAuthzConfig(authToken.getId()).attributes(authToken.getBase().getAttributes()).authzComplete().build());
                } else {
                    // This auth token has restrictions or must use the snapshotted config specified in authToken.getBase().getConfigVersions()
                    // Thus, we won't initialize a "normal" User object. Rather, the user object won't contain any roles, 
                    // as these would not refer to the current configuration. Code which is supposed to support auth tokens with frozen configuration,
                    // needs to use the SpecialPrivilegesEvaluationContextProvider API to retrieve the correct configuration

                    onSuccess.accept(User.forUser(authToken.getUserName()).authDomainInfo(credentials.getAuthDomainInfo().authBackendType(getType()))
                            .subName("AuthToken " + authToken.getTokenName() + " [" + authToken.getId() + "]").type(AuthTokenService.USER_TYPE)
                            .specialAuthzConfig(authToken.getId()).attributes(authToken.getBase().getAttributes()).authzComplete().build());
                }
            }, (noSuchAuthTokenException) -> {
                onFailure.accept(new ElasticsearchSecurityException(noSuchAuthTokenException.getMessage(), noSuchAuthTokenException));
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
        // This is only related to impersonation. Auth tokens don't support impersonation.
        return false;
    }

    @Override
    public UserCachingPolicy userCachingPolicy() {
        return UserCachingPolicy.NEVER;
    }

}
