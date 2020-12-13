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

import com.floragunn.searchguard.auth.AuthenticationBackend;
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
                System.out.println("ATAB " + authToken);
                
                onSuccess.accept(User.forUser(authToken.getUserName())
                        .subName("AuthToken " + authToken.getTokenName() + " [" + authToken.getId() + "]").type(AuthTokenService.USER_TYPE)
                        .specialAuthzConfig(authToken.getId()).attributes(authToken.getBase().getAttributes()).authzComplete().build());
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
