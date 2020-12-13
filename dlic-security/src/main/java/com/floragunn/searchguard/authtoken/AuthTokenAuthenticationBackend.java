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

import org.elasticsearch.ElasticsearchSecurityException;

import com.floragunn.searchguard.auth.SyncAuthenticationBackend;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

public class AuthTokenAuthenticationBackend implements SyncAuthenticationBackend {

    private AuthTokenService authTokenService;

    public AuthTokenAuthenticationBackend(AuthTokenService authTokenService) {
        this.authTokenService = authTokenService;
    }

    @Override
    public String getType() {
        return "sg_auth_token";
    }

    @Override
    public User authenticate(AuthCredentials credentials) throws ElasticsearchSecurityException {
        try {
            AuthToken authToken = authTokenService.getByClaims(credentials.getClaims());

            return User.forUser(authToken.getUserName()).subName("AuthToken " + authToken.getTokenName() + " [" + authToken.getId() + "]")
                    .type(AuthTokenService.USER_TYPE).specialAuthzConfig(authToken.getId()).attributes(authToken.getBase().getAttributes())
                    .authzComplete().build();

        } catch (NoSuchAuthTokenException | InvalidTokenException e) {
            throw new ElasticsearchSecurityException(e.getMessage(), e);
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
