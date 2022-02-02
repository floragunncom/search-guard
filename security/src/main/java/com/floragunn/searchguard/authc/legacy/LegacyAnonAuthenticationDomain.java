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

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;

import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.AuthenticationBackend.UserMapper;
import com.floragunn.searchguard.authc.rest.authenticators.HTTPAuthenticator;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

public class LegacyAnonAuthenticationDomain implements AuthenticationDomain<HTTPAuthenticator> {

    @Override
    public HTTPAuthenticator getFrontend() {
        return frontend;
    }

    @Override
    public String getId() {
        return "anon";
    }

    @Override
    public boolean accept(RequestMetaData<?> request) {
        return true;
    }

    @Override
    public boolean accept(AuthCredentials authCredentials) {
        return true;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    private final HTTPAuthenticator frontend = new HTTPAuthenticator() {

        @Override
        public String getType() {
            return "anon";
        }

        @Override
        public boolean reRequestAuthentication(RestChannel channel, AuthCredentials credentials) {
            return false;
        }

        @Override
        public AuthCredentials extractCredentials(RestRequest request, ThreadContext context) throws ElasticsearchSecurityException {
            if (request.header("authorization") == null) {
                return AuthCredentials.forUser("sg_anonymous").backendRoles("sg_anonymous_backendrole").complete().build();
            } else {
                return null;
            }
        }
    };

    @Override
    public String toString() {
        return "anon";
    }

    @Override
    public CompletableFuture<User> authenticate(AuthCredentials authCredentials) throws AuthenticatorUnavailableException, CredentialsException {
        return CompletableFuture.completedFuture(UserMapper.DIRECT.map(authCredentials));
    }

    @Override
    public CompletableFuture<User> impersonate(User originalUser, AuthCredentials authCredentials)
            throws AuthenticatorUnavailableException, CredentialsException {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public String getType() {
        return "anon";
    }

    @Override
    public boolean cacheUser() {
        return false;
    }

}
