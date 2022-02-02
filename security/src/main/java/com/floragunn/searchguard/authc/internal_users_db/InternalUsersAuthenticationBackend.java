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

package com.floragunn.searchguard.authc.internal_users_db;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import org.bouncycastle.crypto.generators.OpenBSDBCrypt;

import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.authc.AuthenticationBackend;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.UserInformationBackend;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchsupport.util.ImmutableMap;

public class InternalUsersAuthenticationBackend implements AuthenticationBackend, UserInformationBackend {

    public static final String TYPE = "internal_users_db";

    public static interface UserMappingAttributes {
        public static final String USER_ENTRY = "user_entry";
    }

    private final InternalUsersDatabase internalUsersDatabase;

    InternalUsersAuthenticationBackend(InternalUsersDatabase internalUsersDatabase) {
        this.internalUsersDatabase = internalUsersDatabase;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public CompletableFuture<AuthCredentials> authenticate(AuthCredentials authCredentials)
            throws AuthenticatorUnavailableException, CredentialsException {

        InternalUser internalUser = internalUsersDatabase.get(authCredentials.getUsername());

        if (internalUser == null) {
            return CompletableFuture.completedFuture(null);
        }

        byte[] password = authCredentials.getPassword();

        if (password == null || password.length == 0) {
            return CompletableFuture.completedFuture(null);
        }

        ByteBuffer wrap = ByteBuffer.wrap(password);
        CharBuffer buf = StandardCharsets.UTF_8.decode(wrap);
        char[] array = new char[buf.limit()];
        buf.get(array);

        Arrays.fill(password, (byte) 0);

        try {
            if (OpenBSDBCrypt.checkPassword(internalUser.getPasswordHash(), array)) {
                authCredentials = authCredentials.copy()//
                        .backendRoles(internalUser.getBackendRoles())//
                        .searchGuardRoles(internalUser.getSearchGuardRoles())//
                        .userMappingAttribute(UserMappingAttributes.USER_ENTRY,
                                internalUser.toRedactedBasicObject().with("name", authCredentials.getUsername()))//
                        .authDomainInfo(authCredentials.getAuthDomainInfo().authBackendType(getType()))//
                        .build();

                return CompletableFuture.completedFuture(authCredentials);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        } finally {
            Arrays.fill(wrap.array(), (byte) 0);
            Arrays.fill(buf.array(), '\0');
            Arrays.fill(array, '\0');
        }
    }

    @Override
    public CompletableFuture<AuthCredentials> getUserInformation(AuthCredentials authCredentials) throws AuthenticatorUnavailableException {

        InternalUser internalUser = internalUsersDatabase.get(authCredentials.getUsername());

        if (internalUser == null) {
            return CompletableFuture.completedFuture(null);
        }

        return CompletableFuture.completedFuture(authCredentials.copy()//
                .backendRoles(internalUser.getBackendRoles())//
                .searchGuardRoles(internalUser.getSearchGuardRoles())//
                .userMappingAttribute(UserMappingAttributes.USER_ENTRY,
                        internalUser.toRedactedBasicObject().with("name", authCredentials.getUsername()))//
                .build());

    }

    public static class Info implements TypedComponent.Info<AuthenticationBackend> {
        private final InternalUsersDatabase internalUsersDatabase;

        public Info(InternalUsersDatabase internalUsersDatabase) {
            this.internalUsersDatabase = internalUsersDatabase;
        }

        @Override
        public Class<AuthenticationBackend> getType() {
            return AuthenticationBackend.class;
        }

        @Override
        public String getName() {
            return TYPE;
        }

        @Override
        public TypedComponent.Factory<AuthenticationBackend> getFactory() {
            return (config, context) -> new InternalUsersAuthenticationBackend(internalUsersDatabase);
        }

    }

    public static class UserInformationBackendInfo implements TypedComponent.Info<UserInformationBackend> {
        private final InternalUsersDatabase internalUsersDatabase;

        public UserInformationBackendInfo(InternalUsersDatabase internalUsersDatabase) {
            this.internalUsersDatabase = internalUsersDatabase;
        }

        @Override
        public Class<UserInformationBackend> getType() {
            return UserInformationBackend.class;
        }

        @Override
        public String getName() {
            return TYPE;
        }

        @Override
        public TypedComponent.Factory<UserInformationBackend> getFactory() {
            return (config, context) -> new InternalUsersAuthenticationBackend(internalUsersDatabase);
        }

    }

    
    @Override
    public ImmutableMap<String, String> describeAvailableUserMappingAttributes() {
        return ImmutableMap.of(UserMappingAttributes.USER_ENTRY, "The user entry from the internal users db");
    }

}
