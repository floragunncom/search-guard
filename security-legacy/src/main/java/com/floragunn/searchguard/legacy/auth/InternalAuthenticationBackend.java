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
package com.floragunn.searchguard.legacy.auth;

import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.internal_users_db.InternalUser;
import com.floragunn.searchguard.authc.internal_users_db.InternalUsersDatabase;
import com.floragunn.searchguard.authc.legacy.LegacyAuthenticationBackend;
import com.floragunn.searchguard.authc.legacy.LegacyAuthorizationBackend;
import com.floragunn.searchguard.user.Attributes;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.jayway.jsonpath.JsonPath;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.bouncycastle.crypto.generators.OpenBSDBCrypt;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;

public class InternalAuthenticationBackend implements LegacyAuthenticationBackend, LegacyAuthorizationBackend {

    private final InternalUsersDatabase internalUsersDatabase;
    private Map<String, JsonPath> attributeMapping;

    InternalAuthenticationBackend(Settings settings, InternalUsersDatabase internalUsersDatabase) {
        this.internalUsersDatabase = internalUsersDatabase;
        attributeMapping = Attributes.getAttributeMapping(settings.getAsSettings("map_db_attrs_to_user_attrs"));
    }

    @Override
    public boolean exists(User user) throws AuthenticatorUnavailableException {

        if (user == null || internalUsersDatabase == null) {
            return false;
        }

        InternalUser internalUser = internalUsersDatabase.get(user.getName());

        if (internalUser != null) {
            user.addRoles(internalUser.getBackendRoles());
            //FIX https://github.com/opendistro-for-elasticsearch/security/pull/23
            //Credits to @turettn
            final Map<String, Object> customAttributes = internalUser.getAttributes();
            Map<String, String> attributeMap = new HashMap<>();

            if (customAttributes != null) {
                for (Entry<String, Object> attributeEntry : customAttributes.entrySet()) {
                    attributeMap.put("attr.internal." + attributeEntry.getKey(),
                            attributeEntry.getValue() != null ? attributeEntry.getValue().toString() : null);
                }
            }

            Set<String> searchGuardRoles = internalUser.getSearchGuardRoles();
            if (searchGuardRoles != null) {
                user.addSearchGuardRoles(searchGuardRoles);
            }

            user.addAttributes(attributeMap);
            user.addStructuredAttributesByJsonPath(attributeMapping, customAttributes);
            return true;
        }

        return false;
    }

    @Override
    public User authenticate(AuthCredentials credentials) throws AuthenticatorUnavailableException {

        InternalUser internalUser = internalUsersDatabase.get(credentials.getUsername());

        if (internalUser == null) {
            return null;
        }

        final byte[] password = credentials.getPassword();

        if (password == null || password.length == 0) {
            throw new ElasticsearchSecurityException("empty passwords not supported");
        }

        ByteBuffer wrap = ByteBuffer.wrap(password);
        CharBuffer buf = StandardCharsets.UTF_8.decode(wrap);
        char[] array = new char[buf.limit()];
        buf.get(array);

        Arrays.fill(password, (byte) 0);

        try {
            if (OpenBSDBCrypt.checkPassword(internalUser.getPasswordHash(), array)) {
                Set<String> backendRoles = internalUser.getBackendRoles();
                Map<String, Object> customAttributes = internalUser.getAttributes();
                if (customAttributes != null) {
                    credentials = credentials.copy().prefixOldAttributes("attr.internal.", customAttributes).build();
                }

                Set<String> searchGuardRoles = internalUser.getSearchGuardRoles();

                User user = User.forUser(credentials.getUsername()).authDomainInfo(credentials.getAuthDomainInfo().authBackendType(getType()))
                        .backendRoles(backendRoles).searchGuardRoles(searchGuardRoles).attributes(credentials.getStructuredAttributes())
                        .attributesByJsonPath(attributeMapping, customAttributes).oldAttributes(credentials.getAttributes()).build();

                return user;
            } else {
                throw new ElasticsearchSecurityException("password does not match");
            }
        } finally {
            Arrays.fill(wrap.array(), (byte) 0);
            Arrays.fill(buf.array(), '\0');
            Arrays.fill(array, '\0');
        }
    }

    @Override
    public String getType() {
        return "internal";
    }

    @Override
    public void fillRoles(User user, AuthCredentials credentials) throws ElasticsearchSecurityException, AuthenticatorUnavailableException {
        InternalUser internalUser = internalUsersDatabase.get(user.getName());

        if (internalUser != null) {
            final Set<String> roles = internalUser.getBackendRoles();

            if (roles != null && !roles.isEmpty() && user != null) {
                user.addRoles(roles);
            }
        }
    }

    public static class AuthcBackendInfo implements TypedComponent.Info<LegacyAuthenticationBackend> {
        private final InternalUsersDatabase internalUsersDatabase;

        public AuthcBackendInfo(InternalUsersDatabase internalUsersDatabase) {
            this.internalUsersDatabase = internalUsersDatabase;
        }

        @Override
        public Class<LegacyAuthenticationBackend> getType() {
            return LegacyAuthenticationBackend.class;
        }

        @Override
        public String getName() {
            return "internal";
        }

        @Override
        public com.floragunn.searchguard.TypedComponent.Factory<LegacyAuthenticationBackend> getFactory() {
            return com.floragunn.searchguard.legacy.LegacyComponentFactory
                    .adapt((settings, path) -> new InternalAuthenticationBackend(settings, internalUsersDatabase));
        }
    }

    public static class AuthzBackendInfo implements TypedComponent.Info<LegacyAuthorizationBackend> {
        private final InternalUsersDatabase internalUsersDatabase;

        public AuthzBackendInfo(InternalUsersDatabase internalUsersDatabase) {
            this.internalUsersDatabase = internalUsersDatabase;
        }

        @Override
        public Class<LegacyAuthorizationBackend> getType() {
            return LegacyAuthorizationBackend.class;
        }

        @Override
        public String getName() {
            return "internal";
        }

        @Override
        public com.floragunn.searchguard.TypedComponent.Factory<LegacyAuthorizationBackend> getFactory() {
            return com.floragunn.searchguard.legacy.LegacyComponentFactory
                    .adapt((settings, path) -> new InternalAuthenticationBackend(settings, internalUsersDatabase));
        }
    }
}
