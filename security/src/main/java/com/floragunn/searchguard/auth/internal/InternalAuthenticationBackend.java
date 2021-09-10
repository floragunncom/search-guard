/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard.auth.internal;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.bouncycastle.crypto.generators.OpenBSDBCrypt;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.settings.Settings;

import com.floragunn.searchguard.auth.api.SyncAuthenticationBackend;
import com.floragunn.searchguard.auth.api.SyncAuthorizationBackend;
import com.floragunn.searchguard.modules.SearchGuardComponentRegistry.ComponentFactory;
import com.floragunn.searchguard.sgconf.internal_users_db.InternalUsersDatabase;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchguard.user.UserAttributes;
import com.jayway.jsonpath.JsonPath;

public class InternalAuthenticationBackend implements SyncAuthenticationBackend, SyncAuthorizationBackend {

    private final InternalUsersDatabase internalUsersModel;
    private Map<String, JsonPath> attributeMapping;

    InternalAuthenticationBackend(Settings settings, InternalUsersDatabase internalUsersDatabase) {
        this.internalUsersModel = internalUsersDatabase;
        attributeMapping = UserAttributes.getAttributeMapping(settings.getAsSettings("map_db_attrs_to_user_attrs"));
    }
    
    @Override
    public boolean exists(User user) {

        if(user == null || internalUsersModel == null) {
            return false;
        }

        final boolean exists = internalUsersModel.exists(user.getName());
        
        if(exists) {
            user.addRoles(internalUsersModel.getBackenRoles(user.getName()));
            //FIX https://github.com/opendistro-for-elasticsearch/security/pull/23
            //Credits to @turettn
            final Map<String, Object> customAttributes = internalUsersModel.getAttributes(user.getName());
            Map<String, String> attributeMap = new HashMap<>();

            if(customAttributes != null) {
                for(Entry<String, Object> attributeEntry: customAttributes.entrySet()) {
                    attributeMap.put("attr.internal."+attributeEntry.getKey(), attributeEntry.getValue() != null ? attributeEntry.getValue().toString() : null);
                }
            }

            final List<String> searchGuardRoles = internalUsersModel.getSearchGuardRoles(user.getName());
            if(searchGuardRoles != null) {
                user.addSearchGuardRoles(searchGuardRoles);
            }
            
            user.addAttributes(attributeMap);
            user.addStructuredAttributesByJsonPath(attributeMapping, customAttributes);
            return true;
        }

        return false;
    }
    
    @Override
    public User authenticate(AuthCredentials credentials) {

        if (internalUsersModel == null) {
            throw new OpenSearchSecurityException("Internal authentication backend not configured. May be Search Guard is not initialized. See https://docs.search-guard.com/latest/sgadmin");
        }
                
        if(!internalUsersModel.exists(credentials.getUsername())) {
            throw new OpenSearchSecurityException(credentials.getUsername() + " not found");
        }
        
        final byte[] password = credentials.getPassword();
        
        if(password == null || password.length == 0) {
            throw new OpenSearchSecurityException("empty passwords not supported");
        }

        ByteBuffer wrap = ByteBuffer.wrap(password);
        CharBuffer buf = StandardCharsets.UTF_8.decode(wrap);
        char[] array = new char[buf.limit()];
        buf.get(array);
        
        Arrays.fill(password, (byte)0);
       
        try {
            if (OpenBSDBCrypt.checkPassword(internalUsersModel.getHash(credentials.getUsername()), array)) {
                final List<String> backendRoles = internalUsersModel.getBackenRoles(credentials.getUsername());
                final Map<String, Object> customAttributes = internalUsersModel.getAttributes(credentials.getUsername());
                if(customAttributes != null) {
                    credentials = credentials.copy().prefixOldAttributes("attr.internal.", customAttributes).build();
                }
                
                final List<String> searchGuardRoles = internalUsersModel.getSearchGuardRoles(credentials.getUsername());

                User user = User.forUser(credentials.getUsername()).authDomainInfo(credentials.getAuthDomainInfo().authBackendType(getType()))
                        .backendRoles(backendRoles).searchGuardRoles(searchGuardRoles).attributes(credentials.getStructuredAttributes())
                        .attributesByJsonPath(attributeMapping, customAttributes).oldAttributes(credentials.getAttributes()).build();
         
                return user;
            } else {
                throw new OpenSearchSecurityException("password does not match");
            }
        } finally {
            Arrays.fill(wrap.array(), (byte)0);
            Arrays.fill(buf.array(), '\0');
            Arrays.fill(array, '\0');
        }
    }

    @Override
    public String getType() {
        return "internal";
    }

    @Override
    public void fillRoles(User user, AuthCredentials credentials) throws OpenSearchSecurityException {
        
        if (internalUsersModel == null) {
            throw new OpenSearchSecurityException("Internal authentication backend not configured. May be Search Guard is not initialized. See https://docs.search-guard.com/latest/sgadmin");

        }

        if(exists(user)) {
            final List<String> roles = internalUsersModel.getBackenRoles(user.getName());
            if(roles != null && !roles.isEmpty() && user != null) {
                user.addRoles(roles);
            }
        }
        
        
    }

    public static class Factory implements ComponentFactory<InternalAuthenticationBackend> {
        private final InternalUsersDatabase internalUsersDatabase;

        public Factory(InternalUsersDatabase internalUsersDatabase) {
            this.internalUsersDatabase = internalUsersDatabase;
        }

        @Override
        public InternalAuthenticationBackend create(Settings settings, Path configPath) {
           return new InternalAuthenticationBackend(settings, internalUsersDatabase);            
        }

        @Override
        public String getClassName() {
            return InternalAuthenticationBackend.class.getName();
        }
        
        
    }

  
}
