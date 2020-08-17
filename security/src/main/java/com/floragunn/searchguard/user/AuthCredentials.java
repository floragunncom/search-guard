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

package com.floragunn.searchguard.user;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticsearch.ElasticsearchSecurityException;

/**
 * AuthCredentials are an abstraction to encapsulate credentials like passwords or generic
 * native credentials like GSS tokens.
 *
 */
public final class AuthCredentials {

    public static Builder forUser(String username) {
        return new Builder().userName(username);
    }

    private static final String DIGEST_ALGORITHM = "SHA-256";
    private final String username;
    private final String subUserName;
    private byte[] password;
    private Object nativeCredentials;
    private final Set<String> backendRoles;
    private final boolean complete;
    private final byte[] internalPasswordHash;
    private final Map<String, String> attributes;

    private AuthCredentials(String username, String subUserName, byte[] password, Object nativeCredentials, Set<String> backendRoles,
            boolean complete, byte[] internalPasswordHash, Map<String, String> attributes) {
        super();
        this.username = username;
        this.subUserName = subUserName;
        this.password = password;
        this.nativeCredentials = nativeCredentials;
        this.backendRoles = Collections.unmodifiableSet(backendRoles);
        this.complete = complete;
        this.internalPasswordHash = internalPasswordHash;
        this.attributes = Collections.unmodifiableMap(attributes);
    }

    /**
     * Wipe password and native credentials
     */
    public void clearSecrets() {
        if (password != null) {
            Arrays.fill(password, (byte) '\0');
            password = null;
        }

        nativeCredentials = null;
    }

    public String getUsername() {
        return username;
    }

    public String getSubUserName() {
        return subUserName;
    }

    public byte[] getPassword() {
        // make defensive copy
        return password == null ? null : Arrays.copyOf(password, password.length);
    }

    public Object getNativeCredentials() {
        return nativeCredentials;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(internalPasswordHash);
        result = prime * result + ((username == null) ? 0 : username.hashCode());
        result = prime * result + ((subUserName == null) ? 0 : subUserName.hashCode());

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AuthCredentials other = (AuthCredentials) obj;
        if (internalPasswordHash == null || other.internalPasswordHash == null
                || !MessageDigest.isEqual(internalPasswordHash, other.internalPasswordHash))
            return false;
        if (username == null) {
            if (other.username != null)
                return false;
        } else if (!username.equals(other.username))
            return false;

        if (subUserName == null) {
            if (other.subUserName != null) {
                return false;
            }
        } else {
            if (!subUserName.equals(other.subUserName)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString() {
        return "AuthCredentials [username=" + username + ", password empty=" + (password == null) + ", nativeCredentials empty="
                + (nativeCredentials == null) + ",backendRoles=" + backendRoles + "]";
    }

    /**
     *
     * @return Defensive copy of the roles this user is member of.
     */
    public Set<String> getBackendRoles() {
        return backendRoles;
    }

    public boolean isComplete() {
        return complete;
    }

    public Map<String, String> getAttributes() {
        return this.attributes;
    }

    public Builder copy() {
        Builder builder = new Builder();
        builder.userName = this.username;
        builder.subUserName = this.subUserName;
        builder.password = this.password;
        builder.nativeCredentials = this.nativeCredentials;
        builder.backendRoles.addAll(this.backendRoles);
        builder.complete = this.complete;
        builder.internalPasswordHash = this.internalPasswordHash;
        builder.attributes.putAll(this.attributes);
        return builder;
    }

    public static class Builder {
        private String userName;
        private String subUserName;
        private byte[] password;
        private Object nativeCredentials;
        private Set<String> backendRoles = new HashSet<>();
        private boolean complete;
        private byte[] internalPasswordHash;
        private Map<String, String> attributes = new HashMap<>();

        public Builder() {

        }

        public Builder userName(String userName) {
            this.userName = userName;
            return this;
        }

        public Builder subUserName(String subUserName) {
            this.subUserName = subUserName;
            return this;
        }

        public Builder password(byte[] password) {
            if (password == null || password.length == 0) {
                throw new IllegalArgumentException("password must not be null or empty");
            }

            this.password = Arrays.copyOf(password, password.length);

            try {
                MessageDigest digester = MessageDigest.getInstance(DIGEST_ALGORITHM);
                internalPasswordHash = digester.digest(this.password);
            } catch (NoSuchAlgorithmException e) {
                throw new ElasticsearchSecurityException("Unable to digest password", e);
            }

            Arrays.fill(password, (byte) '\0');

            return this;
        }
        
        public Builder password(String password) {
            return this.password(password.getBytes(StandardCharsets.UTF_8));
        }

        public Builder nativeCredentials(Object nativeCredentials) {
            if (nativeCredentials == null) {
                throw new IllegalArgumentException("nativeCredentials must not be null or empty");
            }
            this.nativeCredentials = nativeCredentials;
            return this;
        }

        public Builder backendRoles(String... backendRoles) {
            if (backendRoles == null) {
                return this;
            }
            
            this.backendRoles.addAll(Arrays.asList(backendRoles));
            return this;
        }

        /**
         * If the credentials are complete and no further roundtrips with the originator are due
         * then this method <b>must</b> be called so that the authentication flow can proceed.
         * <p/>
         * If this credentials are already marked a complete then a call to this method does nothing.
         */
        public Builder complete() {
            this.complete = true;
            return this;
        }

        public Builder attribute(String name, String value) {
            if (name != null && !name.isEmpty()) {
                this.attributes.put(name, value);
            }
            return this;
        }

        public Builder attributes(Map<String, String> map) {
            this.attributes.putAll(map);
            return this;
        }

        public Builder prefixAttributes(String keyPrefix, Map<String, ?> map) {
            for (Entry<String, ?> entry : map.entrySet()) {
                this.attributes.put(keyPrefix + entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null);
            }
            return this;
        }

        public String getUserName() {
            return userName;
        }

        public AuthCredentials build() {
            AuthCredentials result = new AuthCredentials(userName, subUserName, password, nativeCredentials, backendRoles, complete,
                    internalPasswordHash, attributes);
            this.password = null;
            this.nativeCredentials = null;
            this.internalPasswordHash = null;
            return result;
        }
    }
}
