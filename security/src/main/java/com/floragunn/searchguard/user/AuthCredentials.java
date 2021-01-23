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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;

import com.jayway.jsonpath.JsonPath;

/**
 * AuthCredentials are an abstraction to encapsulate credentials like passwords or generic
 * native credentials like GSS tokens.
 *
 */
public final class AuthCredentials {
    private static final Logger log = LogManager.getLogger(AuthCredentials.class);

    public static Builder forUser(String username) {
        return new Builder().userName(username);
    }

    private static final String DIGEST_ALGORITHM = "SHA-256";
    private final String username;
    private final String subUserName;
    private byte[] password;

    /**
     * Raw credentials like a unparsed token
     */
    private Object nativeCredentials;
    private final Set<String> backendRoles;
    private boolean complete;
    private final boolean authzComplete;
    private final byte[] internalPasswordHash;
    private boolean secretsCleared;
    private Exception secretsClearedAt;

    /**
     * Attributes which will be passed on to further authz mechanism like DLS/FLS.  Passed on to the User object.
     * See https://docs.search-guard.com/latest/document-level-security#ldap-and-jwt-user-attributes 
     */
    private final Map<String, String> attributes;
    private final Map<String, Object> structuredAttributes;
    
    /**
     * Claims or assertions from the authc information. In contrast to attributes, these don't have prefixes and may be complex valued. This is for inter-module communication during the authz phase.
     *  These attributes won't be automatically made available in the user object. 
     */
    private final Map<String, Object> claims;

    private AuthCredentials(String username, String subUserName, byte[] password, Object nativeCredentials, Set<String> backendRoles,
            boolean complete, boolean authzComplete, byte[] internalPasswordHash, Map<String, Object> structuredAttributes,
            Map<String, String> attributes, Map<String, Object> claims) {
        super();
        this.username = username;
        this.subUserName = subUserName;
        this.password = password;
        this.nativeCredentials = nativeCredentials;
        this.backendRoles = Collections.unmodifiableSet(backendRoles);
        this.complete = complete;
        this.authzComplete = authzComplete;
        this.internalPasswordHash = internalPasswordHash;
        this.attributes = Collections.unmodifiableMap(attributes);
        this.structuredAttributes = Collections.unmodifiableMap(structuredAttributes);
        this.claims = Collections.unmodifiableMap(claims);
    }

    @Deprecated
    public AuthCredentials(final String username, final Object nativeCredentials) {
        this(username, null, nativeCredentials);

        if (nativeCredentials == null) {
            throw new IllegalArgumentException("nativeCredentials must not be null or empty");
        }
    }

    @Deprecated
    public AuthCredentials(final String username, final byte[] password) {
        this(username, password, null);

        if (password == null || password.length == 0) {
            throw new IllegalArgumentException("password must not be null or empty");
        }
    }

    @Deprecated
    public AuthCredentials(final String username, String... backendRoles) {
        this(username, null, null, backendRoles);
    }

    @Deprecated
    private AuthCredentials(final String username, byte[] password, Object nativeCredentials, String... backendRoles) {
        super();

        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException("username must not be null or empty");
        }

        this.username = username;
        // make defensive copy
        this.password = password == null ? null : Arrays.copyOf(password, password.length);
        this.subUserName = null;
        this.complete = false;
        this.authzComplete = false;

        if (this.password != null) {
            try {
                MessageDigest digester = MessageDigest.getInstance(DIGEST_ALGORITHM);
                internalPasswordHash = digester.digest(this.password);
            } catch (NoSuchAlgorithmException e) {
                throw new ElasticsearchSecurityException("Unable to digest password", e);
            }
        } else {
            internalPasswordHash = null;
        }

        if (password != null) {
            Arrays.fill(password, (byte) '\0');
            password = null;
        }

        this.nativeCredentials = nativeCredentials;
        nativeCredentials = null;

        if (backendRoles != null && backendRoles.length > 0) {
            this.backendRoles = new HashSet<>(Arrays.asList(backendRoles));
        } else {
            this.backendRoles = new HashSet<>();
        }

        this.attributes = new HashMap<>();
        this.structuredAttributes = new HashMap<>();
        this.claims = new HashMap<>();
    }

    /**
     * Wipe password and native credentials
     */
    public void clearSecrets() {
        if (secretsCleared) {
            return;
        }
        
        secretsCleared = true;
        
        if (log.isDebugEnabled()) {
            secretsClearedAt = new Exception("clearSecrets() called at:");
        }
        
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
        if (secretsCleared) {
            throw new IllegalStateException("Secrets for " + this + " have been already cleared", secretsClearedAt);
        }
        
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

    public boolean isAuthzComplete() {
        return authzComplete;
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
        builder.structuredAttributes.putAll(this.structuredAttributes);
        return builder;
    }

    @Deprecated
    public AuthCredentials markComplete() {
        this.complete = true;
        return this;
    }

    @Deprecated
    public void addAttribute(String name, String value) {
        if (name != null && !name.isEmpty()) {
            this.attributes.put(name, value);
        }
    }

    public static class Builder {
        private String userName;
        private String subUserName;
        private byte[] password;
        private Object nativeCredentials;
        private Set<String> backendRoles = new HashSet<>();
        private boolean complete;
        private boolean authzComplete;
        private byte[] internalPasswordHash;
        private Map<String, String> attributes = new HashMap<>();
        private Map<String, Object> structuredAttributes = new HashMap<>();
        private Map<String, Object> claims = new HashMap<>();
        
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

        public Builder oldAttribute(String name, String value) {
            if (name != null && !name.isEmpty()) {
                this.attributes.put(name, value);
            }
            return this;
        }

        public Builder authzComplete() {
            this.authzComplete = true;
            return this;
        }

        public Builder oldAttributes(Map<String, String> map) {
            this.attributes.putAll(map);
            return this;
        }

        public Builder prefixOldAttributes(String keyPrefix, Map<String, ?> map) {
            for (Entry<String, ?> entry : map.entrySet()) {
                this.attributes.put(keyPrefix + entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null);
            }
            return this;
        }

        public Builder attribute(String name, Object value) {
            UserAttributes.validate(value);
           
            if (name != null && !name.isEmpty()) {
                this.structuredAttributes.put(name, value);
            }
            return this;
        }

        public Builder attributes(Map<String, Object> map) {
            UserAttributes.validate(map);
            this.structuredAttributes.putAll(map);
            return this;
        }
        
        public Builder attributesByJsonPath(Map<String, JsonPath> jsonPathMap, Object source) {     
            UserAttributes.addAttributesByJsonPath(jsonPathMap, source, this.structuredAttributes);           
            return this;
        }
        
        public Builder claims(Map<String, Object> map) {
            this.claims.putAll(map);
            return this;
        }

        
        public String getUserName() {
            return userName;
        }

        public AuthCredentials build() {
            AuthCredentials result = new AuthCredentials(userName, subUserName, password, nativeCredentials, backendRoles, complete, authzComplete,
                    internalPasswordHash, structuredAttributes, attributes, claims);
            this.password = null;
            this.nativeCredentials = null;
            this.internalPasswordHash = null;
            return result;
        }
    }

    public Map<String, Object> getStructuredAttributes() {
        return structuredAttributes;
    }

    public Map<String, Object> getClaims() {
        return claims;
    }
}
