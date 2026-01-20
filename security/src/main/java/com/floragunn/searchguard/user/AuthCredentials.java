/*
 * Copyright 2015-2021 floragunn GmbH
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.jayway.jsonpath.JsonPath;

/**
 * AuthCredentials are an abstraction to encapsulate credentials like passwords or generic
 * native credentials like GSS tokens.
 *
 */
public final class AuthCredentials implements UserInformation {
    private static final Logger log = LogManager.getLogger(AuthCredentials.class);

    public static Builder forUser(String username) {
        return new Builder().userName(username);
    }

    private static final String DIGEST_ALGORITHM = "SHA-256";
    private final String username;
    private final String subUserName;
    private final AuthDomainInfo authDomainInfo;
    private byte[] password;

    /**
     * Raw credentials like a unparsed token
     */
    private Object nativeCredentials;
    private final ImmutableSet<String> backendRoles;
    private final ImmutableSet<String> searchGuardRoles;
    private boolean complete;
    private final boolean authzComplete;
    private final byte[] internalPasswordHash;
    private boolean secretsCleared;
    private Exception secretsClearedAt;
    private String redirectUri;

    private final Map<String, Object> structuredAttributes;

    /**
     * Claims or assertions from the authc information. In contrast to attributes, these don't have prefixes and may be complex valued. This is for inter-module communication during the authz phase.
     *  These attributes won't be automatically made available in the user object. 
     */
    private final Map<String, Object> claims;

    private final ImmutableMap<String, Object> attributesForUserMapping;

    private AuthCredentials(String username, String subUserName, AuthDomainInfo authDomainInfo, byte[] password, Object nativeCredentials,
            ImmutableSet<String> backendRoles, ImmutableSet<String> searchGuardRoles, boolean complete, boolean authzComplete,
            byte[] internalPasswordHash, Map<String, Object> structuredAttributes, ImmutableMap<String, Object> attributesForUserMapping,
            Map<String, Object> claims, String redirectUri) {
        super();
        this.username = username;
        this.subUserName = subUserName;
        this.authDomainInfo = authDomainInfo;
        this.password = password;
        this.nativeCredentials = nativeCredentials;
        this.backendRoles = backendRoles;
        this.searchGuardRoles = searchGuardRoles;
        this.complete = complete;
        this.authzComplete = authzComplete;
        this.internalPasswordHash = internalPasswordHash;
        this.structuredAttributes = Collections.unmodifiableMap(structuredAttributes);
        this.attributesForUserMapping = attributesForUserMapping;
        this.claims = Collections.unmodifiableMap(claims);
        this.redirectUri = redirectUri;
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
        this.authDomainInfo = AuthDomainInfo.UNKNOWN;

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
            this.backendRoles = ImmutableSet.ofArray(backendRoles);
        } else {
            this.backendRoles = ImmutableSet.empty();
        }

        this.searchGuardRoles = ImmutableSet.empty();
        this.attributesForUserMapping = ImmutableMap.empty();
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

    public Set<String> getBackendRoles() {
        return backendRoles;
    }

    public boolean isComplete() {
        return complete;
    }

    public boolean isAuthzComplete() {
        return authzComplete;
    }

    public AuthCredentials userName(String newUserName) {
        return new AuthCredentials(newUserName, subUserName, authDomainInfo, password, nativeCredentials, backendRoles, searchGuardRoles, complete,
                authzComplete, internalPasswordHash, structuredAttributes, attributesForUserMapping, claims, redirectUri);
    }

    public AuthCredentials with(AuthCredentials other) {
        if (other == this || other == null) {
            return this;
        }

        return new AuthCredentials(username, subUserName, authDomainInfo, password, nativeCredentials, backendRoles.with(other.backendRoles),
                searchGuardRoles.with(other.searchGuardRoles), complete, authzComplete, internalPasswordHash,
                mergeMaps(structuredAttributes, other.structuredAttributes), mergeMaps(attributesForUserMapping, other.attributesForUserMapping),
                claims, redirectUri);
    }

    public AuthCredentials with(AuthDomainInfo authDomainInfo) {
        return new AuthCredentials(username, subUserName, authDomainInfo, password, nativeCredentials, backendRoles, searchGuardRoles, complete,
                authzComplete, internalPasswordHash, structuredAttributes, attributesForUserMapping, claims, redirectUri);
    }

    public Builder copy() {
        return new Builder(this);
    }

    @Deprecated
    public AuthCredentials markComplete() {
        this.complete = true;
        return this;
    }

    public static class Builder {
        private String userName;
        private String subUserName;
        private AuthDomainInfo authDomainInfo = AuthDomainInfo.UNKNOWN;
        private byte[] password;
        private Object nativeCredentials;
        private ImmutableSet.Builder<String> backendRoles;
        private ImmutableSet.Builder<String> searchGuardRoles;
        private boolean complete;
        private boolean authzComplete;
        private byte[] internalPasswordHash;
        private ImmutableMap.Builder<String, Object> structuredAttributes;
        private ImmutableMap.Builder<String, Object> attributesForUserMapping;
        private ImmutableMap.Builder<String, Object> claims;
        private String redirectUri;

        public Builder() {
            this.backendRoles = new ImmutableSet.Builder<>();
            this.searchGuardRoles = new ImmutableSet.Builder<>();
            this.structuredAttributes = new ImmutableMap.Builder<>();
            this.claims = new ImmutableMap.Builder<>();
            this.attributesForUserMapping = new ImmutableMap.Builder<>();
        }

        Builder(AuthCredentials authCredentials) {
            this.userName = authCredentials.username;
            this.subUserName = authCredentials.subUserName;
            this.password = authCredentials.password;
            this.backendRoles = new ImmutableSet.Builder<String>(authCredentials.backendRoles);
            this.searchGuardRoles = new ImmutableSet.Builder<String>(authCredentials.searchGuardRoles);
            this.complete = authCredentials.complete;
            this.internalPasswordHash = authCredentials.internalPasswordHash;
            this.structuredAttributes = new ImmutableMap.Builder<>(authCredentials.structuredAttributes);
            this.attributesForUserMapping = new ImmutableMap.Builder<String, Object>(authCredentials.attributesForUserMapping);
            this.authDomainInfo = authCredentials.authDomainInfo;
            this.redirectUri = authCredentials.redirectUri;
            this.claims = new ImmutableMap.Builder<String, Object>(authCredentials.claims);
        }

        public Builder userName(String userName) {
            this.userName = userName;
            this.attributesForUserMapping.with("credentials", ImmutableMap.of("user_name", userName));
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

            this.backendRoles.addAll(backendRoles);
            return this;
        }

        public Builder backendRoles(Collection<String> backendRoles) {
            if (backendRoles == null) {
                return this;
            }

            this.backendRoles.addAll(backendRoles);
            return this;
        }

        public Builder searchGuardRoles(Collection<String> searchGuardRoles) {
            if (searchGuardRoles == null) {
                return this;
            }

            this.searchGuardRoles.addAll(searchGuardRoles);
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

        public Builder authzComplete() {
            this.authzComplete = true;
            return this;
        }

        public Builder attribute(String name, Object value) {
            Attributes.validate(value);

            if (name != null && !name.isEmpty()) {
                this.structuredAttributes.put(name, value);
            }
            return this;
        }

        public Builder attributes(Map<String, Object> map) {
            Attributes.validate(map);
            this.structuredAttributes.putAll(map);
            return this;
        }

        public Builder attributesByJsonPath(Map<String, JsonPath> jsonPathMap, Object source) {
            Attributes.addAttributesByJsonPath(jsonPathMap, source, this.structuredAttributes);
            return this;
        }

        public Builder userMappingAttribute(String name, Object value) {
            if (name != null && !name.isEmpty()) {
                this.attributesForUserMapping.with(name, value);
            }
            return this;
        }

        public Builder claims(Map<String, Object> map) {
            this.claims.putAll(map);
            return this;
        }

        public Builder authenticatorType(String authDomainType) {
            this.authDomainInfo = this.authDomainInfo.authenticatorType(authDomainType);
            return this;
        }

        public Builder authDomainInfo(AuthDomainInfo authDomainInfo) {
            this.authDomainInfo = this.authDomainInfo.add(authDomainInfo);
            return this;
        }

        public String getUserName() {
            return userName;
        }

        public String getRedirectUri() {
            return redirectUri;
        }

        public Builder redirectUri(String redirectUri) {
            this.redirectUri = redirectUri;
            return this;
        }

        public AuthCredentials build() {
            AuthCredentials result = new AuthCredentials(userName, subUserName, authDomainInfo, password, nativeCredentials, backendRoles.build(),
                    searchGuardRoles.build(), complete, authzComplete, internalPasswordHash, structuredAttributes.build(),
                    attributesForUserMapping.build(), claims.build(), redirectUri);
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

    public AuthDomainInfo getAuthDomainInfo() {
        return authDomainInfo;
    }

    @Override
    public String getName() {
        return username;
    }

    @Override
    public String getSubName() {
        return subUserName;
    }

    @Override
    public String getAuthDomain() {
        return authDomainInfo != null ? authDomainInfo.toInfoString() : null;
    }

    public String getRedirectUri() {
        return redirectUri;
    }

    public ImmutableMap<String, Object> getAttributesForUserMapping() {
        return attributesForUserMapping;
    }

    public AuthCredentials userMappingAttributes(ImmutableMap<String, Object> attributesForUserMapping) {
        return new AuthCredentials(username, subUserName, authDomainInfo, password, nativeCredentials, backendRoles, searchGuardRoles, complete,
                authzComplete, internalPasswordHash, structuredAttributes, this.attributesForUserMapping.with(attributesForUserMapping), claims,
                redirectUri);
    }

    public AuthCredentials userMappingAttribute(String key, Object value) {
        return new AuthCredentials(username, subUserName, authDomainInfo, password, nativeCredentials, backendRoles, searchGuardRoles, complete,
                authzComplete, internalPasswordHash, structuredAttributes, this.attributesForUserMapping.with(key, value), claims, redirectUri);
    }

    public ImmutableSet<String> getSearchGuardRoles() {
        return searchGuardRoles;
    }

    @Override
    public String toString() {
        return "AuthCredentials [username=" + username + ", subUserName=" + subUserName + ", authDomainInfo=" + authDomainInfo + ", password="
                + (password != null ? "REDACTED" : null) + ", nativeCredentials=" + (nativeCredentials != null ? "REDACTED" : null)
                + ", backendRoles=" + backendRoles + ", searchGuardRoles=" + searchGuardRoles + ", complete=" + complete + ", authzComplete="
                + authzComplete + ", redirectUri=" + redirectUri + ", structuredAttributes=" + structuredAttributes + ", claims=" + claims
                + ", attributesForUserMapping=" + attributesForUserMapping + "]";
    }

    @SuppressWarnings("unchecked")
    private static ImmutableMap<String, Object> mergeMaps(Map<String, Object> map1, Map<String, Object> map2) {
        if (map2 == null || map2.size() == 0) {
            return ImmutableMap.of(map1);
        }

        if (map1 == null || map1.size() == 0) {
            return ImmutableMap.of(map2);
        }

        ImmutableMap.Builder<String, Object> result = new ImmutableMap.Builder<String, Object>(map1);

        for (Map.Entry<String, Object> entry : map2.entrySet()) {
            String key = entry.getKey();
            Object value2 = entry.getValue();
            Object value1 = result.get(key);

            if (value1 == null) {
                result.put(key, value2);
            } else if (value1 instanceof Collection && value2 instanceof Collection) {
                if (value1 instanceof Set) {
                    result.put(key, ImmutableSet.of((Collection<Object>) value1).with((Collection<?>) value2));
                } else {
                    result.put(key, ImmutableList.of((Collection<Object>) value1).with((Collection<?>) value2));
                }
            } else if (value1 instanceof Map && value2 instanceof Map) {
                result.put(key, mergeMaps((Map<String, Object>) value1, (Map<String, Object>) value2));
            }
        }

        return result.build();
    }
}
