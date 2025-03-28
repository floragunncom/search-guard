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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.config.templates.AttributeSource;
import com.google.common.collect.Lists;
import com.jayway.jsonpath.JsonPath;

/**
 * A authenticated user and attributes associated to them (like roles, tenant, custom attributes)
 * <p/>
 * <b>Do not subclass from this class!</b>
 *
 */
public class User implements Serializable, UserInformation, AttributeSource {
    private static final Logger log = LogManager.getLogger(User.class);

    public static Builder forUser(String username) {
        return new Builder().name(username);
    }

    public static final User ANONYMOUS = new User("sg_anonymous", AuthDomainInfo.ANON, Lists.newArrayList("sg_anonymous_backendrole"), null);
    public static final String USER_TENANT = "__user__";

    private static final long serialVersionUID = -5500938501822658596L;
    private final String name;
    private final String subName;
    private final String type;
    private String authDomain;

    /**
     * roles == backend_roles
     */
    private final Set<String> roles;
    private final Set<String> searchGuardRoles;
    private final Object specialAuthzConfig;
    private String requestedTenant;
    private Map<String, String> attributes;
    private Map<String, Object> structuredAttributes;

    //unused, but we keep it for java serialization interop    
    @SuppressWarnings("unused")
    private boolean isInjected = false;
    private transient boolean authzComplete = false;

    public User(String name, String subName, AuthDomainInfo authDomainInfo, String type, Set<String> roles, Set<String> searchGuardRoles,
            Object specialAuthzConfig, String requestedTenant, Map<String, Object> structuredAttributes, Map<String, String> attributes, boolean authzComplete) {
        super();
        this.name = name;
        this.subName = subName;
        this.authDomain = authDomainInfo != null ? authDomainInfo.toInfoString() : null;
        this.type = type;
        this.roles = roles;
        this.searchGuardRoles = searchGuardRoles;
        this.specialAuthzConfig = specialAuthzConfig;
        this.requestedTenant = requestedTenant;
        this.structuredAttributes = structuredAttributes;
        this.attributes = attributes;
        this.authzComplete = authzComplete;
    }

    /**
     * Create a new authenticated user
     * 
     * @param name The username (must not be null or empty)
     * @param roles Roles of which the user is a member off (maybe null)
     * @param customAttributes Custom attributes associated with this (maybe null)
     * @throws IllegalArgumentException if name is null or empty
     */
    public User(final String name, AuthDomainInfo authDomainInfo, final Collection<String> roles, final AuthCredentials customAttributes) {
        super();

        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name must not be null or empty");
        }

        this.name = name;
        this.authDomain = authDomainInfo != null ? authDomainInfo.toInfoString() : null;
        this.subName = null;
        this.type = null;
        this.roles = new HashSet<String>();
        this.searchGuardRoles = new HashSet<String>();
        this.attributes = new HashMap<>();
        this.structuredAttributes = new HashMap<>();
        this.specialAuthzConfig = null;
        if (roles != null) {
            this.addRoles(roles);
        }

        if (customAttributes != null) {
            this.structuredAttributes.putAll(customAttributes.getStructuredAttributes());
        }

    }

    @Deprecated
    public User(final String name, final Collection<String> roles, final AuthCredentials customAttributes) {
        this(name, null, roles, customAttributes);
    }

    /**
     * Create a new authenticated user without roles and attributes
     * 
     * @param name The username (must not be null or empty)
     * @throws IllegalArgumentException if name is null or empty
     */
    public User(String name) {
        this(name, null, null, null);
    }

    public User(String name, AuthDomainInfo authDomainInfo) {
        this(name, authDomainInfo, null, null);
    }

    public final String getName() {
        return name;
    }

    public String getSubName() {
        return subName;
    }

    /**
     * 
     * @return A unmodifiable set of the backend roles this user is a member of
     */
    public final Set<String> getRoles() {
        return Collections.unmodifiableSet(roles);
    }

    /**
     * Associate this user with a backend role
     * 
     * @param role The backend role
     */
    public final void addRole(final String role) {
        this.roles.add(role);
    }

    /**
     * Associate this user with a set of backend roles
     * 
     * @param roles The backend roles
     */
    public final void addRoles(final Collection<String> roles) {
        if (roles != null) {
            this.roles.addAll(roles);
        }
    }

    /**
     * Check if this user is a member of a backend role
     * 
     * @param role The backend role
     * @return true if this user is a member of the backend role, false otherwise
     */
    public final boolean isUserInRole(final String role) {
        return this.roles.contains(role);
    }

    /**
     * @deprecated Use structured attributes instead
     */
    @Deprecated
    public final void addAttributes(final Map<String, String> attributes) {
        if (attributes != null) {
            this.attributes.putAll(attributes);
        }
    }

    public final String getRequestedTenant() {
        return requestedTenant;
    }

    public final void setRequestedTenant(String requestedTenant) {
        this.requestedTenant = requestedTenant;
    }

    public final String toStringWithAttributes() {
        StringBuilder result = new StringBuilder("User ").append(name);

        if (subName != null && subName.length() > 0) {
            result.append(" (").append(subName).append(")");
        }

        if (authDomain != null && authDomain.length() > 0) {
            result.append(" <").append(authDomain).append(">");
        }

        if (roles != null) {
            result.append(" ").append(roles);
        } else {
            result.append(" []");
        }

        if (searchGuardRoles != null) {
            result.append("/").append(searchGuardRoles);
        } else {
            result.append("/[]");
        }

        if (structuredAttributes != null) {
            result.append(" ").append(structuredAttributes);
        } else {
            result.append("{}");
        }

        return result.toString();

    }

    @Override
    public final String toString() {
        return toString(false);
    }

    private String toString(boolean includeAttributes) {
        StringBuilder result = new StringBuilder("User ").append(name);

        if (subName != null && subName.length() > 0) {
            result.append(" (").append(subName).append(")");
        }

        if (authDomain != null && authDomain.length() > 0) {
            result.append(" <").append(authDomain).append(">");
        }

        boolean propsAdded = false;

        if (roles != null && roles.size() > 0) {
            if (!propsAdded) {
                result.append(" [");
                propsAdded = true;
            } else {
                result.append(" ");
            }
            result.append("backend_roles=").append(roles);
        }

        if (searchGuardRoles != null && searchGuardRoles.size() > 0) {
            if (!propsAdded) {
                result.append(" [");
                propsAdded = true;
            } else {
                result.append(" ");
            }
            result.append("sg_roles=").append(searchGuardRoles);
        }

        if (requestedTenant != null && requestedTenant.length() > 0) {
            if (!propsAdded) {
                result.append(" [");
                propsAdded = true;
            } else {
                result.append(" ");
            }
            result.append("requestedTenant=").append(requestedTenant);
        }

        if (includeAttributes && attributes != null && attributes.size() > 0) {
            if (!propsAdded) {
                result.append(" [");
                propsAdded = true;
            } else {
                result.append(" ");
            }
            result.append("attributes=").append(attributes);
        }

        if (propsAdded) {
            result.append("]");
        }

        return result.toString();
    }

    @Override
    public final int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (name == null ? 0 : name.hashCode());
        return result;
    }

    @Override
    public final boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final User other = (User) obj;
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        return true;
    }

    public Map<String, Object> getStructuredAttributes() {
        return structuredAttributes;
    }

    public String getAttributeAsString(String key) {
        Object value = this.structuredAttributes.get(key);

        if (value != null) {
            return value.toString();
        } else {
            return null;
        }
    }

    public void addStructuredAttribute(String key, Object value) {
        structuredAttributes.put(key, value);
    }

    public void addStructuredAttributesByJsonPath(Map<String, JsonPath> jsonPathMap, Object source) {
        Attributes.addAttributesByJsonPath(jsonPathMap, source, this.structuredAttributes);
    }

    public final void addSearchGuardRoles(final Collection<String> sgRoles) {
        if (sgRoles != null && this.searchGuardRoles != null) {
            this.searchGuardRoles.addAll(sgRoles);
        }
    }

    public final Set<String> getSearchGuardRoles() {
        return this.searchGuardRoles == null ? Collections.emptySet() : Collections.unmodifiableSet(this.searchGuardRoles);
    }

    public String getType() {
        return type;
    }

    public Object getSpecialAuthzConfig() {
        return specialAuthzConfig;
    }

    public boolean isAuthzComplete() {
        return authzComplete;
    }

    public Builder copy() {
        Builder builder = new Builder();
        builder.name = name;
        builder.subName = subName;
        builder.type = type;
        builder.backendRoles.addAll(roles);
        builder.searchGuardRoles.addAll(searchGuardRoles);
        builder.requestedTenant = requestedTenant;
        builder.attributes.putAll(attributes);
        builder.structuredAttributes.putAll(structuredAttributes);
        builder.specialAuthzConfig = specialAuthzConfig;
        builder.authzComplete = authzComplete;

        return builder;
    }

    public static class Builder {
        private String name;
        private String subName;
        private AuthDomainInfo authDomainInfo;
        private String type;
        private final Set<String> backendRoles = new HashSet<String>();
        private final Set<String> searchGuardRoles = new HashSet<String>();
        private String requestedTenant;
        private Map<String, String> attributes = new HashMap<>();
        private Map<String, Object> structuredAttributes = new HashMap<>();
        private Object specialAuthzConfig;
        private boolean authzComplete;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder subName(String subName) {
            this.subName = subName;
            return this;
        }

        public Builder authDomainInfo(AuthDomainInfo authDomainInfo) {
            if (this.authDomainInfo == null) {
                this.authDomainInfo = authDomainInfo;
            } else {
                this.authDomainInfo = this.authDomainInfo.add(authDomainInfo);
            }
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder requestedTenant(String requestedTenant) {
            this.requestedTenant = requestedTenant;
            return this;
        }

        public Builder with(AuthCredentials authCredentials) {
            this.authDomainInfo(authCredentials.getAuthDomainInfo());
            this.backendRoles(authCredentials.getBackendRoles());
            this.searchGuardRoles(authCredentials.getSearchGuardRoles());
            this.attributes(authCredentials.getStructuredAttributes());
            return this;
        }

        public Builder backendRoles(String... backendRoles) {
            return this.backendRoles(Arrays.asList(backendRoles));
        }

        public Builder backendRoles(Collection<String> backendRoles) {
            if (backendRoles != null) {
                this.backendRoles.addAll(backendRoles);
            }
            return this;
        }

        public Builder searchGuardRoles(String... searchGuardRoles) {
            return this.searchGuardRoles(Arrays.asList(searchGuardRoles));
        }

        public Builder searchGuardRoles(Collection<String> searchGuardRoles) {
            if (searchGuardRoles != null) {
                this.searchGuardRoles.addAll(searchGuardRoles);
            }
            return this;
        }

        @Deprecated
        public Builder oldAttributes(Map<String, String> attributes) {
            this.attributes.putAll(attributes);
            return this;
        }

        public Builder attributes(Map<String, Object> attributes) {
            Attributes.validate(attributes);
            this.structuredAttributes.putAll(attributes);
            return this;
        }

        public Builder attribute(String key, Object value) {
            Attributes.validate(value);
            this.structuredAttributes.put(key, value);
            return this;
        }

        public Builder attributesByJsonPath(Map<String, JsonPath> jsonPathMap, Object source) {
            Attributes.addAttributesByJsonPath(jsonPathMap, source, this.structuredAttributes);
            return this;
        }

        @Deprecated
        public Builder oldAttribute(String key, String value) {
            this.attributes.put(key, value);
            return this;
        }

        public Builder specialAuthzConfig(Object specialAuthzConfig) {
            this.specialAuthzConfig = specialAuthzConfig;
            return this;
        }

        public Builder authzComplete() {
            this.authzComplete = true;
            return this;
        }

        public User build() {
            return new User(name, subName, authDomainInfo, type, backendRoles, searchGuardRoles, specialAuthzConfig, requestedTenant,
                    structuredAttributes, attributes, authzComplete);
        }
    }

    public String getAuthDomain() {
        return authDomain;
    }

    public void setAuthDomain(String authDomain) {
        if (authDomain != null) {
            throw new IllegalStateException("AuthDomain has been already set: " + this);
        }

        this.authDomain = authDomain;
    }

    @Override
    public Object getAttributeValue(String attributeName) {
        if (attributeName.equals("user.name") || attributeName.equals("user_name")) {
            return getName();
        } else if (attributeName.equals("user.roles") || attributeName.equals("user_roles")) {
            return getRoles();
        } else if (attributeName.equals("user.attrs")) {
            return getStructuredAttributes();
        } else if (attributeName.startsWith("user.attrs.")) {
            return getStructuredAttributes().get(attributeName.substring("user.attrs.".length()));
        } else if (attributeName.startsWith("attr.") || attributeName.startsWith("_")) {
            log.error("The attribute syntax ${" + attributeName
                    + "} is no longer supported. Please change to new user attributes. See here for details: https://docs.search-guard.com/latest/document-level-security#user-attributes");
            return "${" + attributeName + "}";
        } else {
            return null;
        }
    }
}
