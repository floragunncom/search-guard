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

import com.google.common.collect.Lists;

/**
 * A authenticated user and attributes associated to them (like roles, tenant, custom attributes)
 * <p/>
 * <b>Do not subclass from this class!</b>
 *
 */
public class User implements Serializable, CustomAttributesAware {

    public static Builder forUser(String username) {
        return new Builder().name(username);
    }

    public static final User ANONYMOUS = new User("sg_anonymous", Lists.newArrayList("sg_anonymous_backendrole"), null);
    public static final String USER_TENANT = "__user__";

    private static final long serialVersionUID = -5500938501822658596L;
    private final String name;
    private final String subName;

    /**
     * roles == backend_roles
     */
    private final Set<String> roles;
    private final Set<String> searchGuardRoles;
    private String requestedTenant;
    private Map<String, String> attributes;
    private boolean isInjected = false;

    public User(String name, String subName, Set<String> roles, Set<String> searchGuardRoles, String requestedTenant, Map<String, String> attributes,
            boolean isInjected) {
        super();
        this.name = name;
        this.subName = subName;
        this.roles = roles;
        this.searchGuardRoles = searchGuardRoles;
        this.requestedTenant = requestedTenant;
        this.attributes = attributes;
        this.isInjected = isInjected;
    }

    /**
     * Create a new authenticated user
     * 
     * @param name The username (must not be null or empty)
     * @param roles Roles of which the user is a member off (maybe null)
     * @param customAttributes Custom attributes associated with this (maybe null)
     * @throws IllegalArgumentException if name is null or empty
     */
    public User(final String name, final Collection<String> roles, final AuthCredentials customAttributes) {
        super();

        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name must not be null or empty");
        }

        this.name = name;
        this.subName = null;
        this.roles = new HashSet<String>();
        this.searchGuardRoles = new HashSet<String>();
        this.attributes = new HashMap<>();
        if (roles != null) {
            this.addRoles(roles);
        }

        if (customAttributes != null) {
            this.attributes.putAll(customAttributes.getAttributes());
        }

    }

    /**
     * Create a new authenticated user without roles and attributes
     * 
     * @param name The username (must not be null or empty)
     * @throws IllegalArgumentException if name is null or empty
     */
    public User(final String name) {
        this(name, null, null);
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
     * Associate this user with a set of roles
     * 
     * @param roles The roles
     */
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

    public boolean isInjected() {
        return isInjected;
    }

    public void setInjected(boolean isInjected) {
        this.isInjected = isInjected;
    }

    public final String toStringWithAttributes() {
        return "User [name=" + name + ", backend_roles=" + roles + ", requestedTenant=" + requestedTenant + ", attributes=" + attributes + "]";
    }

    @Override
    public final String toString() {
        return "User [name=" + name + ", backend_roles=" + roles + ", requestedTenant=" + requestedTenant + "]";
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

    /**
     * Get the custom attributes associated with this user
     * 
     * @return A modifiable map with all the current custom attributes associated with this user
     */
    public synchronized final Map<String, String> getCustomAttributesMap() {
        if (attributes == null) {
            attributes = new HashMap<>();
        }
        return attributes;
    }

    public final void addSearchGuardRoles(final Collection<String> sgRoles) {
        if (sgRoles != null && this.searchGuardRoles != null) {
            this.searchGuardRoles.addAll(sgRoles);
        }
    }

    public final Set<String> getSearchGuardRoles() {
        return this.searchGuardRoles == null ? Collections.emptySet() : Collections.unmodifiableSet(this.searchGuardRoles);
    }

    public static class Builder {
        private String name;
        private String subName;
        private final Set<String> backendRoles = new HashSet<String>();
        private final Set<String> searchGuardRoles = new HashSet<String>();
        private String requestedTenant;
        private Map<String, String> attributes = new HashMap<>();
        private boolean injected;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder subName(String subName) {
            this.subName = subName;
            return this;
        }

        public Builder requestedTenant(String requestedTenant) {
            this.requestedTenant = requestedTenant;
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

        public Builder attributes(Map<String, String> attributes) {
            this.attributes.putAll(attributes);
            return this;
        }

        public Builder injected() {
            this.injected = true;
            return this;
        }

        public User build() {
            return new User(name, subName, backendRoles, searchGuardRoles, requestedTenant, attributes, injected);
        }
    }
}
