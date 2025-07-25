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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.sgconf.history.ConfigSnapshot;
import com.floragunn.searchguard.sgconf.history.ConfigVersionSet;
import com.google.common.collect.ImmutableMap;

public class AuthTokenPrivilegeBase implements ToXContentObject, Writeable, Serializable {

    static final String PARAM_COMPACT = "compact";

    static final Params COMPACT = new ToXContent.MapParams(ImmutableMap.of(PARAM_COMPACT, "true"));

    private static final long serialVersionUID = -7176396883010611335L;

    private final List<String> backendRoles;

    private final List<String> searchGuardRoles;
    private final ConfigVersionSet configVersions;
    private final Map<String, Object> attributes;

    private transient ConfigSnapshot configSnapshot;

    public AuthTokenPrivilegeBase(Collection<String> backendRoles, Collection<String> searchGuardRoles, Map<String, Object> attributes,
            ConfigVersionSet configVersions) {
        this.configVersions = configVersions;
        this.backendRoles = Collections.unmodifiableList(new ArrayList<>(backendRoles));
        this.searchGuardRoles = Collections.unmodifiableList(new ArrayList<>(searchGuardRoles));
        this.attributes = Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
    }

    public AuthTokenPrivilegeBase(StreamInput in) throws IOException {
        this.backendRoles = in.readStringCollectionAsList();
        this.searchGuardRoles = in.readStringCollectionAsList();
        this.attributes = in.readGenericMap();
        this.configVersions = in.readOptionalWriteable(ConfigVersionSet::new);
    }

    public List<String> getBackendRoles() {
        return backendRoles;
    }

    public List<String> getSearchGuardRoles() {
        return searchGuardRoles;
    }

    public ConfigVersionSet getConfigVersions() {
        return configVersions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean compact = params.paramAsBoolean(PARAM_COMPACT, false);

        builder.startObject();

        if (backendRoles.size() > 0) {
            builder.field(compact ? "r_be" : "roles_be", backendRoles);
        }

        if (searchGuardRoles.size() > 0) {
            builder.field(compact ? "r_sg" : "roles_sg", searchGuardRoles);
        }

        if (attributes.size() > 0) {
            builder.field(compact ? "a" : "attrs", attributes);
        }

        if (configVersions != null) {
            if (compact) {
                builder.array("c", new long[] { configVersions.get(CType.ROLES).getVersion(), configVersions.get(CType.ROLESMAPPING).getVersion(),
                        configVersions.get(CType.ACTIONGROUPS).getVersion(), configVersions.get(CType.TENANTS).getVersion() });
            } else {
                ToXContent toXContent = configVersions;
                builder.field("config", toXContent);
            }
        }

        builder.endObject();
        return builder;
    }

    public static AuthTokenPrivilegeBase parse(DocNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

        ConfigVersionSet configVersions = null;

        List<String> backendRoles = vJsonNode.get("roles_be").asList().withEmptyListAsDefault().ofStrings();
        List<String> searchGuardRoles = vJsonNode.get("roles_sg").asList().withEmptyListAsDefault().ofStrings();
        Map<String, Object> attributes;

        if (vJsonNode.hasNonNull("config")) {
            try {
                configVersions = ConfigVersionSet.parse(jsonNode.getAsNode("config"));
            } catch (ConfigValidationException e) {
                validationErrors.add("config", e);
            }
        }

        DocNode attrsNode = jsonNode.getAsNode("attrs");

        if (attrsNode != null && !attrsNode.isNull()) {
            attributes = attrsNode.toMap();
        } else {
            attributes = new LinkedHashMap<>();
        }

        return new AuthTokenPrivilegeBase(backendRoles, searchGuardRoles, attributes, configVersions);
    }

    public ConfigSnapshot peekConfigSnapshot() {
        return configSnapshot;
    }

    public ConfigSnapshot getConfigSnapshot() {
        if (configSnapshot == null) {
            if (configVersions == null) {
                return null;
            } else {
                throw new IllegalStateException("ConfigSnapshot has not been loaded yet. configVersions: " + configVersions);
            }
        }
        return configSnapshot;
    }

    void setConfigSnapshot(ConfigSnapshot configSnapshot) {
        if (configSnapshot != null && !this.configVersions.equals(configSnapshot.getConfigVersions())) {
            throw new IllegalArgumentException("ConfigSnapshot " + configSnapshot + " does not match stored versions " + this.configVersions);
        }

        this.configSnapshot = configSnapshot;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(this.backendRoles);
        out.writeStringCollection(this.searchGuardRoles);
        out.writeGenericMap(this.attributes);
        out.writeOptionalWriteable(this.configVersions);
    }

    @Override
    public String toString() {
        return "AuthTokenPrivilegeBase [backendRoles=" + backendRoles + ", searchGuardRoles=" + searchGuardRoles + ", configVersions="
                + configVersions + ", attributes=" + attributes + ", configSnapshot=" + configSnapshot + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
        result = prime * result + ((backendRoles == null) ? 0 : backendRoles.hashCode());
        result = prime * result + ((configVersions == null) ? 0 : configVersions.hashCode());
        result = prime * result + ((searchGuardRoles == null) ? 0 : searchGuardRoles.hashCode());
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
        AuthTokenPrivilegeBase other = (AuthTokenPrivilegeBase) obj;
        if (attributes == null) {
            if (other.attributes != null)
                return false;
        } else if (!attributes.equals(other.attributes))
            return false;
        if (backendRoles == null) {
            if (other.backendRoles != null)
                return false;
        } else if (!backendRoles.equals(other.backendRoles))
            return false;
        if (configVersions == null) {
            if (other.configVersions != null)
                return false;
        } else if (!configVersions.equals(other.configVersions))
            return false;
        if (searchGuardRoles == null) {
            if (other.searchGuardRoles != null)
                return false;
        } else if (!searchGuardRoles.equals(other.searchGuardRoles))
            return false;
        return true;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }
}
