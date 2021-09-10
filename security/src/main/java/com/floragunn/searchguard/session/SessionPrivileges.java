/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.session;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.json.JacksonTools;
import com.google.common.collect.ImmutableMap;

public class SessionPrivileges implements ToXContentObject, Writeable, Serializable {

    private static final long serialVersionUID = 8836312044879615458L;

    static final String PARAM_COMPACT = "compact";

    static final Params COMPACT = new ToXContent.MapParams(ImmutableMap.of(PARAM_COMPACT, "true"));

    private final List<String> backendRoles;

    private final List<String> searchGuardRoles;
    private final Map<String, Object> attributes;


    public SessionPrivileges(Collection<String> backendRoles, Collection<String> searchGuardRoles, Map<String, Object> attributes) {
        this.backendRoles = Collections.unmodifiableList(new ArrayList<>(backendRoles));
        this.searchGuardRoles = Collections.unmodifiableList(new ArrayList<>(searchGuardRoles));
        this.attributes = Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
    }

    public SessionPrivileges(StreamInput in) throws IOException {
        this.backendRoles = in.readStringList();
        this.searchGuardRoles = in.readStringList();
        this.attributes = in.readMap();
    }

    public List<String> getBackendRoles() {
        return backendRoles;
    }

    public List<String> getSearchGuardRoles() {
        return searchGuardRoles;
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

        builder.endObject();
        return builder;
    }

    public static SessionPrivileges parse(JsonNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);


        List<String> backendRoles = vJsonNode.stringList("roles_be", Collections.emptyList());
        List<String> searchGuardRoles = vJsonNode.stringList("roles_sg", Collections.emptyList());
        Map<String, Object> attributes;

        ObjectNode attrsNode = vJsonNode.getObjectNode("attrs");

        if (attrsNode != null) {
            attributes = JacksonTools.toMap(attrsNode);
        } else {
            attributes = new LinkedHashMap<>();
        }

        return new SessionPrivileges(backendRoles, searchGuardRoles, attributes);
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(this.backendRoles);
        out.writeStringCollection(this.searchGuardRoles);
        out.writeMap(this.attributes);
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
        result = prime * result + ((backendRoles == null) ? 0 : backendRoles.hashCode());
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
        SessionPrivileges other = (SessionPrivileges) obj;
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
