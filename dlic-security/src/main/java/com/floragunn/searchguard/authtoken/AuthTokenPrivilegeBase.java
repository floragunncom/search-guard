package com.floragunn.searchguard.authtoken;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.searchguard.sgconf.history.ConfigSnapshot;
import com.floragunn.searchguard.sgconf.history.ConfigVersionSet;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.MissingAttribute;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidationErrors;

public class AuthTokenPrivilegeBase implements ToXContentObject, Writeable, Serializable {
   
    private static final long serialVersionUID = -7176396883010611335L;

    private final List<String> backendRoles;

    private final List<String> searchGuardRoles;
    private final ConfigVersionSet configVersions;
    private final Map<String, String> attributes;

    private transient ConfigSnapshot configSnapshot;

    public AuthTokenPrivilegeBase(Collection<String> backendRoles, Collection<String> searchGuardRoles, Map<String, String> attributes,
            ConfigVersionSet configVersions) {
        this.configVersions = configVersions;
        this.backendRoles = Collections.unmodifiableList(new ArrayList<>(backendRoles));
        this.searchGuardRoles = Collections.unmodifiableList(new ArrayList<>(searchGuardRoles));
        this.attributes = Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
    }

    public AuthTokenPrivilegeBase(StreamInput in) throws IOException {
        this.backendRoles = in.readStringList();
        this.searchGuardRoles = in.readStringList();
        this.attributes = in.readMap(StreamInput::readString, StreamInput::readString);
        this.configVersions = new ConfigVersionSet(in);
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
        builder.startObject();

        if (backendRoles.size() > 0) {
            builder.field("roles_be", backendRoles);
        }

        if (searchGuardRoles.size() > 0) {
            builder.field("roles_sg", searchGuardRoles);
        }

        if (attributes.size() > 0) {
            builder.field("attrs", attributes);
        }

        builder.field("config", (ToXContent) configVersions);

        builder.endObject();
        return builder;
    }

    public static AuthTokenPrivilegeBase parse(JsonNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        ConfigVersionSet configVersions = null;

        List<String> backendRoles = vJsonNode.stringList("roles_be", Collections.emptyList());
        List<String> searchGuardRoles = vJsonNode.stringList("roles_sg", Collections.emptyList());
        Map<String, String> attributes = new LinkedHashMap<>();

        if (vJsonNode.hasNonNull("config")) {
            try {
                configVersions = ConfigVersionSet.parse(vJsonNode.get("config"));
            } catch (ConfigValidationException e) {
                validationErrors.add("config", e);
            }
        } else {
            validationErrors.add(new MissingAttribute("config", jsonNode));
        }

        ObjectNode attrsNode = vJsonNode.getObjectNode("attrs");

        if (attrsNode != null) {
            Iterator<String> fieldNames = attrsNode.fieldNames();

            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                attributes.put(fieldName, attrsNode.get(fieldName).textValue());
            }
        }

        return new AuthTokenPrivilegeBase(backendRoles, searchGuardRoles, attributes, configVersions);
    }

    public ConfigSnapshot getConfigSnapshot() {
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
        out.writeMap(this.attributes, StreamOutput::writeString, StreamOutput::writeString);
        this.configVersions.writeTo(out);
    }

    @Override
    public String toString() {
        return "AuthTokenPrivilegeBase [backendRoles=" + backendRoles + ", searchGuardRoles=" + searchGuardRoles + ", configVersions="
                + configVersions + ", attributes=" + attributes + ", configSnapshot=" + configSnapshot + "]";
    }
}
