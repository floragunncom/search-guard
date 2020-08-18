package com.floragunn.searchguard.authtoken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.searchguard.sgconf.history.ConfigVersionSet;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.MissingAttribute;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidationErrors;

public class AuthTokenPrivilegeBase implements ToXContentObject {
    private final List<String> backendRoles;

    private final List<String> searchGuardRoles;
    private final ConfigVersionSet configVersions;
    private final Map<String, String> attributes;

    public AuthTokenPrivilegeBase(Collection<String> backendRoles, Collection<String> searchGuardRoles, Map<String, String> attributes,
            ConfigVersionSet configVersions) {
        this.configVersions = configVersions;
        this.backendRoles = Collections.unmodifiableList(new ArrayList<>(backendRoles));
        this.searchGuardRoles = Collections.unmodifiableList(new ArrayList<>(searchGuardRoles));
        this.attributes = Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
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

        List<String> backendRoles = vJsonNode.stringList("roles_be");
        List<String> searchGuardRoles = vJsonNode.stringList("roles_sg");
        Map<String, String> attributes = new LinkedHashMap<>();

        if (vJsonNode.hasNonNull("config")) {
            try {
                configVersions = ConfigVersionSet.parse(vJsonNode.get("config"));
            } catch (ConfigValidationException e) {
                validationErrors.add("base", e);
            }
        } else {
            validationErrors.add(new MissingAttribute("base", jsonNode));
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
}
