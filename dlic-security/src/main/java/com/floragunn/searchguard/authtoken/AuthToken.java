package com.floragunn.searchguard.authtoken;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.sgconf.history.ConfigVersionSet;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.MissingAttribute;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidationErrors;

public class AuthToken implements ToXContentObject {
    private final String userName;
    private final String tokenName;
    private final String id;
    private final ConfigVersionSet configVersions;
    private final RequestedPrivileges requestedPrivilges;

    AuthToken(String id, String userName, String tokenName, RequestedPrivileges requestedPrivilges, ConfigVersionSet configVersions) {
        this.id = id;
        this.userName = userName;
        this.tokenName = tokenName;
        this.requestedPrivilges = requestedPrivilges;
        this.configVersions = configVersions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("user_name", userName);
        builder.field("token_name", tokenName);
        builder.field("requested", requestedPrivilges);
        builder.field("base", (ToXContent) configVersions);

        builder.endObject();
        return builder;
    }

    public String getId() {
        return id;
    }

    public ConfigVersionSet getConfigVersions() {
        return configVersions;
    }

    public RequestedPrivileges getRequestedPrivilges() {
        return requestedPrivilges;
    }

    public String getUserName() {
        return userName;
    }

    public String getTokenName() {
        return tokenName;
    }
    
    public static AuthToken parse(String id, JsonNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        String userName = vJsonNode.requiredString("user_name");
        String tokenName = vJsonNode.string("token_name");
        ConfigVersionSet configVersions = null;
        RequestedPrivileges requestedPrivilges = null;

        if (vJsonNode.hasNonNull("base")) {
            try {
                configVersions = ConfigVersionSet.parse(vJsonNode.get("base"));
            } catch (ConfigValidationException e) {
                validationErrors.add("base", e);
            }
        } else {
            validationErrors.add(new MissingAttribute("base", jsonNode));
        }

        if (vJsonNode.hasNonNull("requested")) {
            try {
                requestedPrivilges = RequestedPrivileges.parse(vJsonNode.get("requested"));
            } catch (ConfigValidationException e) {
                validationErrors.add("requested", e);
            }
        } else {
            validationErrors.add(new MissingAttribute("requested", jsonNode));
        }
        
        return new AuthToken(id, userName, tokenName, requestedPrivilges, configVersions);
    }
}
