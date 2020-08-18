package com.floragunn.searchguard.authtoken;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.MissingAttribute;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidationErrors;

public class AuthToken implements ToXContentObject {
    private final String userName;
    private final String tokenName;
    private final String id;

    private final RequestedPrivileges requestedPrivilges;
    private final AuthTokenPrivilegeBase base;

    AuthToken(String id, String userName, String tokenName, RequestedPrivileges requestedPrivilges, AuthTokenPrivilegeBase base) {
        this.id = id;
        this.userName = userName;
        this.tokenName = tokenName;
        this.requestedPrivilges = requestedPrivilges;
        this.base = base;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("user_name", userName);
        builder.field("token_name", tokenName);
        builder.field("requested", requestedPrivilges);
        builder.field("base", base);

        builder.endObject();
        return builder;
    }

    public String getId() {
        return id;
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

    public AuthTokenPrivilegeBase getBase() {
        return base;
    }

    public static AuthToken parse(String id, JsonNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        String userName = vJsonNode.requiredString("user_name");
        String tokenName = vJsonNode.string("token_name");
        AuthTokenPrivilegeBase base = null;
        RequestedPrivileges requestedPrivilges = null;

        if (vJsonNode.hasNonNull("base")) {
            try {
                base = AuthTokenPrivilegeBase.parse(vJsonNode.get("base"));
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
        
        return new AuthToken(id, userName, tokenName, requestedPrivilges, base);
    }

}
