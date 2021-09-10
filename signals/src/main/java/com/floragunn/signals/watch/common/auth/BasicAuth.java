package com.floragunn.signals.watch.common.auth;

import java.io.IOException;

import org.opensearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;

public class BasicAuth extends Auth {
    private String username;
    private String password;

    public BasicAuth(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public static BasicAuth create(JsonNode jsonObject) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonObject, validationErrors);

        String username = vJsonNode.requiredString("username");
        String password = null;

        if (jsonObject.hasNonNull("username")) {
            username = jsonObject.get("username").asText();
        }

        if (jsonObject.hasNonNull("password")) {
            password = jsonObject.get("password").asText();
        }

        validationErrors.throwExceptionForPresentErrors();

        return new BasicAuth(username, password);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field("type", "basic");

        if (username != null) {
            builder.field("username", username);
        }
        if (password != null) {
            builder.field("password", password);
        }
        builder.endObject();
        return builder;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
