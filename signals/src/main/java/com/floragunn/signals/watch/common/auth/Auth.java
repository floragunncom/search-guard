package com.floragunn.signals.watch.common.auth;

import org.elasticsearch.common.xcontent.ToXContentObject;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.InvalidAttributeValue;
import com.floragunn.searchsupport.jobs.config.validation.MissingAttribute;

public abstract class Auth implements ToXContentObject {

    public static final String INCLUDE_CREDENTIALS = "INCLUDE_CREDENTIALS";

    public static Auth create(JsonNode jsonNode) throws ConfigValidationException {

        if (!jsonNode.hasNonNull("type")) {
            throw new ConfigValidationException(new MissingAttribute("type", jsonNode));
        }

        String type = jsonNode.get("type").textValue();

        switch (type) {
        case "basic":
            return BasicAuth.create(jsonNode);

        default:
            throw new ConfigValidationException(new InvalidAttributeValue("type", type, "basic", jsonNode));
        }
    }
}
