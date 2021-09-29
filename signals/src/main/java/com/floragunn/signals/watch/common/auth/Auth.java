package com.floragunn.signals.watch.common.auth;

import org.elasticsearch.common.xcontent.ToXContentObject;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.MissingAttribute;

public abstract class Auth implements ToXContentObject {

    public static final String INCLUDE_CREDENTIALS = "INCLUDE_CREDENTIALS";

    public static Auth create(DocNode jsonNode) throws ConfigValidationException {

        if (!jsonNode.hasNonNull("type")) {
            throw new ConfigValidationException(new MissingAttribute("type", jsonNode));
        }

        String type = jsonNode.getAsString("type");

        switch (type) {
        case "basic":
            return BasicAuth.create(jsonNode);

        default:
            throw new ConfigValidationException(new InvalidAttributeValue("type", type, "basic", jsonNode));
        }
    }
}
