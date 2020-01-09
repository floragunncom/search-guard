package com.floragunn.searchsupport.jobs.config.validation;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.searchguard.DefaultObjectMapper;

public class ValidatingJsonParser {
    public static JsonNode readTree(String string) throws ConfigValidationException {
        try {
            return DefaultObjectMapper.readTree(string);
        } catch (JsonParseException e) {
            throw new ConfigValidationException(new JsonValidationError(null, e));
        } catch (IOException e) {
            throw new ConfigValidationException(new ValidationError(null, "Error while parsing JSON document: " + e.getMessage(), null).cause(e));
        }
    }

    public static ObjectNode readObject(String string) throws ConfigValidationException {
        JsonNode jsonNode = readTree(string);

        if (jsonNode instanceof ObjectNode) {
            return (ObjectNode) jsonNode;
        } else {
            throw new ConfigValidationException(new ValidationError(null, "The JSON root node must be an object"));
        }
    }

}
