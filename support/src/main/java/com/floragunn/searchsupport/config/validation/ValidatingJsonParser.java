package com.floragunn.searchsupport.config.validation;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.elasticsearch.SpecialPermission;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ValidatingJsonParser {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static JsonNode readTree(String string) throws ConfigValidationException {
        try {
            return readTree0(string);
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

    private static JsonNode readTree0(String string) throws IOException {

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<JsonNode>() {
                @Override
                public JsonNode run() throws Exception {
                    return objectMapper.readTree(string);
                }
            });
        } catch (final PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }

}
