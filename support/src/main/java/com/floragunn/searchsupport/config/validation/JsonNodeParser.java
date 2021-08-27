package com.floragunn.searchsupport.config.validation;
import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.codova.validation.ConfigValidationException;

@FunctionalInterface
public interface JsonNodeParser<ValueType> {
    ValueType parse(JsonNode jsonNode) throws ConfigValidationException;

    default String getExpectedValue() {
        return null;
    }

}
