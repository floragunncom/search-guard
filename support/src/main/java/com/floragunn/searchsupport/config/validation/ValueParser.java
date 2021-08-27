package com.floragunn.searchsupport.config.validation;

import com.floragunn.codova.validation.ConfigValidationException;

public interface ValueParser<ValueType> {
    ValueType parse(String string) throws ConfigValidationException;

    String getExpectedValue();
}
