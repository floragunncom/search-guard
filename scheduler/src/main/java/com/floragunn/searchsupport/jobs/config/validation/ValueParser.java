package com.floragunn.searchsupport.jobs.config.validation;

public interface ValueParser<ValueType> {
    ValueType parse(String string) throws ConfigValidationException;

    String getExpectedValue();
}
