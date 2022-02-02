/*
 * Copyright 2020-2021 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.codova.validation.errors;

import java.util.LinkedHashMap;
import java.util.Map;

public class InvalidAttributeValue extends ValidationError {
    private final Object value;

    public InvalidAttributeValue(String attribute, Object value, Object expected, Object jsonNode) {
        super(attribute, "Invalid value");
        this.expected(expected);
        this.value = value;
        this.docNode(jsonNode);
    }

    public InvalidAttributeValue(String attribute, Object value, Object expected) {
        this(attribute, value, expected, null);
    }

    @Override
    public Map<String, Object> toBasicObject() {
        Map<String, Object> result = new LinkedHashMap<>();

        result.put("error", getMessage());

        result.put("value", value);

        if (getExpected() != null) {
            result.put("expected", getExpectedAsString());
        }

        return result;
    }

    @Override
    public String toString() {
        return getMessage() + " " + (getExpected() != null ? ("; expected: " + getExpectedAsString()) : "") + "; value: " + value + "; attribute: "
                + getAttribute();
    }

    @Override
    public String toValidationErrorsOverviewString() {
        StringBuilder result = new StringBuilder(getMessage());

        if (getExpected() != null) {
            result.append("; expected: ").append(getExpectedAsString());
        }

        result.append("; got: ").append(value);

        return result.toString();
    }

    public InvalidAttributeValue expected(Object expected) {
        super.expected(expected);
        return this;
    }

    @Override
    protected InvalidAttributeValue clone() {
        // TODO introduce generic base class to make casting not necessary
        return (InvalidAttributeValue) new InvalidAttributeValue(getAttribute(), value, getExpected()).cause(getCause()).docNode(getDocNode())
                .message(getMessage());
    }
}