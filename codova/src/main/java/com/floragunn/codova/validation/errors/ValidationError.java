/*
 * Copyright 2020-2022 floragunn GmbH
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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonLocation;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;

public class ValidationError implements Document<ValidationError> {
    private static final Logger log = LoggerFactory.getLogger(ValidationError.class);

    private String attribute;
    private String message;
    private Throwable cause;
    private Object docNode;
    private Object expected;

    public ValidationError(String attribute, String message) {
        this.attribute = attribute != null ? attribute : "_";
        this.message = message;
    }

    public ValidationError(String attribute, String message, Object jsonNode) {
        this.attribute = attribute != null ? attribute : "_";
        this.message = message;
        this.docNode = jsonNode;
    }

    public String getAttribute() {
        return attribute;
    }

    public String getMessage() {
        return message;
    }

    public ValidationError message(String message) {
        this.message = message;
        return this;
    }

    public ValidationError cause(Throwable cause) {
        this.cause = cause;
        return this;
    }

    public ValidationError expected(Object expected) {
        this.expected = expected;
        return this;
    }

    public Throwable getCause() {
        return cause;
    }

    @Override
    public Map<String, Object> toBasicObject() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("error", message);
        if (expected != null) {
            result.put("expected", getExpectedAsString());
        }
        return result;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    @Override
    public String toString() {
        return message + (getExpected() != null ? ("; expected: " + getExpectedAsString()) : "") + "; attribute: " + getAttribute();
    }

    public String toValidationErrorsOverviewString() {
        if (getExpected() != null) {
            return message + "; expected: " + getExpectedAsString();
        } else {
            return message;
        }
    }

    public static List<ValidationError> parseArray(String attribute, DocNode docNode) {
        if (!docNode.isList()) {
            return Collections.singletonList(parse(attribute, docNode));
        } else {
            ArrayList<ValidationError> result = new ArrayList<>(docNode.size());

            for (DocNode subDocNode : docNode.getAsListOfNodes(null)) {
                result.add(parse(attribute, subDocNode));
            }

            return result;
        }
    }

    public static ValidationError parse(String attribute, DocNode docNode) {
        if (!docNode.isMap()) {
            return new ValidationError(attribute, docNode.getAsString(null));
        }

        if (!(docNode.get("error") instanceof String)) {
            return new ValidationError(attribute, docNode.getAsString(null));
        }

        String error = docNode.getAsString("error");

        if (error.equalsIgnoreCase("Invalid value") && docNode.containsKey("value")) {
            return new InvalidAttributeValue(attribute, docNode.get("value"), docNode.get("expected"));
        } else if (error.equalsIgnoreCase("File does not exist")) {
            try {
                return new FileDoesNotExist(attribute, docNode.hasNonNull("value") ? new File(docNode.getAsString("value")) : null);
            } catch (Exception e) {
                return new ValidationError(attribute, "File nodes not exist: " + docNode.getAsString("value"));
            }
        } else if (error.startsWith("Invalid JSON")) {
            JsonLocation jsonLocation = null;

            if (docNode.get("line") instanceof Number && docNode.get("column") instanceof Number) {
                try {
                    jsonLocation = new JsonLocation(null, -1, docNode.getNumber("line").intValue(), docNode.getNumber("column").intValue());
                } catch (ConfigValidationException e) {
                    log.warn("Error while parsing JsonLocation in " + docNode, e);
                }
            }

            return new JsonValidationError(attribute, error, jsonLocation, docNode.getAsString("context"));
        } else if (error.equalsIgnoreCase("Required attribute is missing")) {
            return new MissingAttribute(attribute);
        } else if (error.equalsIgnoreCase("Unsupported attribute")) {
            return new UnsupportedAttribute(attribute, docNode.get("value"), null);
        } else {
            return new ValidationError(attribute, error);
        }
    }

    public Object getDocNode() {
        return docNode;
    }

    public ValidationError docNode(Object docNode) {
        this.docNode = docNode;
        return this;
    }

    @Override
    protected ValidationError clone() {
        return new ValidationError(attribute, message).cause(cause).docNode(docNode);
    }

    public Object getExpected() {
        return expected;
    }

    public String getExpectedAsString() {
        return expectedToString(getExpected());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static String expectedToString(Object expected) {
        if (expected == null) {
            return null;
        } else if (expected instanceof Class<?> && ((Class<?>) expected).isEnum()) {
            return getEnumValues((Class<Enum>) expected);
        } else {
            return expected.toString();
        }
    }

    private static <E extends Enum<E>> String getEnumValues(Class<E> enumClass) {
        StringBuilder result = new StringBuilder();

        for (E e : enumClass.getEnumConstants()) {
            if (result.length() > 0) {
                result.append("|");
            }

            result.append(e.name());
        }

        return result.toString();
    }

}