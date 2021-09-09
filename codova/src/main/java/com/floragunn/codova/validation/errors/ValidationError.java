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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.core.JsonLocation;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;

public class ValidationError implements ToXContentObject {
    private static final Logger log = LogManager.getLogger(ValidationError.class);

    private String attribute;
    private String message;
    private Exception cause;
    private Object docNode;

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

    public ValidationError cause(Exception cause) {
        this.cause = cause;
        return this;
    }

    public Exception getCause() {
        return cause;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("error", message);
        builder.endObject();
        return builder;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    @Override
    public String toString() {
        return "ValidationError [message=" + message + ", cause=" + cause + "]";
    }

    public String toValidationErrorsOverviewString() {
        return message;
    }
    
    public static List<ValidationError> parseArray(String attribute, DocNode docNode) {
        if (!docNode.isList()) {
            return Collections.singletonList(parse(attribute, docNode));
        } else {
            ArrayList<ValidationError> result = new ArrayList<>(docNode.size());

            try {
                for (DocNode subDocNode : docNode.getListOfNodes(null)) {
                    result.add(parse(attribute, subDocNode));
                }
            } catch (ConfigValidationException e) {
                throw new RuntimeException(e);
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
}