/*
 * Copyright 2021-2022 floragunn GmbH
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

package com.floragunn.searchsupport.action;

import java.util.LinkedHashMap;
import java.util.Map;

import org.elasticsearch.ExceptionsHelper;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.OrderedImmutableMap;
import com.floragunn.searchsupport.action.Action.UnparsedMessage;

public class StandardResponse extends Action.Response {
    
    public static StandardResponse internalServerError() {
        return new StandardResponse(500, new Error("Internal Server Error"));
    }
    
    private Error error;
    private String message;
    private Object data;

    public StandardResponse() {

    }

    public StandardResponse(int status) {
        status(status);
    }

    public StandardResponse(int status, String message) {
        status(status);
        this.message = message;
    }

    public StandardResponse(int status, Error error) {
        status(status);
        this.error = error;
    }
    
    public StandardResponse(ConfigValidationException e) {
        status(400);
        this.error = new Error(e.getMessage()).details(e.getValidationErrors().toBasicObject());
    }
    
    public StandardResponse(Exception e) {
        if (e instanceof ConfigValidationException) {
            status(400);
            this.error = new Error(e.getMessage()).details(((ConfigValidationException) e).getValidationErrors().toBasicObject());            
        } else {
            status(ExceptionsHelper.status(e).getStatus());
            this.error = new Error(e.getMessage());
        }
    }

    public StandardResponse(UnparsedMessage message) throws ConfigValidationException {
        super(message);
        DocNode docNode = message.requiredDocNode();

        this.message = docNode.getAsString("message");
        this.error = docNode.hasNonNull("error") ? new Error(docNode.getAsNode("error")) : null;
        this.data = docNode.hasNonNull("data") ? docNode.get("data") : null;
    }

    public StandardResponse data(Object data) {
        this.data = data;
        return this;
    }

    public StandardResponse data(Map<?, ? extends Document<?>> map) {
        if(map == null) {
            this.data = null;
        } else {

            Map<String, Object> plainMap = new LinkedHashMap<>(map.size());

            for (Map.Entry<?, ? extends Document<?>> entry : map.entrySet()) {
                plainMap.put(String.valueOf(entry.getKey()), entry.getValue() != null ? entry.getValue().toBasicObject() : null);
            }

            this.data = plainMap;
        }
        return this;
    }

    public StandardResponse message(String message) {
        this.message = message;
        return this;
    }

    public StandardResponse error(Error error) {
        this.error = error;
        return this;
    }

    public StandardResponse error(String message) {
        this.error = new Error(message);
        return this;
    }

    public StandardResponse error(String code, String message, Object details) {
        this.error = new Error(code, message, details);
        return this;
    }

    public StandardResponse error(ConfigValidationException e) {
        this.error = new Error(null, e.getMessage(), e.getValidationErrors().toMap());
        return this;
    }

    public StandardResponse error(ValidationErrors validationErrors) {
        return this.error(new ConfigValidationException(validationErrors));
    }

    @Override
    public StandardResponse eTag(String concurrencyControlEntityTag) {
        super.eTag(concurrencyControlEntityTag);
        return this;
    }

    @Override
    public Object toBasicObject() {
        return OrderedImmutableMap.ofNonNull("status", getStatus(), "error", error != null ? error.toBasicObject() : null, "message", message, "data",
                data);
    }

    public static class Error implements Document<Error> {
        private final String message;
        private final String code;
        private final Object details;

        public Error(String message) {
            this.message = message;
            this.code = null;
            this.details = null;
        }

        public Error(DocNode docNode) {
            this.message = docNode.getAsString("message");
            this.code = docNode.getAsString("code");
            this.details = docNode.get("details");
        }

        private Error(String code, String message, Object details) {
            this.message = message;
            this.code = code;
            this.details = details;
        }

        public Error details(Object details) {
            return new Error(code, message, details);
        }

        public Error code(String code) {
            return new Error(code, message, details);
        }

        @Override
        public Object toBasicObject() {
            return OrderedImmutableMap.ofNonNull("code", code, "message", message, "details", details);
        }
    }
}
