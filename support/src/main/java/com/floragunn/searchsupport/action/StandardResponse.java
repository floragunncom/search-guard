/*
 * Copyright 2021 floragunn GmbH
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

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchsupport.action.Action.UnparsedMessage;

public class StandardResponse extends Action.Response {
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
    
    @Override
    public StandardResponse eTag(String concurrencyControlEntityTag) {
        super.eTag(concurrencyControlEntityTag);
        return this;
    }

    @Override
    public Object toBasicObject() {
        Map<String, Object> result = new LinkedHashMap<>();

        result.put("status", getStatus());

        if (data != null) {
            result.put("data", data);
        }

        if (error != null) {
            result.put("error", error.toBasicObject());
        }

        if (message != null) {
            result.put("message", message);
        }

        return result;
    }

    public static class Error implements Document {
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
            Map<String, Object> result = new LinkedHashMap<>(3);

            if (code != null) {
                result.put("code", code);
            }
            result.put("message", message);

            if (details != null) {
                result.put("details", details);
            }

            return result;
        }

    }
}
