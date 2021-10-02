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

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;

public class JsonValidationError extends ValidationError {
    private JsonLocation jsonLocation;
    private String context;

    public JsonValidationError(String attribute, JsonProcessingException jsonProcessingException) {
        super(attribute, "Invalid JSON document: " + jsonProcessingException.getOriginalMessage());
        cause(jsonProcessingException);
        this.jsonLocation = jsonProcessingException.getLocation();

        if (jsonProcessingException instanceof JsonParseException) {
            this.context = ((JsonParseException) jsonProcessingException).getRequestPayloadAsString();
        }
    }

    public JsonValidationError(String attribute, String message, JsonLocation jsonLocation, String context) {
        super(attribute, message);
        this.jsonLocation = jsonLocation;
        this.context = context;
    }

    @Override
    public Map<String, Object> toBasicObject() {
        Map<String, Object> result = new LinkedHashMap<>();

        result.put("error", getMessage());

        if (jsonLocation != null) {
            result.put("line", jsonLocation.getLineNr());
            result.put("column", jsonLocation.getColumnNr());
        }

        if (context != null) {
            result.put("context", context);
        }

        return result;
    }

    @Override
    public String toValidationErrorsOverviewString() {
        if (jsonLocation != null) {
            return getMessage() + "; line: " + jsonLocation.getLineNr() + "; column: " + jsonLocation.getColumnNr();
        } else {
            return getMessage();
        }
    }

}
