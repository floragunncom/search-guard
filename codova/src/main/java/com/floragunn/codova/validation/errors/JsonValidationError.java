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

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;

public class JsonValidationError extends ValidationError {
    private JsonLocation jsonLocation;
    private String context;

    public JsonValidationError(String attribute, JsonProcessingException jsonProcessingException) {
        super(attribute, "Error while parsing JSON document: " + jsonProcessingException.getOriginalMessage());
        cause(jsonProcessingException);
        this.jsonLocation = jsonProcessingException.getLocation();

        if (jsonProcessingException instanceof JsonParseException) {
            this.context = ((JsonParseException) jsonProcessingException).getRequestPayloadAsString();
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("error", getMessage());

        if (jsonLocation != null) {
            builder.field("line", jsonLocation.getLineNr());
        }

        if (jsonLocation != null) {
            builder.field("column", jsonLocation.getColumnNr());
        }

        if (context != null) {
            builder.field("context", context);
        }

        builder.endObject();
        return builder;
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
