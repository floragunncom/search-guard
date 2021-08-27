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

package com.floragunn.searchsupport.config.validation;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.UnexpectedDocumentStructureException;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.JsonValidationError;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchsupport.xcontent.JacksonXContentParser;

public class ValidatingJsonParser {

    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

    public static JsonNode readTree(String string) throws ConfigValidationException {
        try {
            return readTree0(string, jsonMapper);
        } catch (JsonParseException e) {
            throw new ConfigValidationException(new JsonValidationError(null, e));
        } catch (IOException e) {
            throw new ConfigValidationException(new ValidationError(null, "Error while parsing JSON document: " + e.getMessage(), null).cause(e));
        }
    }

    public static JsonNode readYamlTree(String string) throws ConfigValidationException {
        try {
            return readTree0(string, yamlMapper);
        } catch (JsonParseException e) {
            throw new ConfigValidationException(new JsonValidationError(null, e));
        } catch (IOException e) {
            throw new ConfigValidationException(new ValidationError(null, "Error while parsing YAML document: " + e.getMessage(), null).cause(e));
        }
    }

    public static JsonNode readTree(BytesReference data, XContentType contentType) throws ConfigValidationException {
        try {
            return JacksonXContentParser.readTree(data, contentType);
        } catch (JsonParseException e) {
            throw new ConfigValidationException(new JsonValidationError(null, e));
        } catch (IOException e) {
            throw new ConfigValidationException(new ValidationError(null, "Error while parsing JSON document: " + e.getMessage(), null).cause(e));
        }
    }

    public static ObjectNode readObject(String string) throws ConfigValidationException {
        JsonNode jsonNode = readTree(string);

        if (jsonNode instanceof ObjectNode) {
            return (ObjectNode) jsonNode;
        } else {
            throw new ConfigValidationException(new ValidationError(null, "The JSON root node must be an object"));
        }
    }

    public static Map<String, Object> readObjectAsMap(String string) throws ConfigValidationException {
        try {
            return DocReader.readObject(string);
        } catch (UnexpectedDocumentStructureException e) {
            throw new ConfigValidationException(new ValidationError(null, "The JSON root node must be an object").cause(e));
        } catch (JsonProcessingException e) {
            throw new ConfigValidationException(new JsonValidationError(null, e));
        }
    }


    public static Map<String, Object> readObjectAsMap(String string, XContentType contentType) throws ConfigValidationException {
        try {
            return DocReader.readObject(string, contentType);
        } catch (UnexpectedDocumentStructureException e) {
            throw new ConfigValidationException(new ValidationError(null, "The JSON root node must be an object").cause(e));
        } catch (JsonProcessingException e) {
            throw new ConfigValidationException(new JsonValidationError(null, e));
        }
    }
    
    private static JsonNode readTree0(String string, ObjectMapper objectMapper) throws IOException {

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<JsonNode>() {
                @Override
                public JsonNode run() throws Exception {
                    return objectMapper.readTree(string);
                }
            });
        } catch (final PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }

}
