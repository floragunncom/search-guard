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

package com.floragunn.codova.documents;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * A lightweight, reflection-less way of parsing JSON. Parses JSON to these basic Java types:
 * 
 * - List
 * - Map
 * - String
 * - Boolean
 * - Number
 */
public class DocReader {

    public static Object read(JsonParser parser) throws JsonProcessingException, IOException {
        return new DocReader(parser).read();
    }

    public static Object read(InputStream in) throws JsonProcessingException, IOException {
        return read(in, jsonFactory);
    }

    public static Object read(Reader in) throws JsonProcessingException, IOException {
        try (JsonParser parser = jsonFactory.createParser(in)) {
            return new DocReader(parser).read();
        }
    }

    public static Object read(String string) throws JsonProcessingException {
        return read(string, XContentType.JSON);
    }

    public static Object read(String string, XContentType contentType) throws JsonProcessingException {
        try (JsonParser parser = getJsonFactory(contentType).createParser(string)) {
            return new DocReader(parser).read();
        } catch (JsonProcessingException e) {
            throw e;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object read(BytesReference data, XContentType contentType) throws JsonProcessingException {
        try {
            return read(new ByteArrayInputStream(BytesReference.toBytes(data)), getJsonFactory(contentType));
        } catch (JsonProcessingException e) {
            throw e;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Object> readObject(InputStream in) throws JsonProcessingException, IOException {
        return toJsonObject(read(in));
    }

    public static Map<String, Object> readObject(String string) throws JsonProcessingException {
        return toJsonObject(read(string));
    }

    public static Map<String, Object> readObject(String string, XContentType contentType) throws JsonProcessingException {
        return toJsonObject(read(string, contentType));
    }

    public static Map<String, Object> readObject(BytesReference data, XContentType contentType) throws JsonProcessingException {
        return toJsonObject(read(data, contentType));
    }

    public static Map<String, Object> readObject(File file, XContentType contentType)
            throws UnexpectedDocumentStructureException, JsonProcessingException, FileNotFoundException, IOException {
        return toJsonObject(read(new FileInputStream(file), contentType));
    }

    private static Object read(InputStream in, XContentType contentType) throws JsonProcessingException, IOException {
        return read(in, getJsonFactory(contentType));
    }

    private static Object read(InputStream in, JsonFactory jsonFactory) throws JsonProcessingException, IOException {
        try (JsonParser parser = jsonFactory.createParser(in)) {
            return new DocReader(parser).read();
        }
    }

    private static JsonFactory getJsonFactory(XContentType contentType) {
        switch (contentType) {
        case JSON:
            return jsonFactory;
        case YAML:
            return yamlFactory;
        default:
            throw new IllegalArgumentException("Content-Type " + contentType + " is not supported");
        }
    }

    private static Map<String, Object> toJsonObject(Object parsedDocument) throws UnexpectedDocumentStructureException {
        if (parsedDocument instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> result = (Map<String, Object>) parsedDocument;

            return result;
        } else {
            throw new UnexpectedDocumentStructureException(
                    "Expected a JSON object. Got: " + (parsedDocument instanceof List ? "Array" : String.valueOf(parsedDocument)));
        }
    }

    private static JsonFactory jsonFactory = new JsonFactory();
    private static YAMLFactory yamlFactory = new YAMLFactory();

    private JsonParser parser;
    private LinkedList<Object> nodeStack = new LinkedList<>();
    private Object currentNode;
    private Object topNode;
    private String currentAttributeName = null;

    public DocReader(JsonParser parser) {
        this.parser = parser;
    }

    public Object read() throws IOException, JsonProcessingException {

        for (JsonToken token = parser.currentToken() != null ? parser.currentToken() : parser.nextToken(); token != null; token = parser
                .nextToken()) {

            switch (token) {

            case START_OBJECT:
                if (currentNode != null) {
                    nodeStack.add(currentNode);
                }

                currentNode = addNode(new LinkedHashMap<String, Object>());
                break;

            case START_ARRAY:
                if (currentNode != null) {
                    nodeStack.add(currentNode);
                }

                currentNode = addNode(new ArrayList<Object>());
                break;

            case END_OBJECT:
            case END_ARRAY:
                if (nodeStack.isEmpty()) {
                    currentNode = null;
                } else {
                    currentNode = nodeStack.removeLast();
                }
                break;

            case FIELD_NAME:
                currentAttributeName = parser.currentName();
                break;

            case VALUE_TRUE:
                addNode(Boolean.TRUE);
                break;

            case VALUE_FALSE:
                addNode(Boolean.FALSE);
                break;

            case VALUE_NULL:
                addNode(null);
                break;

            case VALUE_NUMBER_FLOAT:
            case VALUE_NUMBER_INT:
                addNode(parser.getNumberValue());
                break;

            case VALUE_STRING:
                addNode(parser.getText());
                break;

            case VALUE_EMBEDDED_OBJECT:
                addNode(parser.getEmbeddedObject());
                break;

            default:
                throw new JsonParseException(parser, "Unexpected token: " + token);

            }

            if (nodeStack.isEmpty() && currentNode == null) {
                break;
            }
        }

        parser.clearCurrentToken();

        return topNode;
    }

    private Object addNode(Object newNode) throws JsonProcessingException {
        if (topNode == null) {
            topNode = newNode;
        }

        if (currentNode instanceof Collection) {
            @SuppressWarnings("unchecked")
            Collection<Object> collection = (Collection<Object>) currentNode;

            collection.add(newNode);
        } else if (currentNode instanceof Map) {
            if (currentAttributeName == null) {
                throw new JsonParseException(parser, "Missing attribute name");
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) currentNode;

            map.put(currentAttributeName, newNode);
        } else if (currentNode != null) {
            throw new JsonParseException(parser, "Node in wrong context");
        }

        currentAttributeName = null;

        return newNode;
    }
}
