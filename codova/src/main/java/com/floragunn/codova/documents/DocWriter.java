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

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * A lightweight, reflection-less way of serializing JSON. Is capable of handling these basic Java types:
 * 
 * - Collection
 * - Map
 * - String
 * - Character
 * - Boolean
 * - Number
 * - Enum
 */
public class DocWriter {

    public static String writeAsString(Object object) {
        return writeAsString(object, XContentType.JSON);
    }
    
    public static String writeAsString(Object object, XContentType contentType) {
        try (StringWriter writer = new StringWriter(); JsonGenerator generator = getJsonFactory(contentType).createGenerator(writer)) {
            new DocWriter(generator).write(object);

            generator.flush();
            writer.flush();
            
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
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
    
    private static JsonFactory jsonFactory = new JsonFactory();
    private static YAMLFactory yamlFactory = new YAMLFactory();
       
     private JsonGenerator generator;
    private int maxDepth = 20;

    public DocWriter(JsonGenerator generator) {
        this.generator = generator;
    }

    public void write(Object object) throws IOException {
        write(object, 0);
    }

    private void write(Object object, int depth) throws IOException {
        if (depth > maxDepth) {
            throw new JsonGenerationException("Max JSON depth exceeded", generator);
        }

        if (object instanceof Collection) {
            @SuppressWarnings("unchecked")
            Collection<Object> collection = (Collection<Object>) object;

            generator.writeStartArray();

            for (Object element : collection) {
                write(element, depth + 1);
            }

            generator.writeEndArray();
        } else if (object instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<Object, Object> map = (Map<Object, Object>) object;

            generator.writeStartObject();

            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                generator.writeFieldName(String.valueOf(entry.getKey()));

                write(entry.getValue(), depth + 1);
            }

            generator.writeEndObject();
        } else if (object instanceof String) {
            generator.writeString((String) object);
        } else if (object instanceof Character) {
            generator.writeString(object.toString());
        } else if (object instanceof Integer) {
            generator.writeNumber(((Integer) object).intValue());
        } else if (object instanceof Long) {
            generator.writeNumber(((Long) object).longValue());
        } else if (object instanceof Short) {
            generator.writeNumber(((Short) object).shortValue());
        } else if (object instanceof Float) {
            generator.writeNumber(((Float) object).floatValue());
        } else if (object instanceof Double) {
            generator.writeNumber(((Double) object).doubleValue());
        } else if (object instanceof BigDecimal) {
            generator.writeNumber((BigDecimal) object);
        } else if (object instanceof BigInteger) {
            generator.writeNumber((BigInteger) object);
        } else if (object instanceof Number) {
            generator.writeNumber(object.toString());
        } else if (object instanceof Boolean) {
            generator.writeBoolean(((Boolean) object).booleanValue());
        } else if (object instanceof Enum) {
            generator.writeString(((Enum<?>) object).name());
        } else if (object == null) {
            generator.writeNull();
        } else {
            throw new JsonGenerationException("Unsupported object type: " + object, generator);
        }
    }
}
