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

package com.floragunn.codova.documents;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

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
 * - Date
 * - Instant
 * - LocalDateTime/OffsetDateTime
 * 
 * Additionally, it handles these codova types:
 * 
 * - Document (and thus DocNode)
 * - UnparsedDoc
 */
public class DocWriter {

    public static DocWriter format(Format format) {
        return new DocWriter(format.getJsonFactory());
    }

    public static DocWriter json() {
        return format(Format.JSON);
    }

    public static DocWriter yaml() {
        return format(Format.YAML);
    }

    public static DocWriter smile() {
        return format(Format.SMILE);
    }

    private JsonFactory jsonFactory;
    private int maxDepth = 100;
    private DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_INSTANT;
    private boolean pretty;
    private Function<Object, Object> valueMapper;

    public DocWriter(JsonFactory jsonFactory) {
        this.jsonFactory = jsonFactory;
    }

    public DocWriter pretty() {
        this.pretty = true;
        return this;
    }

    public DocWriter pretty(boolean pretty) {
        this.pretty = pretty;
        return this;
    }
    
    public DocWriter mapValues(Function<Object, Object> valueMapper) {
        this.valueMapper = valueMapper;
        return this;
    }

    public String writeAsString(Object object) {
        try (StringWriter writer = new StringWriter(); JsonGenerator generator = jsonFactory.createGenerator(writer)) {
            if (pretty) {
                generator.useDefaultPrettyPrinter();
            }

            write(generator, object);

            generator.flush();
            writer.flush();

            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] writeAsBytes(Object object) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(); JsonGenerator generator = jsonFactory.createGenerator(out)) {
            write(generator, object);

            generator.flush();
            out.flush();

            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void write(File file, Object object) throws IOException {
        try (FileWriter writer = new FileWriter(file); JsonGenerator generator = jsonFactory.createGenerator(writer)) {
            write(generator, object);

            generator.flush();
            writer.flush();
        }
    }

    public String writeAsString(Document<?> document) {
        return writeAsString(document != null ? document.toBasicObject() : (Object) null);
    }

    public byte[] writeAsBytes(Document<?> document) {
        return writeAsBytes(document != null ? document.toBasicObject() : (Object) null);
    }

    public void write(File file, Document<?> document) throws IOException {
        write(file, document.toBasicObject());
    }

    private void write(JsonGenerator generator, Object object) throws IOException {
        write(generator, object, 0);
    }

    private void write(JsonGenerator generator, Object object, int depth) throws IOException {
        if (depth > maxDepth) {
            throw new JsonGenerationException("Max JSON depth exceeded", generator);
        }

        if (this.valueMapper != null) {
            object = this.valueMapper.apply(object);
        }
        
        if (object instanceof Document && !(object instanceof UnparsedDocument)) {
            object = ((Document<?>) object).toBasicObject();
        }

        if (object instanceof Collection) {
            @SuppressWarnings("unchecked")
            Collection<Object> collection = (Collection<Object>) object;

            generator.writeStartArray();

            for (Object element : collection) {
                write(generator, element, depth + 1);
            }

            generator.writeEndArray();
        } else if (object instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<Object, Object> map = (Map<Object, Object>) object;

            generator.writeStartObject();

            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                generator.writeFieldName(String.valueOf(entry.getKey()));

                write(generator, entry.getValue(), depth + 1);
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
        } else if (object instanceof Date) {
            generator.writeString(dateTimeFormatter.format(((Date) object).toInstant()));
        } else if (object instanceof TemporalAccessor) {
            generator.writeString(dateTimeFormatter.format((TemporalAccessor) object));
        } else if (object instanceof UnparsedDocument) {
            copy(((UnparsedDocument<?>) object).createParser(), generator);
        } else if (object == null) {
            generator.writeNull();
        } else {
            throw new JsonGenerationException("Unsupported object type: " + object.getClass(), generator);
        }
    }

    private void copy(JsonParser parser, JsonGenerator generator) throws IOException {
        for (JsonToken token = parser.currentToken() != null ? parser.currentToken() : parser.nextToken(); token != null; token = parser
                .nextToken()) {

            switch (token) {

            case START_OBJECT:
                generator.writeStartObject();
                break;

            case START_ARRAY:
                generator.writeStartArray();
                break;

            case END_OBJECT:
                generator.writeEndObject();
                break;

            case END_ARRAY:
                generator.writeEndArray();
                break;

            case FIELD_NAME:
                generator.writeFieldName(parser.currentName());
                break;

            case VALUE_TRUE:
                generator.writeBoolean(Boolean.TRUE);
                break;

            case VALUE_FALSE:
                generator.writeBoolean(Boolean.FALSE);
                break;

            case VALUE_NULL:
                generator.writeNull();
                break;

            case VALUE_NUMBER_FLOAT:
                generator.writeNumber(parser.getFloatValue());
                break;

            case VALUE_NUMBER_INT:
                generator.writeNumber(parser.getIntValue());
                break;

            case VALUE_STRING:
                generator.writeString(parser.getText());
                break;

            case VALUE_EMBEDDED_OBJECT:
                generator.writeEmbeddedObject(parser.getEmbeddedObject());
                break;

            default:
                throw new IllegalStateException("Unexpected token: " + token);

            }

        }
    }
}
