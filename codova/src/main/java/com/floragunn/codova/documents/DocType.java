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

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Charsets;

public class DocType {
    private static List<DocType> registeredDocTypes = new ArrayList<>();
    private static Map<String, DocType> registeredDocTypesByContentType = new HashMap<>();

    public static DocType JSON = new DocType("JSON", Encoding.TEXT, new JsonFactory(), "json", "application/json", "text/x-json");
    public static DocType YAML = new DocType("YAML", Encoding.TEXT, new YAMLFactory(), "ya?ml", "application/x-yaml", "application/yaml", "text/yaml",
            "text/x-yaml", "text/vnd.yaml");
    public static DocType SMILE = new DocType("SMILE", Encoding.BINARY, "com.fasterxml.jackson.dataformat.smile.SmileFactory", "sml",
            "application/x-jackson-smile", "application/smile");

    public static DocType getByContentType(String contentType) throws UnknownDocTypeException {
        DocType result = peekByContentType(contentType);

        if (result != null) {
            return result;
        } else {
            throw new UnknownDocTypeException(contentType);
        }
    }

    public static DocType peekByContentType(String contentType) {
        int paramSeparator = contentType.indexOf(';');

        if (paramSeparator != -1) {
            contentType = contentType.substring(0, paramSeparator).trim();
        }

        DocType result = registeredDocTypesByContentType.get(contentType);

        if (result != null) {
            return result;
        } else {
            return null;
        }
    }

    public static DocType getByFileName(String fileName, DocType fallbackDocType) {
        for (DocType docType : registeredDocTypes) {
            if (docType.fileNamePattern == null) {
                continue;
            }

            Matcher matcher = docType.fileNamePattern.matcher(fileName);

            if (matcher.matches()) {
                return docType;
            }
        }

        return fallbackDocType;
    }

    public static DocType getByMimeType(String mimeType) throws UnknownDocTypeException {
        DocType result = registeredDocTypesByContentType.get(mimeType);

        if (result != null) {
            return result;
        } else {
            throw new UnknownDocTypeException(mimeType);
        }
    }

    private static void register(DocType docType) {
        registeredDocTypes.add(docType);
        registeredDocTypesByContentType.put(docType.getContentType().toLowerCase(), docType);

        for (String alias : docType.contentTypeAliases) {
            registeredDocTypesByContentType.put(alias.toLowerCase(), docType);
        }
    }

    private final String name;
    private final String contentType;
    private final Set<String> contentTypeAliases;
    private final JsonFactory jsonFactory;
    private final Exception jsonFactoryUnavailabilityReason;
    private final Pattern fileNamePattern;
    private final Encoding encoding;
    private final Charset defaultCharset;

    public DocType(String name, Encoding encoding, JsonFactory jsonFactory, String fileNameSuffixPattern, String contentType,
            String... contentTypeAliases) {
        this.name = name;
        this.encoding = encoding;
        this.contentType = contentType;
        this.jsonFactory = jsonFactory;
        this.jsonFactoryUnavailabilityReason = null;
        this.contentTypeAliases = new HashSet<>(Arrays.asList(contentTypeAliases));
        this.fileNamePattern = fileNameSuffixPattern != null ? Pattern.compile("\\." + fileNameSuffixPattern + "$", Pattern.CASE_INSENSITIVE) : null;
        this.defaultCharset = encoding == Encoding.TEXT ? Charsets.UTF_8 : null;

        register(this);
    }

    public DocType(String name, Encoding encoding, String jsonFactoryClass, String fileNameSuffixPattern, String contentType,
            String... contentTypeAliases) {
        JsonFactory jsonFactory;
        Exception jsonFactoryUnavailabilityReason;

        try {
            jsonFactory = createJsonFactory(jsonFactoryClass);
            jsonFactoryUnavailabilityReason = null;
        } catch (Exception e) {
            jsonFactory = null;
            jsonFactoryUnavailabilityReason = e;
        }

        this.name = name;
        this.encoding = encoding;
        this.contentType = contentType;
        this.jsonFactory = jsonFactory;
        this.jsonFactoryUnavailabilityReason = jsonFactoryUnavailabilityReason;
        this.contentTypeAliases = new HashSet<>(Arrays.asList(contentTypeAliases));
        this.fileNamePattern = fileNameSuffixPattern != null ? Pattern.compile("\\." + fileNameSuffixPattern + "$", Pattern.CASE_INSENSITIVE) : null;
        this.defaultCharset = encoding == Encoding.TEXT ? Charsets.UTF_8 : null;

        register(this);
    }

    public String getContentType() {
        return contentType;
    }

    public JsonFactory getJsonFactory() {
        if (jsonFactory != null) {
            return jsonFactory;
        } else {
            throw new RuntimeException("Support for " + this.name + " is not available", this.jsonFactoryUnavailabilityReason);
        }
    }

    public Set<String> getContentTypeAliases() {
        return contentTypeAliases;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }

    public boolean isBinary() {
        return encoding == Encoding.BINARY;
    }

    public static class UnknownDocTypeException extends Exception {

        private static final long serialVersionUID = -3964199452899731782L;

        public UnknownDocTypeException(String contentType) {
            super("Unknown content type: " + contentType);
        }
    }

    public static enum Encoding {
        TEXT, BINARY;
    }

    public Charset getDefaultCharset() {
        return defaultCharset;
    }

    private static JsonFactory createJsonFactory(String name) throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
        Class<?> clazz = Class.forName(name);
        return (JsonFactory) clazz.getConstructor().newInstance();
    }

}
