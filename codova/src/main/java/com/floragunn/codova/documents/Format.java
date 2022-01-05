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

public class Format {
    private static List<Format> registeredDocTypes = new ArrayList<>();
    private static Map<String, Format> registeredDocTypesByMediaType = new HashMap<>();

    public static Format JSON = new Format("JSON", Encoding.TEXT, new JsonFactory(), "json", "application/json", "text/x-json", "text/json");
    public static Format YAML = new Format("YAML", Encoding.TEXT, new YAMLFactory(), "ya?ml", "application/x-yaml", "application/yaml", "text/yaml",
            "text/x-yaml", "text/vnd.yaml");
    public static Format SMILE = new Format("SMILE", Encoding.BINARY, "com.fasterxml.jackson.dataformat.smile.SmileFactory", "sml",
            "application/x-jackson-smile", "application/smile");

    public static Format getByContentType(String contentType) throws UnknownDocTypeException {
        Format result = peekByContentType(contentType);

        if (result != null) {
            return result;
        } else {
            throw new UnknownDocTypeException(contentType);
        }
    }

    public static Format peekByContentType(String contentType) {
        int paramSeparator = contentType.indexOf(';');

        if (paramSeparator != -1) {
            contentType = contentType.substring(0, paramSeparator).trim();
        }

        Format result = registeredDocTypesByMediaType.get(contentType);

        if (result != null) {
            return result;
        } else {
            return null;
        }
    }

    public static Format getByFileName(String fileName, Format fallbackDocType) {
        for (Format format : registeredDocTypes) {
            if (format.fileNamePattern == null) {
                continue;
            }

            Matcher matcher = format.fileNamePattern.matcher(fileName);

            if (matcher.matches()) {
                return format;
            }
        }

        return fallbackDocType;
    }

    public static Format getByMediaType(String mediaType) throws UnknownDocTypeException {
        Format result = registeredDocTypesByMediaType.get(mediaType);

        if (result != null) {
            return result;
        }

        int plus = mediaType.lastIndexOf('+');
        int slash = mediaType.indexOf('/');

        if (plus != -1 && slash != -1) {
            String suffix = mediaType.substring(plus + 1);
            String prefix = mediaType.substring(0, slash);

            result = registeredDocTypesByMediaType.get(prefix + "/" + suffix);

            if (result != null) {
                return result;
            }
        }

        throw new UnknownDocTypeException(mediaType);
    }

    private static void register(Format format) {
        registeredDocTypes.add(format);
        registeredDocTypesByMediaType.put(format.getMediaType().toLowerCase(), format);

        for (String alias : format.mediaTypeAliases) {
            registeredDocTypesByMediaType.put(alias.toLowerCase(), format);
        }
    }

    private final String name;
    private final String mediaType;
    private final ContentType contentType;
    private final Set<String> mediaTypeAliases;
    private final JsonFactory jsonFactory;
    private final Exception jsonFactoryUnavailabilityReason;
    private final Pattern fileNamePattern;
    private final Encoding encoding;
    private final Charset defaultCharset;

    public Format(String name, Encoding encoding, JsonFactory jsonFactory, String fileNameSuffixPattern, String mediaType,
            String... mediaTypeAliases) {
        this.name = name;
        this.encoding = encoding;
        this.mediaType = mediaType;
        this.contentType = new ContentType(mediaType, mediaType, this, null);
        this.jsonFactory = jsonFactory;
        this.jsonFactoryUnavailabilityReason = null;
        this.mediaTypeAliases = new HashSet<>(Arrays.asList(mediaTypeAliases));
        this.fileNamePattern = fileNameSuffixPattern != null ? Pattern.compile(".*\\." + fileNameSuffixPattern + "$", Pattern.CASE_INSENSITIVE)
                : null;
        this.defaultCharset = encoding == Encoding.TEXT ? Charsets.UTF_8 : null;

        register(this);
    }

    public Format(String name, Encoding encoding, String jsonFactoryClass, String fileNameSuffixPattern, String mediaType,
            String... mediaTypeAliases) {
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
        this.mediaType = mediaType;
        this.contentType = new ContentType(mediaType, mediaType, this, null);
        this.jsonFactory = jsonFactory;
        this.jsonFactoryUnavailabilityReason = jsonFactoryUnavailabilityReason;
        this.mediaTypeAliases = new HashSet<>(Arrays.asList(mediaTypeAliases));
        this.fileNamePattern = fileNameSuffixPattern != null ? Pattern.compile(".*\\." + fileNameSuffixPattern + "$", Pattern.CASE_INSENSITIVE)
                : null;
        this.defaultCharset = encoding == Encoding.TEXT ? Charsets.UTF_8 : null;

        register(this);
    }

    public String getMediaType() {
        return mediaType;
    }

    public ContentType getContentType() {
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
        return mediaTypeAliases;
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
