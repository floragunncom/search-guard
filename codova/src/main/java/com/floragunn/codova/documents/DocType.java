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

public class DocType {
    private static List<DocType> registeredDocTypes = new ArrayList<>();
    private static Map<String, DocType> registeredDocTypesByContentType = new HashMap<>();

    public static DocType JSON = new DocType("JSON", new JsonFactory(), "json", "application/json");
    public static DocType YAML = new DocType("YAML", new YAMLFactory(), "ya?ml", "application/x-yaml", "application/yaml", "text/yaml", "text/x-yaml",
            "text/vnd.yaml");

    public static DocType getByContentType(String contentType) throws UnknownContentTypeException {
        DocType result = peekByContentType(contentType);

        if (result != null) {
            return result;
        } else {
            throw new UnknownContentTypeException(contentType);
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
    private final Pattern fileNamePattern;

    public DocType(String name, JsonFactory jsonFactory, String fileNameSuffixPattern, String contentType, String... contentTypeAliases) {
        this.name = name;
        this.contentType = contentType;
        this.jsonFactory = jsonFactory;
        this.contentTypeAliases = new HashSet<>(Arrays.asList(contentTypeAliases));
        this.fileNamePattern = fileNameSuffixPattern != null ? Pattern.compile("\\." + fileNameSuffixPattern + "$", Pattern.CASE_INSENSITIVE) : null;

        register(this);
    }

    public String getContentType() {
        return contentType;
    }

    public JsonFactory getJsonFactory() {
        return jsonFactory;
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

    public static class UnknownContentTypeException extends Exception {

        private static final long serialVersionUID = -3964199452899731782L;

        public UnknownContentTypeException(String contentType) {
            super("Unknown content type: " + contentType);
        }
    }

}
