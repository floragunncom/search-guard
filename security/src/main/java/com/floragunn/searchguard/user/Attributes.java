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

package com.floragunn.searchguard.user;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.codova.documents.BasicJsonPathDefaultConfiguration;
import com.floragunn.fluent.collections.ImmutableMap;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;

public class Attributes {

    public static final String FRONTEND_CONFIG_ID = "__fe_cnf_id";
    public static final String AUTH_TYPE = "__auth_type";

    private static final Logger log = LogManager.getLogger(Attributes.class);
    private static final Configuration JSON_PATH_CONFIG = BasicJsonPathDefaultConfiguration.defaultConfiguration()
            .setOptions(Option.SUPPRESS_EXCEPTIONS);

    public static Map<String, JsonPath> getAttributeMapping(Settings settings) {
        HashMap<String, JsonPath> result = new HashMap<>();

        if (settings == null) {
            return result;
        }

        for (String key : settings.keySet()) {
            try {
                result.put(key, JsonPath.compile(settings.get(key)));
            } catch (InvalidPathException e) {
                log.error("Error in configuration: Invalid JSON path supplied for " + key, e);
            }
        }

        return result;
    }

    public static Map<String, String> getFlatAttributeMapping(Settings settings) {
        HashMap<String, String> result = new HashMap<>();

        if (settings == null) {
            return result;
        }
        for (String key : settings.keySet()) {
            result.put(key, settings.get(key));
        }

        return result;
    }

    public static void validate(Object value) {
        validate(value, 0);
    }

    static void addAttributesByJsonPath(Map<String, JsonPath> jsonPathMap, Object source, Map<String, Object> target) {
        for (Map.Entry<String, JsonPath> entry : jsonPathMap.entrySet()) {
            Object values = JsonPath.using(JSON_PATH_CONFIG).parse(source).read(entry.getValue());
            try {
                Attributes.validate(values);
            } catch (IllegalArgumentException e) {
                throw new ElasticsearchSecurityException(
                        "Error while initializing user attributes. Mapping for " + entry.getKey() + " produced invalid values:\n" + e.getMessage(),
                        e);
            }

            target.put(entry.getKey(), values);
        }
    }

    static void addAttributesByJsonPath(Map<String, JsonPath> jsonPathMap, Object source, ImmutableMap.Builder<String, Object> target) {
        for (Map.Entry<String, JsonPath> entry : jsonPathMap.entrySet()) {
            Object values = JsonPath.using(JSON_PATH_CONFIG).parse(source).read(entry.getValue());
            try {
                Attributes.validate(values);
            } catch (IllegalArgumentException e) {
                throw new ElasticsearchSecurityException(
                        "Error while initializing user attributes. Mapping for " + entry.getKey() + " produced invalid values:\n" + e.getMessage(),
                        e);
            }

            target.put(entry.getKey(), values);
        }
    }

    private static void validate(Object value, int depth) {
        if (depth > 10) {
            throw new IllegalArgumentException("Value exceeds max allowed nesting (or the value contains loops)");
        }

        if (value == null) {
            return;
        } else if (value instanceof String || value instanceof Number || value instanceof Boolean || value instanceof Character) {
            return;
        } else if (value instanceof Collection) {
            for (Object element : ((Collection<?>) value)) {
                validate(element, depth + 1);
            }
        } else if (value instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                validate(entry.getKey(), depth + 1);
                validate(entry.getValue(), depth + 1);
            }
        } else {
            throw new IllegalArgumentException(
                    "Illegal value type. In user attributes the only allowed types are: String, Number, Boolean, Character, Collection, Map. Got: "
                            + value);
        }
    }

}
