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

package com.floragunn.codova.validation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class ConfigVariableProviders {

    private static final Logger log = LogManager.getLogger(ConfigVariableProviders.class);

    public final static Function<String, Object> FILE = new Function<String, Object>() {
        public Object apply(String fileName) {

            try {
                return Files.asCharSource(new File(fileName), Charsets.UTF_8).read();
            } catch (FileNotFoundException e) {
                log.debug("File not found: " + fileName, e);
                return null;
            } catch (IOException e) {
                log.warn("Exception while reading " + fileName, e);
                return null;
            }

        }
    };

    public final static Function<String, Object> FILE_PRIVILEGED = new Function<String, Object>() {
        public Object apply(String fileName) {
            return AccessController.doPrivileged(new PrivilegedAction<Object>() {
                @Override
                public Object run() {
                    return FILE.apply(fileName);
                }
            });
        }
    };

    public final static Function<String, Object> ENV = new Function<String, Object>() {
        public String apply(String name) {
            return System.getenv(name);
        }
    };

    public final static ConfigVariableProviders ALL_PRIVILEGED = new ConfigVariableProviders().with("file", FILE_PRIVILEGED).with("env", ENV);
    public final static ConfigVariableProviders ALL = new ConfigVariableProviders().with("file", FILE).with("env", ENV);

    private final Map<String, Function<String, Object>> map = new HashMap<>();

    public ConfigVariableProviders with(String name, Function<String, Object> function) {
        ConfigVariableProviders result = new ConfigVariableProviders();
        result.map.putAll(this.map);
        result.map.put(name, function);
        return result;
    }

    public Map<String, Function<String, Object>> toMap() {
        return Collections.unmodifiableMap(map);
    }

}
