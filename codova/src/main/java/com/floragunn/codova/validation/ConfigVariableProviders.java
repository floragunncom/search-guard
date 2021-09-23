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
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.floragunn.codova.validation.errors.FileDoesNotExist;
import com.floragunn.codova.validation.errors.ValidationError;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class ConfigVariableProviders {

    private static final Logger log = LoggerFactory.getLogger(ConfigVariableProviders.class);

    public final static ValidatingFunction<String, Object> FILE = new ValidatingFunction<String, Object>() {
        public Object apply(String fileName) throws ConfigValidationException {

            try {
                return Files.asCharSource(new File(fileName), Charsets.UTF_8).read();
            } catch (FileNotFoundException e) {
                log.debug("File not found: " + fileName, e);
                throw new ConfigValidationException(new FileDoesNotExist(null, new File(fileName)).cause(e));
            } catch (IOException e) {
                log.warn("Exception while reading " + fileName, e);
                throw new ConfigValidationException(new ValidationError(null, "Error while reading file: " + e).cause(e));
            } catch (AccessControlException e) {
                log.warn("AccessControlException while reading " + fileName, e);

                if (e.toString().contains("java.io.FilePermission")) {
                    throw new ConfigValidationException(
                            new ValidationError(null, "The current Java security policy does not allow accessing the file " + fileName).cause(e));
                } else {
                    throw new ConfigValidationException(new ValidationError(null, "Error while reading file: " + e).cause(e));
                }
            }

        }
    };

    public final static ValidatingFunction<String, Object> FILE_PRIVILEGED = new ValidatingFunction<String, Object>() {
        public Object apply(String fileName) throws ConfigValidationException, Exception {
            try {
                return AccessController.doPrivileged(new PrivilegedExceptionAction<Object>() {
                    @Override
                    public Object run() throws ConfigValidationException, Exception {
                        return FILE.apply(fileName);
                    }
                });
            } catch (PrivilegedActionException e) {
                if (e.getCause() instanceof Exception) {
                    throw (Exception) e.getCause();
                } else if (e.getCause() instanceof Error) {
                    throw (Error) e.getCause();
                } else {
                    throw new RuntimeException(e);
                }
            }
        }
    };

    public final static ValidatingFunction<String, Object> ENV = new ValidatingFunction<String, Object>() {
        public String apply(String name) {
            return System.getenv(name);
        }
    };

    public final static ConfigVariableProviders ALL_PRIVILEGED = new ConfigVariableProviders().with("file", FILE_PRIVILEGED).with("env", ENV);
    public final static ConfigVariableProviders ALL = new ConfigVariableProviders().with("file", FILE).with("env", ENV);

    private final Map<String, ValidatingFunction<String, Object>> map = new HashMap<>();

    public ConfigVariableProviders with(String name, ValidatingFunction<String, Object> function) {
        ConfigVariableProviders result = new ConfigVariableProviders();
        result.map.putAll(this.map);
        result.map.put(name, function);
        return result;
    }

    public Map<String, ValidatingFunction<String, Object>> toMap() {
        return Collections.unmodifiableMap(map);
    }

}
