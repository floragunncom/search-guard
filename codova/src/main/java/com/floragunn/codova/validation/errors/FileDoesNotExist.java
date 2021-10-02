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

package com.floragunn.codova.validation.errors;

import java.io.File;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

public class FileDoesNotExist extends ValidationError {
    private final File file;

    public FileDoesNotExist(String attribute, File file) {
        super(attribute, "File does not exist", null);
        this.file = file;
    }

    public File getFile() {
        return file;
    }

    @Override
    public Map<String, Object> toBasicObject() {
        return ImmutableMap.of("error", getMessage(), "value", file.getAbsolutePath());
    }
    
    @Override
    public String toString() {
        return "File does not exist: " + file;
    }
}
