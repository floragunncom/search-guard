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

package com.floragunn.signals.support;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.script.ScriptException;

import com.floragunn.codova.validation.errors.ValidationError;

public class ScriptValidationError extends ValidationError {

    private String context;

    public ScriptValidationError(String attribute, ScriptException scriptException) {
        super(attribute, getMessage(scriptException));
        cause(scriptException);

        if (scriptException.getScriptStack() != null && scriptException.getScriptStack().size() > 0) {
            context = Strings.join(scriptException.getScriptStack(), '\n');
        }
    }

    @Override
    public Map<String, Object> toBasicObject() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("error", getMessage());

        if (context != null) {
            result.put("context", context);
        }

        return result;
    }

    private static String getMessage(ScriptException scriptException) {
        if ("compile error".equals(scriptException.getMessage())) {
            if (scriptException.getCause() != null) {
                return scriptException.getCause().getMessage();
            } else {
                return "Compilation Error";
            }
        } else {
            return scriptException.getMessage();
        }
    }
}