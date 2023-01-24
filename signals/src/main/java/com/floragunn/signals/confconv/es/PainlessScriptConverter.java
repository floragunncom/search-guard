/*
 * Copyright 2023 floragunn GmbH
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
package com.floragunn.signals.confconv.es;

import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.signals.confconv.ConversionResult;

public class PainlessScriptConverter {
    private final String script;

    PainlessScriptConverter(String script) {
        this.script = script;
    }

    public ConversionResult<String> convertToSignals() {
        if (script == null) {
            return new ConversionResult<String>(null);
        }

        ValidationErrors validationErrors = new ValidationErrors();

        String convertedScript = script;

        if (script.contains("ctx.payload.")) {
            convertedScript = convertedScript.replace("ctx.payload.", "data.");
        }

        if (script.contains("params.")) {
            validationErrors.add(new ValidationError(null, "params script attribute is not supported by Signals"));
        }

        if (script.contains("ctx.metadata.")) {
            convertedScript = convertedScript.replace("ctx.metadata.", "data.");
        }

        if (script.contains("ctx.trigger.")) {
            convertedScript = convertedScript.replace("ctx.trigger.", "trigger.");
        }

        return new ConversionResult<String>(convertedScript);
    }

}
