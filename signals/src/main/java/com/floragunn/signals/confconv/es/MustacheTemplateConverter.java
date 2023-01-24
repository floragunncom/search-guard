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

public class MustacheTemplateConverter {
    private final String script;

    MustacheTemplateConverter(String script) {
        this.script = script;
    }

    public ConversionResult<String> convertToSignals() {
        if (script == null) {
            return new ConversionResult<String>(null);
        }

        ValidationErrors validationErrors = new ValidationErrors();

        StringBuilder result = new StringBuilder();

        for (int i = 0; i < script.length();) {
            int expressionStart = script.indexOf("{{", i);

            if (expressionStart == -1) {
                result.append(script.substring(i));
                break;
            }

            int expressionEnd = script.indexOf("}}", expressionStart + 1);

            if (expressionEnd == -1) {
                result.append(script.substring(i));
                break;
            }

            result.append(script.substring(i, expressionStart));
            result.append("{{");

            String expression = script.substring(expressionStart + 2, expressionEnd);
            String convertedExpression = expression;

            if (expression.contains("ctx.payload.")) {
                convertedExpression = convertedExpression.replace("ctx.payload.", "data.");
            } else if (expression.contains("params.")) {
                validationErrors.add(new ValidationError(null, "params script attribute is not supported by Signals"));
            } else if (expression.contains("ctx.metadata.")) {
                convertedExpression = convertedExpression.replace("ctx.metadata.", "data.");
            } else if (expression.contains("ctx.trigger.")) {
                convertedExpression = convertedExpression.replace("ctx.trigger.", "trigger.");
            }

            result.append(convertedExpression);
            result.append("}}");

            i = expressionEnd + 2;
        }

        return new ConversionResult<String>(result.toString());
    }

}
