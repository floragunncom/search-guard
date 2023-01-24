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

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.signals.confconv.ConversionResult;
import com.floragunn.signals.watch.checks.Check;
import com.floragunn.signals.watch.checks.Condition;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConditionConverter {

    private final DocNode conditionJsonNode;

    public ConditionConverter(DocNode conditionJsonNode) {
        this.conditionJsonNode = conditionJsonNode;
    }

    public ConversionResult<List<Check>> convertToSignals() {
        ValidationErrors validationErrors = new ValidationErrors();

        List<Check> result = new ArrayList<>();

        if (conditionJsonNode.hasNonNull("never")) {
            result.add(new Condition(null, "false", null, null));
        }

        if (conditionJsonNode.hasNonNull("compare")) {
            ConversionResult<List<Check>> convertedCondition = createCompareCondition(conditionJsonNode.getAsNode("compare"));

            result.addAll(convertedCondition.getElement());
            validationErrors.add("compare", convertedCondition.getSourceValidationErrors());
        }

        if (conditionJsonNode.hasNonNull("array_compare")) {
            ConversionResult<List<Check>> convertedCondition = createArrayCompareCondition(conditionJsonNode.getAsNode("array_compare"));

            result.addAll(convertedCondition.getElement());
            validationErrors.add("array_compare", convertedCondition.getSourceValidationErrors());
        }

        if (conditionJsonNode.hasNonNull("script")) {
            ConversionResult<List<Check>> convertedCondition = createScriptCondition(conditionJsonNode.getAsNode("script"));

            result.addAll(convertedCondition.getElement());
            validationErrors.add("script", convertedCondition.getSourceValidationErrors());
        }

        return new ConversionResult<List<Check>>(result, validationErrors);
    }

    private ConversionResult<List<Check>> createCompareCondition(DocNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();
        List<Check> result = new ArrayList<>();

        if (!(jsonNode.isMap())) {
            validationErrors.add(new InvalidAttributeValue(null, jsonNode, "JSON Object"));
            return new ConversionResult<List<Check>>(result, validationErrors);
        }

        for (Map.Entry<String, DocNode> entry : jsonNode.toMapOfNodes().entrySet()) {

            String operand1 = entry.getKey();

            if (!(entry.getValue().isMap())) {
                validationErrors.add(new InvalidAttributeValue(entry.getKey(), entry.getValue(), "JSON Object"));
                continue;
            }

            ConversionResult<String> convertedOperand1 = new PainlessScriptConverter(operand1).convertToSignals();
            validationErrors.add(operand1, convertedOperand1.getSourceValidationErrors());

            operand1 = convertedOperand1.getElement();

            for (Map.Entry<String, DocNode> subEntry : entry.getValue().toMapOfNodes().entrySet()) {
                String operator;
                try {
                    operator = operatorToPainless(subEntry.getKey());
                } catch (ConfigValidationException e) {
                    validationErrors.add(entry.getKey(), e);
                    continue;
                }

                if (subEntry.getValue().toBasicObject() instanceof Number) {
                    String operand2 = subEntry.getValue().toString();

                    result.add(new Condition(null, operand1 + " " + operator + " " + operand2, null, null));
                } else {
                    String operand2 = subEntry.getValue().toString();

                    if (operand2.contains("{{")) {

                        operand2 = mustacheToPainless(operand2);

                        result.add(new Condition(null, operand1 + " " + operator + " " + operand2, null, null));

                    } else if (operand2.startsWith("<") && operand2.endsWith(">")) {
                        validationErrors.add(new ValidationError(entry.getKey(), "Date math is not supported by this import"));
                        operand2 = '"' + operand2 + '"';

                        result.add(new Condition(null, operand1 + " " + operator + " " + operand2, null, null));
                    } else {
                        operand2 = '"' + operand2 + '"';

                        result.add(new Condition(null, operand1 + " " + operator + " " + operand2, null, null));
                    }
                }

            }

        }

        return new ConversionResult<List<Check>>(result, validationErrors);
    }

    private ConversionResult<List<Check>> createArrayCompareCondition(DocNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();
        List<Check> result = new ArrayList<>();

        if (!(jsonNode.isMap())) {
            validationErrors.add(new InvalidAttributeValue(null, jsonNode, "JSON Object"));
            return new ConversionResult<List<Check>>(result, validationErrors);
        }

        for (Map.Entry<String, DocNode> entry : jsonNode.toMapOfNodes().entrySet()) {

            String operand1 = entry.getKey();

            ConversionResult<String> convertedOperand1 = new PainlessScriptConverter(operand1).convertToSignals();
            validationErrors.add(operand1, convertedOperand1.getSourceValidationErrors());

            operand1 = convertedOperand1.getElement();

            if (!(entry.getValue().isMap())) {
                validationErrors.add(new InvalidAttributeValue(entry.getKey(), entry.getValue(), "JSON Object"));
                continue;
            }

            String path = null;

            if (entry.getValue().hasNonNull("path")) {
                path = entry.getValue().getAsString("path");
            }

            for (Map.Entry<String, DocNode> subEntry : entry.getValue().toMapOfNodes().entrySet()) {

                if (subEntry.getKey().equals("path")) {
                    continue;
                }
                String operator;
                try {
                    operator = operatorToPainless(subEntry.getKey());
                } catch (ConfigValidationException e) {
                    validationErrors.add(entry.getKey(), e);
                    continue;
                }

                boolean all = false;
                String operand2;

                if (subEntry.getValue().isMap()) {
                    DocNode operand2Node = subEntry.getValue();

                    if (!operand2Node.hasNonNull("value")) {
                        validationErrors.add(new MissingAttribute(entry.getKey() + "." + subEntry.getKey() + ".value", entry.getValue()));
                        continue;
                    }

                    if (operand2Node.get("value") instanceof Number) {
                        operand2 = operand2Node.toString();
                    } else {
                        operand2 = operand2Node.toString();

                        if (operand2.contains("{{")) {

                            ConversionResult<String> convertedOperand2 = new MustacheTemplateConverter(operand2).convertToSignals();
                            validationErrors.add(entry.getKey() + "." + subEntry.getKey(), convertedOperand2.getSourceValidationErrors());
                            operand2 = convertedOperand2.getElement();

                            operand2 = mustacheToPainless(operand2);
                        } else if (operand2.startsWith("<") && operand2.endsWith(">")) {
                            operand2 = '"' + operand2 + '"';
                            validationErrors.add(new ValidationError(entry.getKey() + "." + subEntry.getKey() + ".value",
                                    "Date math is not supported by this import"));
                        } else {
                            operand2 = '"' + operand2 + '"';
                        }
                    }

                    if (operand2Node.hasNonNull("quantifier") && operand2Node.getAsString("quantifier").equalsIgnoreCase("all")) {
                        all = true;
                    }

                } else {
                    if (subEntry.getValue().toBasicObject() instanceof Number) {
                        operand2 = subEntry.getValue().toBasicObject().toString();
                    } else {
                        operand2 = subEntry.getValue().toBasicObject().toString();

                        if (operand2.contains("{{")) {
                            ConversionResult<String> convertedOperand2 = new MustacheTemplateConverter(operand2).convertToSignals();
                            validationErrors.add(entry.getKey() + "." + subEntry.getKey(), convertedOperand2.getSourceValidationErrors());
                            operand2 = convertedOperand2.getElement();

                            operand2 = mustacheToPainless(operand2);
                        } else if (operand2.startsWith("<") && operand2.endsWith(">")) {
                            operand2 = '"' + operand2 + '"';
                            validationErrors.add(new ValidationError(entry.getKey(), "Date math is not supported by this import"));
                        } else {
                            operand2 = '"' + operand2 + '"';
                        }
                    }
                }

                String matchMethod = all ? "allMatch" : "anyMatch";
                String painless = operand1 + ".stream()." + matchMethod + "(current -> current"
                        + (path != null && path.length() > 0 ? "." + path : "") + " " + operator + " " + operand2 + ")";

                result.add(new Condition(null, painless, null, null));

            }

        }

        return new ConversionResult<List<Check>>(result, validationErrors);
    }

    private ConversionResult<List<Check>> createScriptCondition(DocNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

        List<Check> result = new ArrayList<>();

        if (jsonNode.isString()) {
            ConversionResult<String> convertedScript = new PainlessScriptConverter(jsonNode.toString()).convertToSignals();

            result.add(new Condition(null, convertedScript.getElement(), null, null));
            validationErrors.add(null, convertedScript.getSourceValidationErrors());
        } else if (jsonNode.isMap()) {
            if (jsonNode.hasNonNull("id")) {
                validationErrors.add(new ValidationError("id", "Script references are not supported"));
            }

            ConversionResult<String> convertedScript = new PainlessScriptConverter(vJsonNode.get("source").withDefault("").asString())
                    .convertToSignals();

            result.add(new Condition(null, convertedScript.getElement(), vJsonNode.get("lang").asString(), null));
            validationErrors.add("source", convertedScript.getSourceValidationErrors());

        } else {
            validationErrors.add(new InvalidAttributeValue(null, jsonNode, "JSON object or string"));
        }

        return new ConversionResult<List<Check>>(result, validationErrors);
    }

    private String mustacheToPainless(String string) {

        StringBuilder result = new StringBuilder();

        for (int pos = 0; pos < string.length();) {
            int next = string.indexOf("{{", pos);

            if (next == -1) {
                if (result.length() > 0) {
                    result.append(" + ");
                }
                result.append('"').append(escapeStringFromMustacheForPainless(string.substring(pos))).append('"');
                break;
            }

            int end = string.indexOf("}}", next + 2);

            if (end == -1) {
                if (result.length() > 0) {
                    result.append(" + ");
                }
                result.append('"').append(escapeStringFromMustacheForPainless(string.substring(pos))).append('"');
                break;
            }

            if (next != pos) {
                if (result.length() > 0) {
                    result.append(" + ");
                }
                result.append('"').append(escapeStringFromMustacheForPainless(string.substring(pos, next))).append('"');
            }

            String expr = string.substring(next + 2, end);

            if (result.length() > 0) {
                result.append(" + ");
            }

            result.append(expr);

            pos = end;
        }

        return result.toString();
    }

    private String escapeStringFromMustacheForPainless(String string) {
        return string.replace("\"", "\\\"");
    }

    private String operatorToPainless(String op) throws ConfigValidationException {
        switch (op.toLowerCase()) {
        case "eq":
            return "==";
        case "not_eq":
            return "!=";
        case "gt":
            return ">";
        case "gte":
            return ">=";
        case "lt":
            return "<";
        case "lte":
            return "<=";
        default:
            throw new ConfigValidationException(new ValidationError(null, "Invalid comparision operation " + op));
        }
    }

}
