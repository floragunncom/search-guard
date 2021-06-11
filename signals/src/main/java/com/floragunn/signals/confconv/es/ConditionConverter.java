package com.floragunn.signals.confconv.es;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.InvalidAttributeValue;
import com.floragunn.searchsupport.config.validation.MissingAttribute;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidationError;
import com.floragunn.searchsupport.config.validation.ValidationErrors;
import com.floragunn.signals.confconv.ConversionResult;
import com.floragunn.signals.watch.checks.Check;
import com.floragunn.signals.watch.checks.Condition;

public class ConditionConverter {

    private final JsonNode conditionJsonNode;

    public ConditionConverter(JsonNode conditionJsonNode) {
        this.conditionJsonNode = conditionJsonNode;
    }

    public ConversionResult<List<Check>> convertToSignals() {
        ValidationErrors validationErrors = new ValidationErrors();

        List<Check> result = new ArrayList<>();

        if (conditionJsonNode.hasNonNull("never")) {
            result.add(new Condition(null, "false", null, null));
        }

        if (conditionJsonNode.hasNonNull("compare")) {
            ConversionResult<List<Check>> convertedCondition = createCompareCondition(conditionJsonNode.get("compare"));

            result.addAll(convertedCondition.getElement());
            validationErrors.add("compare", convertedCondition.getSourceValidationErrors());
        }

        if (conditionJsonNode.hasNonNull("array_compare")) {
            ConversionResult<List<Check>> convertedCondition = createArrayCompareCondition(conditionJsonNode.get("array_compare"));

            result.addAll(convertedCondition.getElement());
            validationErrors.add("array_compare", convertedCondition.getSourceValidationErrors());
        }

        if (conditionJsonNode.hasNonNull("script")) {
            ConversionResult<List<Check>> convertedCondition = createScriptCondition(conditionJsonNode.get("script"));

            result.addAll(convertedCondition.getElement());
            validationErrors.add("script", convertedCondition.getSourceValidationErrors());
        }

        return new ConversionResult<List<Check>>(result, validationErrors);
    }

    private ConversionResult<List<Check>> createCompareCondition(JsonNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();
        List<Check> result = new ArrayList<>();

        if (!(jsonNode instanceof ObjectNode)) {
            validationErrors.add(new InvalidAttributeValue(null, jsonNode, "JSON Object"));
            return new ConversionResult<List<Check>>(result, validationErrors);
        }

        ObjectNode objectNode = (ObjectNode) jsonNode;

        Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();

        while (iter.hasNext()) {
            Map.Entry<String, JsonNode> entry = iter.next();

            String operand1 = entry.getKey();

            if (!(entry.getValue() instanceof ObjectNode)) {
                validationErrors.add(new InvalidAttributeValue(entry.getKey(), entry.getValue(), "JSON Object"));
                continue;
            }

            ConversionResult<String> convertedOperand1 = new PainlessScriptConverter(operand1).convertToSignals();
            validationErrors.add(operand1, convertedOperand1.getSourceValidationErrors());

            operand1 = convertedOperand1.getElement();

            Iterator<Map.Entry<String, JsonNode>> subIter = ((ObjectNode) entry.getValue()).fields();

            while (subIter.hasNext()) {
                Map.Entry<String, JsonNode> subEntry = subIter.next();
                String operator;
                try {
                    operator = operatorToPainless(subEntry.getKey());
                } catch (ConfigValidationException e) {
                    validationErrors.add(entry.getKey(), e);
                    continue;
                }

                if (subEntry.getValue().isNumber()) {
                    String operand2 = subEntry.getValue().asText();

                    result.add(new Condition(null, operand1 + " " + operator + " " + operand2, null, null));
                } else {
                    String operand2 = subEntry.getValue().asText();

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

    private ConversionResult<List<Check>> createArrayCompareCondition(JsonNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();
        List<Check> result = new ArrayList<>();

        if (!(jsonNode instanceof ObjectNode)) {
            validationErrors.add(new InvalidAttributeValue(null, jsonNode, "JSON Object"));
            return new ConversionResult<List<Check>>(result, validationErrors);
        }

        ObjectNode objectNode = (ObjectNode) jsonNode;

        Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();

        while (iter.hasNext()) {
            Map.Entry<String, JsonNode> entry = iter.next();

            String operand1 = entry.getKey();

            ConversionResult<String> convertedOperand1 = new PainlessScriptConverter(operand1).convertToSignals();
            validationErrors.add(operand1, convertedOperand1.getSourceValidationErrors());

            operand1 = convertedOperand1.getElement();
            
            if (!(entry.getValue() instanceof ObjectNode)) {
                validationErrors.add(new InvalidAttributeValue(entry.getKey(), entry.getValue(), "JSON Object"));
                continue;
            }

            String path = null;

            if (entry.getValue().hasNonNull("path")) {
                path = entry.getValue().get("path").asText();
            }

            Iterator<Map.Entry<String, JsonNode>> subIter = ((ObjectNode) entry.getValue()).fields();

            while (subIter.hasNext()) {
                Map.Entry<String, JsonNode> subEntry = iter.next();

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

                if (subEntry.getValue() instanceof ObjectNode) {
                    ObjectNode operand2Node = (ObjectNode) subEntry.getValue();

                    if (!operand2Node.hasNonNull("value")) {
                        validationErrors.add(new MissingAttribute(entry.getKey() + "." + subEntry.getKey() + ".value", entry.getValue()));
                        continue;
                    }

                    if (operand2Node.get("value").isNumber()) {
                        operand2 = operand2Node.asText();
                    } else {
                        operand2 = operand2Node.asText();

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

                    if (operand2Node.hasNonNull("quantifier") && operand2Node.get("quantifier").asText().equalsIgnoreCase("all")) {
                        all = true;
                    }

                } else {
                    if (subEntry.getValue().isNumber()) {
                        operand2 = subEntry.getValue().asText();
                    } else {
                        operand2 = subEntry.getValue().asText();

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

    private ConversionResult<List<Check>> createScriptCondition(JsonNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        List<Check> result = new ArrayList<>();

        if (jsonNode.isTextual()) {
            ConversionResult<String> convertedScript = new PainlessScriptConverter(jsonNode.asText()).convertToSignals();

            result.add(new Condition(null, convertedScript.getElement(), null, null));
            validationErrors.add(null, convertedScript.getSourceValidationErrors());
        } else if (jsonNode.isObject()) {
            if (jsonNode.hasNonNull("id")) {
                validationErrors.add(new ValidationError("id", "Script references are not supported"));
            }

            ConversionResult<String> convertedScript = new PainlessScriptConverter(vJsonNode.string("source", "")).convertToSignals();

            result.add(new Condition(null, convertedScript.getElement(), vJsonNode.string("lang"), null));
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
