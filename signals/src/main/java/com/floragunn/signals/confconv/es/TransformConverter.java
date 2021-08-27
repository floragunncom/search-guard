package com.floragunn.signals.confconv.es;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.unit.TimeValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.signals.confconv.ConversionResult;
import com.floragunn.signals.watch.checks.AbstractSearchInput;
import com.floragunn.signals.watch.checks.Check;
import com.floragunn.signals.watch.checks.SearchInput;
import com.floragunn.signals.watch.checks.Transform;

public class TransformConverter {

    private final JsonNode transformJsonNode;

    public TransformConverter(JsonNode transformJsonNode) {
        this.transformJsonNode = transformJsonNode;
    }

    public ConversionResult<List<Check>> convertToSignals() {
        return convertToSignals(transformJsonNode);
    }

    private ConversionResult<List<Check>> convertToSignals(JsonNode transformJsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();

        List<Check> result = new ArrayList<>();

        if (transformJsonNode.hasNonNull("script")) {
            ConversionResult<List<Check>> convertedCondition = createScriptTransform(transformJsonNode.get("script"));

            result.addAll(convertedCondition.getElement());
            validationErrors.add("script", convertedCondition.getSourceValidationErrors());
        }

        if (transformJsonNode.hasNonNull("search")) {
            ConversionResult<List<Check>> convertedCondition = createSearchTransform(transformJsonNode.get("search"));

            result.addAll(convertedCondition.getElement());
            validationErrors.add("compare", convertedCondition.getSourceValidationErrors());
        }

        if (transformJsonNode.hasNonNull("chain")) {
            ConversionResult<List<Check>> convertedChain = createTransformChain(transformJsonNode.get("chain"));

            result.addAll(convertedChain.getElement());
            validationErrors.add("chain", convertedChain.getSourceValidationErrors());

        }

        return new ConversionResult<List<Check>>(result, validationErrors);
    }

    private ConversionResult<List<Check>> createSearchTransform(JsonNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        JsonNode requestNode = vJsonNode.requiredObject("request");

        if (requestNode == null) {
            return new ConversionResult<List<Check>>(Collections.emptyList(), validationErrors);
        }

        ValidationErrors requestValidationErrors = new ValidationErrors();
        ValidatingJsonNode vRequestNode = new ValidatingJsonNode(requestNode, requestValidationErrors);

        List<String> indices = vRequestNode.stringList("indices");
        SearchType searchType = vRequestNode.caseInsensitiveEnum("search_type", SearchType.class, null);
        String body = bodyNodeToString(vRequestNode.requiredObject("body"), requestValidationErrors);
        IndicesOptions indicesOptions = null;
        TimeValue timeout = vJsonNode.timeValue("timeout");

        if (requestNode.hasNonNull("indices_options")) {
            try {
                indicesOptions = AbstractSearchInput.parseIndicesOptions(requestNode.get("indices_options"));
            } catch (ConfigValidationException e) {
                requestValidationErrors.add("indices_options", e);
            }
        }

        if (requestNode.hasNonNull("template")) {
            requestValidationErrors.add(new ValidationError("template", "Signals does not support stored search templates"));
        }

        if (vJsonNode.hasNonNull("extract")) {
            validationErrors.add(new ValidationError("extract", "Signals does not support the extract attribute. Use a transform instead."));
        }

        SearchInput searchInput = new SearchInput(null, null, indices, body, searchType, indicesOptions);

        if (timeout != null) {
            searchInput.setTimeout(timeout);
        }

        validationErrors.add("request", requestValidationErrors);
        return new ConversionResult<List<Check>>(Collections.singletonList(searchInput), validationErrors);
    }

    private ConversionResult<List<Check>> createTransformChain(JsonNode chain) {
        ValidationErrors validationErrors = new ValidationErrors();
        List<Check> result = new ArrayList<>();

        if (chain instanceof ArrayNode) {
            for (JsonNode chainMember : chain) {
                ConversionResult<List<Check>> subResult = convertToSignals(chainMember);
                result.addAll(subResult.getElement());
                validationErrors.add(null, subResult.getSourceValidationErrors());
            }
        } else {
            validationErrors.add(new ValidationError(null, "Unexpected node type " + chain));
        }

        return new ConversionResult<List<Check>>(result, validationErrors);
    }

    private ConversionResult<List<Check>> createScriptTransform(JsonNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        List<Check> result = new ArrayList<>();

        if (jsonNode.isTextual()) {
            ConversionResult<String> convertedScript = new PainlessScriptConverter(jsonNode.asText()).convertToSignals();

            result.add(new Transform(null, null, convertedScript.getElement(), null, null));
            validationErrors.add(null, convertedScript.getSourceValidationErrors());
        } else if (jsonNode.isObject()) {
            if (jsonNode.hasNonNull("id")) {
                validationErrors.add(new ValidationError("id", "Script references are not supported"));
            }
            
            ConversionResult<String> convertedScript = new PainlessScriptConverter(vJsonNode.string("source", "")).convertToSignals();


            result.add(new Transform(null, null, convertedScript.getElement(), vJsonNode.string("lang"), null));
            validationErrors.add(null, convertedScript.getSourceValidationErrors());

        } else {
            validationErrors.add(new InvalidAttributeValue(null, jsonNode, "JSON object or string"));
        }

        return new ConversionResult<List<Check>>(result, validationErrors);
    }

    private String bodyNodeToString(JsonNode bodyNode, ValidationErrors requestValidationErrors) {

        if (bodyNode != null) {
            try {
                return DefaultObjectMapper.objectMapper.writeValueAsString(bodyNode);
            } catch (JsonProcessingException e) {
                requestValidationErrors.add(new ValidationError("body", e.getMessage()).cause(e));
                return "{}";
            }
        } else {
            return "{}";
        }
    }
}
