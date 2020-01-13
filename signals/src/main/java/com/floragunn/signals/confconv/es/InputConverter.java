package com.floragunn.signals.confconv.es;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.unit.TimeValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ValidationError;
import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;
import com.floragunn.searchsupport.util.JacksonTools;
import com.floragunn.signals.confconv.ConversionResult;
import com.floragunn.signals.watch.checks.AbstractSearchInput;
import com.floragunn.signals.watch.checks.Check;
import com.floragunn.signals.watch.checks.HttpInput;
import com.floragunn.signals.watch.checks.SearchInput;
import com.floragunn.signals.watch.checks.StaticInput;
import com.floragunn.signals.watch.common.HttpClientConfig;
import com.floragunn.signals.watch.common.HttpRequestConfig;

public class InputConverter {

    private final JsonNode inputJsonNode;

    public InputConverter(JsonNode inputJsonNode) {
        this.inputJsonNode = inputJsonNode;
    }

    ConversionResult<List<Check>> convertToSignals() {
        return convertToSignals(inputJsonNode, null, "_top");
    }

    private ConversionResult<List<Check>> convertToSignals(JsonNode inputJsonNode, String name, String target) {
        ValidationErrors validationErrors = new ValidationErrors();

        List<Check> result = new ArrayList<>();

        if (inputJsonNode.hasNonNull("simple")) {
            result.add(new StaticInput(name, target, JacksonTools.toMap(inputJsonNode.get("simple"))));
            name = null;
        }

        if (inputJsonNode.hasNonNull("search")) {
            ConversionResult<List<Check>> convertedSearch = createSearchInput(inputJsonNode.get("search"), name, target);

            result.addAll(convertedSearch.getElement());
            validationErrors.add("search", convertedSearch.getSourceValidationErrors());

            name = null;
        }

        if (inputJsonNode.hasNonNull("http")) {
            ConversionResult<List<Check>> convertedSearch = createHttpInput(inputJsonNode.get("http"), name, target);

            result.addAll(convertedSearch.getElement());
            validationErrors.add("http", convertedSearch.getSourceValidationErrors());

            name = null;
        }

        if (inputJsonNode.hasNonNull("chain") && inputJsonNode.get("chain").hasNonNull("inputs")) {
            ConversionResult<List<Check>> convertedChain = createInputChain(inputJsonNode.get("chain").get("inputs"), target);

            result.addAll(convertedChain.getElement());
            validationErrors.add("chain", convertedChain.getSourceValidationErrors());

        }

        return new ConversionResult<List<Check>>(result, validationErrors);
    }

    private ConversionResult<List<Check>> createSearchInput(JsonNode jsonNode, String name, String target) {
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
        ConversionResult<String> convertedBody = new MustacheTemplateConverter(body).convertToSignals();
        requestValidationErrors.add("body", convertedBody.getSourceValidationErrors());

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

        SearchInput searchInput = new SearchInput(name, target, indices, convertedBody.getElement(), searchType, indicesOptions);

        if (timeout != null) {
            searchInput.setTimeout(timeout);
        }

        validationErrors.add("request", requestValidationErrors);
        return new ConversionResult<List<Check>>(Collections.singletonList(searchInput), validationErrors);
    }

    private ConversionResult<List<Check>> createHttpInput(JsonNode jsonNode, String name, String target) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        JsonNode requestNode = vJsonNode.requiredObject("request");

        if (requestNode == null) {
            return new ConversionResult<List<Check>>(Collections.emptyList(), validationErrors);
        }

        ConversionResult<HttpRequestConfig> httpRequestConfig = EsWatcherConverter.createHttpRequestConfig(requestNode);

        // TODO
        HttpClientConfig httpClientConfig = new HttpClientConfig(null, null, null);

        if (vJsonNode.hasNonNull("extract")) {
            validationErrors.add(new ValidationError("extract", "Signals does not support the extract attribute. Use a transform instead."));
        }

        return new ConversionResult<List<Check>>(
                Collections.singletonList(new HttpInput(name, target, httpRequestConfig.getElement(), httpClientConfig)));
    }

    private ConversionResult<List<Check>> createInputChain(JsonNode chain, String target) {
        ValidationErrors validationErrors = new ValidationErrors();
        List<Check> result = new ArrayList<>();

        if (chain instanceof ArrayNode) {
            for (JsonNode chainMember : chain) {
                ConversionResult<List<Check>> subResult = createInputChain(chainMember, target);
                result.addAll(subResult.getElement());
                validationErrors.add(null, subResult.getSourceValidationErrors());
            }
        } else if (chain instanceof ObjectNode) {
            ObjectNode chainObject = (ObjectNode) chain;

            Iterator<Map.Entry<String, JsonNode>> iter = chainObject.fields();

            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> entry = iter.next();

                String subTarget = target == null || target.equals("_top") ? entry.getKey() : target + "." + entry.getKey();

                ConversionResult<List<Check>> subResult = convertToSignals(entry.getValue(), entry.getKey(), subTarget);

                result.addAll(subResult.getElement());
                validationErrors.add(entry.getKey(), subResult.getSourceValidationErrors());
            }
        } else {
            validationErrors.add(new ValidationError(null, "Unexpected node type " + chain));
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
