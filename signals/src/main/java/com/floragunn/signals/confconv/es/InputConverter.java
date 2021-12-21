package com.floragunn.signals.confconv.es;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.core.TimeValue;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.signals.confconv.ConversionResult;
import com.floragunn.signals.watch.checks.AbstractSearchInput;
import com.floragunn.signals.watch.checks.Check;
import com.floragunn.signals.watch.checks.HttpInput;
import com.floragunn.signals.watch.checks.SearchInput;
import com.floragunn.signals.watch.checks.StaticInput;
import com.floragunn.signals.watch.common.HttpClientConfig;
import com.floragunn.signals.watch.common.HttpRequestConfig;

public class InputConverter {

    private final DocNode inputJsonNode;

    public InputConverter(DocNode inputJsonNode) {
        this.inputJsonNode = inputJsonNode;
    }

    ConversionResult<List<Check>> convertToSignals() {
        return convertToSignals(inputJsonNode, null, "_top");
    }

    private ConversionResult<List<Check>> convertToSignals(DocNode inputJsonNode, String name, String target) {
        ValidationErrors validationErrors = new ValidationErrors();

        List<Check> result = new ArrayList<>();

        if (inputJsonNode.hasNonNull("simple")) {
            result.add(new StaticInput(name, target, inputJsonNode.getAsNode("simple").toMap()));
            name = null;
        }

        if (inputJsonNode.hasNonNull("search")) {
            ConversionResult<List<Check>> convertedSearch = createSearchInput(inputJsonNode.getAsNode("search"), name, target);

            result.addAll(convertedSearch.getElement());
            validationErrors.add("search", convertedSearch.getSourceValidationErrors());

            name = null;
        }

        if (inputJsonNode.hasNonNull("http")) {
            ConversionResult<List<Check>> convertedSearch = createHttpInput(inputJsonNode.getAsNode("http"), name, target);

            result.addAll(convertedSearch.getElement());
            validationErrors.add("http", convertedSearch.getSourceValidationErrors());

            name = null;
        }

        if (inputJsonNode.hasNonNull("chain") && inputJsonNode.getAsNode("chain").hasNonNull("inputs")) {
            ConversionResult<List<Check>> convertedChain = createInputChain(inputJsonNode.getAsNode("chain").getAsNode("inputs"), target);

            result.addAll(convertedChain.getElement());
            validationErrors.add("chain", convertedChain.getSourceValidationErrors());

        }

        return new ConversionResult<List<Check>>(result, validationErrors);
    }

    private ConversionResult<List<Check>> createSearchInput(DocNode jsonNode, String name, String target) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

        DocNode requestNode = vJsonNode.get("request").required().asDocNode();

        if (requestNode.isNull()) {
            return new ConversionResult<List<Check>>(Collections.emptyList(), validationErrors);
        }

        ValidationErrors requestValidationErrors = new ValidationErrors();
        ValidatingDocNode vRequestNode = new ValidatingDocNode(requestNode, requestValidationErrors);

        List<String> indices = vRequestNode.get("indices").asListOfStrings();
        SearchType searchType = vRequestNode.get("search_type").asEnum(SearchType.class);
        String body = bodyNodeToString(vRequestNode.get("body").required().asDocNode(), requestValidationErrors);
        ConversionResult<String> convertedBody = new MustacheTemplateConverter(body).convertToSignals();
        requestValidationErrors.add("body", convertedBody.getSourceValidationErrors());

        IndicesOptions indicesOptions = null;
        TimeValue timeout = vJsonNode.get("timeout").byString((s) -> TimeValue.parseTimeValue(s, "timeout"));

        if (requestNode.hasNonNull("indices_options")) {
            try {
                indicesOptions = AbstractSearchInput.parseIndicesOptions(requestNode.getAsNode("indices_options"));
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

    private ConversionResult<List<Check>> createHttpInput(DocNode jsonNode, String name, String target) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

        DocNode requestNode = vJsonNode.get("request").required().asDocNode();

        if (requestNode.isNull()) {
            return new ConversionResult<List<Check>>(Collections.emptyList(), validationErrors);
        }

        ConversionResult<HttpRequestConfig> httpRequestConfig = EsWatcherConverter.createHttpRequestConfig(requestNode);

        // TODO
        HttpClientConfig httpClientConfig = new HttpClientConfig(null, null, null, null);

        if (vJsonNode.hasNonNull("extract")) {
            validationErrors.add(new ValidationError("extract", "Signals does not support the extract attribute. Use a transform instead."));
        }

        return new ConversionResult<List<Check>>(
                Collections.singletonList(new HttpInput(name, target, httpRequestConfig.getElement(), httpClientConfig)));
    }

    private ConversionResult<List<Check>> createInputChain(DocNode chain, String target) {
        ValidationErrors validationErrors = new ValidationErrors();
        List<Check> result = new ArrayList<>();

        if (chain.isList()) {
            for (DocNode chainMember : chain.toListOfNodes()) {
                ConversionResult<List<Check>> subResult = createInputChain(chainMember, target);
                result.addAll(subResult.getElement());
                validationErrors.add(null, subResult.getSourceValidationErrors());
            }
        } else if (chain.isMap()) {

            for (Map.Entry<String, DocNode> entry : chain.toMapOfNodes().entrySet()) {

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

    private String bodyNodeToString(DocNode bodyNode, ValidationErrors requestValidationErrors) {

        if (bodyNode != null) {
            try {
                return bodyNode.toJsonString();
            } catch (Exception e) {
                requestValidationErrors.add(new ValidationError("body", e.getMessage()).cause(e));
                return "{}";
            }
        } else {
            return "{}";
        }
    }

}
