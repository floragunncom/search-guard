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
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.signals.confconv.ConversionResult;
import com.floragunn.signals.watch.checks.AbstractSearchInput;
import com.floragunn.signals.watch.checks.Check;
import com.floragunn.signals.watch.checks.SearchInput;
import com.floragunn.signals.watch.checks.Transform;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.core.TimeValue;

public class TransformConverter {

    private final DocNode transformJsonNode;

    public TransformConverter(DocNode transformJsonNode) {
        this.transformJsonNode = transformJsonNode;
    }

    public ConversionResult<List<Check>> convertToSignals() {
        return convertToSignals(transformJsonNode);
    }

    private ConversionResult<List<Check>> convertToSignals(DocNode transformJsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();

        List<Check> result = new ArrayList<>();

        if (transformJsonNode.hasNonNull("script")) {
            ConversionResult<List<Check>> convertedCondition = createScriptTransform(transformJsonNode.getAsNode("script"));

            result.addAll(convertedCondition.getElement());
            validationErrors.add("script", convertedCondition.getSourceValidationErrors());
        }

        if (transformJsonNode.hasNonNull("search")) {
            ConversionResult<List<Check>> convertedCondition = createSearchTransform(transformJsonNode.getAsNode("search"));

            result.addAll(convertedCondition.getElement());
            validationErrors.add("compare", convertedCondition.getSourceValidationErrors());
        }

        if (transformJsonNode.hasNonNull("chain")) {
            ConversionResult<List<Check>> convertedChain = createTransformChain(transformJsonNode.getAsNode("chain"));

            result.addAll(convertedChain.getElement());
            validationErrors.add("chain", convertedChain.getSourceValidationErrors());

        }

        return new ConversionResult<List<Check>>(result, validationErrors);
    }

    private ConversionResult<List<Check>> createSearchTransform(DocNode jsonNode) {
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

        SearchInput searchInput = new SearchInput(null, null, indices, body, searchType, indicesOptions);

        if (timeout != null) {
            searchInput.setTimeout(timeout);
        }

        validationErrors.add("request", requestValidationErrors);
        return new ConversionResult<List<Check>>(Collections.singletonList(searchInput), validationErrors);
    }

    private ConversionResult<List<Check>> createTransformChain(DocNode chain) {
        ValidationErrors validationErrors = new ValidationErrors();
        List<Check> result = new ArrayList<>();

        if (chain.isList()) {
            for (DocNode chainMember : chain.toListOfNodes()) {
                ConversionResult<List<Check>> subResult = convertToSignals(chainMember);
                result.addAll(subResult.getElement());
                validationErrors.add(null, subResult.getSourceValidationErrors());
            }
        } else {
            validationErrors.add(new ValidationError(null, "Unexpected node type " + chain));
        }

        return new ConversionResult<List<Check>>(result, validationErrors);
    }

    private ConversionResult<List<Check>> createScriptTransform(DocNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

        List<Check> result = new ArrayList<>();

        if (jsonNode.isString()) {
            ConversionResult<String> convertedScript = new PainlessScriptConverter(jsonNode.toString()).convertToSignals();

            result.add(new Transform(null, null, convertedScript.getElement(), null, null));
            validationErrors.add(null, convertedScript.getSourceValidationErrors());
        } else if (jsonNode.isMap()) {
            if (jsonNode.hasNonNull("id")) {
                validationErrors.add(new ValidationError("id", "Script references are not supported"));
            }

            ConversionResult<String> convertedScript = new PainlessScriptConverter(vJsonNode.get("source").withDefault("").asString())
                    .convertToSignals();

            result.add(new Transform(null, null, convertedScript.getElement(), vJsonNode.get("lang").asString(), null));
            validationErrors.add(null, convertedScript.getSourceValidationErrors());

        } else {
            validationErrors.add(new InvalidAttributeValue(null, jsonNode, "JSON object or string"));
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
