/*
 * Copyright 2019-2023 floragunn GmbH
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
package com.floragunn.signals.watch.common;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser.Context;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;

import java.util.Objects;

public class InstanceParser {

    public static final String FIELD_INSTANCES = "instances";
    private final ValidationErrors validationErrors;

    public InstanceParser(ValidationErrors validationErrors) {
        this.validationErrors = Objects.requireNonNull(validationErrors, "Validation errors are required");
    }

    public Instances parse(ValidatingDocNode node) {
        return node.get(FIELD_INSTANCES).withDefault(Instances.EMPTY).by(this::parseInstance);
    }

    private Instances parseInstance(DocNode docNode, Context context) {
        ValidatingDocNode instancesNode = new ValidatingDocNode(docNode, validationErrors, context);
        boolean enabled = instancesNode.get(Instances.FIELD_ENABLED).required().asBoolean();
        ImmutableList<String> params = instancesNode.get(Instances.FIELD_PARAMS) //
            .withListDefault() //
            .validatedBy(Instances::isValidParameterName) //
            .ofStrings();
        if( (!params.isEmpty()) && (!enabled)) {
            String message = "Only generic watch is allowed to define instance parameters";
            validationErrors.add(new ValidationError(FIELD_INSTANCES + "." + Instances.FIELD_ENABLED, message));
        }
        if(validationErrors.hasErrors()) {
            return Instances.EMPTY;
        }
        return new Instances(enabled, params);
    }
}
