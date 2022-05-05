/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.searchguard.authz.config;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.documents.Parser.Context;
import com.floragunn.codova.documents.UnexpectedDocumentStructureException;
import com.floragunn.codova.documents.patch.PatchableDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.searchguard.configuration.ConfigurationRepository;

public class AuthorizationConfig implements PatchableDocument<AuthorizationConfig> {
    private final DocNode source;
    private final boolean ignoreUnauthorizedIndices;
    private final String fieldAnonymizationSalt;
    private final boolean debugEnabled;

    AuthorizationConfig(DocNode source, boolean ignoreUnauthorizedIndices, String fieldAnonymizationSalt, boolean debugEnabled) {
        this.ignoreUnauthorizedIndices = ignoreUnauthorizedIndices;
        this.source = source;
        this.fieldAnonymizationSalt = fieldAnonymizationSalt;
        this.debugEnabled = debugEnabled;
    }

    public static ValidationResult<AuthorizationConfig> parse(DocNode docNode, Parser.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode;
        try {
            vNode = new ValidatingDocNode(docNode.splitDottedAttributeNamesToTree(), validationErrors);
        } catch (UnexpectedDocumentStructureException e) {
            return new ValidationResult<AuthorizationConfig>(e.getValidationErrors());
        }

        boolean ignoreUnauthorizedIndices = vNode.get("ignore_unauthorized_indices").withDefault(true).asBoolean();
        String fieldAnonymizationSalt = vNode.get("field_anonymization.salt").asString();
        boolean debugEnabled = vNode.get("debug").withDefault(false).asBoolean();

        if (!validationErrors.hasErrors()) {
            return new ValidationResult<AuthorizationConfig>(
                    new AuthorizationConfig(docNode, ignoreUnauthorizedIndices, fieldAnonymizationSalt, debugEnabled));
        } else {
            return new ValidationResult<AuthorizationConfig>(validationErrors);
        }
    }

    public static AuthorizationConfig parseLegacySgConfig(DocNode docNode, Parser.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode.splitDottedAttributeNamesToTree(), validationErrors);

        boolean ignoreUnauthorizedIndices = vNode.get("dynamic.do_not_fail_on_forbidden").withDefault(true).asBoolean();
        String fieldAnonymizationSalt = vNode.get("dynamic.field_anonymization_salt2").asString();
        validationErrors.throwExceptionForPresentErrors();

        return new AuthorizationConfig(docNode, ignoreUnauthorizedIndices, fieldAnonymizationSalt, false);
    }

    public boolean isIgnoreUnauthorizedIndices() {
        return ignoreUnauthorizedIndices;
    }

    @Override
    public Object toBasicObject() {
        return source;
    }

    @Override
    public String toString() {
        return toJsonString();
    }

    public String getFieldAnonymizationSalt() {
        return fieldAnonymizationSalt;
    }

    public boolean isDebugEnabled() {
        return debugEnabled;
    }

    @Override
    public AuthorizationConfig parseI(DocNode docNode, Context context) throws ConfigValidationException {
        return parse(docNode, (ConfigurationRepository.Context) context).get();
    }
}
