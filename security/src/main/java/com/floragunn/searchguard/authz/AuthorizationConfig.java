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

package com.floragunn.searchguard.authz;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;

public class AuthorizationConfig implements Document<AuthorizationConfig> {
    private final DocNode source;
    private final boolean ignoreUnauthorizedIndices;
    private final String fieldAnonymizationSalt;

    AuthorizationConfig(DocNode source, boolean ignoreUnauthorizedIndices, String fieldAnonymizationSalt) {
        this.ignoreUnauthorizedIndices = ignoreUnauthorizedIndices;
        this.source = source;
        this.fieldAnonymizationSalt = fieldAnonymizationSalt;
    }

    public static ValidationResult<AuthorizationConfig> parse(DocNode docNode, Parser.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors);

        boolean ignoreUnauthorizedIndices = vNode.get("ignore_unauthorized_indices").withDefault(true).asBoolean();
        String fieldAnonymizationSalt = vNode.get("field_anonymization.salt").asString();

        if (!validationErrors.hasErrors()) {
            return new ValidationResult<AuthorizationConfig>(new AuthorizationConfig(docNode, ignoreUnauthorizedIndices, fieldAnonymizationSalt));
        } else {
            return new ValidationResult<AuthorizationConfig>(validationErrors);            
        }
    }

    public static AuthorizationConfig parseLegacySgConfig(DocNode docNode, Parser.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors);

        boolean ignoreUnauthorizedIndices = vNode.get("dynamic.do_not_fail_on_forbidden").withDefault(true).asBoolean();
        String fieldAnonymizationSalt = vNode.get("dynamic.field_anonymization_salt2").asString();
        validationErrors.throwExceptionForPresentErrors();

        return new AuthorizationConfig(docNode, ignoreUnauthorizedIndices, fieldAnonymizationSalt);
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
}
