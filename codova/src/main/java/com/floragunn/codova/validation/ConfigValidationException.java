/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.codova.validation;

import java.util.Map;

import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.errors.ValidationError;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;

public class ConfigValidationException extends Exception implements Document<ConfigValidationException> {

    private static final long serialVersionUID = 8874565903177850402L;

    private ValidationErrors validationErrors;

    public ConfigValidationException(ValidationErrors validationErrors) {
        super(getMessage(validationErrors), validationErrors.getCause());

        this.validationErrors = validationErrors;
    }

    public ConfigValidationException(Multimap<String, ValidationError> validationErrors) {
        this(new ValidationErrors(validationErrors));
    }

    public ConfigValidationException(ValidationError validationError) {
        this(ImmutableListMultimap.of(validationError.getAttribute(), validationError));        
    }

    public ValidationErrors getValidationErrors() {
        return validationErrors;
    }

    private static String getMessage(ValidationErrors validationErrors) {
        int size = validationErrors.size();

        if (size == 1) {
            ValidationError onlyError = validationErrors.getOnlyValidationError();

            if (onlyError.getAttribute() != null && !"_".equals(onlyError.getAttribute())) {
                return "'" + onlyError.getAttribute() + "': " + onlyError.getMessage();
            } else {
                return onlyError.getMessage();
            }

        } else {
            return size + " errors; see detail.";
        }
    }

    public String toString() {
        return "ConfigValidationException: " + this.getMessage() + "\n" + this.validationErrors;
    }

    public String toDebugString() {
        return "ConfigValidationException: " + this.getMessage() + "\n" + this.validationErrors.toDebugString();
    }

    public Map<String, Object> toMap() {
        return validationErrors.toMap();
    }

    @Override
    public Object toBasicObject() {
        return validationErrors.toBasicObject();
    }
    
    

}
