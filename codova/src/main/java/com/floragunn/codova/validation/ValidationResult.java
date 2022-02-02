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

package com.floragunn.codova.validation;

import com.floragunn.codova.validation.errors.ValidationError;

public class ValidationResult<T> {

    private final T parsedObject;
    private final boolean hasResult;
    private final ValidationErrors validationErrors;

    public ValidationResult(T parsedObject) {
        this.parsedObject = parsedObject;
        this.hasResult = true;
        this.validationErrors = null;
    }

    public ValidationResult(ValidationErrors validationErrors) {
        this.parsedObject = null;
        this.hasResult = false;
        this.validationErrors = validationErrors;
    }

    public ValidationResult(ValidationError validationError) {
        this.parsedObject = null;
        this.hasResult = false;
        this.validationErrors = new ValidationErrors(validationError);
    }

    public ValidationResult(T parsedObject, ValidationErrors validationErrors) {
        this.parsedObject = parsedObject;
        this.hasResult = true;
        this.validationErrors = validationErrors;
    }

    public ValidationResult(T parsedObject, ValidationError validationError) {
        this.parsedObject = parsedObject;
        this.hasResult = true;
        this.validationErrors = new ValidationErrors(validationError);
    }

    public T get() throws ConfigValidationException {
        throwExceptionForPresentErrors();

        return parsedObject;
    }

    public T peek() {
        return parsedObject;
    }

    public T partial() throws ConfigValidationException {
        if (hasResult) {
            return parsedObject;
        } else {
            throwExceptionForPresentErrors();
            // This should not happen:
            throw new IllegalStateException("parsedObject is null");
        }
    }

    public void throwExceptionForPresentErrors() throws ConfigValidationException {
        if (validationErrors != null) {
            validationErrors.throwExceptionForPresentErrors();
        }
    }

    public ValidationErrors getValidationErrors() {
        if (validationErrors != null) {
            return validationErrors;
        } else {
            return new ValidationErrors();
        }
    }

    public boolean hasErrors() {
        if (validationErrors != null) {
            return validationErrors.hasErrors();
        } else {
            return false;
        }
    }

    public boolean hasResult() {
        return hasResult;
    }

    public boolean hasFullResult() {
        return !hasErrors();
    }

    private static ValidationResult<?> EMPTY = new ValidationResult<Object>((Object) null);

    @SuppressWarnings("unchecked")
    static <T> ValidationResult<T> empty() {
        return (ValidationResult<T>) EMPTY;
    }
}
