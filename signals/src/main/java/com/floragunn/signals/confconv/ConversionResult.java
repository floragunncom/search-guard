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
package com.floragunn.signals.confconv;

import com.floragunn.codova.validation.ValidationErrors;

public class ConversionResult<Element> {
    public final Element element;
    public final ValidationErrors sourceValidationErrors;

    public ConversionResult(Element element) {
        this.element = element;
        this.sourceValidationErrors = new ValidationErrors();
    }

    public ConversionResult(Element element, ValidationErrors sourceValidationErrors) {
        this.element = element;
        this.sourceValidationErrors = sourceValidationErrors;
    }

    public Element getElement() {
        return element;
    }

    public ValidationErrors getSourceValidationErrors() {
        return sourceValidationErrors;
    }
}
