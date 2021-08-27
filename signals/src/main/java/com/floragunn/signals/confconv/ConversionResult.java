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