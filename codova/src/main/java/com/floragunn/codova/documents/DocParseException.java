package com.floragunn.codova.documents;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.ValidationError;

public class DocParseException extends ConfigValidationException {

    private static final long serialVersionUID = 7108776044115873652L;

    public DocParseException(ValidationError validationError) {
        super(validationError);
    }
    
    public DocParseException(JsonProcessingException jsonProcessingException, DocType docType) {
        this(new DocParseError(null, jsonProcessingException, docType));
    }
    
    public DocParseException(String attribute, JsonProcessingException jsonProcessingException, DocType docType) {
        this(new DocParseError(attribute, jsonProcessingException, docType));
    }

    public static class DocParseError extends ValidationError {
        private JsonLocation jsonLocation;
        private String context;

        public DocParseError(String attribute, JsonProcessingException jsonProcessingException, DocType docType) {
            super(attribute, "Invalid " + docType.getName() + " document: " + jsonProcessingException.getOriginalMessage());
            cause(jsonProcessingException);
            this.jsonLocation = jsonProcessingException.getLocation();

            if (jsonProcessingException instanceof JsonParseException) {
                this.context = ((JsonParseException) jsonProcessingException).getRequestPayloadAsString();
            }
        }

        public DocParseError(String attribute, String message, JsonLocation jsonLocation, String context) {
            super(attribute, message);
            this.jsonLocation = jsonLocation;
            this.context = context;
        }

        @Override
        public Map<String, Object> toMap() {
            Map<String, Object> result = new LinkedHashMap<>();

            result.put("error", getMessage());

            if (jsonLocation != null) {
                result.put("line", jsonLocation.getLineNr());
                result.put("column", jsonLocation.getColumnNr());
            }

            if (context != null) {
                result.put("context", context);
            }

            return result;
        }

        @Override
        public String toValidationErrorsOverviewString() {
            if (jsonLocation != null) {
                return getMessage() + "; line: " + jsonLocation.getLineNr() + "; column: " + jsonLocation.getColumnNr();
            } else {
                return getMessage();
            }
        }

    }

}
