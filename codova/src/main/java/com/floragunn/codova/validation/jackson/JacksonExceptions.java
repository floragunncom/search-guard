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

package com.floragunn.codova.validation.jackson;

import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonMappingException.Reference;
import com.fasterxml.jackson.databind.exc.InvalidNullException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.JsonValidationError;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.codova.validation.errors.UnsupportedAttribute;
import com.floragunn.codova.validation.errors.ValidationError;

public class JacksonExceptions {

    public static ValidationError toValidationError(IllegalArgumentException e) {
        if (e.getCause() instanceof JsonMappingException) {
            return toValidationError((JsonMappingException) e.getCause());
        } else if (e.getCause() instanceof JsonProcessingException) {
            return toValidationError((JsonProcessingException) e.getCause());
        } else {
            throw e;
        }
    }

    public static ValidationError toValidationError(JsonMappingException e) {
        String path = getPathAsString(e.getPath());

        if (e instanceof UnrecognizedPropertyException) {
            return new UnsupportedAttribute(path, null, null).cause(e);
        } else if (e instanceof InvalidNullException) {
            return new MissingAttribute(path).cause(e);
        } else if (e instanceof MismatchedInputException) {
            return new InvalidAttributeValue(path, null, ((MismatchedInputException) e).getTargetType()).cause(e);
        } else {
            return new ValidationError(path, e.getMessage()).cause(e);
        }
    }

    public static ValidationError toValidationError(JsonProcessingException e) {
        return new JsonValidationError(null, e);
    }
    
    public static ConfigValidationException toConfigValidationException(IllegalArgumentException e) {
        return new ConfigValidationException(toValidationError(e));
    }

    public static ConfigValidationException toConfigValidationException(JsonMappingException e) {
        return new ConfigValidationException(toValidationError(e));
    }
        
    private static String getPathAsString(List<Reference> path) {
        StringBuilder result = new StringBuilder();
        
        for (Reference reference : path) {
            if (result.length() != 0) {
                result.append('.');
            }
            
            if (reference.getFieldName() != null) {
                result.append(reference.getFieldName());                
            } else if (reference.getIndex() != -1) {
                result.append(reference.getIndex());
            }            
        }
        
        return result.toString();
    }
}
