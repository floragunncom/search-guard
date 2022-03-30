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

package com.floragunn.codova.validation.errors;

import java.util.LinkedHashMap;
import java.util.Map;

public class InvalidExpression extends ValidationError {
    private final String expression;
    private int column = -1;

    public InvalidExpression(String attribute, String expression) {
        super(attribute, "Invalid expression");
        this.expression = expression;
    }

    @Override
    public Map<String, Object> toBasicObject() {
        Map<String, Object> result = new LinkedHashMap<>();

        result.put("error", getMessage());

        result.put("value", expression);
        
        if (column != -1) {
            result.put("column", column);
        }

        if (getExpected() != null) {
            result.put("expected", getExpectedAsString());
        }

        return result;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder(getMessage());
        
        if (column != -1) {
            result.append(" at column ").append(column);
        }
        
        if (getExpected() != null) {
            result.append("; expected: ").append(getExpectedAsString());
        }
        
        result.append("; expression: ").append(expression);
        
        if (getAttribute() != null) {
            result.append("; attribute: ").append(getAttribute());
        }
        
        return result.toString();
    }

    @Override
    public String toValidationErrorsOverviewString() {
        StringBuilder result = new StringBuilder(getMessage());
        
        if (column != -1) {
            result.append(" at column ").append(column);
        }
        
        if (getExpected() != null) {
            result.append("; expected: ").append(getExpectedAsString());
        }

        result.append("; got: ").append(expression);

        return result.toString();
    }

    public InvalidExpression expected(Object expected) {
        super.expected(expected);
        return this;
    }   
    
    public InvalidExpression message(String message) {
        super.message(message);
        return this;
    }
    
    public InvalidExpression column(int column) {
        this.column = column;
        return this;
    }

    @Override
    protected InvalidExpression clone() {
        return (InvalidExpression) new InvalidExpression(getAttribute(), expression).expected(getExpected()).cause(getCause()).docNode(getDocNode())
                .message(getMessage());
    }
}