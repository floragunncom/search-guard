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

package com.floragunn.codova.config.templates;

import java.util.Collection;
import java.util.function.Function;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingFunction;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

public class Template<T> {

    public static <T> Template<T> constant(T constant, String source) {
        if (constant == null) {
            throw new IllegalArgumentException("null constants are not possible");
        }
        
        return new Template<>(constant, source);
    }
    
    private final T constantValue;
    private final ValidatingFunction<String, T> parser;
    private final ImmutableList<Token> tokens;
    private final int estimatedLength;
    private final Function<String, String> stringEscapeFunction;
    private final String source;

    public Template(String string, ValidatingFunction<String, T> parser) throws ConfigValidationException {
        ImmutableList.Builder<Token> tokens = null;
        T constantValue = null;

        if (containsPlaceholders(string)) {
            tokens = new ImmutableList.Builder<>();

            for (int i = 0;;) {
                int openBracket = string.indexOf("${", i);
                if (openBracket == -1) {
                    if (i < string.length()) {
                        tokens.with(new Token.Constant(string.substring(i)));
                    }
                    break;
                } else {
                    if (i != openBracket) {
                        tokens.with(new Token.Constant(string.substring(i, openBracket)));
                    }

                    PipeExpression.Parser pipeExpressionParser = new PipeExpression.Parser(string, openBracket + 2);
                    tokens.with(new Token.Placeholder(pipeExpressionParser.parse()));
                    i = pipeExpressionParser.getParsePosition() + 1;
                }
            }
        } else {
            try {
                constantValue = parser.apply(string);
            } catch (ConfigValidationException e) {
                throw e;
            } catch (Exception e) {
                throw new ConfigValidationException(new ValidationError(null, e.getMessage()).cause(e));
            }
        }

        this.constantValue = constantValue;
        this.tokens = tokens != null ? tokens.build() : null;
        this.parser = parser;
        this.estimatedLength = tokens != null ? estimatedLength(this.tokens) : string.length();
        this.stringEscapeFunction = Function.identity();
        this.source = string;
    }

    private Template(T constantValue, String source) {
        this.constantValue = constantValue;
        this.source = source;
        this.parser = null;
        this.tokens = null;
        this.estimatedLength = -1;
        this.stringEscapeFunction = Function.identity();        
    }
    
    private Template(T constantValue, String source, ValidatingFunction<String, T> parser, ImmutableList<Token> tokens, int estimatedLength,
            Function<String, String> stringEscapeFunction) {
        this.constantValue = constantValue;
        this.source = source;
        this.parser = parser;
        this.tokens = tokens;
        this.estimatedLength = estimatedLength;
        this.stringEscapeFunction = stringEscapeFunction;
    }

    public Template<T> stringEscapeFunction(Function<String, String> stringEscapeFunction) {
        return new Template<T>(constantValue, source, parser, tokens, estimatedLength, stringEscapeFunction);
    }

    public boolean isConstant() {
        return constantValue != null;
    }

    public T getConstantValue() {
        return constantValue;
    }

    public String renderToString(AttributeSource valueResolver) throws ExpressionEvaluationException {
        if (constantValue != null) {
            return source;
        }

        StringBuilder result = new StringBuilder(estimatedLength);

        for (Token token : this.tokens) {
            result.append(token.render(valueResolver, stringEscapeFunction));
        }

        return result.toString();
    }

    public T render(AttributeSource valueResolver) throws ExpressionEvaluationException {
        if (constantValue != null) {
            return constantValue;
        }

        String string = renderToString(valueResolver);

        try {
            return parser.apply(string);
        } catch (ConfigValidationException e) {
            throw new ExpressionEvaluationException("Rendered value is not a valid object", e);
        } catch (Exception e) {
            throw new ExpressionEvaluationException("Rendered value is not a valid object", e);
        }
    }

    @Override
    public String toString() {
        return source;
    }

    public String getSource() {
        return source;
    }

    private static abstract class Token {
        abstract int estimatedLength();

        abstract String render(AttributeSource attributeSource, Function<String, String> stringEscapeFunction) throws ExpressionEvaluationException;

        static class Constant extends Token {
            private final String string;

            Constant(String string) {
                this.string = string;
            }

            @Override
            int estimatedLength() {
                return string.length();
            }

            @Override
            String render(AttributeSource attributeSource, Function<String, String> stringEscapeFunction) {
                return string;
            }
        }

        static class Placeholder extends Token {
            private final PipeExpression pipeExpression;

            Placeholder(PipeExpression pipeExpression) {
                this.pipeExpression = pipeExpression;
            }

            @Override
            int estimatedLength() {
                return 20;
            }

            @Override
            String render(AttributeSource attributeSource, Function<String, String> stringEscapeFunction) throws ExpressionEvaluationException {
                Object value = pipeExpression.evaluate(attributeSource);

                if (value == null) {
                    throw new ExpressionEvaluationException("No value for " + pipeExpression);
                }

                if (value instanceof Collection) {
                    value = toQuotedCommaSeparatedString((Collection<?>) value);
                } else if (!(value instanceof String)) {
                    value = value.toString();
                }

                return stringEscapeFunction.apply((String) value);
            }

            private static String toQuotedCommaSeparatedString(Collection<?> values) {
                return Joiner.on(',').join(Iterables.transform(values, s -> '"' + String.valueOf(s).replaceAll("\"", "\\\"") + '"'));
            }

        }
    }

    public static boolean containsPlaceholders(String string) {
        return string.contains("${");
    }

    private static int estimatedLength(ImmutableList<Token> tokens) {
        int result = 0;

        for (Token token : tokens) {
            result += token.estimatedLength();
        }

        return result;
    }

    @Override
    public int hashCode() {
        return source.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Template)) {
            return false;
        }

        Template<?> template = (Template<?>) obj;

        return template.getSource().equals(source);
    }

}
