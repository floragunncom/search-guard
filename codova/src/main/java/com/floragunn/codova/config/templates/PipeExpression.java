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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.InvalidExpression;
import com.floragunn.fluent.collections.ImmutableList;
import com.google.common.collect.Iterables;

public class PipeExpression {
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    private final String attributeName;
    private final ImmutableList<PipeFunction> functions;
    private final String source;

    PipeExpression(String attributeName, ImmutableList<PipeFunction> functions, String source) {
        this.attributeName = attributeName;
        this.functions = functions;
        this.source = "${" + source + "}";
    }

    public Object evaluate(Function<String, Object> valueResolver) {
        Object value = valueResolver.apply(attributeName);

        for (PipeFunction pipeFunction : functions) {
            value = pipeFunction.apply(value);
        }

        return value;
    }
    
    public Object evaluate(AttributeSource attributeSource) {
        Object value = attributeSource.getAttributeValue(attributeName);

        for (PipeFunction pipeFunction : functions) {
            value = pipeFunction.apply(value);
        }

        return value;
    }

    @Override
    public String toString() {
        return source;
    }

    public static PipeExpression parse(String string, int start) throws ConfigValidationException {
        return new Parser(string, start).parse();
    }

    @FunctionalInterface
    static interface PipeFunction {
        Object apply(Object value);

        static PipeFunction get(String operation, int stateStart) throws ConfigValidationException {
            if (operation.equals("toString")) {
                return (v) -> v != null ? v.toString() : "null";
            } else if (operation.equals("toJson")) {
                return (v) -> DocWriter.json().writeAsString(v);
            } else if (operation.equals("toList")) {
                return (v) -> v instanceof Collection ? v : Collections.singletonList(v);
            } else if (operation.equals("head")) {
                return (v) -> v instanceof Collection ? Iterables.getFirst((Collection<?>) v, null) : v;
            } else if (operation.equals("tail")) {
                return (v) -> v instanceof Collection ? tail((Collection<?>) v) : Collections.emptyList();
            } else if (operation.equals("toRegexFragment")) {
                return (v) -> toRegexFragment(v);
            } else {
                throw new ConfigValidationException(new InvalidExpression(null, operation).message("Unsupported operation").column(stateStart)
                        .expected("toString|toJson|head|tail|toRegexFragment"));
            }
        }
    }

    private static String toRegexFragment(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Collection) {
            StringBuilder result = new StringBuilder("(");
            boolean first = true;

            for (Object element : (Collection<?>) value) {
                if (element != null) {
                    if (!first) {
                        result.append("|");
                    } else {
                        first = false;
                    }

                    result.append(Pattern.quote(element.toString()));
                }
            }

            result.append(")");

            return result.toString();
        } else {
            return "(" + Pattern.quote(value.toString()) + ")";
        }
    }

    private static List<?> tail(Collection<?> collection) {
        if (collection.size() <= 1) {
            return Collections.emptyList();
        }

        List<Object> result = new ArrayList<>(collection.size() - 1);

        Iterator<?> iter = collection.iterator();
        iter.next();

        while (iter.hasNext()) {
            result.add(iter.next());
        }

        return result;
    }

    public static class Parser {
        private final String string;
        private int i;
        private int start;

        public Parser(String string, int start) {
            this.string = string;
            this.i = start;
            this.start = start;
        }

        public PipeExpression parse() throws ConfigValidationException {
            int openBracket = i;

            String attributeName = readAttributeName(openBracket);
            ImmutableList.Builder<PipeFunction> functions = new ImmutableList.Builder<>();

            skipSpaces();

            for (;;) {
                if (i >= string.length()) {
                    throw new ConfigValidationException(error("Unterminated expression").column(openBracket));
                }

                char c = string.charAt(i);
                char c2 = i < string.length() - 1 ? string.charAt(i + 1) : 0;

                if (c == '|') {
                    i++;
                    skipSpaces();
                    int functionStart = i;
                    String functionName = readFunctionName(openBracket);
                    functions.with(PipeFunction.get(functionName, functionStart));
                } else if (c == '?' && c2 == ':') {
                    i += 2;
                    skipSpaces();
                    Object defaultValue = readJson(i);
                    functions.with((v) -> v != null ? v : defaultValue);
                } else if (c == ':' && c2 == '-') {
                    i += 2;
                    int closeBracket = string.indexOf('}', i);

                    if (closeBracket == -1) {
                        throw new ConfigValidationException(error("Unterminated expression").column(openBracket));
                    }

                    String defaultValue = string.substring(i, closeBracket);
                    functions.with((v) -> v != null ? v : defaultValue);

                    i = closeBracket;

                } else if (c == '}') {
                    i++;
                    break;
                } else if (Character.isWhitespace(c)) {
                    skipSpaces();
                } else {
                    throw new ConfigValidationException(error("Unexpected character in expression").column(i));
                }

            }

            String source = string.substring(start, i);

            return new PipeExpression(attributeName, functions.build(), source);
        }

        public int getParsePosition() {
            return i;
        }

        private String readAttributeName(int openBracket) throws ConfigValidationException {
            for (;; i++) {
                if (i >= string.length()) {
                    throw new ConfigValidationException(error("Unterminated expression").column(openBracket));
                }

                char c = string.charAt(i);

                if (!Character.isLetter(c) && !Character.isDigit(c) && c != '.' && c != '_') {
                    String attributeName = string.substring(openBracket + 2, i);

                    return attributeName;
                }
            }
        }

        private String readFunctionName(int openBracket) throws ConfigValidationException {
            int functionStart = i;

            for (i += 2;; i++) {
                if (i >= string.length()) {
                    throw new ConfigValidationException(error("Unterminated expression").column(openBracket));
                }

                char c = string.charAt(i);

                if (!Character.isLetter(c)) {
                    String functionName = string.substring(functionStart, i);

                    return functionName;
                }
            }
        }

        private void skipSpaces() {
            for (;;) {
                if (i < string.length() && Character.isSpaceChar(string.charAt(i))) {
                    i++;
                } else {
                    break;
                }
            }
        }

        private Object readJson(int start) throws ConfigValidationException {
            try {
                JsonParser parser = JSON_FACTORY.createParser(string.substring(start));

                Object result = new DocReader(Format.JSON, parser).read();

                i = start + (int) parser.getTokenLocation().getCharOffset() + parser.getLastClearedToken().asString().length();

                return result;

            } catch (DocumentParseException | IOException e) {
                throw new ConfigValidationException(error("Expression contains invalid JSON").column(start));
            }
        }

        private InvalidExpression error(String message) {
            return new InvalidExpression(null, string).message(message).column(i);
        }

    }
}
