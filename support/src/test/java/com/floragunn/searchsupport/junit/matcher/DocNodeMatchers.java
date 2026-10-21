/*
 * Copyright 2023-2024 floragunn GmbH
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

package com.floragunn.searchsupport.junit.matcher;

import com.floragunn.codova.documents.DocNode;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public class DocNodeMatchers {

    private DocNodeMatchers(){}

    public static Matcher<DocNode> containsFieldPointedByJsonPath(String jsonPath, String fieldName) {
        return new ContainsFieldPointedByJsonPathMatcher(jsonPath, fieldName);
    }

    public static Matcher<DocNode> docNodeSizeEqualTo(String jsonPath, int expectedSize) {
        return new DocNodeSizeEqualToMatcher(jsonPath, expectedSize);
    }

    public static Matcher<DocNode> containsValue(String jsonPath, Object value) {
        return new ContainsFieldValuePointedByJsonPathMatcher(jsonPath, value);
    }

    public static Matcher<DocNode> containsNullValue(String jsonPath) {
        return new ContainsNullValuePointedByJsonPathMatcher(jsonPath);
    }

    public static <T> Matcher<DocNode> containsAnyValues(String jsonPatch, T...values) {
        Objects.requireNonNull(values, "Expected values array is null");
        Class<?> elementClass = values.getClass().getComponentType();
        Matcher[] matchers = Arrays.stream(values).map(CoreMatchers::equalTo).toArray(size -> new Matcher[size]);
        return valueSatisfiesMatcher(jsonPatch, elementClass, CoreMatchers.anyOf(matchers));
    }

    public static Matcher<DocNode> containSubstring(String jsonPath, String desiredSubstring) {
        return new FieldContainSubstringMatcher(jsonPath, desiredSubstring);
    }

    public static <T> Matcher<DocNode> valueSatisfiesMatcher(String jsonPath, Class<T> valueClazz, Matcher<T> matcher) {
        return new JsonPathValueSatisfiesMatcher<T>(jsonPath, matcher, valueClazz);
    }

    public static Matcher<DocNode> fieldIsNull(String jsonPath) {
        return new FieldIsNullMatcher(jsonPath);
    }

    public static Matcher<DocNode> containsOnlyFields(String jsonPath,String...fieldNames) {
        return new ContainsOnlyFieldsMatcher(jsonPath, fieldNames);
    }

    public static Matcher<DocNode> containsOnlyFields(String jsonPath, Collection<String> fieldNames) {
        return new ContainsOnlyFieldsMatcher(jsonPath, fieldNames.toArray(String[]::new));
    }
}
