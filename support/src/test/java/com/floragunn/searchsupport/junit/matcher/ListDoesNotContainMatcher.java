/*
 * Copyright 2025 floragunn GmbH
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
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;
import static org.hamcrest.Matchers.not;

class ListDoesNotContainMatcher extends TypeSafeDiagnosingMatcher<DocNode> {

    private static final Logger log = LogManager.getLogger(ListDoesNotContainMatcher.class);

    private final String jsonPath;
    private final List<Matcher<?>> matchers;

    public ListDoesNotContainMatcher(String jsonPath, Matcher<?>... matchers) {
        this.jsonPath = requireNonNull(jsonPath);
        this.matchers = Arrays.asList(requireNonNull(matchers));
        if (this.matchers.isEmpty()) {
            throw new IllegalArgumentException("Matchers cannot be empty");
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("List pointed by json path ").appendValue(jsonPath).appendText(" should not contain elements matching: [");
        IntStream.range(0, matchers.size())
                .forEach(index -> {
                    matchers.get(index).describeTo(description);
                    if (index != matchers.size() - 1) {
                        description.appendText(", ");
                    }
                });
        description.appendText("]");
    }

    @Override
    protected boolean matchesSafely(DocNode docNode, Description mismatchDescription) {
        try {
            List<?> list = docNode.findByJsonPath(jsonPath);
            boolean mismatchNotFound = true;
            for (int i = 0; i < matchers.size(); i++) {
                Matcher<?> currentMatcher = not(matchers.get(i));
                for (int j = 0; j < list.size(); j++) {
                    Object currentValue = list.get(j);
                    if (! currentMatcher.matches(currentValue)) {
                        if (! mismatchNotFound) {
                            mismatchDescription.appendText("\n");
                        }
                        mismatchNotFound = false;

                        mismatchDescription.appendText("Element at index [" + j + "] doesn't match, ");
                        currentMatcher.describeMismatch(currentValue, mismatchDescription);
                    }
                }
            }
            return mismatchNotFound;
        } catch (PathNotFoundException e) {
            log.debug("Patch {} not found in DocNode {}.", jsonPath, docNode, e);
            mismatchDescription.appendText("path ").appendValue(jsonPath).appendText(" does not exists in doc node ")
                .appendValue(docNode.toJsonString());
            return false;
        }
    }
}
