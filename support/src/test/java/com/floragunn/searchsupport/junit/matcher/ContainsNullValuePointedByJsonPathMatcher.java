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

package com.floragunn.searchsupport.junit.matcher;

import com.floragunn.codova.documents.DocNode;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

class ContainsNullValuePointedByJsonPathMatcher extends TypeSafeDiagnosingMatcher<DocNode> {

    private static final Logger log = LogManager.getLogger(ContainsNullValuePointedByJsonPathMatcher.class);

    private final String jsonPath;

    public ContainsNullValuePointedByJsonPathMatcher(String jsonPath) {
        this.jsonPath = requireNonNull(jsonPath);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("DocNode should contain field ").appendValue(jsonPath)//
            .appendText(" with null value ");
    }

    @Override
    protected boolean matchesSafely(DocNode docNode, Description mismatchDescription) {
        if(log.isDebugEnabled()) {
            log.debug("Checking if DocNode {} contain field {} with null value {}", docNode.toPrettyJsonString(), jsonPath);
        }
        try {
            Object nodeValue = docNode.findSingleValueByJsonPath(jsonPath, Object.class);
            if (!Objects.isNull(nodeValue)) {
                mismatchDescription.appendText("Node pointed by json path ").appendValue(jsonPath).appendText(" is not null, actual value: ").appendValue(
                        nodeValue);
                return false;
            }
            return true;
        } catch (PathNotFoundException e) {
            log.debug("Patch {} not found in DocNode {}.", jsonPath, docNode, e);
            mismatchDescription.appendText(" path ").appendValue(jsonPath).appendText(" does not exists in doc node ")
                .appendValue(docNode.toJsonString());
            return false;
        }
    }
}
