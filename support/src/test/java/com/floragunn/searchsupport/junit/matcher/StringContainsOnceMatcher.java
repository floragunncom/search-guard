/*
 * Copyright 2020-2023 floragunn GmbH
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

import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.Objects;

class StringContainsOnceMatcher extends TypeSafeDiagnosingMatcher<String> {

    private final String substring;

    public StringContainsOnceMatcher(String substring) {
        this.substring = Objects.requireNonNull(substring, "Substring is required");
    }

    @Override
    protected boolean matchesSafely(String item, Description mismatchDescription) {
        String[] split = item.split("\\Q" + substring + "\\E");
        if(split.length != 1){
            mismatchDescription.appendText(" found incorrect number of substring occurrences.");
            return false;
        }
        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(" should contains one occurrence of substring ").appendValue(substring);
    }
}
