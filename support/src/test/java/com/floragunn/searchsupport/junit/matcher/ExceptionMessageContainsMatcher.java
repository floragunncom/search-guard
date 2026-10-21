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

import org.elasticsearch.common.Strings;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public class ExceptionMessageContainsMatcher extends TypeSafeDiagnosingMatcher<Throwable> {
    private final String substring;

    public ExceptionMessageContainsMatcher(String substring) {
        this.substring = Strings.requireNonEmpty(substring, "substring is required");
    }

    @Override
    protected boolean matchesSafely(Throwable item, Description mismatchDescription) {
        String message = item.getMessage();
        if(message == null) {
            mismatchDescription.appendText(", message is null ");
            return false;
        }
        if( ! message.contains(substring)) {
            mismatchDescription.appendText(", actual message: ").appendValue(message);
            return false;
        }
        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Exception message contains substring ").appendValue(substring);
    }
}
