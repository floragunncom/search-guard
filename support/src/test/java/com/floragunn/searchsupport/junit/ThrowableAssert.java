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
package com.floragunn.searchsupport.junit;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ThrowableAssert {

    public static Throwable assertThatThrown(ThrowingOperation operation, Matcher<Throwable>...matchers) {
        Throwable throwable = null;
        try {
            operation.throwMaybe();
        } catch (Throwable e) {
            throwable = e;
        }
        assertThat("Expected exception was not thrown.", throwable, notNullValue());
        assertThat(throwable, CoreMatchers.allOf(matchers));
        return throwable;
    }

    @FunctionalInterface
    public interface ThrowingOperation {
        void throwMaybe() throws Throwable;
    }
}
