/*
 * Copyright 2024 floragunn GmbH
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

package com.floragunn.searchsupport.meta;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;

public class ComponentTest {

    @Test
    public void extractComponent_returnsFailuresComponent_whenExpressionEndsWithFailuresSuffix() {
        String expression = "my-data-stream::failures";

        Component result = Component.extractComponent(expression);

        assertThat(result, is(Component.FAILURES));
    }

    @Test
    public void extractComponent_returnsNoneComponent_whenExpressionHasNoComponentSuffix() {
        String expression = "my-data-stream";

        Component result = Component.extractComponent(expression);

        assertThat(result, is(Component.NONE));
    }

    @Test
    public void extractComponent_returnsNoneComponent_whenExpressionIsEmptyString() {
        String expression = "";

        Component result = Component.extractComponent(expression);

        assertThat(result, is(Component.NONE));
    }

    @Test
    public void extractComponent_throwsNullPointerException_whenExpressionIsNull() {
        assertThatThrown(() -> Component.extractComponent(null), instanceOf(NullPointerException.class));
    }

    @Test
    public void extractComponent_returnsNoneComponent_whenExpressionContainsFailuresButNotAtEnd() {
        String expression = "failures-my-data-stream";

        Component result = Component.extractComponent(expression);

        assertThat(result, is(Component.NONE));
    }

    @Test
    public void extractComponent_returnsNoneComponent_whenExpressionContainsSeparatorButNoComponentName() {
        String expression = "my-data-stream::";

        Component result = Component.extractComponent(expression);

        assertThat(result, is(Component.NONE));
    }

    @Test
    public void extractComponent_returnsNoneComponent_whenExpressionContainsFailuresWithoutSeparator() {
        String expression = "my-data-streamfailures";

        Component result = Component.extractComponent(expression);

        assertThat(result, is(Component.NONE));
    }

    @Test
    public void extractComponent_returnsFailuresComponent_whenExpressionIsOnlyFailuresSuffix() {
        String expression = "::failures";

        Component result = Component.extractComponent(expression);

        assertThat(result, is(Component.FAILURES));
    }

    @Test
    public void extractComponent_returnsFailuresComponent_forComplexIndexNameWithFailuresSuffix() {
        String expression = "logs-2024-01-01-000001::failures";

        Component result = Component.extractComponent(expression);

        assertThat(result, is(Component.FAILURES));
    }

    @Test
    public void extractComponent_returnsFailuresComponent_forDataStreamWithMultipleHyphens() {
        String expression = "logs-app-prod-us-east-1::failures";

        Component result = Component.extractComponent(expression);

        assertThat(result, is(Component.FAILURES));
    }

    @Test
    public void extractComponent_returnsNoneComponent_whenExpressionHasSeparatorInMiddle() {
        String expression = "my::data::stream";

        Component result = Component.extractComponent(expression);

        assertThat(result, is(Component.NONE));
    }

    @Test
    public void extractComponent_returnsFailuresComponent_whenExpressionHasMultipleSeparatorsButEndsWithFailures() {
        String expression = "my::data::stream::failures";

        Component result = Component.extractComponent(expression);

        assertThat(result, is(Component.FAILURES));
    }

    @Test
    public void extractComponent_returnsNoneComponent_whenExpressionEndsWithDifferentComponentName() {
        String expression = "my-data-stream::success";

        Component result = Component.extractComponent(expression);

        assertThat(result, is(Component.NONE));
    }

    @Test
    public void extractComponent_returnsNoneComponent_whenExpressionEndsWithPartialFailuresSuffix() {
        String expression = "my-data-stream::fail";

        Component result = Component.extractComponent(expression);

        assertThat(result, is(Component.NONE));
    }

    @Test
    public void extractComponent_isCaseSensitive_returnsNoneForUppercaseFailures() {
        String expression = "my-data-stream::FAILURES";

        Component result = Component.extractComponent(expression);

        assertThat(result, is(Component.NONE));
    }

    @Test
    public void extractComponent_isCaseSensitive_returnsNoneForMixedCaseFailures() {
        String expression = "my-data-stream::Failures";

        Component result = Component.extractComponent(expression);

        assertThat(result, is(Component.NONE));
    }

    @Test
    public void extractComponent_returnsNoneComponent_whenExpressionHasWhitespaceBeforeFailures() {
        String expression = "my-data-stream:: failures";

        Component result = Component.extractComponent(expression);

        assertThat(result, is(Component.NONE));
    }

    @Test
    public void extractComponent_returnsNoneComponent_whenExpressionHasWhitespaceAfterFailures() {
        String expression = "my-data-stream::failures ";

        Component result = Component.extractComponent(expression);

        assertThat(result, is(Component.NONE));
    }
}