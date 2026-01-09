/*
 * Copyright 2026 floragunn GmbH
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

package com.floragunn.searchguard.authz.actions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Test;

import com.floragunn.searchsupport.meta.Meta;

public class ParsedIndexReferenceTest {

    // Tests for simple index names without component selector

    @Test
    public void of_simpleIndex_returnsBaseNameWithoutFailureStore() {
        ParsedIndexReference ref = ParsedIndexReference.of("my-index");

        assertThat(ref.baseName(), is("my-index"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is("my-index"));
    }

    @Test
    public void of_indexWithDashes_returnsBaseNameWithoutFailureStore() {
        ParsedIndexReference ref = ParsedIndexReference.of("my-app-logs-2024");

        assertThat(ref.baseName(), is("my-app-logs-2024"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is("my-app-logs-2024"));
    }

    @Test
    public void of_indexWithDots_returnsBaseNameWithoutFailureStore() {
        ParsedIndexReference ref = ParsedIndexReference.of(".ds-my-data-stream-2024.03.22-000001");

        assertThat(ref.baseName(), is(".ds-my-data-stream-2024.03.22-000001"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is(".ds-my-data-stream-2024.03.22-000001"));
    }

    // Tests for data streams without component selector

    @Test
    public void of_dataStreamName_returnsBaseNameWithoutFailureStore() {
        ParsedIndexReference ref = ParsedIndexReference.of("logs-apache-default");

        assertThat(ref.baseName(), is("logs-apache-default"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is("logs-apache-default"));
    }

    @Test
    public void of_dataStreamWithWildcard_returnsBaseNameWithoutFailureStore() {
        ParsedIndexReference ref = ParsedIndexReference.of("logs-*");

        assertThat(ref.baseName(), is("logs-*"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is("logs-*"));
    }

    // Tests for data streams with ::failures selector

    @Test
    public void of_dataStreamWithFailuresSelector_returnsBaseNameWithFailureStore() {
        ParsedIndexReference ref = ParsedIndexReference.of("logs-apache-default::failures");

        assertThat(ref.baseName(), is("logs-apache-default"));
        assertThat(ref.failureStore(), is(true));
        assertThat(ref.metaName(), is("logs-apache-default" + Meta.FAILURES_SUFFIX));
    }

    @Test
    public void of_dataStreamWildcardWithFailuresSelector_returnsBaseNameWithFailureStore() {
        ParsedIndexReference ref = ParsedIndexReference.of("logs-*::failures");

        assertThat(ref.baseName(), is("logs-*"));
        assertThat(ref.failureStore(), is(true));
        assertThat(ref.metaName(), is("logs-*" + Meta.FAILURES_SUFFIX));
    }

    @Test
    public void of_complexDataStreamWithFailuresSelector_returnsBaseNameWithFailureStore() {
        ParsedIndexReference ref = ParsedIndexReference.of("my-app-logs-2024.01::failures");

        assertThat(ref.baseName(), is("my-app-logs-2024.01"));
        assertThat(ref.failureStore(), is(true));
        assertThat(ref.metaName(), is("my-app-logs-2024.01::failures"));
    }

    // Tests for data streams with ::data selector

    @Test
    public void of_dataStreamWithDataSelector_returnsBaseNameWithoutFailureStore() {
        ParsedIndexReference ref = ParsedIndexReference.of("logs-apache-default::data");

        assertThat(ref.baseName(), is("logs-apache-default"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is("logs-apache-default"));
    }

    @Test
    public void of_dataStreamWildcardWithDataSelector_returnsBaseNameWithoutFailureStore() {
        ParsedIndexReference ref = ParsedIndexReference.of("logs-*::data");

        assertThat(ref.baseName(), is("logs-*"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is("logs-*"));
    }

    @Test
    public void of_complexDataStreamWithDataSelector_returnsBaseNameWithoutFailureStore() {
        ParsedIndexReference ref = ParsedIndexReference.of("my-app-logs-2024.01::data");

        assertThat(ref.baseName(), is("my-app-logs-2024.01"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is("my-app-logs-2024.01"));
    }

    // Tests for null and edge cases

    @Test
    public void of_null_returnsNullBaseNameWithoutFailureStore() {
        ParsedIndexReference ref = ParsedIndexReference.of(null);

        assertThat(ref.baseName(), is(nullValue()));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is(nullValue()));
    }

    @Test
    public void of_emptyString_returnsEmptyBaseNameWithoutFailureStore() {
        ParsedIndexReference ref = ParsedIndexReference.of("");

        assertThat(ref.baseName(), is(""));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is(""));
    }

    @Test
    public void of_onlyFailuresSuffix_returnsEmptyBaseNameWithFailureStore() {
        ParsedIndexReference ref = ParsedIndexReference.of("::failures");

        assertThat(ref.baseName(), is(""));
        assertThat(ref.failureStore(), is(true));
        assertThat(ref.metaName(), is("::failures"));
    }

    @Test
    public void of_onlyDataSuffix_returnsEmptyBaseNameWithoutFailureStore() {
        ParsedIndexReference ref = ParsedIndexReference.of("::data");

        assertThat(ref.baseName(), is(""));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is(""));
    }

    // Tests for withIndexName method

    @Test
    public void withIndexName_preservesFailureStore() {
        ParsedIndexReference original = ParsedIndexReference.of("original-index::failures");
        ParsedIndexReference modified = original.withIndexName("new-index");

        assertThat(modified.baseName(), is("new-index"));
        assertThat(modified.failureStore(), is(true));
        assertThat(modified.metaName(), is("new-index::failures"));
    }

    @Test
    public void withIndexName_preservesNonFailureStore() {
        ParsedIndexReference original = ParsedIndexReference.of("original-index");
        ParsedIndexReference modified = original.withIndexName("new-index");

        assertThat(modified.baseName(), is("new-index"));
        assertThat(modified.failureStore(), is(false));
        assertThat(modified.metaName(), is("new-index"));
    }

    @Test
    public void withIndexName_fromDataSelectorPreservesNonFailureStore() {
        ParsedIndexReference original = ParsedIndexReference.of("original-index::data");
        ParsedIndexReference modified = original.withIndexName("new-index");

        assertThat(modified.baseName(), is("new-index"));
        assertThat(modified.failureStore(), is(false));
        assertThat(modified.metaName(), is("new-index"));
    }



    // Tests for realistic Elasticsearch data stream expressions

    @Test
    public void of_realisticDataStreamExpression_logsApache() {
        ParsedIndexReference refNoSelector = ParsedIndexReference.of("logs-apache-default");
        ParsedIndexReference refDataSelector = ParsedIndexReference.of("logs-apache-default::data");
        ParsedIndexReference refFailuresSelector = ParsedIndexReference.of("logs-apache-default::failures");

        assertThat(refNoSelector.baseName(), is("logs-apache-default"));
        assertThat(refNoSelector.failureStore(), is(false));

        assertThat(refDataSelector.baseName(), is("logs-apache-default"));
        assertThat(refDataSelector.failureStore(), is(false));

        assertThat(refFailuresSelector.baseName(), is("logs-apache-default"));
        assertThat(refFailuresSelector.failureStore(), is(true));
    }

    @Test
    public void of_realisticDataStreamExpression_metricsSystem() {
        ParsedIndexReference refNoSelector = ParsedIndexReference.of("metrics-system.cpu-default");
        ParsedIndexReference refDataSelector = ParsedIndexReference.of("metrics-system.cpu-default::data");
        ParsedIndexReference refFailuresSelector = ParsedIndexReference.of("metrics-system.cpu-default::failures");

        assertThat(refNoSelector.baseName(), is("metrics-system.cpu-default"));
        assertThat(refNoSelector.failureStore(), is(false));

        assertThat(refDataSelector.baseName(), is("metrics-system.cpu-default"));
        assertThat(refDataSelector.failureStore(), is(false));

        assertThat(refFailuresSelector.baseName(), is("metrics-system.cpu-default"));
        assertThat(refFailuresSelector.failureStore(), is(true));
    }

    @Test
    public void of_realisticDataStreamExpression_withWildcards() {
        ParsedIndexReference refNoSelector = ParsedIndexReference.of("logs-*-*");
        ParsedIndexReference refDataSelector = ParsedIndexReference.of("logs-*-*::data");
        ParsedIndexReference refFailuresSelector = ParsedIndexReference.of("logs-*-*::failures");

        assertThat(refNoSelector.baseName(), is("logs-*-*"));
        assertThat(refNoSelector.failureStore(), is(false));

        assertThat(refDataSelector.baseName(), is("logs-*-*"));
        assertThat(refDataSelector.failureStore(), is(false));

        assertThat(refFailuresSelector.baseName(), is("logs-*-*"));
        assertThat(refFailuresSelector.failureStore(), is(true));
    }

    @Test
    public void of_backingIndexOfDataStream() {
        ParsedIndexReference ref = ParsedIndexReference.of(".ds-logs-apache-default-2024.01.15-000001");

        assertThat(ref.baseName(), is(".ds-logs-apache-default-2024.01.15-000001"));
        assertThat(ref.failureStore(), is(false));
    }

    // Tests for unknown/arbitrary component selectors

    @Test(expected = IllegalArgumentException.class)
    public void of_unknownComponentSelector_throwsException() {
        ParsedIndexReference.of("my-index::unknown");
    }

    @Test(expected = IllegalArgumentException.class)
    public void of_randomComponentSelector_throwsException() {
        ParsedIndexReference.of("my-index::something-else");
    }

    // Dedicated tests for failureStore() method

    @Test
    public void failureStore_returnsTrueOnlyForFailuresSuffix() {
        assertThat(ParsedIndexReference.of("logs-apache-default::failures").failureStore(), is(true));
        assertThat(ParsedIndexReference.of("logs-apache-default::data").failureStore(), is(false));
        assertThat(ParsedIndexReference.of("logs-apache-default").failureStore(), is(false));
    }

    @Test
    public void failureStore_preservedByWithIndexName() {
        ParsedIndexReference withFailures = ParsedIndexReference.of("original::failures");
        ParsedIndexReference withoutFailures = ParsedIndexReference.of("original::data");

        assertThat(withFailures.withIndexName("new-name").failureStore(), is(true));
        assertThat(withoutFailures.withIndexName("new-name").failureStore(), is(false));
    }

    // Tests for withFailureStore method

    @Test
    public void withFailureStore_setsFailureStoreToTrue() {
        ParsedIndexReference original = ParsedIndexReference.of("my-index");
        ParsedIndexReference modified = original.withFailureStore(true);

        assertThat(modified.baseName(), is("my-index"));
        assertThat(modified.failureStore(), is(true));
        assertThat(modified.metaName(), is("my-index::failures"));
    }

    @Test
    public void withFailureStore_setsFailureStoreToFalse() {
        ParsedIndexReference original = ParsedIndexReference.of("my-index::failures");
        ParsedIndexReference modified = original.withFailureStore(false);

        assertThat(modified.baseName(), is("my-index"));
        assertThat(modified.failureStore(), is(false));
        assertThat(modified.metaName(), is("my-index"));
    }
}