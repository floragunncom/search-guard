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

import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
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

    // Tests for isExclusion() method

    @Test
    public void isExclusion_returnsTrueForMinusPrefix() {
        assertThat(ParsedIndexReference.of("-my-index").isExclusion(), is(true));
        assertThat(ParsedIndexReference.of("-logs-*").isExclusion(), is(true));
        assertThat(ParsedIndexReference.of("-").isExclusion(), is(true));
    }

    @Test
    public void isExclusion_returnsFalseForNoMinusPrefix() {
        assertThat(ParsedIndexReference.of("my-index").isExclusion(), is(false));
        assertThat(ParsedIndexReference.of("logs-apache-default").isExclusion(), is(false));
        assertThat(ParsedIndexReference.of("").isExclusion(), is(false));
    }

    @Test
    public void isExclusion_returnsFalseForNull() {
        assertThat(ParsedIndexReference.of(null).isExclusion(), is(false));
    }

    @Test
    public void isExclusion_returnsFalseForMinusInMiddle() {
        assertThat(ParsedIndexReference.of("my-index-name").isExclusion(), is(false));
        assertThat(ParsedIndexReference.of("logs-apache-default").isExclusion(), is(false));
    }

    @Test
    public void isExclusion_worksWithComponentSelectors() {
        assertThat(ParsedIndexReference.of("-my-index::failures").isExclusion(), is(true));
        assertThat(ParsedIndexReference.of("-my-index::data").isExclusion(), is(true));
        assertThat(ParsedIndexReference.of("my-index::failures").isExclusion(), is(false));
    }

    // Tests for dropExclusion() method

    @Test
    public void dropExclusion_removesMinusPrefix() {
        ParsedIndexReference ref = ParsedIndexReference.of("-my-index");
        ParsedIndexReference dropped = ref.dropExclusion();

        assertThat(dropped.baseName(), is("my-index"));
        assertThat(dropped.failureStore(), is(false));
    }

    @Test
    public void dropExclusion_preservesFailureStore() {
        ParsedIndexReference ref = ParsedIndexReference.of("-my-index::failures");
        ParsedIndexReference dropped = ref.dropExclusion();

        assertThat(dropped.baseName(), is("my-index"));
        assertThat(dropped.failureStore(), is(true));
    }

    @Test
    public void dropExclusion_returnsSameInstanceWhenNoExclusion() {
        ParsedIndexReference ref = ParsedIndexReference.of("my-index");
        ParsedIndexReference dropped = ref.dropExclusion();

        assertThat(dropped, is(ref));
    }

    @Test
    public void dropExclusion_returnsSameInstanceForNull() {
        ParsedIndexReference ref = ParsedIndexReference.of(null);
        ParsedIndexReference dropped = ref.dropExclusion();

        assertThat(dropped, is(ref));
    }

    @Test
    public void dropExclusion_handlesOnlyMinusPrefix() {
        ParsedIndexReference ref = ParsedIndexReference.of("-");
        ParsedIndexReference dropped = ref.dropExclusion();

        assertThat(dropped.baseName(), is(""));
        assertThat(dropped.failureStore(), is(false));
    }

    @Test
    public void dropExclusion_handlesWildcardWithExclusion() {
        ParsedIndexReference ref = ParsedIndexReference.of("-logs-*::failures");
        ParsedIndexReference dropped = ref.dropExclusion();

        assertThat(dropped.baseName(), is("logs-*"));
        assertThat(dropped.failureStore(), is(true));
    }

    // Tests for containsStarWildcard() method

    @Test
    public void containsStarWildcard_returnsTrueForWildcard() {
        assertThat(ParsedIndexReference.of("logs-*").containsStarWildcard(), is(true));
        assertThat(ParsedIndexReference.of("*").containsStarWildcard(), is(true));
        assertThat(ParsedIndexReference.of("logs-*-default").containsStarWildcard(), is(true));
        assertThat(ParsedIndexReference.of("*-logs-*").containsStarWildcard(), is(true));
    }

    @Test
    public void containsStarWildcard_returnsFalseForNoWildcard() {
        assertThat(ParsedIndexReference.of("logs-apache-default").containsStarWildcard(), is(false));
        assertThat(ParsedIndexReference.of("my-index").containsStarWildcard(), is(false));
        assertThat(ParsedIndexReference.of("").containsStarWildcard(), is(false));
    }

    @Test
    public void containsStarWildcard_returnsFalseForNull() {
        assertThat(ParsedIndexReference.of(null).containsStarWildcard(), is(false));
    }

    @Test
    public void containsStarWildcard_worksWithComponentSelectors() {
        assertThat(ParsedIndexReference.of("logs-*::failures").containsStarWildcard(), is(true));
        assertThat(ParsedIndexReference.of("logs-*::data").containsStarWildcard(), is(true));
        assertThat(ParsedIndexReference.of("logs-apache::failures").containsStarWildcard(), is(false));
    }

    @Test
    public void containsStarWildcard_worksWithExclusion() {
        assertThat(ParsedIndexReference.of("-logs-*").containsStarWildcard(), is(true));
        assertThat(ParsedIndexReference.of("-logs-apache").containsStarWildcard(), is(false));
    }

    // Tests for mapBaseName() method

    @Test
    public void mapBaseName_appliesFunction() {
        ParsedIndexReference ref = ParsedIndexReference.of("my-index");
        ParsedIndexReference mapped = ref.mapBaseName(String::toUpperCase);

        assertThat(mapped.baseName(), is("MY-INDEX"));
        assertThat(mapped.failureStore(), is(false));
    }

    @Test
    public void mapBaseName_preservesFailureStore() {
        ParsedIndexReference ref = ParsedIndexReference.of("my-index::failures");
        ParsedIndexReference mapped = ref.mapBaseName(s -> s + "-modified");

        assertThat(mapped.baseName(), is("my-index-modified"));
        assertThat(mapped.failureStore(), is(true));
    }

    @Test
    public void mapBaseName_preservesNonFailureStore() {
        ParsedIndexReference ref = ParsedIndexReference.of("my-index::data");
        ParsedIndexReference mapped = ref.mapBaseName(s -> s + "-modified");

        assertThat(mapped.baseName(), is("my-index-modified"));
        assertThat(mapped.failureStore(), is(false));
    }

    @Test
    public void mapBaseName_canRemovePrefix() {
        ParsedIndexReference ref = ParsedIndexReference.of("-excluded-index::failures");
        ParsedIndexReference mapped = ref.mapBaseName(s -> s.substring(1));

        assertThat(mapped.baseName(), is("excluded-index"));
        assertThat(mapped.failureStore(), is(true));
    }

    @Test
    public void mapBaseName_identityFunctionPreservesValues() {
        ParsedIndexReference ref = ParsedIndexReference.of("my-index::failures");
        ParsedIndexReference mapped = ref.mapBaseName(s -> s);

        assertThat(mapped.baseName(), is("my-index"));
        assertThat(mapped.failureStore(), is(true));
    }

    @Test
    public void mapBaseName_canResolveExpression() {
        ParsedIndexReference ref = ParsedIndexReference.of("<logs-{now/d}>::failures");
        ParsedIndexReference mapped = ref.mapBaseName(s -> "logs-2024.01.15");

        assertThat(mapped.baseName(), is("logs-2024.01.15"));
        assertThat(mapped.failureStore(), is(true));
    }

    // Tests for isRemoteIndex() method

    @Test
    public void testPredicate() {
        assertThat(ParsedIndexReference.of("local_index").isRemoteIndex(), is(false));
        assertThat(ParsedIndexReference.of("server:remote_index").isRemoteIndex(), is(true));
        assertThat(ParsedIndexReference.of("myRemote:anotherIndex").isRemoteIndex(), is(true));
        assertThat(ParsedIndexReference.of("other:*").isRemoteIndex(), is(true));

        assertThat(ParsedIndexReference.of(":remote").isRemoteIndex(), is(true));
        assertThat(ParsedIndexReference.of("remote:").isRemoteIndex(), is(true));
        assertThat(ParsedIndexReference.of("r:").isRemoteIndex(), is(true));
        assertThat(ParsedIndexReference.of("").isRemoteIndex(), is(false));

        assertThatThrown(() -> ParsedIndexReference.of("not:::remote").isRemoteIndex(), instanceOf(IllegalArgumentException.class));
        assertThatThrown(() -> ParsedIndexReference.of("not:::remote").isRemoteIndex(), instanceOf(IllegalArgumentException.class));
        assertThatThrown(() -> ParsedIndexReference.of("not::remote").isRemoteIndex(), instanceOf(IllegalArgumentException.class));
        assertThatThrown(() -> ParsedIndexReference.of("not_remote::").isRemoteIndex(), instanceOf(IllegalArgumentException.class));
        assertThatThrown(() -> ParsedIndexReference.of("::not_remote").isRemoteIndex(), instanceOf(IllegalArgumentException.class));


        //        assertThat(ParsedIndexReference.of("not:remote:index").isRemoteIndex(), is(false));
    }
}