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
import static org.hamcrest.Matchers.is;

import org.junit.Test;

import com.floragunn.searchsupport.meta.Meta;

public class IndexExpressionTest {

    // Tests for simple index names without component selector

    @Test
    public void of_simpleIndex_returnsBaseNameWithoutFailureStore() {
        IndexExpression ref = IndexExpression.of("my-index");

        assertThat(ref.baseName(), is("my-index"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is("my-index"));
    }

    @Test
    public void of_indexWithDashes_returnsBaseNameWithoutFailureStore() {
        IndexExpression ref = IndexExpression.of("my-app-logs-2024");

        assertThat(ref.baseName(), is("my-app-logs-2024"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is("my-app-logs-2024"));
    }

    @Test
    public void of_indexWithDots_returnsBaseNameWithoutFailureStore() {
        IndexExpression ref = IndexExpression.of(".ds-my-data-stream-2024.03.22-000001");

        assertThat(ref.baseName(), is(".ds-my-data-stream-2024.03.22-000001"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is(".ds-my-data-stream-2024.03.22-000001"));
    }

    // Tests for data streams without component selector

    @Test
    public void of_dataStreamName_returnsBaseNameWithoutFailureStore() {
        IndexExpression ref = IndexExpression.of("logs-apache-default");

        assertThat(ref.baseName(), is("logs-apache-default"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is("logs-apache-default"));
    }

    @Test
    public void of_dataStreamWithWildcard_returnsBaseNameWithoutFailureStore() {
        IndexExpression ref = IndexExpression.of("logs-*");

        assertThat(ref.baseName(), is("logs-*"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is("logs-*"));
    }

    // Tests for data streams with ::failures selector

    @Test
    public void of_dataStreamWithFailuresSelector_returnsBaseNameWithFailureStore() {
        IndexExpression ref = IndexExpression.of("logs-apache-default::failures");

        assertThat(ref.baseName(), is("logs-apache-default"));
        assertThat(ref.failureStore(), is(true));
        assertThat(ref.metaName(), is("logs-apache-default" + Meta.FAILURES_SUFFIX));
    }

    @Test
    public void of_dataStreamWildcardWithFailuresSelector_returnsBaseNameWithFailureStore() {
        IndexExpression ref = IndexExpression.of("logs-*::failures");

        assertThat(ref.baseName(), is("logs-*"));
        assertThat(ref.failureStore(), is(true));
        assertThat(ref.metaName(), is("logs-*" + Meta.FAILURES_SUFFIX));
    }

    @Test
    public void of_complexDataStreamWithFailuresSelector_returnsBaseNameWithFailureStore() {
        IndexExpression ref = IndexExpression.of("my-app-logs-2024.01::failures");

        assertThat(ref.baseName(), is("my-app-logs-2024.01"));
        assertThat(ref.failureStore(), is(true));
        assertThat(ref.metaName(), is("my-app-logs-2024.01::failures"));
    }

    // Tests for data streams with ::data selector

    @Test
    public void of_dataStreamWithDataSelector_returnsBaseNameWithoutFailureStore() {
        IndexExpression ref = IndexExpression.of("logs-apache-default::data");

        assertThat(ref.baseName(), is("logs-apache-default"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is("logs-apache-default"));
    }

    @Test
    public void of_dataStreamWildcardWithDataSelector_returnsBaseNameWithoutFailureStore() {
        IndexExpression ref = IndexExpression.of("logs-*::data");

        assertThat(ref.baseName(), is("logs-*"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is("logs-*"));
    }

    @Test
    public void of_complexDataStreamWithDataSelector_returnsBaseNameWithoutFailureStore() {
        IndexExpression ref = IndexExpression.of("my-app-logs-2024.01::data");

        assertThat(ref.baseName(), is("my-app-logs-2024.01"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is("my-app-logs-2024.01"));
    }

    // Tests for null and edge cases

    @Test
    public void of_null_returnsAllBaseNameWithoutFailureStore() {
        IndexExpression ref = IndexExpression.of(null);

        assertThat(ref.baseName(), is("_all"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is("_all"));
    }

    @Test
    public void of_emptyString_returnsEmptyBaseNameWithoutFailureStore() {
        IndexExpression ref = IndexExpression.of("");

        assertThat(ref.baseName(), is(""));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is(""));
    }

    @Test
    public void of_onlyFailuresSuffix_returnsEmptyBaseNameWithFailureStore() {
        IndexExpression ref = IndexExpression.of("::failures");

        assertThat(ref.baseName(), is(""));
        assertThat(ref.failureStore(), is(true));
        assertThat(ref.metaName(), is("::failures"));
    }

    @Test
    public void of_onlyDataSuffix_returnsEmptyBaseNameWithoutFailureStore() {
        IndexExpression ref = IndexExpression.of("::data");

        assertThat(ref.baseName(), is(""));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is(""));
    }

    // Tests for withIndexName method

    @Test
    public void withIndexName_preservesFailureStore() {
        IndexExpression original = IndexExpression.of("original-index::failures");
        IndexExpression modified = original.withIndexName("new-index");

        assertThat(modified.baseName(), is("new-index"));
        assertThat(modified.failureStore(), is(true));
        assertThat(modified.metaName(), is("new-index::failures"));
    }

    @Test
    public void withIndexName_preservesNonFailureStore() {
        IndexExpression original = IndexExpression.of("original-index");
        IndexExpression modified = original.withIndexName("new-index");

        assertThat(modified.baseName(), is("new-index"));
        assertThat(modified.failureStore(), is(false));
        assertThat(modified.metaName(), is("new-index"));
    }

    @Test
    public void withIndexName_fromDataSelectorPreservesNonFailureStore() {
        IndexExpression original = IndexExpression.of("original-index::data");
        IndexExpression modified = original.withIndexName("new-index");

        assertThat(modified.baseName(), is("new-index"));
        assertThat(modified.failureStore(), is(false));
        assertThat(modified.metaName(), is("new-index"));
    }



    // Tests for realistic Elasticsearch data stream expressions

    @Test
    public void of_realisticDataStreamExpression_logsApache() {
        IndexExpression refNoSelector = IndexExpression.of("logs-apache-default");
        IndexExpression refDataSelector = IndexExpression.of("logs-apache-default::data");
        IndexExpression refFailuresSelector = IndexExpression.of("logs-apache-default::failures");

        assertThat(refNoSelector.baseName(), is("logs-apache-default"));
        assertThat(refNoSelector.failureStore(), is(false));

        assertThat(refDataSelector.baseName(), is("logs-apache-default"));
        assertThat(refDataSelector.failureStore(), is(false));

        assertThat(refFailuresSelector.baseName(), is("logs-apache-default"));
        assertThat(refFailuresSelector.failureStore(), is(true));
    }

    @Test
    public void of_realisticDataStreamExpression_metricsSystem() {
        IndexExpression refNoSelector = IndexExpression.of("metrics-system.cpu-default");
        IndexExpression refDataSelector = IndexExpression.of("metrics-system.cpu-default::data");
        IndexExpression refFailuresSelector = IndexExpression.of("metrics-system.cpu-default::failures");

        assertThat(refNoSelector.baseName(), is("metrics-system.cpu-default"));
        assertThat(refNoSelector.failureStore(), is(false));

        assertThat(refDataSelector.baseName(), is("metrics-system.cpu-default"));
        assertThat(refDataSelector.failureStore(), is(false));

        assertThat(refFailuresSelector.baseName(), is("metrics-system.cpu-default"));
        assertThat(refFailuresSelector.failureStore(), is(true));
    }

    @Test
    public void of_realisticDataStreamExpression_withWildcards() {
        IndexExpression refNoSelector = IndexExpression.of("logs-*-*");
        IndexExpression refDataSelector = IndexExpression.of("logs-*-*::data");
        IndexExpression refFailuresSelector = IndexExpression.of("logs-*-*::failures");

        assertThat(refNoSelector.baseName(), is("logs-*-*"));
        assertThat(refNoSelector.failureStore(), is(false));

        assertThat(refDataSelector.baseName(), is("logs-*-*"));
        assertThat(refDataSelector.failureStore(), is(false));

        assertThat(refFailuresSelector.baseName(), is("logs-*-*"));
        assertThat(refFailuresSelector.failureStore(), is(true));
    }

    @Test
    public void of_backingIndexOfDataStream() {
        IndexExpression ref = IndexExpression.of(".ds-logs-apache-default-2024.01.15-000001");

        assertThat(ref.baseName(), is(".ds-logs-apache-default-2024.01.15-000001"));
        assertThat(ref.failureStore(), is(false));
    }

    // Tests for unknown/arbitrary component selectors
    // Unknown selectors are logged as errors, the full expression (including ::) becomes the baseName

    @Test
    public void of_unknownComponentSelector_returnsFullExpressionAsBaseName() {
        IndexExpression ref = IndexExpression.of("my-index::unknown");

        assertThat(ref.baseName(), is("my-index::unknown"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is("my-index::unknown"));
    }

    @Test
    public void of_randomComponentSelector_returnsFullExpressionAsBaseName() {
        IndexExpression ref = IndexExpression.of("my-index::something-else");

        assertThat(ref.baseName(), is("my-index::something-else"));
        assertThat(ref.failureStore(), is(false));
        assertThat(ref.metaName(), is("my-index::something-else"));
    }

    // Dedicated tests for failureStore() method

    @Test
    public void failureStore_returnsTrueOnlyForFailuresSuffix() {
        assertThat(IndexExpression.of("logs-apache-default::failures").failureStore(), is(true));
        assertThat(IndexExpression.of("logs-apache-default::data").failureStore(), is(false));
        assertThat(IndexExpression.of("logs-apache-default").failureStore(), is(false));
    }

    @Test
    public void failureStore_preservedByWithIndexName() {
        IndexExpression withFailures = IndexExpression.of("original::failures");
        IndexExpression withoutFailures = IndexExpression.of("original::data");

        assertThat(withFailures.withIndexName("new-name").failureStore(), is(true));
        assertThat(withoutFailures.withIndexName("new-name").failureStore(), is(false));
    }

    // Tests for isExclusion() method

    @Test
    public void isExclusion_returnsTrueForMinusPrefix() {
        assertThat(IndexExpression.of("-my-index").isExclusion(), is(true));
        assertThat(IndexExpression.of("-logs-*").isExclusion(), is(true));
        assertThat(IndexExpression.of("-").isExclusion(), is(true));
    }

    @Test
    public void isExclusion_returnsFalseForNoMinusPrefix() {
        assertThat(IndexExpression.of("my-index").isExclusion(), is(false));
        assertThat(IndexExpression.of("logs-apache-default").isExclusion(), is(false));
        assertThat(IndexExpression.of("").isExclusion(), is(false));
    }

    @Test
    public void isExclusion_returnsFalseForAll() {
        assertThat(IndexExpression.of(null).isExclusion(), is(false));
    }

    @Test
    public void isExclusion_returnsFalseForMinusInMiddle() {
        assertThat(IndexExpression.of("my-index-name").isExclusion(), is(false));
        assertThat(IndexExpression.of("logs-apache-default").isExclusion(), is(false));
    }

    @Test
    public void isExclusion_worksWithComponentSelectors() {
        assertThat(IndexExpression.of("-my-index::failures").isExclusion(), is(true));
        assertThat(IndexExpression.of("-my-index::data").isExclusion(), is(true));
        assertThat(IndexExpression.of("my-index::failures").isExclusion(), is(false));
    }

    // Tests for dropExclusion() method

    @Test
    public void dropExclusion_removesMinusPrefix() {
        IndexExpression ref = IndexExpression.of("-my-index");
        IndexExpression dropped = ref.dropExclusion();

        assertThat(dropped.baseName(), is("my-index"));
        assertThat(dropped.failureStore(), is(false));
    }

    @Test
    public void dropExclusion_preservesFailureStore() {
        IndexExpression ref = IndexExpression.of("-my-index::failures");
        IndexExpression dropped = ref.dropExclusion();

        assertThat(dropped.baseName(), is("my-index"));
        assertThat(dropped.failureStore(), is(true));
    }

    @Test
    public void dropExclusion_returnsSameInstanceWhenNoExclusion() {
        IndexExpression ref = IndexExpression.of("my-index");
        IndexExpression dropped = ref.dropExclusion();

        assertThat(dropped, is(ref));
    }

    @Test
    public void dropExclusion_returnsSameInstanceForAll() {
        IndexExpression ref = IndexExpression.of(null);
        IndexExpression dropped = ref.dropExclusion();

        assertThat(dropped, is(ref));
    }

    @Test
    public void dropExclusion_handlesOnlyMinusPrefix() {
        IndexExpression ref = IndexExpression.of("-");
        IndexExpression dropped = ref.dropExclusion();

        assertThat(dropped.baseName(), is(""));
        assertThat(dropped.failureStore(), is(false));
    }

    @Test
    public void dropExclusion_handlesWildcardWithExclusion() {
        IndexExpression ref = IndexExpression.of("-logs-*::failures");
        IndexExpression dropped = ref.dropExclusion();

        assertThat(dropped.baseName(), is("logs-*"));
        assertThat(dropped.failureStore(), is(true));
    }

    // Tests for containsStarWildcard() method

    @Test
    public void containsStarWildcard_returnsTrueForWildcard() {
        assertThat(IndexExpression.of("logs-*").containsStarWildcard(), is(true));
        assertThat(IndexExpression.of("*").containsStarWildcard(), is(true));
        assertThat(IndexExpression.of("logs-*-default").containsStarWildcard(), is(true));
        assertThat(IndexExpression.of("*-logs-*").containsStarWildcard(), is(true));
    }

    @Test
    public void containsStarWildcard_returnsFalseForNoWildcard() {
        assertThat(IndexExpression.of("logs-apache-default").containsStarWildcard(), is(false));
        assertThat(IndexExpression.of("my-index").containsStarWildcard(), is(false));
        assertThat(IndexExpression.of("").containsStarWildcard(), is(false));
    }

    @Test
    public void containsStarWildcard_returnsFalseForAll() {
        assertThat(IndexExpression.of(null).containsStarWildcard(), is(false));
    }

    @Test
    public void containsStarWildcard_worksWithComponentSelectors() {
        assertThat(IndexExpression.of("logs-*::failures").containsStarWildcard(), is(true));
        assertThat(IndexExpression.of("logs-*::data").containsStarWildcard(), is(true));
        assertThat(IndexExpression.of("logs-apache::failures").containsStarWildcard(), is(false));
    }

    @Test
    public void containsStarWildcard_worksWithExclusion() {
        assertThat(IndexExpression.of("-logs-*").containsStarWildcard(), is(true));
        assertThat(IndexExpression.of("-logs-apache").containsStarWildcard(), is(false));
    }

    // Tests for containsWildcard() method
    // containsWildcard() differs from containsStarWildcard(): it also treats "_all" as a wildcard

    @Test
    public void containsWildcard_returnsTrueForStarWildcard() {
        assertThat(IndexExpression.of("logs-*").containsWildcard(), is(true));
        assertThat(IndexExpression.of("*").containsWildcard(), is(true));
        assertThat(IndexExpression.of("logs-*-default").containsWildcard(), is(true));
        assertThat(IndexExpression.of("*-logs-*").containsWildcard(), is(true));
    }

    @Test
    public void containsWildcard_returnsTrueForAll() {
        assertThat(IndexExpression.of(null).containsWildcard(), is(true));
        assertThat(IndexExpression.of("_all").containsWildcard(), is(true));
    }

    @Test
    public void containsWildcard_returnsFalseForConcreteIndex() {
        assertThat(IndexExpression.of("logs-apache-default").containsWildcard(), is(false));
        assertThat(IndexExpression.of("my-index").containsWildcard(), is(false));
        assertThat(IndexExpression.of("").containsWildcard(), is(false));
    }

    @Test
    public void containsWildcard_worksWithComponentSelectors() {
        assertThat(IndexExpression.of("logs-*::failures").containsWildcard(), is(true));
        assertThat(IndexExpression.of("logs-*::data").containsWildcard(), is(true));
        assertThat(IndexExpression.of("logs-apache::failures").containsWildcard(), is(false));
    }

    @Test
    public void containsWildcard_worksWithExclusion() {
        assertThat(IndexExpression.of("-logs-*").containsWildcard(), is(true));
        assertThat(IndexExpression.of("-logs-apache").containsWildcard(), is(false));
    }

    // Tests for mapBaseName() method

    @Test
    public void mapBaseName_appliesFunction() {
        IndexExpression ref = IndexExpression.of("my-index");
        IndexExpression mapped = ref.mapBaseName(String::toUpperCase);

        assertThat(mapped.baseName(), is("MY-INDEX"));
        assertThat(mapped.failureStore(), is(false));
    }

    @Test
    public void mapBaseName_preservesFailureStore() {
        IndexExpression ref = IndexExpression.of("my-index::failures");
        IndexExpression mapped = ref.mapBaseName(s -> s + "-modified");

        assertThat(mapped.baseName(), is("my-index-modified"));
        assertThat(mapped.failureStore(), is(true));
    }

    @Test
    public void mapBaseName_preservesNonFailureStore() {
        IndexExpression ref = IndexExpression.of("my-index::data");
        IndexExpression mapped = ref.mapBaseName(s -> s + "-modified");

        assertThat(mapped.baseName(), is("my-index-modified"));
        assertThat(mapped.failureStore(), is(false));
    }

    @Test
    public void mapBaseName_canRemovePrefix() {
        IndexExpression ref = IndexExpression.of("-excluded-index::failures");
        IndexExpression mapped = ref.mapBaseName(s -> s.substring(1));

        assertThat(mapped.baseName(), is("excluded-index"));
        assertThat(mapped.failureStore(), is(true));
    }

    @Test
    public void mapBaseName_identityFunctionPreservesValues() {
        IndexExpression ref = IndexExpression.of("my-index::failures");
        IndexExpression mapped = ref.mapBaseName(s -> s);

        assertThat(mapped.baseName(), is("my-index"));
        assertThat(mapped.failureStore(), is(true));
    }

    @Test
    public void mapBaseName_canResolveExpression() {
        IndexExpression ref = IndexExpression.of("<logs-{now/d}>::failures");
        IndexExpression mapped = ref.mapBaseName(s -> "logs-2024.01.15");

        assertThat(mapped.baseName(), is("logs-2024.01.15"));
        assertThat(mapped.failureStore(), is(true));
    }

    // Tests for isRemoteIndex() method

    @Test
    public void testRemoteIndexPredicate() {
        assertThat(IndexExpression.of("local_index").isRemoteIndex(), is(false));
        assertThat(IndexExpression.of("server:remote_index").isRemoteIndex(), is(true));
        assertThat(IndexExpression.of("myRemote:anotherIndex").isRemoteIndex(), is(true));
        assertThat(IndexExpression.of("other:*").isRemoteIndex(), is(true));

        assertThat(IndexExpression.of(":remote").isRemoteIndex(), is(true));
        assertThat(IndexExpression.of("remote:").isRemoteIndex(), is(true));
        assertThat(IndexExpression.of("r:").isRemoteIndex(), is(true));
        assertThat(IndexExpression.of("").isRemoteIndex(), is(false));

        // Expressions with "::" are parsed as component selectors first.
        // Unknown selectors fold into baseName; the resulting baseName has multiple colons,
        // so isRemoteIndex() returns false.
        assertThat(IndexExpression.of("not:::remote").isRemoteIndex(), is(false));
        assertThat(IndexExpression.of("not::remote").isRemoteIndex(), is(false));
        assertThat(IndexExpression.of("not_remote::").isRemoteIndex(), is(false));
        assertThat(IndexExpression.of("::not_remote").isRemoteIndex(), is(false));
    }
}