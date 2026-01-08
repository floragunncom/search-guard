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

import com.floragunn.fluent.collections.ImmutableSet;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;

public class MetaTest {

    @Test
    public void testIndicesWithPredicate_filtersIndicesByNamePrefix() {
        Meta meta = Meta.Mock.indices("test-index-1", "test-index-2", "prod-index-1");

        ImmutableSet<Meta.Index> result = meta.indices(index ->
            index.name().startsWith("test")
        );

        assertThat(result, hasSize(2));
        assertThat(result.stream().anyMatch(idx -> idx.name().equals("test-index-1")), is(true));
        assertThat(result.stream().anyMatch(idx -> idx.name().equals("test-index-2")), is(true));
        assertThat(result.stream().anyMatch(idx -> idx.name().equals("prod-index-1")), is(false));
    }

    @Test
    public void testIndicesWithPredicate_returnsSingleMatchingIndex() {
        Meta meta = Meta.Mock.indices("index1", "index2", "index3");

        ImmutableSet<Meta.Index> result = meta.indices(index ->
            index.name().equals("index2")
        );

        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().name(), is("index2"));
    }

    @Test
    public void testIndicesWithPredicate_returnsAllIndicesWhenAllMatch() {
        Meta meta = Meta.Mock.indices("index1", "index2", "index3");

        ImmutableSet<Meta.Index> result = meta.indices(index -> true);

        assertThat(result, hasSize(3));
    }

    @Test
    public void testIndicesWithPredicate_returnsEmptySetWhenNoneMatch() {
        Meta meta = Meta.Mock.indices("index1", "index2", "index3");

        ImmutableSet<Meta.Index> result = meta.indices(index -> false);

        assertThat(result, notNullValue());
        assertThat(result, hasSize(0));
        assertThat(result.isEmpty(), is(true));
    }

    @Test
    public void testIndicesWithPredicate_returnsEmptySetWhenNoIndicesExist() {
        Meta meta = Meta.Mock.indices();

        ImmutableSet<Meta.Index> result = meta.indices(index -> true);

        assertThat(result, notNullValue());
        assertThat(result, hasSize(0));
    }

    @Test
    public void testIndicesWithPredicate_throwsNPEWhenPredicateIsNull() {
        Meta meta = Meta.Mock.indices("index1", "index2");

        assertThatThrown(() -> meta.indices(null), instanceOf(NullPointerException.class));
    }

    @Test
    public void testIndicesWithPredicate_filtersIndicesByComplexCondition() {
        Meta meta = Meta.Mock.indices(
            "logs-2024-01-01",
            "logs-2024-01-02",
            "metrics-2024-01-01",
            "traces-2024-01-01"
        );

        ImmutableSet<Meta.Index> result = meta.indices(index ->
            index.name().startsWith("logs") && index.name().contains("01-01")
        );

        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().name(), is("logs-2024-01-01"));
    }

    @Test
    public void testIndicesWithPredicate_doesNotModifyOriginalIndicesSet() {
        // Arrange
        Meta meta = Meta.Mock.indices("index1", "index2", "index3");
        int originalSize = meta.indices().size();

        // Act
        ImmutableSet<Meta.Index> filtered = meta.indices(index ->
            index.name().equals("index1")
        );

        // Assert
        assertThat(filtered, hasSize(1));
        assertThat(meta.indices().size(), is(originalSize));
    }

    // Tests for aliases(Predicate<Alias>)

    @Test
    public void testAliasesWithPredicate_filtersAliasesByNamePrefix() {
        Meta meta = Meta.Mock.alias("test-alias-1").of("index1")
            .alias("test-alias-2").of("index2")
            .alias("prod-alias-1").of("index3");

        ImmutableSet<Meta.Alias> result = meta.aliases(alias ->
            alias.name().startsWith("test")
        );

        assertThat(result, hasSize(2));
        assertThat(result.stream().anyMatch(alias -> alias.name().equals("test-alias-1")), is(true));
        assertThat(result.stream().anyMatch(alias -> alias.name().equals("test-alias-2")), is(true));
        assertThat(result.stream().anyMatch(alias -> alias.name().equals("prod-alias-1")), is(false));
    }

    @Test
    public void testAliasesWithPredicate_returnsSingleMatchingAlias() {
        Meta meta = Meta.Mock.alias("alias1").of("index1")
            .alias("alias2").of("index2")
            .alias("alias3").of("index3");

        ImmutableSet<Meta.Alias> result = meta.aliases(alias ->
            alias.name().equals("alias2")
        );

        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().name(), is("alias2"));
    }

    @Test
    public void testAliasesWithPredicate_returnsAllAliasesWhenAllMatch() {
        Meta meta = Meta.Mock.alias("alias1").of("index1")
            .alias("alias2").of("index2")
            .alias("alias3").of("index3");

        ImmutableSet<Meta.Alias> result = meta.aliases(alias -> true);

        assertThat(result, hasSize(3));
    }

    @Test
    public void testAliasesWithPredicate_returnsEmptySetWhenNoneMatch() {
        Meta meta = Meta.Mock.alias("alias1").of("index1")
            .alias("alias2").of("index2")
            .alias("alias3").of("index3");

        ImmutableSet<Meta.Alias> result = meta.aliases(alias -> false);

        assertThat(result, notNullValue());
        assertThat(result, hasSize(0));
        assertThat(result.isEmpty(), is(true));
    }

    @Test
    public void testAliasesWithPredicate_returnsEmptySetWhenNoAliasesExist() {
        Meta meta = Meta.Mock.indices("index1", "index2");

        ImmutableSet<Meta.Alias> result = meta.aliases(alias -> true);

        assertThat(result, notNullValue());
        assertThat(result, hasSize(0));
    }

    @Test
    public void testAliasesWithPredicate_throwsNPEWhenPredicateIsNull() {
        Meta meta = Meta.Mock.alias("alias1").of("index1")
            .alias("alias2").of("index2");

        assertThatThrown(() -> meta.aliases(null), instanceOf(NullPointerException.class));
    }

    @Test
    public void testAliasesWithPredicate_filtersAliasesByComplexCondition() {
        Meta meta = Meta.Mock.alias("logs-alias-2024-01-01").of("index1")
            .alias("logs-alias-2024-01-02").of("index2")
            .alias("metrics-alias-2024-01-01").of("index3")
            .alias("traces-alias-2024-01-01").of("index4");

        ImmutableSet<Meta.Alias> result = meta.aliases(alias ->
            alias.name().startsWith("logs") && alias.name().contains("01-01")
        );

        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().name(), is("logs-alias-2024-01-01"));
    }

    @Test
    public void testAliasesWithPredicate_doesNotModifyOriginalAliasesSet() {
        Meta meta = Meta.Mock.alias("alias1").of("index1")
            .alias("alias2").of("index2")
            .alias("alias3").of("index3");
        int originalSize = meta.aliases().size();

        ImmutableSet<Meta.Alias> filtered = meta.aliases(alias ->
            alias.name().equals("alias1")
        );

        assertThat(filtered, hasSize(1));
        assertThat(meta.aliases().size(), is(originalSize));
    }

    // Tests for dataStreams(Predicate<DataStream>)

    @Test
    public void testDataStreamsWithPredicate_filtersDataStreamsByNamePrefix() {
        Meta meta = Meta.Mock.dataStream("test-stream-1").of("index1")
            .dataStream("test-stream-2").of("index2")
            .dataStream("prod-stream-1").of("index3");

        ImmutableSet<Meta.DataStream> result = meta.dataStreams(dataStream ->
            dataStream.name().startsWith("test")
        );

        assertThat(result, hasSize(2));
        assertThat(result.stream().anyMatch(ds -> ds.name().equals("test-stream-1")), is(true));
        assertThat(result.stream().anyMatch(ds -> ds.name().equals("test-stream-2")), is(true));
        assertThat(result.stream().anyMatch(ds -> ds.name().equals("prod-stream-1")), is(false));
    }

    @Test
    public void testDataStreamsWithPredicate_returnsSingleMatchingDataStream() {
        Meta meta = Meta.Mock.dataStream("stream1").of("index1")
            .dataStream("stream2").of("index2")
            .dataStream("stream3").of("index3");

        ImmutableSet<Meta.DataStream> result = meta.dataStreams(dataStream ->
            dataStream.name().equals("stream2")
        );

        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().name(), is("stream2"));
    }

    @Test
    public void testDataStreamsWithPredicate_returnsAllDataStreamsWhenAllMatch() {
        Meta meta = Meta.Mock.dataStream("stream1").of("index1")
            .dataStream("stream2").of("index2")
            .dataStream("stream3").of("index3");

        ImmutableSet<Meta.DataStream> result = meta.dataStreams(dataStream -> true);

        assertThat(result, hasSize(3));
    }

    @Test
    public void testDataStreamsWithPredicate_returnsEmptySetWhenNoneMatch() {
        Meta meta = Meta.Mock.dataStream("stream1").of("index1")
            .dataStream("stream2").of("index2")
            .dataStream("stream3").of("index3");

        ImmutableSet<Meta.DataStream> result = meta.dataStreams(dataStream -> false);

        assertThat(result, notNullValue());
        assertThat(result, hasSize(0));
        assertThat(result.isEmpty(), is(true));
    }

    @Test
    public void testDataStreamsWithPredicate_returnsEmptySetWhenNoDataStreamsExist() {
        Meta meta = Meta.Mock.indices("index1", "index2");

        ImmutableSet<Meta.DataStream> result = meta.dataStreams(dataStream -> true);

        assertThat(result, notNullValue());
        assertThat(result, hasSize(0));
    }

    @Test
    public void testDataStreamsWithPredicate_throwsNPEWhenPredicateIsNull() {
        Meta meta = Meta.Mock.dataStream("stream1").of("index1")
            .dataStream("stream2").of("index2");

        assertThatThrown(() -> meta.dataStreams(null), instanceOf(NullPointerException.class));
    }

    @Test
    public void testDataStreamsWithPredicate_filtersDataStreamsByComplexCondition() {
        Meta meta = Meta.Mock.dataStream("logs-2024-01-01").of("index1")
            .dataStream("logs-2024-01-02").of("index2")
            .dataStream("metrics-2024-01-01").of("index3")
            .dataStream("traces-2024-01-01").of("index4");

        ImmutableSet<Meta.DataStream> result = meta.dataStreams(dataStream ->
            dataStream.name().startsWith("logs") && dataStream.name().contains("01-01")
        );

        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().name(), is("logs-2024-01-01"));
    }

    @Test
    public void testDataStreamsWithPredicate_doesNotModifyOriginalDataStreamsSet() {
        Meta meta = Meta.Mock.dataStream("stream1").of("index1")
            .dataStream("stream2").of("index2")
            .dataStream("stream3").of("index3");
        int originalSize = meta.dataStreams().size();

        ImmutableSet<Meta.DataStream> filtered = meta.dataStreams(dataStream ->
            dataStream.name().equals("stream1")
        );

        assertThat(filtered, hasSize(1));
        assertThat(meta.dataStreams().size(), is(originalSize));
    }
}