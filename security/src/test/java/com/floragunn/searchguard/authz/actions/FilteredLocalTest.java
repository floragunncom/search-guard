/*
 * Copyright 2015-2024 floragunn GmbH
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

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.actions.ResolvedIndices.FilteredLocal;
import com.floragunn.searchsupport.meta.Meta;
import com.floragunn.searchsupport.meta.Meta.IndexLikeObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Predicates.alwaysFalse;
import static com.google.common.base.Predicates.alwaysTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FilteredLocalTest {

    @Mock
    private ResolvedIndices.Local mockLocal;

    private ImmutableSet<Meta.Index> pureIndices;
    private ImmutableSet<Meta.Alias> aliases;
    private ImmutableSet<Meta.DataStream> dataStreams;
    private ImmutableSet<Meta.NonExistent> nonExistingIndices;
    private ImmutableSet<IndexLikeObject> union;

    @Before
    public void setUp() {
        // Create test data
        Meta.Index index1 = mock(Meta.Index.class);
        when(index1.name()).thenReturn("index1");
        Meta.Index index2 = mock(Meta.Index.class);
        when(index2.name()).thenReturn("index2");
        Meta.Index index3 = mock(Meta.Index.class);
        when(index3.name()).thenReturn("index3");

        Meta.Alias alias1 = mock(Meta.Alias.class);
        when(alias1.name()).thenReturn("alias1");
        Meta.Alias alias2 = mock(Meta.Alias.class);
        when(alias2.name()).thenReturn("alias2");

        Meta.DataStream dataStream1 = mock(Meta.DataStream.class);
        when(dataStream1.name()).thenReturn("dataStream1");
        Meta.DataStream dataStream2 = mock(Meta.DataStream.class);
        when(dataStream2.name()).thenReturn("dataStream2");

        Meta.NonExistent nonExistent1 = mock(Meta.NonExistent.class);
        when(nonExistent1.name()).thenReturn("nonExistent1");

        pureIndices = ImmutableSet.of(index1, index2, index3);
        aliases = ImmutableSet.of(alias1, alias2);
        dataStreams = ImmutableSet.of(dataStream1, dataStream2);
        nonExistingIndices = ImmutableSet.of(nonExistent1);
        union = ImmutableSet.<IndexLikeObject>of(pureIndices).with(aliases).with(dataStreams).with(nonExistingIndices);
    }

    @Test
    public void isEmpty_shouldReturnTrue_whenUnionIsEmpty() {
        when(mockLocal.getUnion()).thenReturn(ImmutableSet.empty());
        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, alwaysTrue());

        boolean result = filteredLocal.isEmpty();

        assertThat(result, is(true));
        verify(mockLocal).getUnion();
    }

    @Test
    public void isEmpty_shouldReturnFalse_whenUnionIsNotEmpty() {
        ImmutableSet<IndexLikeObject> nonEmptyUnion = ImmutableSet.of(mock(Meta.Index.class));
        when(mockLocal.getUnion()).thenReturn(nonEmptyUnion);
        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, alwaysTrue());

        boolean result = filteredLocal.isEmpty();

        assertThat(result, is(false));
        verify(mockLocal).getUnion();
    }

    @Test
    public void size_shouldReturnSizeOfFilteredUnion() {
        when(mockLocal.getUnion()).thenReturn(union);
        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, alwaysTrue());

        int result = filteredLocal.size();

        assertThat(result, is(union.size()));
        verify(mockLocal).getUnion();
    }

    @Test
    public void size_shouldReturnZero_whenAllElementsAreFilteredOut() {
        when(mockLocal.getUnion()).thenReturn(union);
        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, alwaysFalse());

        int result = filteredLocal.size();

        assertThat(result, is(0));
        verify(mockLocal).getUnion();
    }

    @Test
    public void getPureIndices_shouldReturnFilteredIndices_whenPredicateMatchesSome() {
        when(mockLocal.getPureIndices()).thenReturn(pureIndices);
        Predicate<IndexLikeObject> predicate = indexLike -> indexLike.name().equals("index1") || indexLike.name().equals("index3");
        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, predicate);

        ImmutableSet<Meta.Index> result = filteredLocal.getPureIndices();

        assertThat(indexLikeToNames(result), containsInAnyOrder("index1", "index3"));
        verify(mockLocal).getPureIndices();
    }

    @Test
    public void getPureIndices_shouldReturnEmptySet_whenPredicateMatchesNone() {
        when(mockLocal.getPureIndices()).thenReturn(pureIndices);

        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, alwaysFalse());

        ImmutableSet<Meta.Index> result = filteredLocal.getPureIndices();

        assertThat(result, is(empty()));
        verify(mockLocal).getPureIndices();
    }

    @Test
    public void getPureIndices_shouldReturnAllIndices_whenPredicateMatchesAll() {
        when(mockLocal.getPureIndices()).thenReturn(pureIndices);
        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, alwaysTrue());

        ImmutableSet<Meta.Index> result = filteredLocal.getPureIndices();

        assertThat(result.size(), is(pureIndices.size()));
        verify(mockLocal).getPureIndices();
    }

    @Test
    public void getAliases_shouldReturnFilteredAliases_whenPredicateMatchesSome() {
        when(mockLocal.getAliases()).thenReturn(aliases);
        Predicate<IndexLikeObject> predicate = indexLike -> indexLike.name().equals("alias1");
        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, predicate);

        ImmutableSet<Meta.Alias> result = filteredLocal.getAliases();

        assertThat(indexLikeToNames(result), containsInAnyOrder("alias1"));
        verify(mockLocal).getAliases();
    }

    @Test
    public void getAliases_shouldReturnEmptySet_whenPredicateMatchesNone() {
        when(mockLocal.getAliases()).thenReturn(aliases);
        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, alwaysFalse());

        ImmutableSet<Meta.Alias> result = filteredLocal.getAliases();

        assertThat(result, is(empty()));
        verify(mockLocal).getAliases();
    }

    @Test
    public void getAliases_shouldReturnAllAliases_whenPredicateMatchesAll() {
        when(mockLocal.getAliases()).thenReturn(aliases);
        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, alwaysTrue());

        ImmutableSet<Meta.Alias> result = filteredLocal.getAliases();

        assertThat(result.size(), is(aliases.size()));
        verify(mockLocal).getAliases();
    }

    @Test
    public void getDataStreams_shouldReturnFilteredDataStreams_whenPredicateMatchesSome() {
        when(mockLocal.getDataStreams()).thenReturn(dataStreams);
        Predicate<IndexLikeObject> predicate = indexLike -> indexLike.name().equals("dataStream1");
        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, predicate);

        ImmutableSet<Meta.DataStream> result = filteredLocal.getDataStreams();

        assertThat(indexLikeToNames(result), containsInAnyOrder("dataStream1"));
        verify(mockLocal).getDataStreams();
    }

    @Test
    public void getDataStreams_shouldReturnEmptySet_whenPredicateMatchesNone() {
        when(mockLocal.getDataStreams()).thenReturn(dataStreams);
        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, alwaysFalse());

        ImmutableSet<Meta.DataStream> result = filteredLocal.getDataStreams();

        assertThat(result, is(empty()));
        verify(mockLocal).getDataStreams();
    }

    @Test
    public void getDataStreams_shouldReturnAllDataStreams_whenPredicateMatchesAll() {
        when(mockLocal.getDataStreams()).thenReturn(dataStreams);
        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, alwaysTrue());

        ImmutableSet<Meta.DataStream> result = filteredLocal.getDataStreams();

        assertThat(result.size(), is(dataStreams.size()));
        verify(mockLocal).getDataStreams();
    }

    @Test
    public void getNonExistingIndices_shouldReturnFilteredNonExistingIndices_whenPredicateMatchesSome() {
        when(mockLocal.getNonExistingIndices()).thenReturn(nonExistingIndices);
        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, alwaysTrue());

        ImmutableSet<Meta.NonExistent> result = filteredLocal.getNonExistingIndices();

        assertThat(indexLikeToNames(result), containsInAnyOrder("nonExistent1"));
        verify(mockLocal).getNonExistingIndices();
    }

    @Test
    public void getNonExistingIndices_shouldReturnEmptySet_whenPredicateMatchesNone() {
        when(mockLocal.getNonExistingIndices()).thenReturn(nonExistingIndices);
        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, alwaysFalse());

        ImmutableSet<Meta.NonExistent> result = filteredLocal.getNonExistingIndices();

        assertThat(result, is(empty()));
        verify(mockLocal).getNonExistingIndices();
    }

    @Test
    public void getNonExistingIndices_shouldReturnAllNonExistingIndices_whenPredicateMatchesAll() {
        when(mockLocal.getNonExistingIndices()).thenReturn(nonExistingIndices);
        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, alwaysTrue());

        ImmutableSet<Meta.NonExistent> result = filteredLocal.getNonExistingIndices();

        assertThat(result.size(), is(nonExistingIndices.size()));
        verify(mockLocal).getNonExistingIndices();
    }

    @Test
    public void getUnion_shouldReturnFilteredUnion_whenPredicateMatchesSome() {
        when(mockLocal.getUnion()).thenReturn(union);
        Predicate<IndexLikeObject> predicate = indexLike -> indexLike.name().equals("index1") || indexLike.name().equals("alias1");
        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, predicate);

        ImmutableSet<IndexLikeObject> result = filteredLocal.getUnion();

        assertThat(indexLikeToNames(result), containsInAnyOrder("index1", "alias1"));
        verify(mockLocal, times(1)).getUnion();
    }

    @Test
    public void getUnion_shouldReturnEmptySet_whenPredicateMatchesNone() {
        when(mockLocal.getUnion()).thenReturn(union);
        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, alwaysFalse());

        ImmutableSet<IndexLikeObject> result = filteredLocal.getUnion();

        assertThat(result, is(empty()));
        verify(mockLocal).getUnion();
    }

    @Test
    public void getUnion_shouldReturnAllElements_whenPredicateMatchesAll() {
        when(mockLocal.getUnion()).thenReturn(union);
        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, alwaysTrue());

        ImmutableSet<IndexLikeObject> result = filteredLocal.getUnion();

        assertThat(result.size(), is(union.size()));
        verify(mockLocal).getUnion();
    }

    @Test
    public void getUnion_shouldNotCallGetUnion_whenIsEmptyNotCalled() {
        new FilteredLocal(mockLocal, alwaysTrue());

        verify(mockLocal, never()).getUnion();
    }

    @Test
    public void multipleMethodCalls_shouldUsePredicateConsistently() {
        when(mockLocal.getPureIndices()).thenReturn(pureIndices);
        when(mockLocal.getAliases()).thenReturn(aliases);
        when(mockLocal.getDataStreams()).thenReturn(dataStreams);
        when(mockLocal.getNonExistingIndices()).thenReturn(nonExistingIndices);

        Predicate<IndexLikeObject> predicate = indexLike -> indexLike.name().startsWith("index") || indexLike.name().startsWith("alias");

        FilteredLocal filteredLocal = new FilteredLocal(mockLocal, predicate);

        ImmutableSet<Meta.Index> indices = filteredLocal.getPureIndices();
        ImmutableSet<Meta.Alias> resultAliases = filteredLocal.getAliases();
        ImmutableSet<Meta.DataStream> resultDataStreams = filteredLocal.getDataStreams();
        ImmutableSet<Meta.NonExistent> resultNonExisting = filteredLocal.getNonExistingIndices();

        assertThat(indices.size(), is(3)); // all indices match
        assertThat(resultAliases.size(), is(2)); // all aliases match
        assertThat(resultDataStreams.size(), is(0)); // no dataStreams match
        assertThat(resultNonExisting.size(), is(0)); // no nonExistent match
    }

    private static List<String> indexLikeToNames(Collection<? extends Meta.IndexLikeObject> indexLikeCollection) {
        return indexLikeCollection.stream().map(IndexLikeObject::name).collect(Collectors.toList());
    }
}