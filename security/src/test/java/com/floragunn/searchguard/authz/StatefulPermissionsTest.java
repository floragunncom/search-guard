/*
 * Copyright 2025 floragunn GmbH
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

package com.floragunn.searchguard.authz;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.RoleBasedActionAuthorization.StatefulPermissions;
import com.floragunn.searchsupport.meta.Component;
import com.floragunn.searchsupport.meta.Meta;
import com.floragunn.searchsupport.meta.Meta.IndexLikeObject;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for RoleBasedActionAuthorization.StatefulPermissions
 */
public class StatefulPermissionsTest {

    @Test
    public void covers_shouldReturnTrue_whenAllIndexLikeObjectsExistAndMatch() {
        // Create mock metadata with indices
        Meta meta = Meta.Mock.indices("index1", "index2", "index3");

        // Get actual IndexLikeObject instances from metadata
        IndexLikeObject index1 = meta.getIndexOrLike("index1");
        IndexLikeObject index2 = meta.getIndexOrLike("index2");

        // Create StatefulPermissions
        StatefulPermissions permissions =
            new StatefulPermissions(null, null, null, meta);

        // Test: All objects exist and match
        Set<IndexLikeObject> indexLikeObjects = ImmutableSet.of(index1, index2);
        boolean result = permissions.covers(indexLikeObjects);

        assertThat(result, is(true));
    }

    @Test
    public void covers_shouldReturnFalse_whenIndexLikeObjectDoesNotExist() {
        // Create mock metadata with some indices
        Meta meta = Meta.Mock.indices("index1", "index2");

        // Create a non-existent index
        IndexLikeObject nonExistentIndex = Meta.Index.nonExistent("nonexistent-index");

        // Create StatefulPermissions
        StatefulPermissions permissions =
            new StatefulPermissions(null, null, null, meta);

        // Test: Should return false because the index doesn't exist
        Set<IndexLikeObject> indexLikeObjects = ImmutableSet.of(nonExistentIndex);
        boolean result = permissions.covers(indexLikeObjects);

        assertThat(result, is(false));
    }

    @Test
    public void covers_shouldReturnFalse_whenIndexLikeObjectHasComponentSelector() {
        // Create mock metadata
        Meta meta = Meta.Mock.indices("index1", "index2");

        // Create a mock IndexLikeObject with component selector
        IndexLikeObject indexWithComponent = mock(IndexLikeObject.class);
        when(indexWithComponent.exists()).thenReturn(true);
        when(indexWithComponent.component()).thenReturn(Component.FAILURES);
        when(indexWithComponent.name()).thenReturn("index1");

        // Create StatefulPermissions
        StatefulPermissions permissions =
            new StatefulPermissions(null, null, null, meta);

        // Test: Should return false because the index has a component selector
        Set<IndexLikeObject> indexLikeObjects = ImmutableSet.of(indexWithComponent);
        boolean result = permissions.covers(indexLikeObjects);

        assertThat(result, is(false));
    }

    @Test
    public void covers_shouldReturnFalse_whenMetadataDoesNotContainIndexLikeObject() {
        // Create two separate Meta instances
        Meta meta1 = Meta.Mock.indices("index1", "index2");
        Meta meta2 = Meta.Mock.indices("index3", "index4");

        // Get an index from meta2
        IndexLikeObject index3 = meta2.getIndexOrLike("index3");

        // Create StatefulPermissions with meta1
        StatefulPermissions permissions =
            new StatefulPermissions(null, null, null, meta1);

        // Test: Should return false because meta1 doesn't contain index3
        Set<IndexLikeObject> indexLikeObjects = ImmutableSet.of(index3);
        boolean result = permissions.covers(indexLikeObjects);

        assertThat(result, is(false));
    }

    @Test
    public void covers_shouldReturnFalse_whenIndexLikeObjectIsNotEqualToMetadataVersion() {
        // Create mock metadata
        Meta meta = Meta.Mock.indices("index1");

        // Create a mock IndexLikeObject with same name but different equality
        IndexLikeObject differentIndex = mock(IndexLikeObject.class);
        when(differentIndex.exists()).thenReturn(true);
        when(differentIndex.component()).thenReturn(Component.NONE);
        when(differentIndex.name()).thenReturn("index1");
        // The equals() check will fail because it's a different object

        // Create StatefulPermissions
        StatefulPermissions permissions = new StatefulPermissions(null, null, null, meta);

        // Test: Should return false because the objects are not equal
        Set<IndexLikeObject> indexLikeObjects = ImmutableSet.of(differentIndex);
        boolean result = permissions.covers(indexLikeObjects);

        assertThat(result, is(false));
    }

    @Test
    public void covers_shouldReturnTrue_whenEmptySetProvided() {
        // Create mock metadata
        Meta meta = Meta.Mock.indices("index1", "index2");

        // Create StatefulPermissions
        StatefulPermissions permissions =
            new StatefulPermissions(null, null, null, meta);

        // Test: Empty set should return true (no objects to check)
        Set<IndexLikeObject> emptySet = ImmutableSet.empty();
        boolean result = permissions.covers(emptySet);

        assertThat(result, is(true));
    }

    @Test
    public void covers_shouldReturnTrue_whenAllAliasesExistAndMatch() {
        // Create mock metadata with alias
        Meta meta = Meta.Mock.alias("my-alias").of("index1", "index2");

        // Get the alias from metadata
        IndexLikeObject alias = meta.getIndexOrLike("my-alias");

        // Create StatefulPermissions
        StatefulPermissions permissions =
            new StatefulPermissions(null, null, null, meta);

        // Test: Alias exists and matches
        Set<IndexLikeObject> indexLikeObjects = ImmutableSet.of(alias);
        boolean result = permissions.covers(indexLikeObjects);

        assertThat(result, is(true));
    }

    @Test
    public void covers_shouldReturnTrue_whenMixOfIndicesAndAliasesAllMatch() {
        // Create mock metadata with both indices and aliases
        Meta meta = Meta.Mock.alias("my-alias").of("index1", "index2");

        // Get both indices and alias
        IndexLikeObject index1 = meta.getIndexOrLike("index1");
        IndexLikeObject alias = meta.getIndexOrLike("my-alias");

        // Create StatefulPermissions
        StatefulPermissions permissions =
            new StatefulPermissions(null, null, null, meta);

        // Test: Mix of indices and aliases, all exist and match
        Set<IndexLikeObject> indexLikeObjects = ImmutableSet.of(index1, alias);
        boolean result = permissions.covers(indexLikeObjects);

        assertThat(result, is(true));
    }

    @Test
    public void covers_shouldReturnFalse_whenOneOfMultipleObjectsDoesNotExist() {
        // Create mock metadata
        Meta meta = Meta.Mock.indices("index1", "index2");

        // Get an existing index and create a non-existent one
        IndexLikeObject existingIndex = meta.getIndexOrLike("index1");
        IndexLikeObject nonExistentIndex = Meta.Index.nonExistent("nonexistent");

        // Create StatefulPermissions
        StatefulPermissions permissions =
            new StatefulPermissions(null, null, null, meta);

        // Test: Should return false because one object doesn't exist
        Set<IndexLikeObject> indexLikeObjects = ImmutableSet.of(existingIndex, nonExistentIndex);
        boolean result = permissions.covers(indexLikeObjects);

        assertThat(result, is(false));
    }

    @Test
    public void covers_shouldReturnFalse_whenOneOfMultipleObjectsHasComponentSelector() {
        // Create mock metadata
        Meta meta = Meta.Mock.indices("index1", "index2");

        // Get an existing index
        IndexLikeObject existingIndex = meta.getIndexOrLike("index1");

        // Create a mock IndexLikeObject with component selector
        IndexLikeObject indexWithComponent = mock(IndexLikeObject.class);
        when(indexWithComponent.exists()).thenReturn(true);
        when(indexWithComponent.component()).thenReturn(Component.FAILURES);

        // Create StatefulPermissions
        StatefulPermissions permissions =
            new StatefulPermissions(null, null, null, meta);

        // Test: Should return false because one object has a component selector
        Set<IndexLikeObject> indexLikeObjects = ImmutableSet.of(existingIndex, indexWithComponent);
        boolean result = permissions.covers(indexLikeObjects);

        assertThat(result, is(false));
    }

    @Test
    public void covers_shouldReturnTrue_whenDataStreamExistsAndMatches() {
        // Create mock metadata with data stream
        Meta meta = Meta.Mock.dataStream("my-stream").of("index1", "index2");

        // Get the data stream from metadata
        IndexLikeObject dataStream = meta.getIndexOrLike("my-stream");

        // Create StatefulPermissions
        StatefulPermissions permissions =
            new StatefulPermissions(null, null, null, meta);

        // Test: Data stream exists and matches
        Set<IndexLikeObject> indexLikeObjects = ImmutableSet.of(dataStream);
        boolean result = permissions.covers(indexLikeObjects);

        assertThat(result, is(true));
    }

    @Test
    public void covers_shouldReturnFalse_whenDataStreamDoesNotExist() {
        // Create mock metadata
        Meta meta = Meta.Mock.indices("index1", "index2");

        // Create a non-existent data stream
        IndexLikeObject nonExistentDataStream = Meta.DataStream.nonExistent("nonexistent-stream");

        // Create StatefulPermissions
        StatefulPermissions permissions =
            new StatefulPermissions(null, null, null, meta);

        // Test: Should return false because the data stream doesn't exist
        Set<IndexLikeObject> indexLikeObjects = ImmutableSet.of(nonExistentDataStream);
        boolean result = permissions.covers(indexLikeObjects);

        assertThat(result, is(false));
    }
}