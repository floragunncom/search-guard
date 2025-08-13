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

package com.floragunn.searchguard.authz.actions;

import static com.floragunn.searchguard.authz.actions.ResolvedIndicesMatcher.hasIndices;
import static com.floragunn.searchguard.authz.actions.ResolvedIndicesMatcher.hasNoIndices;
import static com.floragunn.searchsupport.meta.Meta.Mock.indices;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.floragunn.fluent.collections.ImmutableSet;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.hamcrest.Matcher;
import org.junit.Test;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.authz.SystemIndexAccess;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.IndicesRequestInfo;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.IndicesRequestInfo.Scope;
import com.floragunn.searchsupport.meta.Meta;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RunWith(MockitoJUnitRunner.class)
public class ResolvedIndicesTest {

    static final Meta META = indices("index_a11", "index_a12", "index_a21", "index_a22", "index_b1", "index_b2")//
            .dataStream("ds_d11").of(".ds-ds_d11-2024.03.22-000001", ".ds-ds_d11-2024.03.22-000002")//
            .dataStream("ds_d12").of(".ds-ds_d12-2024.03.22-000001", ".ds-ds_d12-2024.03.22-000002")//
            .alias("alias_a").of("index_a11", "index_a12", "index_a21", "index_a22")//
            .alias("alias_a1").of("index_a11", "index_a12")//
            .alias("alias_a2").of("index_a21", "index_a22")//
            .alias("alias_b").of("index_b1", "index_b2");

    static final String NAME_SYSTEM_INDEX_1 = "system_index_1";
    static final String NAME_SYSTEM_INDEX_2 = "system_index_2";
    static final String NAME_SYSTEM_INDEX_3 = "system_index_3";
    static final String NAME_REGULAR_INDEX_1 = "regular_index_1";
    static final String NAME_REGULAR_INDEX_2 = "regular_index_2";

    @Mock
    SystemIndexAccess systemIndexAccess;

    @Test
    public void negationAcrossObjectTypes() {
        ResolvedIndices subject = get(IndicesOptions.LENIENT_EXPAND_OPEN, Scope.ANY, "ds_d1*", "-.ds-ds_d11*");
        assertThat(subject, hasNoIndices().hasNoAliases().hasDataStreams("ds_d12"));
    }

    @Test
    public void negationAcrossObjectTypes2() {
        ResolvedIndices subject = get(IndicesOptions.LENIENT_EXPAND_OPEN, Scope.ANY, "ds_d1*", "-.ds-ds_d11-2024.03.22-000001");
        assertThat(subject, hasIndices(".ds-ds_d11-2024.03.22-000002").hasNoAliases().hasDataStreams("ds_d12"));
    }

    @Test
    public void shouldExcludeSystemIndicesDuringResolveAll() {
        IndexMetadata systemIndex = createIndexMetadata(NAME_SYSTEM_INDEX_1, true);
        IndexMetadata regularIndex = createIndexMetadata(NAME_REGULAR_INDEX_1, false);
        Meta meta = metaForIndexMetadata(systemIndex, regularIndex);
        IndicesRequestInfo indicesRequestInfo = new IndicesRequestInfo(ImmutableList.of("*"),
            IndicesOptions.LENIENT_EXPAND_OPEN,
            Scope.ANY,
            SystemIndexAccess.DISALLOWED,
            meta);

        ResolvedIndices.Local local = getLocalResolver(indicesRequestInfo);

        assertLocalIndices(local, contains(NAME_REGULAR_INDEX_1));
    }

    @Test
    public void shouldIncludeSystemIndicesDuringResolveAll() {
        IndexMetadata systemIndex = createIndexMetadata(NAME_SYSTEM_INDEX_1, true);
        IndexMetadata regularIndex = createIndexMetadata(NAME_REGULAR_INDEX_1, false);
        Meta meta = metaForIndexMetadata(systemIndex, regularIndex);
        when(systemIndexAccess.isAllowed(Mockito.<Meta.IndexLikeObject>any())).thenReturn(true);
        IndicesRequestInfo indicesRequestInfo = new IndicesRequestInfo(ImmutableList.of("*"),
            IndicesOptions.LENIENT_EXPAND_OPEN,
            Scope.ANY,
            systemIndexAccess,
            meta);

        ResolvedIndices.Local local = getLocalResolver(indicesRequestInfo);

        assertLocalIndices(local, containsInAnyOrder(NAME_REGULAR_INDEX_1, NAME_SYSTEM_INDEX_1));
    }

    @Test
    public void shouldExcludeAllSystemIndicesDuringResolveLocal() {
        IndexMetadata systemIndex1 = createIndexMetadata(NAME_SYSTEM_INDEX_1, true);
        IndexMetadata systemIndex2 = createIndexMetadata(NAME_SYSTEM_INDEX_2, true);
        IndexMetadata systemIndex3 = createIndexMetadata(NAME_SYSTEM_INDEX_3, true);
        Meta meta = metaForIndexMetadata(systemIndex1, systemIndex2, systemIndex3);
        IndicesRequestInfo indicesRequestInfo = new IndicesRequestInfo(ImmutableList.of("*"),
            IndicesOptions.LENIENT_EXPAND_OPEN,
            Scope.ANY,
            SystemIndexAccess.DISALLOWED,
            meta);

        ResolvedIndices.Local local = getLocalResolver(indicesRequestInfo);

        assertLocalIndices(local, emptyIterable());
    }

    @Test
    public void shouldFilterOutSystemIndicesDuringResolveLocal() {
        IndexMetadata regularIndex1 = createIndexMetadata(NAME_REGULAR_INDEX_1, false);
        IndexMetadata regularIndex2 = createIndexMetadata(NAME_REGULAR_INDEX_2, false);
        IndexMetadata systemIndex1 = createIndexMetadata(NAME_SYSTEM_INDEX_1, true);
        IndexMetadata systemIndex2 = createIndexMetadata(NAME_SYSTEM_INDEX_2, true);
        IndexMetadata systemIndex3 = createIndexMetadata(NAME_SYSTEM_INDEX_3, true);
        Meta meta = metaForIndexMetadata(regularIndex1, systemIndex1, systemIndex2, regularIndex2, systemIndex3);
        IndicesRequestInfo indicesRequestInfo = new IndicesRequestInfo(ImmutableList.of("*"),
            IndicesOptions.LENIENT_EXPAND_OPEN,
            Scope.ANY,
            SystemIndexAccess.DISALLOWED,
            meta);

        ResolvedIndices.Local local = getLocalResolver(indicesRequestInfo);

        assertLocalIndices(local, containsInAnyOrder(NAME_REGULAR_INDEX_1, NAME_REGULAR_INDEX_2));
    }

    @Test
    public void shouldIncludeNonSystemIndicesDuringResolveLocal() {
        IndexMetadata regularIndex1 = createIndexMetadata(NAME_REGULAR_INDEX_1, false);
        IndexMetadata regularIndex2 = createIndexMetadata(NAME_REGULAR_INDEX_2, false);
        Meta meta = metaForIndexMetadata(regularIndex1, regularIndex2);
        IndicesRequestInfo indicesRequestInfo = new IndicesRequestInfo(ImmutableList.of("*"),
            IndicesOptions.LENIENT_EXPAND_OPEN,
            Scope.ANY,
            SystemIndexAccess.DISALLOWED,
            meta);

        ResolvedIndices.Local local = getLocalResolver(indicesRequestInfo);

        assertLocalIndices(local, containsInAnyOrder(NAME_REGULAR_INDEX_1, NAME_REGULAR_INDEX_2));
    }


    private static void assertLocalIndices(ResolvedIndices.Local local, Matcher<Iterable<? extends String>> expectedLocalIndexNames) {
        List<String> indexNames = local
            .getPureIndices() //
            .stream() //
            .map(Meta.IndexLikeObject::name) //
            .collect(Collectors.toList());
        assertThat(indexNames, expectedLocalIndexNames);
    }

    private static ResolvedIndices.Local getLocalResolver(IndicesRequestInfo indicesRequestInfo) {
        ResolvedIndices resolvedIndices = new ResolvedIndices(true, ResolvedIndices.Local.EMPTY, ImmutableSet.empty(), ImmutableSet.of(
            indicesRequestInfo));

        ResolvedIndices.Local local = resolvedIndices.getLocal();
        return local;
    }

    private static Meta metaForIndexMetadata(IndexMetadata...indexMetadata) {
        Metadata esMetadata = mock(Metadata.class, Mockito.RETURNS_DEEP_STUBS);
        ImmutableOpenMap<String, IndexMetadata> indicesMetadataMap = createIndexMetaMap(indexMetadata);
        ProjectMetadata project = esMetadata.getProject(Metadata.DEFAULT_PROJECT_ID);
        when(project.indices()).thenReturn(indicesMetadataMap);
        when(project.indices()).thenReturn(indicesMetadataMap);
        return Meta.from(esMetadata);
    }

    private static ImmutableOpenMap<String, IndexMetadata> createIndexMetaMap(IndexMetadata...indexMetadata) {
        ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder();
        Arrays.stream(indexMetadata).forEach(index -> builder.put(index.getIndex().getName(), index));
        return builder.build();
    }

    private static IndexMetadata createIndexMetadata(String indexName, boolean system) {
        IndexMetadata systemIndex = IndexMetadata.builder(indexName) //
            .settings(Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.V_7_17_1).build()) //
            .numberOfReplicas(0) //
            .numberOfShards(1) //
            .system(system) //
            .build();
        return systemIndex;
    }

    private static ResolvedIndices get(IndicesOptions indicesOptions, Scope scope, String... indices) {
        IndicesRequestInfo indicesRequestInfo = new IndicesRequestInfo(ImmutableList.ofArray(indices), indicesOptions, scope,
                SystemIndexAccess.DISALLOWED, META);
        return indicesRequestInfo.resolveIndices();
    }
}
