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

import org.elasticsearch.action.support.IndicesOptions;
import org.junit.Test;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.authz.SystemIndexAccess;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.IndicesRequestInfo;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.IndicesRequestInfo.Scope;
import com.floragunn.searchsupport.meta.Meta;

public class ResolvedIndicesTest {

    static final Meta META = indices("index_a11", "index_a12", "index_a21", "index_a22", "index_b1", "index_b2")//
            .dataStream("ds_d11").of(".ds-ds_d11-2024.03.22-000001", ".ds-ds_d11-2024.03.22-000002")//
            .dataStream("ds_d12").of(".ds-ds_d12-2024.03.22-000001", ".ds-ds_d12-2024.03.22-000002")//
            .alias("alias_a").of("index_a11", "index_a12", "index_a21", "index_a22")//
            .alias("alias_a1").of("index_a11", "index_a12")//
            .alias("alias_a2").of("index_a21", "index_a22")//
            .alias("alias_b").of("index_b1", "index_b2");

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

    private static ResolvedIndices get(IndicesOptions indicesOptions, Scope scope, String... indices) {
        IndicesRequestInfo indicesRequestInfo = new IndicesRequestInfo(ImmutableList.ofArray(indices), indicesOptions, scope,
                SystemIndexAccess.DISALLOWED, META);
        return indicesRequestInfo.resolveIndices();
    }
}
