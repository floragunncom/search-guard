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

import static com.floragunn.searchsupport.meta.Meta.Mock.indices;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.SystemIndexAccess;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ActionRequestInfo;
import com.floragunn.searchsupport.meta.Meta;

public class ActionRequestIntrospectorTest {

    static final Actions ACTIONS = new Actions(null);
    static final Meta META = indices("index_a11", "index_a12", "index_a21", "index_a22", "index_b1", "index_b2")//
            .alias("alias_a").of("index_a11", "index_a12", "index_a21", "index_a22")//
            .alias("alias_a1").of("index_a11", "index_a12")//
            .alias("alias_a2").of("index_a21", "index_a22")//
            .alias("alias_b").of("index_b1", "index_b2");

    @Test
    public void getAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest("alias_a1", "alias_a2").indices("index_a11", "index_a12", "index_a21", "index_a22");
        ActionRequestInfo requestInfo = simple().getActionRequestInfo(ACTIONS.get(GetAliasesAction.NAME), request);
        System.out.println(requestInfo);

    }

    @Test
    public void indicesAliasesRequest_nonExistingAlias() {
        IndicesAliasesRequest request = new IndicesAliasesRequest().addAliasAction(AliasActions.add().alias("alias_ax").index("index_a11"));
        ActionRequestInfo requestInfo = simple().getActionRequestInfo(ACTIONS.get(IndicesAliasesAction.NAME), request);

        Assert.assertEquals(requestInfo.toString(), ImmutableSet.of("alias_ax"),
                requestInfo.getResolvedIndices().getLocal().getAliases().map(o -> o.name()));
        Assert.assertEquals(requestInfo.toString(), ImmutableSet.of("index_a11"),
                requestInfo.getResolvedIndices().getLocal().getPureIndices().map(o -> o.name()));

        System.out.println(requestInfo);
    }

    // TODO more IndicesAliasesRequest
    
    static ActionRequestIntrospector simple() {
        return new ActionRequestIntrospector(() -> META, () -> SystemIndexAccess.DISALLOWED, () -> false, null);
    }
}
