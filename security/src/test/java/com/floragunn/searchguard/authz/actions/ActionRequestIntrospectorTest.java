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
import static org.hamcrest.MatcherAssert.assertThat;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.junit.Test;

import com.floragunn.searchguard.authz.SystemIndexAccess;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ActionRequestInfo;
import com.floragunn.searchsupport.meta.Meta;

public class ActionRequestIntrospectorTest {

    static final Actions ACTIONS = new Actions(null);
    static final Meta META = indices("index_a11", "index_a12", "index_a21", "index_a22", "index_b1", "index_b2")//
            .dataStream("ds_d11").of(".ds-ds_d11-2024.03.22-000001", ".ds-ds_d11-2024.03.22-000002")//
            .alias("alias_a").of("index_a11", "index_a12", "index_a21", "index_a22")//
            .alias("alias_a1").of("index_a11", "index_a12")//
            .alias("alias_a2").of("index_a21", "index_a22")//
            .alias("alias_b").of("index_b1", "index_b2");

    @Test
    public void getAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest("alias_a1", "alias_a2").indices("index_a11", "index_a12", "index_a21", "index_a22");
        ActionRequestInfo requestInfo = simple().getActionRequestInfo(ACTIONS.get(GetAliasesAction.NAME), request);
        assertThat(requestInfo, resolved(//
                main().hasIndices("index_a11", "index_a12", "index_a21", "index_a22").hasNoAliases().hasNoDataStreams(),
                additional(Action.AdditionalDimension.ALIASES).hasNoIndices().hasAliases("alias_a1", "alias_a2").hasNoDataStreams()));
    }

    @Test
    public void getAliasesRequest_aliasPattern_noWildcards() {
        GetAliasesRequest request = new GetAliasesRequest("alias_a*").indices("index_a1*")
                .indicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed());
        ActionRequestInfo requestInfo = simple().getActionRequestInfo(ACTIONS.get(GetAliasesAction.NAME), request);
        assertThat(requestInfo, resolved(//
                main().hasIndices("index_a1*").hasNoAliases().hasNoDataStreams(),
                additional(Action.AdditionalDimension.ALIASES).hasNoIndices().hasAliases("alias_a", "alias_a1", "alias_a2").hasNoDataStreams()));
    }

    @Test
    public void indicesAliasesRequest_add_nonExistingAlias() {
        IndicesAliasesRequest request = new IndicesAliasesRequest().addAliasAction(AliasActions.add().alias("alias_ax").index("index_a11"));
        ActionRequestInfo requestInfo = simple().getActionRequestInfo(ACTIONS.get(IndicesAliasesAction.NAME), request);

        assertThat(requestInfo, resolved(//
                main().hasIndices("index_a11").hasNoAliases().hasNoDataStreams(),
                additional(Action.AdditionalDimension.ALIASES).hasNoIndices().hasAliases("alias_ax").hasNoDataStreams()));

    }

    @Test
    public void indicesAliasesRequest_add_aliasPattern() {
        IndicesAliasesRequest request = new IndicesAliasesRequest().addAliasAction(AliasActions.add().alias("alias_a*").index("index_a11"));
        ActionRequestInfo requestInfo = simple().getActionRequestInfo(ACTIONS.get(IndicesAliasesAction.NAME), request);

        assertThat(requestInfo, resolved(//
                main().hasIndices("index_a11").hasNoAliases().hasNoDataStreams(),
                additional(Action.AdditionalDimension.ALIASES).hasNoIndices().hasAliases("alias_a*").hasNoDataStreams()));

    }

    @Test
    public void indicesAliasesRequest_remove_aliasPattern() {
        IndicesAliasesRequest request = new IndicesAliasesRequest().addAliasAction(AliasActions.remove().alias("alias_a*").index("index_a11"));
        ActionRequestInfo requestInfo = simple().getActionRequestInfo(ACTIONS.get(IndicesAliasesAction.NAME), request);

        assertThat(requestInfo, resolved(//
                main().hasIndices("index_a11").hasNoAliases().hasNoDataStreams(), //
                additional(Action.AdditionalDimension.ALIASES).hasNoIndices().hasAliases("alias_a", "alias_a1", "alias_a2").hasNoDataStreams()));

    }

    @Test
    public void indicesAliasesRequest_removeIndex_indexPatternSimple() {
        IndicesAliasesRequest request = new IndicesAliasesRequest().addAliasAction(AliasActions.removeIndex().index("index_a1*"));
        ActionRequestInfo requestInfo = simple().getActionRequestInfo(ACTIONS.get(IndicesAliasesAction.NAME), request);

        assertThat(requestInfo, resolved(//
                main().hasNoIndices().hasNoAliases().hasNoDataStreams(), //
                additional(Action.AdditionalDimension.DELETE_INDEX).hasIndices("index_a11", "index_a12").hasNoAliases().hasNoDataStreams()));
    }

    @Test
    public void indicesAliasesRequest_removeIndex_indexPatternMulti() {
        IndicesAliasesRequest request = new IndicesAliasesRequest()//
                .addAliasAction(AliasActions.removeIndex().index("index_a1*"))
                .addAliasAction(AliasActions.removeIndex().indices("index_a11", "-index_a1*"));
        ActionRequestInfo requestInfo = simple().getActionRequestInfo(ACTIONS.get(IndicesAliasesAction.NAME), request);

        assertThat(requestInfo, main().hasIndices("index_a11", "index_a12").hasNoAliases().hasNoDataStreams());
    }

    @Test
    public void bulkShardRequest_create_datastream() {
        BulkShardRequest request = new BulkShardRequest(null, RefreshPolicy.NONE,
                new BulkItemRequest[] { new BulkItemRequest(1, new IndexRequest("ds_d11")) });
        ActionRequestInfo requestInfo = simple().getActionRequestInfo(ACTIONS.get(TransportShardBulkAction.ACTION_NAME), request);
        assertThat(requestInfo, main().hasNoIndices().hasNoAliases().hasDataStreams("ds_d11"));
    }

    static ActionRequestIntrospector simple() {
        return new ActionRequestIntrospector(() -> META, () -> SystemIndexAccess.DISALLOWED, () -> false, null);
    }

    static DiagnosingMatcher<ActionRequestInfo> resolved(ActionRequestInfoResolvedIndicesMatcher... matchers) {
        return new DiagnosingMatcher<ActionRequestInfo>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("An ActionRequestInfo object where:\n");

                for (ActionRequestInfoResolvedIndicesMatcher matcher : matchers) {
                    description.appendDescriptionOf(matcher);
                    description.appendText("\n");
                }

            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                boolean result = true;

                for (ActionRequestInfoResolvedIndicesMatcher matcher : matchers) {
                    if (!matcher.matches(item, mismatchDescription)) {
                        mismatchDescription.appendText("\n");
                        result = false;
                    }
                }
                return result;
            }

        };
    }

    static ActionRequestInfoResolvedIndicesMatcher main() {
        return new ActionRequestInfoResolvedIndicesMatcher(null, null, null, null);
    }

    static ActionRequestInfoResolvedIndicesMatcher additional(Action.AdditionalDimension role) {
        return new ActionRequestInfoResolvedIndicesMatcher(role, null, null, null);
    }

    static class ActionRequestInfoResolvedIndicesMatcher extends DiagnosingMatcher<ActionRequestInfo> {
        private final Action.AdditionalDimension role;
        private final ResolvedIndicesMatcher.IndicesMatcher indicesMatcher;
        private final ResolvedIndicesMatcher.AliasesMatcher aliasesMatcher;
        private final ResolvedIndicesMatcher.DataStreamsMatcher dataStreamsMatcher;

        ActionRequestInfoResolvedIndicesMatcher(Action.AdditionalDimension role, ResolvedIndicesMatcher.IndicesMatcher indicesMatcher, ResolvedIndicesMatcher.AliasesMatcher aliasesMatcher,
                ResolvedIndicesMatcher.DataStreamsMatcher dataStreamsMatcher) {
            this.role = role;
            this.indicesMatcher = indicesMatcher;
            this.aliasesMatcher = aliasesMatcher;
            this.dataStreamsMatcher = dataStreamsMatcher;
        }

        @Override
        public void describeTo(Description description) {
            if (role == null) {
                description.appendText("main resolved indices where: ");
            } else {
                description.appendText("additional resolved indices of role " + role + " where: ");
            }

            if (indicesMatcher != null) {
                description.appendDescriptionOf(indicesMatcher).appendText("; ");
            }

            if (aliasesMatcher != null) {
                description.appendDescriptionOf(aliasesMatcher).appendText("; ");
            }

            if (dataStreamsMatcher != null) {
                description.appendDescriptionOf(dataStreamsMatcher).appendText("; ");
            }
        }

        @Override
        protected boolean matches(Object item, Description mismatchDescription) {
            if (!(item instanceof ActionRequestInfo)) {
                mismatchDescription.appendValue(item).appendText(" is not an ActionRequestInfo object");
                return false;
            }

            ActionRequestInfo actionRequestInfo = (ActionRequestInfo) item;
            ResolvedIndices resolvedIndices = role == null ? actionRequestInfo.getMainResolvedIndices()
                    : actionRequestInfo.getAdditionalResolvedIndices().get(role);

            if (resolvedIndices == null) {
                if (role == null) {
                    mismatchDescription.appendText("main resolved indices are missing.");
                } else {
                    mismatchDescription.appendText("additional resolved indices with role " + role + " are missing. Available roles: ")
                            .appendValue(actionRequestInfo.getAdditionalResolvedIndices().keySet());
                }
                return false;
            }

            boolean match = true;

            if (indicesMatcher != null) {
                match &= indicesMatcher.matches(resolvedIndices, mismatchDescription);
            }

            if (aliasesMatcher != null) {
                match &= aliasesMatcher.matches(resolvedIndices, mismatchDescription);
            }

            if (dataStreamsMatcher != null) {
                match &= dataStreamsMatcher.matches(resolvedIndices, mismatchDescription);
            }

            if (!match) {
                mismatchDescription.appendText("\n").appendText(role == null ? "main" : this.role.toString()).appendText(": ")
                        .appendValue(resolvedIndices);
            }

            return match;
        }

        public ActionRequestInfoResolvedIndicesMatcher hasIndices(String... expected) {
            return new ActionRequestInfoResolvedIndicesMatcher(role, new ResolvedIndicesMatcher.IndicesMatcher(expected), aliasesMatcher, dataStreamsMatcher);
        }

        public ActionRequestInfoResolvedIndicesMatcher hasAliases(String... expected) {
            return new ActionRequestInfoResolvedIndicesMatcher(role, indicesMatcher, new ResolvedIndicesMatcher.AliasesMatcher(expected), dataStreamsMatcher);
        }

        public ActionRequestInfoResolvedIndicesMatcher hasDataStreams(String... expected) {
            return new ActionRequestInfoResolvedIndicesMatcher(role, indicesMatcher, aliasesMatcher, new ResolvedIndicesMatcher.DataStreamsMatcher(expected));
        }

        public ActionRequestInfoResolvedIndicesMatcher hasNoIndices() {
            return new ActionRequestInfoResolvedIndicesMatcher(role, new ResolvedIndicesMatcher.IndicesMatcher(), aliasesMatcher, dataStreamsMatcher);
        }

        public ActionRequestInfoResolvedIndicesMatcher hasNoAliases() {
            return new ActionRequestInfoResolvedIndicesMatcher(role, indicesMatcher, new ResolvedIndicesMatcher.AliasesMatcher(), dataStreamsMatcher);
        }

        public ActionRequestInfoResolvedIndicesMatcher hasNoDataStreams() {
            return new ActionRequestInfoResolvedIndicesMatcher(role, indicesMatcher, aliasesMatcher, new ResolvedIndicesMatcher.DataStreamsMatcher());
        }

    }

   

}
