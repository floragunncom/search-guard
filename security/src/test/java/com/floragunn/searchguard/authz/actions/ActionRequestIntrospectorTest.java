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
import static org.hamcrest.core.AllOf.allOf;

import java.util.Arrays;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.SystemIndexAccess;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ActionRequestInfo;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.IndicesRequestInfo;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.IndicesRequestInfo.AdditionalInfoRole;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchsupport.meta.Meta;
import com.floragunn.searchsupport.meta.Meta.IndexLikeObject;

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
    public void indicesAliasesRequest_add_nonExistingAlias() {
        IndicesAliasesRequest request = new IndicesAliasesRequest().addAliasAction(AliasActions.add().alias("alias_ax").index("index_a11"));
        ActionRequestInfo requestInfo = simple().getActionRequestInfo(ACTIONS.get(IndicesAliasesAction.NAME), request);

        assertThat(requestInfo, allOf(//
                resolved().hasIndices("index_a11").hasNoAliases().hasNoDataStreams(),
                resolved(IndicesRequestInfo.AdditionalInfoRole.ALIASES).hasNoIndices().hasAliases("alias_ax").hasNoDataStreams()));

    }

    @Test
    public void indicesAliasesRequest_add_aliasPattern() {
        IndicesAliasesRequest request = new IndicesAliasesRequest().addAliasAction(AliasActions.add().alias("alias_a*").index("index_a11"));
        ActionRequestInfo requestInfo = simple().getActionRequestInfo(ACTIONS.get(IndicesAliasesAction.NAME), request);

        assertThat(requestInfo, allOf(//
                resolved().hasIndices("index_a11").hasNoAliases().hasNoDataStreams(),
                resolved(IndicesRequestInfo.AdditionalInfoRole.ALIASES).hasNoIndices().hasAliases("alias_a*").hasNoDataStreams()));

    }

    @Test
    public void indicesAliasesRequest_remove_aliasPattern() {
        IndicesAliasesRequest request = new IndicesAliasesRequest().addAliasAction(AliasActions.remove().alias("alias_a*").index("index_a11"));
        ActionRequestInfo requestInfo = simple().getActionRequestInfo(ACTIONS.get(IndicesAliasesAction.NAME), request);

        assertThat(requestInfo, allOf(//
                resolved().hasIndices("index_a11").hasNoAliases().hasNoDataStreams(), //
                resolved(IndicesRequestInfo.AdditionalInfoRole.ALIASES).hasNoIndices().hasAliases("alias_a", "alias_a1", "alias_a2")
                        .hasNoDataStreams()));

    }

    @Test
    public void indicesAliasesRequest_removeIndex_indexPatternSimple() {
        IndicesAliasesRequest request = new IndicesAliasesRequest().addAliasAction(AliasActions.removeIndex().index("index_a1*"));
        ActionRequestInfo requestInfo = simple().getActionRequestInfo(ACTIONS.get(IndicesAliasesAction.NAME), request);

        assertThat(requestInfo, resolved().hasIndices("index_a11", "index_a12").hasNoAliases().hasNoDataStreams());
    }

    @Test
    public void indicesAliasesRequest_removeIndex_indexPatternMulti() {
        IndicesAliasesRequest request = new IndicesAliasesRequest()//
                .addAliasAction(AliasActions.removeIndex().index("index_a1*"))
                .addAliasAction(AliasActions.removeIndex().indices("index_a11", "-index_a1*"));
        ActionRequestInfo requestInfo = simple().getActionRequestInfo(ACTIONS.get(IndicesAliasesAction.NAME), request);

        assertThat(requestInfo, resolved().hasIndices("index_a11", "index_a12").hasNoAliases().hasNoDataStreams());
    }

    // TODO more IndicesAliasesRequest

    static ActionRequestIntrospector simple() {
        return new ActionRequestIntrospector(() -> META, () -> SystemIndexAccess.DISALLOWED, () -> false, null);
    }

    static ResolvedIndicesMatcher resolved() {
        return new ResolvedIndicesMatcher(null, null, null, null);
    }

    static ResolvedIndicesMatcher resolved(AdditionalInfoRole role) {
        return new ResolvedIndicesMatcher(role, null, null, null);
    }

    static class ResolvedIndicesMatcher extends DiagnosingMatcher<ActionRequestInfo> {
        private final IndicesRequestInfo.AdditionalInfoRole role;
        private final IndicesMatcher indicesMatcher;
        private final AliasesMatcher aliasesMatcher;
        private final DataStreamsMatcher dataStreamsMatcher;

        ResolvedIndicesMatcher(AdditionalInfoRole role, IndicesMatcher indicesMatcher, AliasesMatcher aliasesMatcher,
                DataStreamsMatcher dataStreamsMatcher) {
            super();
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
                mismatchDescription.appendText("additional resolved indices with role " + role + " are missing. Available roles: ")
                        .appendValue(actionRequestInfo.getAdditionalResolvedIndices().keySet());
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

            return match;
        }

        public ResolvedIndicesMatcher hasIndices(String... expected) {
            return new ResolvedIndicesMatcher(role, new IndicesMatcher(expected), aliasesMatcher, dataStreamsMatcher);
        }

        public ResolvedIndicesMatcher hasAliases(String... expected) {
            return new ResolvedIndicesMatcher(role, indicesMatcher, new AliasesMatcher(expected), dataStreamsMatcher);
        }

        public ResolvedIndicesMatcher hasDataStreams(String... expected) {
            return new ResolvedIndicesMatcher(role, indicesMatcher, aliasesMatcher, new DataStreamsMatcher(expected));
        }

        public ResolvedIndicesMatcher hasNoIndices() {
            return new ResolvedIndicesMatcher(role, new IndicesMatcher(), aliasesMatcher, dataStreamsMatcher);
        }

        public ResolvedIndicesMatcher hasNoAliases() {
            return new ResolvedIndicesMatcher(role, indicesMatcher, new AliasesMatcher(), dataStreamsMatcher);
        }

        public ResolvedIndicesMatcher hasNoDataStreams() {
            return new ResolvedIndicesMatcher(role, indicesMatcher, aliasesMatcher, new DataStreamsMatcher());
        }

    }

    static class IndicesMatcher extends AbstractResolvedIndicesMatcher {

        IndicesMatcher(String... expected) {
            super(expected);
        }

        @Override
        protected String objectType() {
            return "indices";
        }

        @Override
        protected ImmutableSet<? extends IndexLikeObject> getObjects(ResolvedIndices resolvedIndices) {
            return resolvedIndices.getLocal().getPureIndices();
        }
    }

    static class AliasesMatcher extends AbstractResolvedIndicesMatcher {

        AliasesMatcher(String... expected) {
            super(expected);
        }

        @Override
        protected String objectType() {
            return "aliases";
        }

        @Override
        protected ImmutableSet<? extends IndexLikeObject> getObjects(ResolvedIndices resolvedIndices) {
            return resolvedIndices.getLocal().getAliases();
        }
    }

    static class DataStreamsMatcher extends AbstractResolvedIndicesMatcher {

        DataStreamsMatcher(String... expected) {
            super(expected);
        }

        @Override
        protected String objectType() {
            return "data streams";
        }

        @Override
        protected ImmutableSet<? extends IndexLikeObject> getObjects(ResolvedIndices resolvedIndices) {
            return resolvedIndices.getLocal().getDataStreams();
        }
    }

    abstract static class AbstractResolvedIndicesMatcher extends DiagnosingMatcher<ResolvedIndices> {

        protected final ImmutableSet<String> expected;

        AbstractResolvedIndicesMatcher(String... expected) {
            this.expected = ImmutableSet.ofArray(expected);
        }

        AbstractResolvedIndicesMatcher(ImmutableSet<String> expected) {
            this.expected = expected;
        }

        abstract protected String objectType();

        abstract protected ImmutableSet<? extends Meta.IndexLikeObject> getObjects(ResolvedIndices resolvedIndices);

        @Override
        public void describeTo(Description description) {
            if (expected.isEmpty()) {
                description.appendText(objectType() + " are empty");
            } else {
                description.appendText(objectType() + " are " + expected);
            }
        }

        @Override
        protected boolean matches(Object item, Description mismatchDescription) {
            if (!(item instanceof ResolvedIndices)) {
                mismatchDescription.appendValue(item).appendText(" is not an ResolvedIndices object");
                return false;
            }

            ResolvedIndices resolvedIndices = (ResolvedIndices) item;

            return match(getObjects(resolvedIndices), mismatchDescription);
        }

        protected boolean match(ImmutableSet<? extends Meta.IndexLikeObject> resolved, Description mismatchDescription) {
            String objectType = objectType();

            ImmutableSet<String> present = resolved.map(Meta.IndexLikeObject::name);

            if (expected.equals(present)) {
                return true;
            } else {
                ImmutableSet<String> missing = expected.without(present);
                ImmutableSet<String> unexpected = present.without(expected);

                if (expected.isEmpty()) {
                    mismatchDescription.appendText(objectType + " are not empty; unexpected: ").appendValue(unexpected);
                } else {
                    if (!missing.isEmpty() && !unexpected.isEmpty()) {
                        mismatchDescription.appendText(objectType + " ").appendValue(resolved)
                                .appendText(" do not match expected " + objectType + " ").appendValue(expected).appendText("; missing: ")
                                .appendValue(missing).appendText("; unexpected: ").appendValue(unexpected);
                    } else if (!missing.isEmpty()) {
                        mismatchDescription.appendText(objectType + " do not match expected " + objectType + "; missing: ").appendValue(missing);
                    } else {
                        // !unexpected.isEmpty()

                        mismatchDescription.appendText(objectType + " do not match expected " + objectType + "; unexpected: ")
                                .appendValue(unexpected);
                    }

                }

                return false;
            }
        }

    }

}
