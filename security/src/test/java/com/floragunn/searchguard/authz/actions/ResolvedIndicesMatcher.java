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

import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchsupport.meta.Meta;
import com.floragunn.searchsupport.meta.Meta.IndexLikeObject;

public class ResolvedIndicesMatcher extends DiagnosingMatcher<ResolvedIndices> {

    private final ResolvedIndicesMatcher.IndicesMatcher indicesMatcher;
    private final ResolvedIndicesMatcher.AliasesMatcher aliasesMatcher;
    private final ResolvedIndicesMatcher.DataStreamsMatcher dataStreamsMatcher;

    ResolvedIndicesMatcher(ResolvedIndicesMatcher.IndicesMatcher indicesMatcher, ResolvedIndicesMatcher.AliasesMatcher aliasesMatcher,
            ResolvedIndicesMatcher.DataStreamsMatcher dataStreamsMatcher) {
        this.indicesMatcher = indicesMatcher;
        this.aliasesMatcher = aliasesMatcher;
        this.dataStreamsMatcher = dataStreamsMatcher;
    }

    @Override
    public void describeTo(Description description) {
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
        if (!(item instanceof ResolvedIndices)) {
            mismatchDescription.appendValue(item).appendText(" is not an ResolvedIndices object");
            return false;
        }

        ResolvedIndices resolvedIndices = (ResolvedIndices) item;

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
            mismatchDescription.appendText("\n").appendValue(resolvedIndices);
        }

        return match;
    }

    public static ResolvedIndicesMatcher hasIndices(String... expected) {
        return new ResolvedIndicesMatcher(new ResolvedIndicesMatcher.IndicesMatcher(expected), null, null);
    }
    
    /*
    public ResolvedIndicesMatcher hasIndices(String... expected) {
        return new ResolvedIndicesMatcher(new ResolvedIndicesMatcher.IndicesMatcher(expected), aliasesMatcher, dataStreamsMatcher);
    }
    */
    
    public ResolvedIndicesMatcher hasAliases(String... expected) {
        return new ResolvedIndicesMatcher(indicesMatcher, new ResolvedIndicesMatcher.AliasesMatcher(expected), dataStreamsMatcher);
    }

    public ResolvedIndicesMatcher hasDataStreams(String... expected) {
        return new ResolvedIndicesMatcher(indicesMatcher, aliasesMatcher, new ResolvedIndicesMatcher.DataStreamsMatcher(expected));
    }

    public static ResolvedIndicesMatcher hasNoIndices() {
        return new ResolvedIndicesMatcher(new ResolvedIndicesMatcher.IndicesMatcher(), null,  null);
    }

    public ResolvedIndicesMatcher hasNoAliases() {
        return new ResolvedIndicesMatcher(indicesMatcher, new ResolvedIndicesMatcher.AliasesMatcher(), dataStreamsMatcher);
    }

    public ResolvedIndicesMatcher hasNoDataStreams() {
        return new ResolvedIndicesMatcher(indicesMatcher, aliasesMatcher, new ResolvedIndicesMatcher.DataStreamsMatcher());
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
