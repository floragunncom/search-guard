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

package com.floragunn.searchguard.test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import com.floragunn.codova.documents.DocNode;

public interface TestIndexLike {
    String getName();

    TestIndexLike dataOnly();

    Set<String> getDocumentIds();

    Map<String, Map<String, ?>> getDocuments();

    DocNode getFieldsMappings();

    default TestIndexLike.Filtered filteredBy(Predicate<DocNode> filter) {
        return new Filtered(this, filter);
    }

    default TestIndexLike intersection(TestIndexLike other) {
        if (other == this) {
            return this;
        }

        if (!this.getName().equals(other.getName())) {
            throw new IllegalArgumentException("Cannot intersect different indices: " + this + " vs " + other);
        }

        if (other instanceof TestIndexLike.Filtered) {
            return ((TestIndexLike.Filtered) other).intersection(this);
        }

        return this;
    }

    default Optional<TestIndexLike> failureStore() {
        return Optional.empty();
    }

    default Map<String, ?> firstDocument() {
        return getDocuments().values().stream().findFirst().orElseThrow();
    }

    public static class Filtered implements TestIndexLike {
        final TestIndexLike testIndexLike;
        final Predicate<DocNode> filter;
        Map<String, Map<String, ?>> cachedDocuments;

        Filtered(TestIndexLike testIndexLike, Predicate<DocNode> filter) {
            this.testIndexLike = testIndexLike;
            this.filter = filter;
        }

        @Override
        public String getName() {
            return testIndexLike.getName();
        }

        @Override
        public TestIndexLike dataOnly() {
            return new Filtered(testIndexLike.dataOnly(), filter);
        }

        @Override
        public Optional<TestIndexLike> failureStore() {
            return testIndexLike.failureStore().map(i -> new  Filtered(i, filter));
        }

        @Override
        public Set<String> getDocumentIds() {
            return getDocuments().keySet();
        }

        @Override
        public Map<String, Map<String, ?>> getDocuments() {
            Map<String, Map<String, ?>> result = this.cachedDocuments;

            if (result == null) {
                result = new HashMap<>();

                for (Map.Entry<String, Map<String, ?>> entry : this.testIndexLike.getDocuments().entrySet()) {
                    if (this.filter.test(DocNode.wrap(entry.getValue()))) {
                        result.put(entry.getKey(), entry.getValue());
                    }
                }

                this.cachedDocuments = Collections.unmodifiableMap(result);
            }

            return result;
        }

        @Override
        public DocNode getFieldsMappings() {
            return testIndexLike.getFieldsMappings();
        }

        @Override
        public TestIndexLike intersection(TestIndexLike other) {
            if (other == this) {
                return this;
            }

            if (other instanceof Filtered) {
                return new Filtered(this.testIndexLike, node -> this.filter.test(node) && ((Filtered) other).filter.test(node));
            } else {
                return this;
            }
        }

        @Override
        public String toString() {
            return testIndexLike + " [filtered]";
        }

    }

}
