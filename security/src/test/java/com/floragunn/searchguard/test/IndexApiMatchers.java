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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;

import com.floragunn.codova.documents.BasicJsonPathDefaultConfiguration;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format.UnknownDocTypeException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;

public class IndexApiMatchers {
    private static final Pattern DS_BACKING_INDEX_PATTERN = Pattern.compile("\\.ds-(.+)-[0-9\\.]+-[0-9]+");
    private static final Pattern SHORT_ISO_TIMESTAMP = Pattern.compile("\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z\\b");
    private static final Pattern GEO_COORD_PATTERN = Pattern.compile("([0-9-]+)\\.(\\d+),\\s*([0-9-]+)\\.(\\d+)");

    public static IndexMatcher containsExactly(TestIndexLike... testIndices) {
        Map<String, TestIndexLike> indexNameMap = new HashMap<>();
        boolean containsSearchGuardIndices = false;
        boolean containsEsInternalIndices = false;

        for (TestIndexLike testIndex : testIndices) {
            if (testIndex == SEARCH_GUARD_INDICES) {
                containsSearchGuardIndices = true;
            } else if (testIndex == ES_INTERNAL_INDICES) {
                containsEsInternalIndices = true;
            } else {
                indexNameMap.put(testIndex.getName(), testIndex);
            }
        }

        return new ContainsExactlyMatcher(indexNameMap, containsSearchGuardIndices, containsEsInternalIndices);
    }

    public static IndexMatcher limitedTo(TestIndexLike... testIndices) {
        Map<String, TestIndexLike> indexNameMap = new HashMap<>();

        for (TestIndexLike testIndex : testIndices) {
            indexNameMap.put(testIndex.getName(), testIndex);
        }

        return new LimitedToMatcher(indexNameMap);
    }

    public static IndexMatcher unlimited() {
        return new UnlimitedMatcher();
    }

    public static IndexMatcher unlimitedIncludingEsInternalIndices() {
        return new UnlimitedMatcher(false, true);
    }

    public static IndexMatcher unlimitedIncludingSearchGuardIndices() {
        return new UnlimitedMatcher(true, true);
    }

    public static IndexMatcher limitedToNone() {
        return new LimitedToMatcher(Collections.emptyMap());
    }

    /**
     * This returns a magic TestIndexLike object which matches all internal Search Guard indices.
     */
    public static TestIndexLike searchGuardIndices() {
        return SEARCH_GUARD_INDICES;
    }

    /**
     * This returns a magic TestIndexLike object which matches all internal ES indices like ilm-history*, deprecation logs, etc.
     */
    public static TestIndexLike esInternalIndices() {
        return ES_INTERNAL_INDICES;
    }

    private final static TestIndexLike SEARCH_GUARD_INDICES = new TestIndexLike() {

        @Override
        public String getName() {
            return ".searchguard*";
        }

        @Override
        public Map<String, Map<String, ?>> getDocuments() {
            return null;
        }

        @Override
        public DocNode getFieldsMappings() {
            return DocNode.EMPTY;
        }

        @Override
        public Set<String> getDocumentIds() {
            return null;
        }
    };

    private final static TestIndexLike ES_INTERNAL_INDICES = new TestIndexLike() {

        @Override
        public String getName() {
            return ".es-internal*";
        }

        @Override
        public Map<String, Map<String, ?>> getDocuments() {
            return null;
        }

        @Override
        public DocNode getFieldsMappings() {
            return DocNode.EMPTY;
        }

        @Override
        public Set<String> getDocumentIds() {
            return null;
        }
    };

    public static class ContainsExactlyMatcher extends AbstractIndexMatcher implements IndexMatcher {

        public ContainsExactlyMatcher(Map<String, TestIndexLike> indexNameMap, boolean containsSearchGuardIndices,
                boolean containsEsInternalIndices) {
            super(indexNameMap, containsSearchGuardIndices, containsEsInternalIndices);
        }

        public ContainsExactlyMatcher(Map<String, TestIndexLike> indexNameMap, boolean containsSearchGuardIndices, boolean containsEsInternalIndices,
                String jsonPath, int statusCodeWhenEmpty) {
            super(indexNameMap, containsSearchGuardIndices, containsEsInternalIndices, jsonPath, statusCodeWhenEmpty);
        }

        @Override
        public void describeTo(Description description) {
            if (indexNameMap.isEmpty()) {
                if (this.statusCodeWhenEmpty == 200) {
                    description.appendText("a 200 OK response with an empty result set");
                } else {
                    description.appendText("a response with status code " + this.statusCodeWhenEmpty);
                }
            } else {
                description
                        .appendText("a 200 OK response with exactly the indices " + indexNameMap.keySet().stream().collect(Collectors.joining(", ")));
            }
        }

        @Override
        protected boolean matchesImpl(Collection<?> collection, Description mismatchDescription, GenericRestClient.HttpResponse response) {

            boolean checkDocs = false;

            // Flatten the collection
            collection = collection.stream().flatMap(e -> e instanceof Collection ? ((Collection<?>) e).stream() : Stream.of(e))
                    .collect(Collectors.toSet());

            for (Object object : collection) {
                if (object instanceof String) {
                    checkDocs = false;
                    break;
                } else if (object instanceof Map && ((Map<?, ?>) object).containsKey("_index")) {
                    checkDocs = true;
                    break;
                } else {
                    mismatchDescription.appendText("unexpected value ").appendValue(collection);
                    return false;
                }
            }

            if (checkDocs) {
                return matchesByDocs(collection, mismatchDescription, response);
            } else {
                return matchesByIndices(collection, mismatchDescription, response);
            }
        }

        protected boolean matchesByDocs(Collection<?> collection, Description mismatchDescription, GenericRestClient.HttpResponse response) {
            Set<String> pendingDocuments = this.getExpectedDocuments();
            ImmutableSet.Builder<String> seenSearchGuardIndicesBuilder = new ImmutableSet.Builder<String>();

            for (Object object : collection) {
                DocNode docNode = DocNode.wrap(object);
                String indexName = docNode.getAsString("_index");

                if (containsSearchGuardIndices && (indexName.startsWith(".searchguard") || indexName.equals("searchguard"))) {
                    seenSearchGuardIndicesBuilder.add(indexName);
                    continue;
                }

                TestIndexLike index = indexNameMap.get(indexName);

                if (index == null) {
                    mismatchDescription.appendText("result contains unknown index: ").appendValue(docNode.getAsString("_index"))
                            .appendText("; expected: ").appendValue(indexNameMap.keySet()).appendText("\ndocument: ")
                            .appendText(docNode.toJsonString());
                    mismatchDescription.appendText("\n\n").appendValue(formatResponse(response));

                    return false;
                }

                Map<String, ?> document = index.getDocuments().get(docNode.getAsString("_id"));

                if (document == null) {
                    mismatchDescription.appendText("result contains unknown document id ").appendValue(docNode.getAsString("_id"))
                            .appendText(" for index ").appendValue(docNode.getAsString("_index")).appendText("\ndocument: ")
                            .appendText(docNode.toJsonString());
                    mismatchDescription.appendText("\n\n").appendValue(formatResponse(response));

                    return false;
                }

                if (!document.equals(docNode.get("_source"))) {
                    mismatchDescription.appendText("result document ").appendValue(docNode.getAsString("_id")).appendText(" in index ")
                            .appendValue(docNode.getAsString("_index")).appendText(" does not match expected document:\n")
                            .appendText(docNode.getAsNode("_source").toJsonString()).appendText("\n")
                            .appendText(DocNode.wrap(document).toJsonString());
                    mismatchDescription.appendText("\n\n").appendValue(formatResponse(response));

                    return false;
                }

                pendingDocuments.remove(docNode.getAsString("_index") + "/" + docNode.getAsString("_id"));

            }

            if (!pendingDocuments.isEmpty()) {
                mismatchDescription.appendText("result does not contain expected documents: ").appendValue(pendingDocuments);
                mismatchDescription.appendText("\n\n").appendValue(formatResponse(response));

                return false;
            }

            if (containsSearchGuardIndices && seenSearchGuardIndicesBuilder.size() == 0) {
                mismatchDescription.appendText("result does not contain expected .searchguard index");
                mismatchDescription.appendText("\n\n").appendValue(formatResponse(response));

                return false;
            }

            return true;
        }

        protected boolean matchesByIndices(Collection<?> collection, Description mismatchDescription, GenericRestClient.HttpResponse response) {
            ImmutableSet<String> expectedIndices = this.getExpectedIndices();
            ImmutableSet.Builder<String> seenIndicesBuilder = new ImmutableSet.Builder<String>(expectedIndices.size());
            ImmutableSet.Builder<String> seenSearchGuardIndicesBuilder = new ImmutableSet.Builder<String>();

            for (Object object : collection) {
                String index = object.toString();

                if (containsSearchGuardIndices && (index.startsWith(".searchguard") || index.equals("searchguard"))) {
                    seenSearchGuardIndicesBuilder.add(index);
                } else if (containsEsInternalIndices && (index.startsWith(".logs-deprecation") || index.startsWith(".ds-.logs-deprecation")
                        || index.startsWith(".logs-elasticsearch.deprecation") || index.startsWith(".ds-.logs-elasticsearch.deprecation"))) {
                    // We will just ignore these, as they actually might not exist on embedded clusters
                } else if (index.startsWith(".ds-")) {
                    // We do a special treatment for data stream backing indices. We convert these to the normal data streams if expected indices contains these.
                    java.util.regex.Matcher matcher = DS_BACKING_INDEX_PATTERN.matcher(index);

                    if (matcher.matches() && expectedIndices.contains(matcher.group(1))) {
                        seenIndicesBuilder.add(matcher.group(1));
                    } else {
                        seenIndicesBuilder.add(index);
                    }
                } else {
                    seenIndicesBuilder.add(index);
                }
            }

            ImmutableSet<String> seenIndices = seenIndicesBuilder.build();

            ImmutableSet<String> unexpectedIndices = seenIndices.without(expectedIndices);
            ImmutableSet<String> missingIndices = expectedIndices.without(seenIndices);

            if (containsSearchGuardIndices && seenSearchGuardIndicesBuilder.size() == 0) {
                missingIndices = missingIndices.with(".searchguard indices");
            }

            if (unexpectedIndices.isEmpty() && missingIndices.isEmpty()) {
                return true;
            } else {
                if (!missingIndices.isEmpty()) {
                    mismatchDescription.appendText("result does not contain expected indices; found: ").appendValue(seenIndices)
                            .appendText("; missing: ").appendValue(missingIndices).appendText("\n\n").appendText(formatResponse(response));
                }

                if (!unexpectedIndices.isEmpty()) {
                    mismatchDescription.appendText("result does contain indices that were not expected: ").appendValue(unexpectedIndices)
                            .appendText("\n\n").appendText(formatResponse(response));
                }
                return false;
            }
        }

        private Set<String> getExpectedDocuments() {
            Set<String> pendingDocuments = new HashSet<>();

            for (Map.Entry<String, TestIndexLike> entry : indexNameMap.entrySet()) {
                for (String id : entry.getValue().getDocumentIds()) {
                    pendingDocuments.add(entry.getKey() + "/" + id);
                }
            }

            return pendingDocuments;
        }

        private ImmutableSet<String> getExpectedIndices() {
            return ImmutableSet.of(indexNameMap.keySet());
        }

        @Override
        public IndexMatcher but(IndexMatcher other) {
            if (other instanceof LimitedToMatcher) {
                return new ContainsExactlyMatcher(intersection(this.indexNameMap, ((LimitedToMatcher) other).indexNameMap), //
                        this.containsSearchGuardIndices && other.containsSearchGuardIndices(), //
                        this.containsEsInternalIndices && other.containsEsInternalIndices(), //
                        this.jsonPath, this.statusCodeWhenEmpty);
            } else if (other instanceof ContainsExactlyMatcher) {
                return new ContainsExactlyMatcher(intersection(this.indexNameMap, ((ContainsExactlyMatcher) other).indexNameMap), //
                        this.containsSearchGuardIndices && other.containsSearchGuardIndices(), //
                        this.containsEsInternalIndices && other.containsEsInternalIndices(), //
                        this.jsonPath, this.statusCodeWhenEmpty);
            } else if (other instanceof UnlimitedMatcher) {
                return new ContainsExactlyMatcher(this.indexNameMap, //
                        this.containsSearchGuardIndices && other.containsSearchGuardIndices(), //
                        this.containsEsInternalIndices && other.containsEsInternalIndices(), //
                        this.jsonPath, this.statusCodeWhenEmpty);
            } else {
                throw new RuntimeException("Unexpected argument " + other);
            }
        }

        private Map<String, TestIndexLike> intersection(Map<String, TestIndexLike> map1, Map<String, TestIndexLike> map2) {
            Map<String, TestIndexLike> result = new HashMap<>();

            for (Map.Entry<String, TestIndexLike> entry : map1.entrySet()) {
                String key = entry.getKey();
                TestIndexLike index1 = entry.getValue();
                TestIndexLike index2 = map2.get(key);

                if (index2 == null) {
                    continue;
                }

                result.put(key, index1.intersection(index2));
            }

            return Collections.unmodifiableMap(result);
        }

        @Override
        public boolean isCoveredBy(IndexMatcher other) {
            // Returns true of other provides at least all indices as this
            // Examples:
            //
            // this:  a, b, c
            // other:    b, c
            // -> a missing -> false
            //
            // this:  a, b
            // other: a, b, c
            // -> true

            if (other instanceof LimitedToMatcher) {
                return ((LimitedToMatcher) other).getExpectedIndices().containsAll(this.getExpectedIndices());
            } else if (other instanceof ContainsExactlyMatcher) {
                return ((ContainsExactlyMatcher) other).getExpectedIndices().containsAll(this.getExpectedIndices());
            } else if (other instanceof UnlimitedMatcher) {
                return true;
            } else {
                throw new RuntimeException("Unexpected argument " + other);
            }
        }

        @Override
        public IndexMatcher at(String jsonPath) {
            return new ContainsExactlyMatcher(indexNameMap, containsSearchGuardIndices, containsEsInternalIndices, jsonPath, statusCodeWhenEmpty);
        }

        @Override
        public IndexMatcher whenEmpty(int statusCode) {
            return new ContainsExactlyMatcher(indexNameMap, containsSearchGuardIndices, containsEsInternalIndices, jsonPath, statusCode);
        }

        @Override
        public boolean covers(TestIndex testIndex) {
            return indexNameMap.containsKey(testIndex.getName());
        }

    }

    public static class LimitedToMatcher extends AbstractIndexMatcher implements IndexMatcher {

        public LimitedToMatcher(Map<String, TestIndexLike> indexNameMap) {
            super(indexNameMap, false, false);
        }

        public LimitedToMatcher(Map<String, TestIndexLike> indexNameMap, String jsonPath, int statusCodeWhenEmpty) {
            super(indexNameMap, false, false, jsonPath, statusCodeWhenEmpty);
        }

        @Override
        public void describeTo(Description description) {
            if (indexNameMap.isEmpty()) {
                if (this.statusCodeWhenEmpty == 200) {
                    description.appendText("a 200 OK response with an empty result set");
                } else {
                    description.appendText("a response with status code " + this.statusCodeWhenEmpty);
                }
            } else {
                description.appendText("a 200 OK response no indices other than " + indexNameMap.keySet().stream().collect(Collectors.joining(", ")));
            }
        }

        @Override
        protected boolean matchesImpl(Collection<?> collection, Description mismatchDescription, GenericRestClient.HttpResponse response) {
            boolean checkDocs = false;

            for (Object object : collection) {
                if (object instanceof String) {
                    checkDocs = false;
                    break;
                } else if (object instanceof Map && ((Map<?, ?>) object).containsKey("_index")) {
                    checkDocs = true;
                    break;
                } else {
                    mismatchDescription.appendText("unexpected value ").appendValue(object).appendText(" (")
                            .appendValue(object != null ? object.getClass().toString() : "null").appendText(")\n\n")
                            .appendText(formatResponse(response));
                    return false;
                }
            }

            if (checkDocs) {
                return matchesByDocs(collection, mismatchDescription, response);
            } else {
                return matchesByIndices(collection, mismatchDescription, response);
            }
        }

        @Override
        public IndexMatcher but(IndexMatcher other) {
            if (other instanceof LimitedToMatcher) {
                return new LimitedToMatcher(ImmutableMap.of(this.indexNameMap).intersection(((LimitedToMatcher) other).getExpectedIndices()),
                        this.jsonPath, this.statusCodeWhenEmpty);
            } else if (other instanceof ContainsExactlyMatcher) {
                return new ContainsExactlyMatcher(
                        ImmutableMap.of(this.indexNameMap).intersection(((ContainsExactlyMatcher) other).getExpectedIndices()), false, false,
                        this.jsonPath, this.statusCodeWhenEmpty);
            } else if (other instanceof UnlimitedMatcher) {
                return this;
            } else {
                throw new RuntimeException("Unexpected argument " + other);
            }
        }
        

        @Override
        public boolean covers(TestIndex testIndex) {
            return indexNameMap.containsKey(testIndex.getName());
        }

        @Override
        public boolean isCoveredBy(IndexMatcher other) {
            if (other instanceof LimitedToMatcher) {
                return ((LimitedToMatcher) other).getExpectedIndices().containsAll(this.getExpectedIndices());
            } else if (other instanceof ContainsExactlyMatcher) {
                return ((ContainsExactlyMatcher) other).getExpectedIndices().containsAll(this.getExpectedIndices());
            } else if (other instanceof UnlimitedMatcher) {
                return true;
            } else {
                throw new RuntimeException("Unexpected argument " + other);
            }
        }

        protected boolean matchesByDocs(Collection<?> collection, Description mismatchDescription, GenericRestClient.HttpResponse response) {
            ImmutableSet<String> expectedIndices = this.getExpectedIndices();
            ImmutableSet.Builder<String> seenIndicesBuilder = new ImmutableSet.Builder<String>(expectedIndices.size());

            for (Object object : collection) {
                seenIndicesBuilder.add(DocNode.wrap(object).getAsString("_index"));
            }

            ImmutableSet<String> seenIndices = seenIndicesBuilder.build();
            ImmutableSet<String> unexpectedIndices = seenIndices.without(expectedIndices);

            if (unexpectedIndices.isEmpty()) {
                return true;
            } else {
                mismatchDescription.appendText("result does contain indices that were not expected: ").appendValue(unexpectedIndices)
                        .appendText("\n\n").appendValue(formatResponse(response));
                return false;
            }
        }

        protected boolean matchesByIndices(Collection<?> collection, Description mismatchDescription, GenericRestClient.HttpResponse response) {
            ImmutableSet<String> expectedIndices = this.getExpectedIndices();
            ImmutableSet.Builder<String> seenIndicesBuilder = new ImmutableSet.Builder<String>(expectedIndices.size());

            for (Object object : collection) {
                seenIndicesBuilder.add(object.toString());
            }

            ImmutableSet<String> seenIndices = seenIndicesBuilder.build();
            ImmutableSet<String> unexpectedIndices = seenIndices.without(expectedIndices);

            if (unexpectedIndices.isEmpty()) {
                return true;
            } else {
                mismatchDescription.appendText("result does contain indices that were not expected: ").appendValue(unexpectedIndices)
                        .appendText("\n\n").appendValue(formatResponse(response));
                return false;
            }
        }

        private ImmutableSet<String> getExpectedIndices() {
            return ImmutableSet.of(indexNameMap.keySet());
        }

        @Override
        public IndexMatcher at(String jsonPath) {
            return new ContainsExactlyMatcher(indexNameMap, containsSearchGuardIndices, containsEsInternalIndices, jsonPath, statusCodeWhenEmpty);
        }

        @Override
        public IndexMatcher whenEmpty(int statusCode) {
            return new ContainsExactlyMatcher(indexNameMap, containsSearchGuardIndices, containsEsInternalIndices, jsonPath, statusCode);
        }

    }

    public static class UnlimitedMatcher extends DiagnosingMatcher<Object> implements IndexMatcher {

        private final boolean containsSearchGuardIndices;
        private final boolean containsEsInternalIndices;

        public UnlimitedMatcher() {
            this.containsSearchGuardIndices = false;
            this.containsEsInternalIndices = false;
        }

        public UnlimitedMatcher(boolean containsSearchGuardIndices, boolean containsEsInternalIndices) {
            this.containsSearchGuardIndices = containsSearchGuardIndices;
            this.containsEsInternalIndices = containsEsInternalIndices;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("unlimited indices");
        }

        @Override
        protected boolean matches(Object item, Description mismatchDescription) {
            if (item instanceof GenericRestClient.HttpResponse) {
                GenericRestClient.HttpResponse response = (GenericRestClient.HttpResponse) item;

                if (response.getStatusCode() != 200) {
                    mismatchDescription.appendText("Expected status code 200 but status was: ")
                            .appendValue(response.getStatusCode() + " " + response.getStatusReason());
                    return false;
                }
            }

            return true;
        }

        @Override
        public IndexMatcher but(IndexMatcher other) {
            return other;
        }

        @Override
        public boolean isCoveredBy(IndexMatcher other) {
            if (other instanceof UnlimitedMatcher) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public IndexMatcher at(String jsonPath) {
            return this;
        }

        @Override
        public IndexMatcher whenEmpty(int statusCode) {
            return this;
        }

        @Override
        public IndexMatcher butFailIfIncomplete(IndexMatcher other, int statusCode) {
            return this;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean containsSearchGuardIndices() {
            return containsSearchGuardIndices;
        }

        @Override
        public boolean containsEsInternalIndices() {
            return containsEsInternalIndices;
        }

        @Override
        public int size() {
            throw new IllegalStateException("The UnlimitedMatcher cannot specify a size");
        }

        @Override
        public IndexMatcher aggregateTerm(String term) {
            return null;
        }

        @Override
        public boolean containsDocument(String id) {
            return true;
        }
        

        @Override
        public boolean covers(TestIndex testIndex) {
            return true;
        }
    }

    public static class StatusCodeMatcher extends DiagnosingMatcher<Object> implements IndexMatcher {
        private int expectedStatusCode = 403;

        public StatusCodeMatcher(int expectedStatusCode) {
            this.expectedStatusCode = expectedStatusCode;
        }

        public StatusCodeMatcher withStatus(int expectedStatusCode) {
            this.expectedStatusCode = expectedStatusCode;
            return this;
        }

        @Override
        public IndexMatcher but(IndexMatcher other) {
            return this;
        }

        @Override
        public IndexMatcher at(String jsonPath) {
            return this;
        }

        @Override
        public IndexMatcher whenEmpty(int statusCode) {
            return this;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("a response with status code " + this.expectedStatusCode);
        }

        @Override
        public IndexMatcher butFailIfIncomplete(IndexMatcher other, int statusCode) {
            return new StatusCodeMatcher(statusCode);
        }

        @Override
        protected boolean matches(Object item, Description mismatchDescription) {
            if (item instanceof GenericRestClient.HttpResponse) {
                GenericRestClient.HttpResponse response = (GenericRestClient.HttpResponse) item;

                if (response.getStatusCode() != this.expectedStatusCode) {
                    mismatchDescription.appendText("Status was: ").appendValue(response.getStatusCode() + " " + response.getStatusReason())
                            .appendText("\n\n").appendText(formatResponse(response));
                    return false;
                } else {
                    return true;
                }
            } else {
                mismatchDescription.appendText("Did not get HttpResponse ").appendValue(item);

                return false;
            }
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public boolean containsSearchGuardIndices() {
            return true;
        }

        @Override
        public boolean isCoveredBy(IndexMatcher other) {
            return false;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean containsEsInternalIndices() {
            return true;
        }

        @Override
        public IndexMatcher aggregateTerm(String term) {
            return null;
        }

        @Override
        public boolean containsDocument(String id) {
            return false;
        }

        @Override
        public boolean covers(TestIndex testIndex) {
            return false;
        }
    }

    public static interface IndexMatcher extends Matcher<Object> {
        IndexMatcher but(IndexMatcher other);

        IndexMatcher butFailIfIncomplete(IndexMatcher other, int statusCode);

        IndexMatcher at(String jsonPath);

        IndexMatcher whenEmpty(int statusCode);

        IndexMatcher aggregateTerm(String term);

        boolean isEmpty();

        int size();

        boolean isCoveredBy(IndexMatcher other);

        default IndexMatcher butForbiddenIfIncomplete(IndexMatcher other) {
            return butFailIfIncomplete(other, 403);
        }

        boolean containsSearchGuardIndices();

        boolean containsEsInternalIndices();
        
        boolean containsDocument(String id);
        
        boolean covers(TestIndex testIndex);
    }

    static abstract class AbstractIndexMatcher extends DiagnosingMatcher<Object> implements IndexMatcher {
        protected final Map<String, TestIndexLike> indexNameMap;
        protected final String jsonPath;
        protected final int statusCodeWhenEmpty;
        protected final boolean containsSearchGuardIndices;
        protected final boolean containsEsInternalIndices;

        AbstractIndexMatcher(Map<String, TestIndexLike> indexNameMap, boolean containsSearchGuardIndices, boolean containsEsInternalIndices) {
            this.indexNameMap = indexNameMap;
            this.jsonPath = null;
            this.statusCodeWhenEmpty = 200;
            this.containsSearchGuardIndices = containsSearchGuardIndices;
            this.containsEsInternalIndices = containsEsInternalIndices;
        }

        AbstractIndexMatcher(Map<String, TestIndexLike> indexNameMap, boolean containsSearchGuardIndices, boolean containsEsInternalIndices,
                String jsonPath, int statusCodeWhenEmpty) {
            this.indexNameMap = indexNameMap;
            this.jsonPath = jsonPath;
            this.statusCodeWhenEmpty = statusCodeWhenEmpty;
            this.containsSearchGuardIndices = containsSearchGuardIndices;
            this.containsEsInternalIndices = containsEsInternalIndices;
        }

        @Override
        protected boolean matches(Object item, Description mismatchDescription) {
            GenericRestClient.HttpResponse response = null;

            if (item instanceof GenericRestClient.HttpResponse) {
                response = (GenericRestClient.HttpResponse) item;

                if (indexNameMap.isEmpty()) {
                    if (response.getStatusCode() != this.statusCodeWhenEmpty) {
                        mismatchDescription.appendText("Status was: ").appendValue(response.getStatusCode() + " " + response.getStatusReason())
                                .appendText("\n\n").appendText(formatResponse(response));
                        return false;
                    }

                    if (response.getStatusCode() != 200) {
                        return true;
                    }
                }

                try {
                    item = DocReader.json().read(((GenericRestClient.HttpResponse) item).getBody());
                } catch (DocumentParseException e) {
                    mismatchDescription.appendText("Unable to parse body: ").appendValue(e.getMessage());
                    return false;
                }
            } else if (item instanceof DocNode) {
                item = ((DocNode) item).toDeepBasicObject();
            }

            if (jsonPath != null) {
                Configuration config = Configuration.builder().options(Option.SUPPRESS_EXCEPTIONS)
                        .jsonProvider(BasicJsonPathDefaultConfiguration.JSON_PROVIDER)
                        .mappingProvider(BasicJsonPathDefaultConfiguration.MAPPING_PROVIDER).build();

                item = JsonPath.using(config).parse(item).read(jsonPath);

                if (item == null) {
                    mismatchDescription.appendText("Unable to find JSON Path: ").appendValue(jsonPath).appendText("\n\n")
                            .appendText(formatResponse(response));
                    return false;
                }
            }

            if (!(item instanceof Collection)) {
                item = Collections.singleton(item);
            }

            return matchesImpl((Collection<?>) item, mismatchDescription, response);
        }

        protected abstract boolean matchesImpl(Collection<?> collection, Description mismatchDescription, GenericRestClient.HttpResponse response);

        @Override
        public IndexMatcher butFailIfIncomplete(IndexMatcher other, int statusCode) {
            if (other instanceof UnlimitedMatcher) {
                return this;
            }

            HashMap<String, TestIndexLike> unmatched = new HashMap<>(this.indexNameMap);
            unmatched.keySet().removeAll(((AbstractIndexMatcher) other).indexNameMap.keySet());

            if (!unmatched.isEmpty()) {
                return new StatusCodeMatcher(statusCode);
            } else {
                return this.but(other);
            }
        }

        @Override
        public boolean isEmpty() {
            return indexNameMap.isEmpty();
        }

        @Override
        public int size() {
            if (!containsSearchGuardIndices) {
                return indexNameMap.size();
            } else {
                throw new RuntimeException("Size cannot be exactly specified because containsSearchGuardIndices is true");
            }
        }

        @Override
        public boolean containsSearchGuardIndices() {
            return containsSearchGuardIndices;
        }

        @Override
        public boolean containsEsInternalIndices() {
            return containsEsInternalIndices;
        }

        @Override
        public IndexMatcher aggregateTerm(String term) {
            return new TermAggregationMatcher(term, indexNameMap, containsSearchGuardIndices, containsEsInternalIndices, this);
        }

        @Override
        public boolean containsDocument(String id) {
            for (TestIndexLike indexLike : this.indexNameMap.values()) {
                if (indexLike.getDocumentIds().contains(id)) {
                    return true;
                }
            }

            return false;
        }

    }

    static class TermAggregationMatcher extends AbstractIndexMatcher implements IndexMatcher {

        private IndexMatcher base;
        private final String term;

        TermAggregationMatcher(String term, Map<String, TestIndexLike> indexNameMap, boolean containsSearchGuardIndices,
                boolean containsEsInternalIndices, AbstractIndexMatcher base) {
            super(indexNameMap, containsSearchGuardIndices, containsEsInternalIndices);
            this.term = term;
            this.base = base;
        }

        TermAggregationMatcher(String term, Map<String, TestIndexLike> indexNameMap, boolean containsSearchGuardIndices,
                boolean containsEsInternalIndices, String jsonPath, int statusCodeWhenEmpty, AbstractIndexMatcher base) {
            super(indexNameMap, containsSearchGuardIndices, containsEsInternalIndices, jsonPath, statusCodeWhenEmpty);
            this.term = term;
            this.base = base;
        }

        @Override
        public void describeTo(Description description) {
            base.describeTo(description);
        }

        @Override
        protected boolean matchesImpl(Collection<?> collection, Description mismatchDescription, GenericRestClient.HttpResponse response) {

            HashMap<String, List<String>> aggregatedDocIds = new HashMap<>();

            for (TestIndexLike indexLike : indexNameMap.values()) {
                for (Map.Entry<String, Map<String, ?>> entry : indexLike.getDocuments().entrySet()) {
                    String id = entry.getKey();
                    Map<String, ?> doc = entry.getValue();
                    String termValue = String.valueOf(doc.get(term));
                    aggregatedDocIds.computeIfAbsent(termValue, k -> new ArrayList<>()).add(id);
                }
            }

            // Flatten the collection
            collection = collection.stream().flatMap(e -> e instanceof Collection ? ((Collection<?>) e).stream() : Stream.of(e))
                    .collect(Collectors.toSet());

            int errors = 0;

            for (Object object : collection) {
                DocNode docNode = DocNode.wrap(object);
                String key = docNode.getAsString("key");

                if (key == null) {
                    mismatchDescription.appendText("Unexpected value ").appendValue(docNode).appendText("\n");
                    errors++;
                    continue;
                }

                int docCount;

                try {
                    docCount = docNode.getNumber("doc_count").intValue();
                } catch (Exception e1) {
                    mismatchDescription.appendText("Error while reading ").appendValue(key).appendText(" ").appendValue(e1.toString())
                            .appendText("\n");
                    errors++;
                    continue;
                }

                List<String> reference = aggregatedDocIds.remove(key);

                if (reference == null) {
                    mismatchDescription.appendText("Unexpected key ").appendValue(key).appendText("\n");
                    errors++;
                } else if (reference.size() != docCount) {
                    mismatchDescription.appendText("doc_count mismatch for ").appendValue(key).appendText("; got: ").appendValue(docCount)
                            .appendText("; expected: ").appendValue(reference.size());
                    errors++;
                }
            }

            if (!aggregatedDocIds.isEmpty()) {
                for (String key : aggregatedDocIds.keySet()) {
                    mismatchDescription.appendText("Missing expected key ").appendValue(key).appendText("\n");
                    errors++;
                }
            }

            return errors == 0;
        }

        @Override
        public IndexMatcher but(IndexMatcher other) {
            AbstractIndexMatcher base = (AbstractIndexMatcher) this.base.but(other);
            return new TermAggregationMatcher(this.term, base.indexNameMap, base.containsSearchGuardIndices, base.containsEsInternalIndices,
                    base.jsonPath, base.statusCodeWhenEmpty, base);
        }

        @Override
        public boolean isCoveredBy(IndexMatcher other) {
            return base.isCoveredBy(other);
        }

        @Override
        public IndexMatcher at(String jsonPath) {
            AbstractIndexMatcher base = (AbstractIndexMatcher) this.base.at(jsonPath);
            return new TermAggregationMatcher(this.term, base.indexNameMap, base.containsSearchGuardIndices, base.containsEsInternalIndices, jsonPath,
                    base.statusCodeWhenEmpty, base);
        }

        @Override
        public IndexMatcher whenEmpty(int statusCode) {
            AbstractIndexMatcher base = (AbstractIndexMatcher) this.base.whenEmpty(statusCode);
            return new TermAggregationMatcher(this.term, base.indexNameMap, base.containsSearchGuardIndices, base.containsEsInternalIndices,
                    base.jsonPath, statusCode, base);
        }

        @Override
        public boolean covers(TestIndex testIndex) {
            return base.covers(testIndex);
        }

    }

    private static String formatResponse(GenericRestClient.HttpResponse response) {
        if (response == null) {
            return "";
        }

        String start = response.getStatusCode() + " " + response.getStatusReason() + "\n";

        if (response.getContentType().startsWith("application/json")) {
            try {
                return start + response.getBodyAsDocNode().toPrettyJsonString();
            } catch (DocumentParseException | UnknownDocTypeException e) {
                return start + response.getBody();
            }
        } else {
            return start + response.getBody();
        }
    }
}
