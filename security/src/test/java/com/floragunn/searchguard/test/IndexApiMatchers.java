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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
    public static IndexMatcher containsExactly(TestIndexLike... testIndices) {
        Map<String, TestIndexLike> indexNameMap = new HashMap<>();

        for (TestIndexLike testIndex : testIndices) {
            indexNameMap.put(testIndex.getName(), testIndex);
        }

        return containsExactly(indexNameMap);
    }

    public static IndexMatcher containsExactly(Map<String, TestIndexLike> indexNameMap) {
        return new ContainsExactlyMatcher(indexNameMap);
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

    public static IndexMatcher limitedToNone() {
        return new LimitedToMatcher(Collections.emptyMap());
    }

    public static class ContainsExactlyMatcher extends AbstractIndexMatcher implements IndexMatcher {

        public ContainsExactlyMatcher(Map<String, TestIndexLike> indexNameMap) {
            super(indexNameMap);
        }

        public ContainsExactlyMatcher(Map<String, TestIndexLike> indexNameMap, String jsonPath, int statusCodeWhenEmpty) {
            super(indexNameMap, jsonPath, statusCodeWhenEmpty);
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

            for (Object object : collection) {

                DocNode docNode = DocNode.wrap(object);
                TestIndexLike index = indexNameMap.get(docNode.getAsString("_index"));

                if (index == null) {
                    mismatchDescription.appendText("result contains unknown index: ").appendValue(docNode.getAsString("_index"))
                            .appendText("; expected: ").appendValue(indexNameMap.keySet()).appendText("\ndocument: ")
                            .appendText(docNode.toJsonString());
                    return false;
                }

                Map<String, ?> document = index.getDocuments().get(docNode.getAsString("_id"));

                if (document == null) {
                    mismatchDescription.appendText("result contains unknown document id ").appendValue(docNode.getAsString("_id"))
                            .appendText(" for index ").appendValue(docNode.getAsString("_index")).appendText("\ndocument: ")
                            .appendText(docNode.toJsonString());
                    return false;
                }

                if (!document.equals(docNode.get("_source"))) {
                    mismatchDescription.appendText("result document ").appendValue(docNode.getAsString("_id")).appendText(" in index ")
                            .appendValue(docNode.getAsString("_index")).appendText(" does not match expected document:\n")
                            .appendText(docNode.getAsNode("_source").toJsonString()).appendText("\n")
                            .appendText(DocNode.wrap(document).toJsonString());
                    return false;
                }

                pendingDocuments.remove(docNode.getAsString("_index") + "/" + docNode.getAsString("_id"));

            }

            if (!pendingDocuments.isEmpty()) {
                mismatchDescription.appendText("result does not contain expected documents: ").appendValue(pendingDocuments);
                return false;
            }

            return true;
        }

        protected boolean matchesByIndices(Collection<?> collection, Description mismatchDescription, GenericRestClient.HttpResponse response) {
            ImmutableSet<String> expectedIndices = this.getExpectedIndices();
            ImmutableSet.Builder<String> seenIndicesBuilder = new ImmutableSet.Builder<String>(expectedIndices.size());

            for (Object object : collection) {
                seenIndicesBuilder.add(object.toString());
            }

            ImmutableSet<String> seenIndices = seenIndicesBuilder.build();

            ImmutableSet<String> unexpectedIndices = seenIndices.without(expectedIndices);
            ImmutableSet<String> missingIndices = expectedIndices.without(seenIndices);

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
                return new ContainsExactlyMatcher(ImmutableMap.of(this.indexNameMap).intersection(((LimitedToMatcher) other).getExpectedIndices()),
                        this.jsonPath, this.statusCodeWhenEmpty);
            } else if (other instanceof ContainsExactlyMatcher) {
                return new ContainsExactlyMatcher(
                        ImmutableMap.of(this.indexNameMap).intersection(((ContainsExactlyMatcher) other).getExpectedIndices()), this.jsonPath,
                        this.statusCodeWhenEmpty);
            } else if (other instanceof UnlimitedMatcher) {
                return this;
            } else {
                throw new RuntimeException("Unexpected argument " + other);
            }
        }

        @Override
        public IndexMatcher at(String jsonPath) {
            return new ContainsExactlyMatcher(indexNameMap, jsonPath, statusCodeWhenEmpty);
        }

        @Override
        public IndexMatcher whenEmpty(int statusCode) {
            return new ContainsExactlyMatcher(indexNameMap, jsonPath, statusCode);
        }
    }

    public static class LimitedToMatcher extends AbstractIndexMatcher implements IndexMatcher {

        public LimitedToMatcher(Map<String, TestIndexLike> indexNameMap) {
            super(indexNameMap);
        }

        public LimitedToMatcher(Map<String, TestIndexLike> indexNameMap, String jsonPath, int statusCodeWhenEmpty) {
            super(indexNameMap, jsonPath, statusCodeWhenEmpty);
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
                        ImmutableMap.of(this.indexNameMap).intersection(((ContainsExactlyMatcher) other).getExpectedIndices()), this.jsonPath,
                        this.statusCodeWhenEmpty);
            } else if (other instanceof UnlimitedMatcher) {
                return this;
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
            return new ContainsExactlyMatcher(indexNameMap, jsonPath, statusCodeWhenEmpty);
        }

        @Override
        public IndexMatcher whenEmpty(int statusCode) {
            return new ContainsExactlyMatcher(indexNameMap, jsonPath, statusCode);
        }

    }

    public static class UnlimitedMatcher extends DiagnosingMatcher<Object> implements IndexMatcher {

        public UnlimitedMatcher() {
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

    }

    public static interface IndexMatcher extends Matcher<Object> {
        IndexMatcher but(IndexMatcher other);

        IndexMatcher butFailIfIncomplete(IndexMatcher other, int statusCode);

        IndexMatcher at(String jsonPath);

        IndexMatcher whenEmpty(int statusCode);
        
        boolean isEmpty();

        default IndexMatcher butForbiddenIfIncomplete(IndexMatcher other) {
            return butFailIfIncomplete(other, 403);
        }

    }

    static abstract class AbstractIndexMatcher extends DiagnosingMatcher<Object> implements IndexMatcher {
        protected final Map<String, TestIndexLike> indexNameMap;
        protected final String jsonPath;
        protected final int statusCodeWhenEmpty;

        AbstractIndexMatcher(Map<String, TestIndexLike> indexNameMap) {
            this.indexNameMap = indexNameMap;
            this.jsonPath = null;
            this.statusCodeWhenEmpty = 200;
        }

        AbstractIndexMatcher(Map<String, TestIndexLike> indexNameMap, String jsonPath, int statusCodeWhenEmpty) {
            this.indexNameMap = indexNameMap;
            this.jsonPath = jsonPath;
            this.statusCodeWhenEmpty = statusCodeWhenEmpty;
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
                return this;
            }
        }
        
        @Override
        public boolean isEmpty() {
            return indexNameMap.isEmpty();
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
