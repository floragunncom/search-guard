package com.floragunn.searchguard.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.documents.BasicJsonPathDefaultConfiguration;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format.UnknownDocTypeException;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.internal.filter.ValueNodes.JsonNode;

public class RestMatchers {
    public static DiagnosingMatcher<HttpResponse> isOk() {
        return new DiagnosingMatcher<HttpResponse>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("Response has status 200 OK");
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof HttpResponse)) {
                    mismatchDescription.appendValue(item).appendText(" is not a HttpResponse");
                    return false;
                }

                HttpResponse response = (HttpResponse) item;

                if (response.getStatusCode() == 200) {
                    return true;
                } else {
                    mismatchDescription.appendText("Status is not 200 OK: ").appendValue(item);
                    return false;
                }

            }

        };
    }

    public static DiagnosingMatcher<HttpResponse> isCreated() {
        return new DiagnosingMatcher<HttpResponse>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("Response has status 201 Created");
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof HttpResponse)) {
                    mismatchDescription.appendValue(item).appendText(" is not a HttpResponse");
                    return false;
                }

                HttpResponse response = (HttpResponse) item;

                if (response.getStatusCode() == 201) {
                    return true;
                } else {
                    mismatchDescription.appendText("Status is not 201 Created: ").appendValue(item);
                    return false;
                }

            }

        };
    }

    public static DiagnosingMatcher<HttpResponse> isNotFound() {
        return new DiagnosingMatcher<HttpResponse>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("Response has status 404 Not Found");
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof HttpResponse)) {
                    mismatchDescription.appendValue(item).appendText(" is not a HttpResponse");
                    return false;
                }

                HttpResponse response = (HttpResponse) item;

                if (response.getStatusCode() == 404) {
                    return true;
                } else {
                    mismatchDescription.appendText("Status is not 404 Not Found: ").appendValue(item);
                    return false;
                }

            }

        };
    }

    public static DiagnosingMatcher<HttpResponse> isForbidden() {
        return new DiagnosingMatcher<HttpResponse>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("Response has status 403 Forbidden");
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof HttpResponse)) {
                    mismatchDescription.appendValue(item).appendText(" is not a HttpResponse");
                    return false;
                }

                HttpResponse response = (HttpResponse) item;

                if (response.getStatusCode() == 403) {
                    return true;
                } else {
                    mismatchDescription.appendText("Status is not 403 Forbidden: ").appendValue(item);
                    return false;
                }

            }

        };
    }

    public static DiagnosingMatcher<HttpResponse> isUnauthorized() {
        return new DiagnosingMatcher<HttpResponse>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("Response has status 401 Unauthorized");
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof HttpResponse)) {
                    mismatchDescription.appendValue(item).appendText(" is not a HttpResponse");
                    return false;
                }

                HttpResponse response = (HttpResponse) item;

                if (response.getStatusCode() == 401) {
                    return true;
                } else {
                    mismatchDescription.appendText("Status is not 401 Unauthorized: ").appendValue(item);
                    return false;
                }

            }

        };
    }

    public static DiagnosingMatcher<HttpResponse> isBadRequest() {
        return new DiagnosingMatcher<HttpResponse>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("Response has status 400 Bad Request");
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof HttpResponse)) {
                    mismatchDescription.appendValue(item).appendText(" is not a HttpResponse");
                    return false;
                }

                HttpResponse response = (HttpResponse) item;

                if (response.getStatusCode() == 400) {
                    return true;
                } else {
                    mismatchDescription.appendText("Status is not 400 Bad Request: ").appendValue(item);
                    return false;
                }

            }

        };
    }

    public static DiagnosingMatcher<HttpResponse> isBadRequest(String jsonPath, String patternString) {
        return new DiagnosingMatcher<HttpResponse>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("Response has status 400 Bad Request with the value ").appendValue(patternString).appendText(" at ")
                        .appendValue(jsonPath);
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof HttpResponse)) {
                    mismatchDescription.appendValue(item).appendText(" is not a HttpResponse");
                    return false;
                }

                HttpResponse response = (HttpResponse) item;

                if (response.getStatusCode() != 400) {
                    mismatchDescription.appendText("Status is not 400 Bad Request: ").appendText("\n").appendValue(item);
                    return false;
                }

                try {
                    String value = response.getBodyAsDocNode().findSingleValueByJsonPath(jsonPath, String.class);

                    if (value == null) {
                        mismatchDescription.appendText("Could not find value at " + jsonPath).appendText("\n").appendValue(item);
                        return false;
                    }

                    Pattern pattern = Pattern.create(patternString);
                    if (pattern.test(value)) {
                        return true;
                    } else {
                        mismatchDescription.appendText("Value at " + jsonPath + " does not match ").appendValue(patternString).appendText("\n")
                                .appendValue(item);
                        return false;
                    }
                } catch (Exception e) {
                    mismatchDescription.appendText("Parsing request body failed with " + e).appendText("\n").appendValue(item);
                    return false;
                }
            }
        };
    }

    public static DiagnosingMatcher<HttpResponse> isInternalServerError() {
        return new DiagnosingMatcher<HttpResponse>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("Response has status 500 Internal Server Error");
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof HttpResponse)) {
                    mismatchDescription.appendValue(item).appendText(" is not a HttpResponse");
                    return false;
                }

                HttpResponse response = (HttpResponse) item;

                if (response.getStatusCode() == 500) {
                    return true;
                } else {
                    mismatchDescription.appendText("Status is not 500 Internal Server Error: ").appendValue(item);
                    return false;
                }

            }

        };
    }

    public static DiagnosingMatcher<HttpResponse> json(BaseMatcher<?>... subMatchers) {
        return new DiagnosingMatcher<HttpResponse>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("Got JSON");

                if (subMatchers.length > 0) {
                    description.appendText(" where ");
                }

                for (BaseMatcher<?> subMatcher : subMatchers) {
                    subMatcher.describeTo(description);
                }
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof HttpResponse)) {
                    mismatchDescription.appendValue(item).appendText(" is not a HttpResponse");
                    return false;
                }

                HttpResponse response = (HttpResponse) item;

                String contentType = response.getContentType() != null ? response.getContentType().toLowerCase() : "";

                if (!(contentType.startsWith("application/json"))) {
                    mismatchDescription.appendText("Response does not have the content type application/json: ")
                            .appendValue(response.getContentType() + "; " + response.getHeaders());
                    return false;
                }

                try {
                    DocNode docNode = response.getBodyAsDocNode();
                    boolean ok = true;

                    for (BaseMatcher<?> subMatcher : subMatchers) {
                        if (!subMatcher.matches(docNode)) {
                            subMatcher.describeMismatch(docNode, mismatchDescription);
                            mismatchDescription.appendText("\nResponse Body:\n").appendText(response.getBody());
                            ok = false;
                        }
                    }

                    return ok;

                } catch (UnknownDocTypeException | DocumentParseException e) {
                    mismatchDescription.appendText("Response cannot be parsed as JSON: " + e.toString()).appendValue(response.getBody());
                    return false;
                }
            }

        };
    }

    public static DiagnosingMatcher<JsonNode> nodeAt(String jsonPath, Matcher<?> subMatcher) {
        return new DiagnosingMatcher<JsonNode>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("element at ").appendValue(jsonPath).appendText(" matches ").appendDescriptionOf(subMatcher);
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof DocNode)) {
                    mismatchDescription.appendValue(item != null ? item.getClass() : "null").appendText(" is not a DocNode");
                    return false;
                }

                Configuration config = Configuration.builder().options(Option.SUPPRESS_EXCEPTIONS)
                        .jsonProvider(BasicJsonPathDefaultConfiguration.JSON_PROVIDER)
                        .mappingProvider(BasicJsonPathDefaultConfiguration.MAPPING_PROVIDER).build();

                Object value = JsonPath.using(config).parse(item).read(jsonPath);

                if (value == null) {
                    mismatchDescription.appendText("No value at " + jsonPath + " ").appendValue(item);
                    return false;
                }

                if (value instanceof DocNode) {
                    value = ((DocNode) value).toBasicObject();
                }

                if (subMatcher.matches(value)) {
                    return true;
                } else {
                    mismatchDescription.appendText("at " + jsonPath + ": ").appendValue(value).appendText("\n");
                    subMatcher.describeMismatch(value, mismatchDescription);
                    return false;
                }
            }

        };
    }
    
    public static DiagnosingMatcher<JsonNode> singleNodeAt(String jsonPath, Matcher<?> subMatcher) {
        return new DiagnosingMatcher<JsonNode>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("element at ").appendValue(jsonPath).appendText(" matches ").appendDescriptionOf(subMatcher);
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof DocNode)) {
                    mismatchDescription.appendValue(item != null ? item.getClass() : "null").appendText(" is not a DocNode");
                    return false;
                }

                Configuration config = Configuration.builder().options(Option.SUPPRESS_EXCEPTIONS)
                        .jsonProvider(BasicJsonPathDefaultConfiguration.JSON_PROVIDER)
                        .mappingProvider(BasicJsonPathDefaultConfiguration.MAPPING_PROVIDER).build();

                Object value = JsonPath.using(config).parse(item).read(jsonPath);

                if (value == null) {
                    mismatchDescription.appendText("No value at " + jsonPath + " ").appendValue(item);
                    return false;
                }
                
                if (value instanceof Collection) {
                    if (((Collection<?>) value).isEmpty()) {
                        value = null;
                    } else {
                        value = ((Collection<?>) value).iterator().next();                        
                    }
                }

                if (value instanceof DocNode) {
                    value = ((DocNode) value).toBasicObject();
                }

                if (subMatcher.matches(value)) {
                    return true;
                } else {
                    mismatchDescription.appendText("at " + jsonPath + ": ").appendValue(value).appendText("\n");
                    subMatcher.describeMismatch(value, mismatchDescription);
                    return false;
                }
            }

        };
    }

    public static DiagnosingMatcher<JsonNode> noValueAt(String jsonPath) {
        return new DiagnosingMatcher<JsonNode>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("element at ").appendValue(jsonPath).appendText(" has not value ");
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof DocNode)) {
                    mismatchDescription.appendValue(item != null ? item.getClass() : "null").appendText(" is not a DocNode");
                    return false;
                }

                Configuration config = Configuration.builder().options(Option.SUPPRESS_EXCEPTIONS)
                        .jsonProvider(BasicJsonPathDefaultConfiguration.JSON_PROVIDER)
                        .mappingProvider(BasicJsonPathDefaultConfiguration.MAPPING_PROVIDER).build();

                Object value = JsonPath.using(config).parse(item).read(jsonPath);

                if (value == null) {

                    return true;
                } else {
                    mismatchDescription.appendText("Unexpected non null value present at " + jsonPath + " ").appendValue(item);
                    return false;
                }
            }

        };
    }

    public static DiagnosingMatcher<JsonNode> distinctNodesAt(String jsonPath, Matcher<?> subMatcher) {
        return new DiagnosingMatcher<JsonNode>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("element at ").appendValue(jsonPath).appendText(" matches ").appendDescriptionOf(subMatcher);
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof DocNode)) {
                    mismatchDescription.appendValue(item != null ? item.getClass() : "null").appendText(" is not a DocNode");
                    return false;
                }

                Configuration config = Configuration.builder().options(Option.SUPPRESS_EXCEPTIONS)
                        .jsonProvider(BasicJsonPathDefaultConfiguration.JSON_PROVIDER)
                        .mappingProvider(BasicJsonPathDefaultConfiguration.MAPPING_PROVIDER).build();

                Object value = JsonPath.using(config).parse(item).read(jsonPath);

                if (value == null) {
                    mismatchDescription.appendText("No value at " + jsonPath + " ").appendValue(item);
                    return false;
                }

                if (value instanceof DocNode) {
                    value = ((DocNode) value).toBasicObject();
                }

                if (value instanceof Collection) {
                    value = new HashSet<Object>((Collection<?>) value);
                }

                if (subMatcher.matches(value)) {
                    return true;
                } else {
                    String valueString = value.toString();

                    if (valueString.length() < 80) {
                        mismatchDescription.appendText("at " + jsonPath + ": ").appendValue(valueString).appendText("\n");
                    } else {
                        mismatchDescription.appendText("at " + jsonPath + ": ").appendText("\n");
                    }

                    subMatcher.describeMismatch(value, mismatchDescription);
                    return false;
                }
            }

        };
    }

    public static DiagnosingMatcher<Object> matches(TestIndex... testIndices) {
        Map<String, TestIndex> indexNameMap = new HashMap<>();

        for (TestIndex testIndex : testIndices) {
            indexNameMap.put(testIndex.getName(), testIndex);
        }

        return matches(indexNameMap);
    }

    public static DiagnosingMatcher<Object> matches(String prefix, TestIndex... testIndices) {
        Map<String, TestIndex> indexNameMap = new HashMap<>();

        for (TestIndex testIndex : testIndices) {
            indexNameMap.put(prefix + ":" + testIndex.getName(), testIndex);
        }

        return matches(indexNameMap);
    }

    public static DiagnosingMatcher<Object> matches(TestIndex testIndex1, String prefix2, TestIndex testIndex2) {
        return matches(ImmutableMap.of(testIndex1.getName(), testIndex1, prefix2 + ":" + testIndex2.getName(), testIndex2));
    }

    public static DiagnosingMatcher<Object> matches(Map<String, TestIndex> indexNameMap) {

        Set<String> pendingDocuments = new HashSet<>();

        for (Map.Entry<String, TestIndex> entry : indexNameMap.entrySet()) {
            for (String id : entry.getValue().getTestData().getRetainedDocuments().keySet()) {
                pendingDocuments.add(entry.getKey() + "/" + id);
            }
        }

        return new DiagnosingMatcher<Object>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("content of indices " + indexNameMap.keySet().stream().collect(Collectors.joining(", ")));
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof Collection)) {
                    mismatchDescription.appendText("Not a collection: ").appendValue(item);
                    return false;
                }

                Collection<?> collection = (Collection<?>) item;

                for (Object object : collection) {
                    DocNode docNode = DocNode.wrap(object);
                    TestIndex index = indexNameMap.get(docNode.getAsString("_index"));

                    if (index == null) {
                        mismatchDescription.appendText("result contains unknown index: ").appendValue(docNode.getAsString("_index"))
                                .appendText("; expected: ").appendValue(indexNameMap.keySet()).appendText("\ndocument: ")
                                .appendText(docNode.toJsonString());
                        return false;
                    }

                    Map<String, ?> document = index.getTestData().getRetainedDocuments().get(docNode.getAsString("_id"));

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

        };
    }

    public static DiagnosingMatcher<Object> matchesDocCount(TestIndex... testIndices) {
        Map<String, TestIndex> indexNameMap = new HashMap<>();

        for (TestIndex testIndex : testIndices) {
            indexNameMap.put(testIndex.getName(), testIndex);
        }

        return matchesDocCount(indexNameMap);
    }

    public static DiagnosingMatcher<Object> matchesDocCount(Map<String, TestIndex> indexNameMap) {

        Set<String> pendingIndices = new HashSet<>(indexNameMap.keySet());

        return new DiagnosingMatcher<Object>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("doc count of indices " + indexNameMap.keySet().stream().collect(Collectors.joining(", ")));
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof Collection)) {
                    mismatchDescription.appendText("Not a collection: ").appendValue(item);
                    return false;
                }

                Collection<?> collection = (Collection<?>) item;

                for (Object object : collection) {
                    DocNode docNode = DocNode.wrap(object);
                    TestIndex index = indexNameMap.get(docNode.getAsString("key"));

                    if (index == null) {
                        mismatchDescription.appendText("result contains unknown index: ").appendValue(docNode.getAsString("key"))
                                .appendText("; expected: ").appendValue(indexNameMap.keySet()).appendText("\ndocument: ")
                                .appendText(docNode.toJsonString());
                        return false;
                    }

                    int expectedDocCount = index.getTestData().getSize();
                    Number actualDocCount;
                    try {
                        actualDocCount = docNode.getAsNode("doc_count").toNumber();
                    } catch (ConfigValidationException e) {
                        mismatchDescription.appendText("result doc count").appendValue(docNode.getAsNode("doc_count")).appendText(" for index ")
                                .appendValue(docNode.getAsString("key")).appendText(" is not a number; expected doc count ")
                                .appendValue(expectedDocCount);
                        return false;
                    }

                    if (actualDocCount == null || actualDocCount.intValue() != expectedDocCount) {
                        mismatchDescription.appendText("result doc count").appendValue(actualDocCount).appendText(" for index ")
                                .appendValue(docNode.getAsString("key")).appendText(" does not match expected doc count ")
                                .appendValue(expectedDocCount);
                        return false;
                    }

                    pendingIndices.remove(docNode.getAsString("key"));
                }

                if (!pendingIndices.isEmpty()) {
                    mismatchDescription.appendText("result does not contain expected indices: ").appendValue(pendingIndices);
                    return false;
                }

                return true;
            }

        };
    }

}
