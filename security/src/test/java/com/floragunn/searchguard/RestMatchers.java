package com.floragunn.searchguard;

import java.io.IOException;

import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient.HttpResponse;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;

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

    public static DiagnosingMatcher<HttpResponse> json(BaseMatcher<?>... subMatchers) {
        return new DiagnosingMatcher<HttpResponse>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("Content type of response body is application/json");
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
                    JsonNode jsonNode = DefaultObjectMapper.objectMapper.readTree(response.getBody());
                    boolean ok = true;

                    for (BaseMatcher<?> subMatcher : subMatchers) {
                        if (subMatcher.matches(jsonNode)) {
                        } else {
                            subMatcher.describeMismatch(jsonNode, mismatchDescription);
                            ok = false;
                        }
                    }

                    return ok;

                } catch (IOException e) {
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
                description.appendText("JSON element at ").appendValue(jsonPath).appendText(" matches ").appendDescriptionOf(subMatcher);
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof JsonNode)) {
                    mismatchDescription.appendValue(item).appendText(" is not a JsonNode");
                    return false;
                }

                Configuration config = Configuration.builder().options(Option.SUPPRESS_EXCEPTIONS).jsonProvider(new JacksonJsonNodeJsonProvider())
                        .mappingProvider(new JacksonMappingProvider()).build();

                Object value = JsonPath.using(config).parse(item).read(jsonPath);

                if (value == null) {
                    mismatchDescription.appendText("No value at " + jsonPath + " ").appendValue(item);
                    return false;
                }

                if (value instanceof JsonNode) {
                    value = new ObjectMapper().convertValue(value, Object.class);
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

}
