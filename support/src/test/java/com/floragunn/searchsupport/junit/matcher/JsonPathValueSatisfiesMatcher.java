package com.floragunn.searchsupport.junit.matcher;

import com.floragunn.codova.documents.DocNode;
import com.jayway.jsonpath.PathNotFoundException;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import static java.util.Objects.requireNonNull;

class JsonPathValueSatisfiesMatcher<T> extends TypeSafeDiagnosingMatcher<DocNode> {

    private final String jsonPath;
    private final Matcher<T> matcher;
    private final Class<T> clazz;

    public JsonPathValueSatisfiesMatcher(String jsonPath, Matcher<T> matcher, Class<T> clazz) {
        this.jsonPath = requireNonNull(jsonPath);
        this.matcher = requireNonNull(matcher);
        this.clazz = requireNonNull(clazz);
    }

    @Override
    protected boolean matchesSafely(DocNode docNode, Description mismatchDescription) {
        try {
            T value = docNode.findSingleValueByJsonPath(jsonPath, clazz);
            if (! matcher.matches(value)) {
                matcher.describeMismatch(value, mismatchDescription);
                return false;
            }
        } catch (PathNotFoundException e) {
            mismatchDescription.appendText(" DocNode ").appendText(docNode.toJsonString())//
                .appendText(" does not contain property pointed out by path ").appendValue(jsonPath);
            return false;
        }
        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(" value pointed by json path ").appendValue(jsonPath).appendText(" must be ");
        matcher.describeTo(description);
    }
}
