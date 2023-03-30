package com.floragunn.searchsupport.junit.matcher;

import com.floragunn.codova.documents.DocNode;
import org.hamcrest.Matcher;

public class DocNodeMatchers {

    private DocNodeMatchers(){}

    public static Matcher<DocNode> containsFieldPointedByJsonPath(String jsonPath, String fieldName) {
        return new ContainsFieldPointedByJsonPathMatcher(jsonPath, fieldName);
    }

    public static Matcher<DocNode> docNodeSizeEqualTo(String jsonPath, int expectedSize) {
        return new DocNodeSizeEqualToMatcher(jsonPath, expectedSize);
    }

    public static Matcher<DocNode> containsValue(String jsonPath, Object value) {
        return new ContainsFieldValuePointedByJsonPathMatcher(jsonPath, value);
    }

    public static Matcher<DocNode> containSubstring(String jsonPath, String desiredSubstring) {
        return new FieldContainSubstringMatcher(jsonPath, desiredSubstring);
    }
}
