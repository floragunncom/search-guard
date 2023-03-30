package com.floragunn.searchsupport.junit.matcher;

import com.floragunn.codova.documents.DocNode;
import com.jayway.jsonpath.PathNotFoundException;
import org.elasticsearch.common.Strings;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.Objects;

class FieldContainSubstringMatcher extends TypeSafeDiagnosingMatcher<DocNode> {

    private final String jsonPath;
    private final String substring;

    public FieldContainSubstringMatcher(String jsonPath, String substring) {
        this.jsonPath = Objects.requireNonNull(jsonPath, "Json path is required");
        this.substring = Objects.requireNonNull(substring, "Substring is required");
    }

    @Override
    protected boolean matchesSafely(DocNode docNode, Description mismatchDescription) {
        try {
            String textFieldValue = docNode.findSingleValueByJsonPath(jsonPath, String.class);
            if (textFieldValue == null) {
                mismatchDescription.appendText(" the field is null");
                return false;
            }
            if (Strings.isEmpty(textFieldValue)) {
                mismatchDescription.appendText(" the field is empty");
                return false;
            }
            if (!textFieldValue.contains(substring)) {
                mismatchDescription.appendText(" the field with value ").appendValue(textFieldValue)//
                    .appendText(" does not contain desired substring");
                return false;
            }
        } catch (PathNotFoundException e) {
            mismatchDescription.appendText("DocNode ").appendText(docNode.toJsonString())//
                .appendText(" does not contain property pointed out by path ").appendValue(jsonPath);
            return false;
        }
        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("DocNode should contain substring ").appendValue(substring).appendText(" in field pointed out by path ")//
            .appendValue(jsonPath);
    }
}
