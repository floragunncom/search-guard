package com.floragunn.searchsupport.junit.matcher;

import com.floragunn.codova.documents.DocNode;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import static java.util.Objects.requireNonNull;

class ContainsFieldValuePointedByJsonPathMatcher extends TypeSafeDiagnosingMatcher<DocNode> {

    private static final Logger log = LogManager.getLogger(ContainsFieldValuePointedByJsonPathMatcher.class);

    private final String jsonPath;
    private final Object fieldValue;

    public ContainsFieldValuePointedByJsonPathMatcher(String jsonPath, Object fieldValue) {
        this.jsonPath = requireNonNull(jsonPath);
        this.fieldValue = requireNonNull(fieldValue);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("DocNode should contain field ").appendValue(jsonPath)//
            .appendText(" with value ").appendValue(fieldValue);//
    }

    @Override
    protected boolean matchesSafely(DocNode docNode, Description mismatchDescription) {
        if(log.isDebugEnabled()) {
            log.debug("Checking if DocNode '{}' contain field '{}' with value '{}'", docNode.toPrettyJsonString(), jsonPath, fieldValue);
        }
        try {
            Object nodeValue = docNode.findSingleValueByJsonPath(jsonPath, fieldValue.getClass());
            if (!fieldValue.equals(nodeValue)) {
                mismatchDescription.appendText("Node pointed by json path ").appendValue(jsonPath).appendText(" does not contain value ").appendValue(
                    fieldValue).appendText(" actual value ").appendValue(nodeValue);
                return false;
            }
            return true;
        } catch (PathNotFoundException e) {
            log.debug("Patch '{}' not found in DocNode '{}'.", jsonPath, docNode, e);
            mismatchDescription.appendText(" path ").appendValue(jsonPath).appendText(" does not exists in doc node ")
                .appendValue(docNode.toJsonString());
            return false;
        }
    }
}
