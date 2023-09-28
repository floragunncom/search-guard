package com.floragunn.searchsupport.junit.matcher;

import com.floragunn.codova.documents.DocNode;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import static java.util.Objects.requireNonNull;

class FieldIsNullMatcher extends TypeSafeDiagnosingMatcher<DocNode> {

    private static final Logger log = LogManager.getLogger(FieldIsNullMatcher.class);

    private final String jsonPath;

    public FieldIsNullMatcher(String jsonPath) {
        this.jsonPath = requireNonNull(jsonPath, "Json path is required.");
    }

    @Override
    protected boolean matchesSafely(DocNode docNode, Description mismatchDescription) {
        try {
            Object fieldValue = docNode.findSingleValueByJsonPath(jsonPath, Object.class);
            if (fieldValue != null) {
                mismatchDescription.appendText(" field is not null and has assigned value ").appendValue(fieldValue);
                return false;
            }
            return true;
        } catch (PathNotFoundException e) {
            log.debug("Patch {} not found in DocNode {}.", jsonPath, docNode, e);
            mismatchDescription.appendText(" path ").appendValue(jsonPath).appendText(" does not exists in doc node ")
                .appendValue(docNode.toJsonString());
            return false;
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Field pointed by json path ").appendValue(jsonPath).appendText(" is null");
    }
}
