package com.floragunn.searchsupport.junit.matcher;

import com.floragunn.codova.documents.DocNode;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.List;

import static java.util.Objects.requireNonNull;

class ContainsFieldPointedByJsonPathMatcher extends TypeSafeDiagnosingMatcher<DocNode> {

    private static final Logger log = LogManager.getLogger(ContainsFieldPointedByJsonPathMatcher.class);

    private final String jsonPath;
    private final String fieldName;

    public ContainsFieldPointedByJsonPathMatcher(String jsonPath, String fieldName) {
        this.jsonPath = requireNonNull(jsonPath);
        this.fieldName = requireNonNull(fieldName);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("DocNode should contain field ").appendValue(fieldName)//
            .appendText(" pointed out by json path ").appendValue(jsonPath);//
    }

    @Override
    protected boolean matchesSafely(DocNode docNode, Description mismatchDescription) {
        if(log.isDebugEnabled()) {
            log.debug("Checking id DocNode {} contain field {} at path {}", docNode.toPrettyJsonString(), fieldName, jsonPath);
        }
        try {
            List<DocNode> nodes = docNode.findNodesByJsonPath(jsonPath);
            int numberOfNodes = nodes.size();
            if (numberOfNodes != 1) {
                mismatchDescription.appendText("Found ")
                    .appendValue(numberOfNodes)
                    .appendText(" nodes pointed by json path ")
                    .appendValue(jsonPath)
                    .appendText(" whereas expected number of nodes is 1.");
                return false;
            }
            DocNode selectedDocNode = nodes.get(0);
            if (!selectedDocNode.containsKey(fieldName)) {
                mismatchDescription.appendText("Node pointed by json path ").appendValue(jsonPath).appendText(" does not contain field ").appendValue(fieldName);
                return false;
            }
            return true;
        } catch (PathNotFoundException e) {
            log.debug("Patch {} not found in DocNode {}.", jsonPath, docNode, e);
            mismatchDescription.appendText("path ").appendValue(jsonPath).appendText(" does not exists in doc node ")
                .appendValue(docNode.toJsonString());
            return false;
        }
    }
}
