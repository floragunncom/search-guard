package com.floragunn.searchsupport.junit.matcher;

import com.floragunn.codova.documents.DocNode;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class DocNodeSizeEqualToMatcher extends TypeSafeDiagnosingMatcher<DocNode> {

    private static final Logger log = LogManager.getLogger(DocNodeSizeEqualToMatcher.class);

    private final String jsonPath;
    private final int expectedSize;

    public DocNodeSizeEqualToMatcher(String jsonPath, int expectedSize) {
        this.jsonPath = Objects.requireNonNull(jsonPath);
        this.expectedSize = expectedSize;
    }

    @Override
    protected boolean matchesSafely(DocNode docNode, Description mismatchDescription) {
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
            int currentSize = selectedDocNode.size();
            if (currentSize != expectedSize) {
                String keys = selectedDocNode.keySet().stream().collect(Collectors.joining(", "));
                mismatchDescription.appendText("node size is ").appendValue(currentSize).appendText(", keys present in node ")
                    .appendValue(keys);
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
        description.appendText("Node pointed by path ").appendValue(jsonPath).appendText(" should have size equal to ")
            .appendValue(expectedSize);
    }
}
