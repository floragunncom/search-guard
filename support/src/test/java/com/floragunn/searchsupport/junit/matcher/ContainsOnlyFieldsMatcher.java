package com.floragunn.searchsupport.junit.matcher;

import com.floragunn.codova.documents.DocNode;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

class ContainsOnlyFieldsMatcher extends TypeSafeDiagnosingMatcher<DocNode> {

    private static final Logger log = LogManager.getLogger(ContainsOnlyFieldsMatcher.class);

    private final String jsonPath;
    private final String[] fieldNames;

    public ContainsOnlyFieldsMatcher(String jsonPath, String...expectedFieldsNames) {
        this.jsonPath = requireNonNull(jsonPath);
        this.fieldNames = requireNonNull(expectedFieldsNames);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("DocNode ").appendValue(jsonPath).appendText(" should contain only the following fields ") //
            .appendValue(String.join(", ", fieldNames));
    }

    @Override
    protected boolean matchesSafely(DocNode docNode, Description mismatchDescription) {
        if(log.isDebugEnabled()) {
            log.debug("Checking id DocNode {} contain fields {} at path {}", docNode.toPrettyJsonString(), fieldNames, jsonPath);
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
            Set<String> allNodeKeys = new HashSet<>(selectedDocNode.keySet());
            for(String currentFieldName : fieldNames) {
                if (!selectedDocNode.containsKey(currentFieldName)) {
                    mismatchDescription.appendText("Node pointed by json path ").appendValue(jsonPath).appendText(" does not contain field ") //
                        .appendValue(currentFieldName);
                    return false;
                }
                allNodeKeys.remove(currentFieldName);
            }
            if(!allNodeKeys.isEmpty()) {
                mismatchDescription.appendText("The DocNode ").appendValue(jsonPath).appendText(" contains undesired fields ") //
                    .appendValue(String.join(", ", allNodeKeys));
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
