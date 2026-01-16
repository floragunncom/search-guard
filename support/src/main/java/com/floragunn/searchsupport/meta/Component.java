package com.floragunn.searchsupport.meta;

import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.Index;

import java.util.Optional;

public enum Component {

    NONE("") , FAILURES("failures");

    public static final String COMPONENT_SEPARATOR = "::";

    private final String componentSuffix;

    Component(String componentSuffix) {
        this.componentSuffix = componentSuffix;
    }

    public String indexLikeNameWithComponentSuffix(String indexLikeName) {
        // TODO CS: this method should be removed
        if (this.componentSuffix.isEmpty()) {
            return indexLikeName;
        } else {
            if (indexLikeName.endsWith(COMPONENT_SEPARATOR.concat(componentSuffix))) {
                return indexLikeName;
            }
            return indexLikeName.concat(COMPONENT_SEPARATOR).concat(componentSuffix);
        }
    }
}
