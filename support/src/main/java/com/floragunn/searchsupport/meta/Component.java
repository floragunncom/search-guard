package com.floragunn.searchsupport.meta;

import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.Index;

import java.util.Optional;

public enum Component {

    NONE("") {
        @Override
        public Optional<Meta.IndexLikeObject> extractWriteTargetForDataStreamAlias
                (ProjectMetadata project,
                DataStreamAlias dataStreamAlias,
                ImmutableMap.Builder<String, Meta.IndexLikeObject> nameMap) {
            return Optional.ofNullable(dataStreamAlias.getWriteDataStream())
                    .map(this::indexLikeNameWithComponentSuffix)
                    .map(nameMap::get);
        }
    }, FAILURES("failures") {
        @Override
        public Optional<Meta.IndexLikeObject> extractWriteTargetForDataStreamAlias(
                ProjectMetadata project,
                DataStreamAlias dataStreamAlias,
                ImmutableMap.Builder<String, Meta.IndexLikeObject> nameMap) {
            Index writeFailureIndex = project.getIndicesLookup().get(dataStreamAlias.getAlias()).getWriteFailureIndex(project);
            return Optional.ofNullable(writeFailureIndex)
                    .map(Index::getName)
                    .map(nameMap::get);
        }
    };

    public static final String COMPONENT_SEPARATOR = "::";

    private final String componentSuffix;

    Component(String componentSuffix) {
        this.componentSuffix = componentSuffix;
    }

    public String indexLikeNameWithComponentSuffix(String indexLikeName) {
        if (this.componentSuffix.isEmpty()) {
            return indexLikeName;
        } else {
            if (indexLikeName.endsWith(COMPONENT_SEPARATOR.concat(componentSuffix))) {
                return indexLikeName;
            }
            return indexLikeName.concat(COMPONENT_SEPARATOR).concat(componentSuffix);
        }
    }

    public abstract Optional<Meta.IndexLikeObject> extractWriteTargetForDataStreamAlias(ProjectMetadata project, DataStreamAlias dataStreamAlias,
            ImmutableMap.Builder<String, Meta.IndexLikeObject> nameMap);
}
