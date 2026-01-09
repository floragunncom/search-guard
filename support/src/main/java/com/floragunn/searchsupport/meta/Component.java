package com.floragunn.searchsupport.meta;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.Index;

import java.util.Arrays;
import java.util.Optional;

public enum Component {

    NONE("", ImmutableList.of("", "data")) {
        @Override
        public Optional<Meta.IndexLikeObject> extractWriteTargetForDataStreamAlias
                (ProjectMetadata project,
                DataStreamAlias dataStreamAlias,
                ImmutableMap.Builder<String, Meta.IndexLikeObject> nameMap) {
            return Optional.ofNullable(dataStreamAlias.getWriteDataStream())
                    .map(this::indexLikeNameWithComponentSuffix)
                    .map(nameMap::get);
        }
    }, FAILURES("failures", ImmutableList.of("failures")) {
        @Override
        public Optional<Meta.IndexLikeObject> extractWriteTargetForDataStreamAlias(
                ProjectMetadata project,
                DataStreamAlias dataStreamAlias,
                ImmutableMap.Builder<String, Meta.IndexLikeObject> nameMap) {
            Index writeFailureIndex = project.getIndicesLookup().get(dataStreamAlias.getAlias()).getWriteFailureIndex(project);
            return Optional.ofNullable(writeFailureIndex)
                    .map(Index::getName)
                    .map(this::indexLikeNameWithComponentSuffix)
                    .map(nameMap::get);
        }
    };

    public static final String COMPONENT_SEPARATOR = "::";

    private final String componentSuffix;
    private final ImmutableList<String> coveredSuffixes;

    Component(String componentSuffix, ImmutableList<String> coveredSuffixes) {
        this.componentSuffix = componentSuffix;
        this.coveredSuffixes = coveredSuffixes;
    }

    public static Component getBySuffix(String componentSuffix) {
        return Arrays.stream(values())
                .filter(component -> component.coveredSuffixes.contains(componentSuffix))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Unknown component suffix: " + componentSuffix));
    }

    public String indexLikeNameWithComponentSuffix(String indexLikeName) {
        if (indexLikeName == null || this.componentSuffix.isEmpty()) {
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
