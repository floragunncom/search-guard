package com.floragunn.searchsupport.meta;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.Index;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
                    .map(this::indexLikeNameWithComponentSuffix)
                    .map(nameMap::get);
        }
    };



    public static final String COMPONENT_SEPARATOR = "::";

    private static final ImmutableMap<String, Component> SUFFIX_TO_COMPONENT = createSuffixToComponentMap();

    private static ImmutableMap<String, Component> createSuffixToComponentMap() {
        ImmutableMap.Builder<String, Component> mapBuilder = new ImmutableMap.Builder<>();
        for (Component c : ImmutableList.ofArray(Component.values()).without(Collections.singleton(NONE))) {
            mapBuilder.put(c.getComponentSuffixWithSeparator(), c);
        }
        return mapBuilder.build();
    }

    private final String componentSuffix;

    Component(String componentSuffix) {
        this.componentSuffix = componentSuffix;
    }

    public String getComponentSuffixWithSeparator() {
        return componentSuffix.isEmpty() ? "" : COMPONENT_SEPARATOR + componentSuffix;
    }

    public static Component extractComponent(String expression) {
        // Currently the map SUFFIX_TO_COMPONENT contains only one element, but this future-proof the code e.g. when new enum constant is added
        for(Map.Entry<String, Component> entry : SUFFIX_TO_COMPONENT.entrySet()) {
            if (expression.endsWith(entry.getKey())) {
                return entry.getValue();
            }
        }
        return NONE;
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
