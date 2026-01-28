/*
 * Copyright 2024 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.searchsupport.meta;

import java.util.Arrays;
import java.util.Collection;

import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.fluent.collections.UnmodifiableCollection;
import com.floragunn.searchsupport.meta.MetaImpl.DefaultMetaImpl;
import org.elasticsearch.common.logging.internal.LoggerFactoryImpl;
import org.elasticsearch.logging.internal.spi.LoggerFactory;

/**
 * Abstraction for the ES index space metadata. 
 * 
 * Provides unified, controlled and uncluttered interfaces to the ES metadata.
 */
public interface Meta extends Document<Meta> {

    String FAILURES_SUFFIX = "::failures";

    static String indexLikeNameWithFailuresSuffix(String indexLikeName) {
        if (indexLikeName == null) {
            return null;
        }
        return indexLikeName.concat(FAILURES_SUFFIX);
    }

    static String indexLikeNameWithoutFailuresSuffix(String indexLikeName) {
        if (indexLikeName == null) {
            return null;
        }
        if (indexLikeName.endsWith(FAILURES_SUFFIX)) {
            return indexLikeName.substring(0, indexLikeName.indexOf(FAILURES_SUFFIX));
        } else {
            return indexLikeName;
        }
    }

    ImmutableMap<String, IndexLikeObject> indexLikeObjects();

    ImmutableSet<Index> indices();

    ImmutableSet<Alias> aliases();

    ImmutableSet<DataStream> dataStreams();

    /**
     * Returns both aliases and dataStreams
     */
    ImmutableSet<IndexCollection> indexCollections();

    /**
     * Returns indices that are not contained in an alias or data stream
     */
    ImmutableSet<Index> indicesWithoutParents();

    /**
     * Returns indices that are not hidden 
     */
    ImmutableSet<Index> nonHiddenIndices();

    /**
     * Returns indices that are not hidden and which are not contained in an alias or data stream
     */
    ImmutableSet<Index> nonHiddenIndicesWithoutParents();

    ImmutableSet<Alias> nonHiddenAliases();

    ImmutableSet<DataStream> nonHiddenDataStreams();

    /**
     * Returns indices that are not system indices (but may be possibly hidden indices)
     */
    ImmutableSet<Index> nonSystemIndices();

    /**
     * Returns indices that are not contained in an alias or data stream and that are not system indices (but may be possibly hidden indices)
     */
    ImmutableSet<Index> nonSystemIndicesWithoutParents();

    IndexLikeObject getIndexOrLike(String name);

    boolean equals(Object other);

    int hashCode();

    Mock.AliasBuilder alias(String aliasName);

    Mock.DataStreamBuilder dataStream(String dataStreamName);

    org.elasticsearch.cluster.metadata.Metadata esMetadata();

    long version();

    interface IndexLikeObject extends Document<IndexLikeObject> {
        /**
         * Returns just the name for all indices as well as aliases and data streams representing the data component.
         * For aliases and data streams representing the failure component, it returns the name suffixed with the {@link Meta#FAILURES_SUFFIX}.
         */
        String name();

        /**
         * Returns just the name, always without the {@link Meta#FAILURES_SUFFIX}) suffix.
         */
        default String nameForIndexPatternMatching() {
            return indexLikeNameWithoutFailuresSuffix(name());
        }

        ImmutableSet<IndexOrNonExistent> resolveDeep(Alias.ResolutionMode resolutionMode);

        ImmutableSet<String> resolveDeepToNames(Alias.ResolutionMode resolutionMode);

        /**
         * Like {@link #resolveDeepToNames(Alias.ResolutionMode)}, but it always returns all names without the {@link Meta#FAILURES_SUFFIX} suffix.
         */
        default ImmutableSet<String> resolveDeepToNamesForIndexPatternMatching(Alias.ResolutionMode resolutionMode) {
            return resolveDeepToNames(resolutionMode).map(Meta::indexLikeNameWithoutFailuresSuffix);
        }

        ImmutableSet<Alias> parentAliases();

        DataStream parentDataStream();

        /**
         * Returns the name of the parent data stream, which may include the {@link Meta#FAILURES_SUFFIX} suffix if the data stream represents the failure component.
         */
        String parentDataStreamName();

        /**
         * Like {@link #parentDataStreamName()}}, but it always returns the name without the {@link Meta#FAILURES_SUFFIX} suffix.
         */
        default String parentDataStreamNameForIndexPatternMatching() {
            return indexLikeNameWithoutFailuresSuffix(parentDataStreamName());
        }

        /**
         * Returns the names of the aliases containing this index. The names may include the {@link Meta#FAILURES_SUFFIX} suffix if the alias represents the failure component.
         */
        Collection<String> parentAliasNames();

        /**
         * Returns the names of the aliases containing this index. Additionally, if this is a data stream backing or failure index, this also returns any aliases containing the data stream.
         * The names may include the {@link Meta#FAILURES_SUFFIX} suffix if the alias points to the data stream that represent the failure component.
         */
        Collection<String> ancestorAliasNames();

        /**
         * Like {@link #ancestorAliasNames()}}, but it always returns the names without the {@link Meta#FAILURES_SUFFIX} suffix.
         */
        default Collection<String> ancestorAliasNamesForIndexPatternMatching() {
            return ancestorAliasNames().stream().map(Meta::indexLikeNameWithoutFailuresSuffix).toList();
        }

        boolean equals(Object other);

        int hashCode();

        boolean isHidden();

        boolean exists();

        static ImmutableSet<Index> resolveDeep(ImmutableSet<? extends Meta.IndexLikeObject> objects) {
            return resolveDeep(objects, Alias.ResolutionMode.NORMAL);
        }

        static ImmutableSet<Index> resolveDeep(ImmutableSet<? extends Meta.IndexLikeObject> objects, Alias.ResolutionMode resolutionMode) {
            if (objects.size() == 0) {
                return ImmutableSet.empty();
            }

            if (objects.size() == 1) {
                Meta.IndexLikeObject object = objects.only();

                if (object instanceof Meta.Index) {
                    return ImmutableSet.of((Meta.Index) object);
                } else if (object instanceof Meta.IndexCollection) {
                    return ((Meta.IndexCollection) object).resolveDeepAsIndex(resolutionMode);
                }
            }

            ImmutableSet.Builder<Meta.Index> result = new ImmutableSet.Builder<>(objects.size() * 20);

            for (Meta.IndexLikeObject object : objects) {
                if (object instanceof Meta.Index) {
                    result.add((Meta.Index) object);
                } else if (object instanceof Meta.IndexCollection) {
                    result.addAll(((Meta.IndexCollection) object).resolveDeepAsIndex(resolutionMode));
                }
            }

            return result.build();
        }
    }

    interface Index extends IndexOrNonExistent {
        boolean isOpen();

        boolean isSystem();

        boolean isDataStreamBackingIndex();
        
        static Index nonExistent(String name) {
            return new MetaImpl.NonExistentIndexImpl(name);
        }
    }

    interface IndexCollection extends IndexLikeObject {
        UnmodifiableCollection<IndexLikeObject> members();

        ImmutableSet<Index> resolveDeepAsIndex(Alias.ResolutionMode resolutionMode);

        static ImmutableSet<Index> resolveDeep(ImmutableSet<? extends Meta.IndexCollection> aliasesAndDataStreams,
                Alias.ResolutionMode resolutionMode) {
            if (aliasesAndDataStreams.size() == 0) {
                return ImmutableSet.empty();
            }

            if (aliasesAndDataStreams.size() == 1) {
                return aliasesAndDataStreams.only().resolveDeepAsIndex(resolutionMode);
            }

            ImmutableSet.Builder<Index> result = new ImmutableSet.Builder<>(aliasesAndDataStreams.size() * 20);

            for (Meta.IndexCollection object : aliasesAndDataStreams) {
                result.addAll(object.resolveDeepAsIndex(resolutionMode));
            }

            return result.build();
        }
    }

    interface Alias extends IndexCollection {
        IndexLikeObject writeTarget();

        UnmodifiableCollection<IndexLikeObject> resolve(ResolutionMode resolutionMode);

        static enum ResolutionMode {
            NORMAL, TO_WRITE_TARGET
        }

        static Alias nonExistent(String name) {
            return new MetaImpl.NonExistentAliasImpl(name);
        }
    }

    interface DataStream extends IndexCollection {
        static DataStream nonExistent(String name) {
            return new MetaImpl.NonExistentDataStreamImpl(name);
        }
    }

    interface NonExistent extends IndexOrNonExistent {
        static NonExistent of(String name) {
            return new MetaImpl.NonExistentImpl(name);
        }

        static final NonExistent BLANK = of("_");
        static final NonExistent STAR = of("*");
    }

    interface IndexOrNonExistent extends IndexLikeObject {

    }

    static Meta from(org.elasticsearch.cluster.metadata.Metadata esMetadata) {
        return new DefaultMetaImpl(esMetadata);
    }

    static Meta from(org.elasticsearch.cluster.service.ClusterService clusterService) {
        return DefaultMetaImpl.from(clusterService);
    }

    /**
     * For testing and mocking purposes
     */
    interface Mock {

        static Meta indices(String... indexNames) {
            initLogging();
            return DefaultMetaImpl.indices(indexNames);
        }

        static AliasBuilder alias(String aliasName) {
            initLogging();
            return new DefaultMetaImpl.AliasBuilderImpl(aliasName);
        }

        static DataStreamBuilder dataStream(String dataStreamName) {
            initLogging();
            return new DefaultMetaImpl.DataStreamBuilderImpl(dataStreamName);
        }

        private static void initLogging() {
            LoggerFactoryImpl factory = new LoggerFactoryImpl();
            LoggerFactory.setInstance(factory);
        }

        /**
         * For testing and mocking purposes
         */
        static interface AliasBuilder {
            Meta of(String... indexNames);

            default void validateIndexNames(String aliasName, String... indexNames) {
                boolean aliasWithDataComponent = ! aliasName.endsWith(FAILURES_SUFFIX);
                if (aliasWithDataComponent) {
                    if (Arrays.stream(indexNames).anyMatch(index -> index.endsWith(FAILURES_SUFFIX))) {
                        throw new RuntimeException("An alias representing the data component cannot have members whose names end with: " + FAILURES_SUFFIX);
                    } else if (Arrays.stream(indexNames).anyMatch(index -> ! index.endsWith(FAILURES_SUFFIX))) {
                        throw new RuntimeException("An alias representing the failure component must have members whose names end with: " + FAILURES_SUFFIX);
                    }
                }
            }
        }

        /**
         * For testing and mocking purposes
         */
        static interface DataStreamBuilder {
            Meta of(String... indexNames);

            default void validateIndexNames(String dataStreamName, String... indexNames) {
                boolean dataStreamWithDataComponent = ! dataStreamName.endsWith(FAILURES_SUFFIX);
                if (dataStreamWithDataComponent) {
                    if (Arrays.stream(indexNames).anyMatch(index -> ! index.startsWith(".ds"))) {
                        throw new RuntimeException("A data stream representing the data component must have indices whose names start with '.ds'");
                    }
                } else if (Arrays.stream(indexNames).anyMatch(index -> ! index.startsWith(".fs"))) {
                    throw new RuntimeException("A data stream representing the failure component must have indices whose names start with '.fs'");
                }
            }
        }
    }
}