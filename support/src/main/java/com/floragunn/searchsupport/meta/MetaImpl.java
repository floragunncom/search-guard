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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.fluent.collections.UnmodifiableCollection;

import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.ProjectMetadata;

import org.elasticsearch.index.IndexMode;

public abstract class MetaImpl implements Meta {
    private static final Logger log = LogManager.getLogger(MetaImpl.class);

    public static class IndexImpl extends AbstractIndexLike<IndexImpl> implements Meta.Index {
        private final boolean open;
        private final boolean system;

        public IndexImpl(DefaultMetaImpl root, String name, Collection<String> parentAliasNames, String parentDataStreamName, boolean hidden,
                boolean system, org.elasticsearch.cluster.metadata.IndexMetadata.State state) {
            super(root, name, parentAliasNames, parentDataStreamName, hidden);
            this.open = state == org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN;
            this.system = system;
        }

        IndexImpl(DefaultMetaImpl root, String name, Collection<String> parentAliasNames, String parentDataStreamName) {
            this(root, name, parentAliasNames, parentDataStreamName, false, false, org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN);
        }

        @Override
        protected ImmutableSet<Meta.IndexOrNonExistent> resolveDeepImpl(Alias.ResolutionMode resolutionMode) {
            return ImmutableSet.of(this);
        }

        @Override
        protected ImmutableSet<String> resolveDeepToNamesImpl(Alias.ResolutionMode resolutionMode) {
            return ImmutableSet.of(this.name());
        }

        @Override
        protected AbstractIndexLike<IndexImpl> withAlias(String alias) {
            return new IndexImpl(null, name(), ImmutableSet.of(this.parentAliasNames()).with(alias), parentDataStreamName(), isHidden(), system,
                    this.open ? org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN
                            : org.elasticsearch.cluster.metadata.IndexMetadata.State.CLOSE);
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public boolean isSystem() {
            return system;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            } else if (other instanceof Meta.Index) {
                return ((Meta.Index) other).name().equals(this.name());
            } else {
                return false;
            }
        }

        @Override
        protected IndexImpl copy() {
            return new IndexImpl(null, name(), parentAliasNames(), parentDataStreamName(), isHidden(), system,
                    open ? org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN
                            : org.elasticsearch.cluster.metadata.IndexMetadata.State.CLOSE);
        }

        @Override
        public Collection<String> ancestorAliasNames() {
            if (this.parentDataStreamName() == null) {
                return this.parentAliasNames();
            } else {
                Collection<String> dataStreamParentAliases = this.parentDataStream().parentAliasNames();

                if (this.parentAliasNames().isEmpty()) {
                    return dataStreamParentAliases;
                } else {
                    return ImmutableList.concat(dataStreamParentAliases, this.parentAliasNames());
                }
            }
        }

        @Override
        public Object toBasicObject() {
            return DocNode.of("name", this.name(), "open", open, "system", isSystem(), "hidden", isHidden());
        }

        @Override
        public boolean isDataStreamBackingIndex() {
            return parentDataStreamName() != null;
        }

    }

    public static class AliasImpl extends AbstractIndexCollection<AliasImpl> implements Meta.Alias {
        private final IndexLikeObject writeTarget;
        private final ImmutableSet<IndexLikeObject> writeTargetAsSet;

        public AliasImpl(DefaultMetaImpl root, String name, UnmodifiableCollection<IndexLikeObject> members, boolean hidden,
                IndexLikeObject writeTarget) {
            super(root, name, ImmutableSet.empty(), null, members, hidden);
            this.writeTarget = writeTarget;
            this.writeTargetAsSet = writeTarget != null ? ImmutableSet.of(writeTarget) : ImmutableSet.empty();
        }

        @Override
        protected AbstractIndexLike<AliasImpl> withAlias(String alias) {
            throw new RuntimeException("Aliases cannot point to aliases");
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            } else if (other instanceof Meta.Alias) {
                return ((Meta.Alias) other).name().equals(this.name());
            } else {
                return false;
            }
        }

        @Override
        protected AliasImpl copy() {
            return new AliasImpl(null, name(), members(), isHidden(), writeTarget);
        }

        @Override
        public Collection<String> parentAliasNames() {
            return ImmutableSet.empty();
        }

        @Override
        public Collection<String> ancestorAliasNames() {
            return ImmutableSet.empty();
        }

        @Override
        public Object toBasicObject() {
            return DocNode.of("name", this.name(), "members", members(), "hidden", isHidden());
        }

        @Override
        public IndexLikeObject writeTarget() {
            return writeTarget;
        }

        @Override
        public UnmodifiableCollection<IndexLikeObject> resolve(ResolutionMode resolutionMode) {
            if (resolutionMode == ResolutionMode.TO_WRITE_TARGET) {
                return writeTargetAsSet;
            } else {
                return members();
            }
        }

        @Override
        public ImmutableSet<Meta.Index> resolveDeepAsIndex(Alias.ResolutionMode resolutionMode) {
            if (resolutionMode == Alias.ResolutionMode.TO_WRITE_TARGET) {
                if (writeTarget == null) {
                    return ImmutableSet.empty();
                } else if (writeTarget instanceof Meta.Index) {
                    @SuppressWarnings({ "unchecked", "rawtypes" }) // TODO
                    ImmutableSet<Index> result = (ImmutableSet<Index>) (ImmutableSet) writeTargetAsSet;
                    return result;
                } else if (writeTarget instanceof Meta.DataStream) {
                    return ((Meta.DataStream) writeTarget).resolveDeepAsIndex(resolutionMode);
                } else {
                    return super.resolveDeepAsIndex(resolutionMode);
                }
            } else {
                return super.resolveDeepAsIndex(resolutionMode);
            }
        }

        @Override
        protected ImmutableSet<String> resolveDeepToNamesImpl(Alias.ResolutionMode resolutionMode) {
            if (resolutionMode == Alias.ResolutionMode.TO_WRITE_TARGET) {
                if (writeTarget == null) {
                    return ImmutableSet.empty();
                } else if (writeTarget instanceof Meta.Index) {
                    return ImmutableSet.of(writeTarget.name());
                } else if (writeTarget instanceof Meta.DataStream) {
                    return writeTarget.resolveDeepToNames(resolutionMode);
                } else {
                    return super.resolveDeepToNamesImpl(resolutionMode);
                }
            } else {
                return super.resolveDeepToNamesImpl(resolutionMode);
            }
        }
    }

    public static class DataStreamImpl extends AbstractIndexCollection<DataStreamImpl> implements Meta.DataStream {
        public DataStreamImpl(DefaultMetaImpl root, String name, Collection<String> parentAliasNames, UnmodifiableCollection<IndexLikeObject> members,
                boolean hidden) {
            super(root, name, parentAliasNames, null, members, hidden);
        }

        @Override
        protected AbstractIndexLike<DataStreamImpl> withAlias(String alias) {
            return new DataStreamImpl(null, name(), ImmutableSet.of(this.parentAliasNames()).with(alias), members(), isHidden());
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            } else if (other instanceof Meta.DataStream) {
                return ((Meta.DataStream) other).name().equals(this.name());
            } else {
                return false;
            }
        }

        @Override
        protected DataStreamImpl copy() {
            return new DataStreamImpl(null, name(), parentAliasNames(), members(), isHidden());
        }

        @Override
        public Collection<String> ancestorAliasNames() {
            return parentAliasNames();
        }

        @Override
        public Object toBasicObject() {
            return DocNode.of("name", this.name(), "members", members(), "hidden", isHidden());
        }
    }

    static abstract class AbstractIndexLike<T> implements Meta.IndexLikeObject {
        private final String name;
        private final Collection<String> parentAliasNames;
        private final String parentDataStreamName;
        private final boolean hidden;
        private DefaultMetaImpl root;
        private ImmutableSet<Meta.IndexOrNonExistent> cachedResolveDeep;
        private ImmutableSet<Meta.IndexOrNonExistent> cachedResolveDeepWrite;
        private ImmutableSet<String> cachedResolveDeepToNames;
        private ImmutableSet<String> cachedResolveDeepToNamesWrite;
        private ImmutableSet<Meta.Alias> cachedParentAliases;

        public AbstractIndexLike(DefaultMetaImpl root, String name, Collection<String> parentAliasNames, String parentDataStreamName,
                boolean hidden) {
            this.name = Objects.requireNonNull(name);
            this.parentAliasNames = parentAliasNames != null ? parentAliasNames : ImmutableSet.empty();
            this.parentDataStreamName = parentDataStreamName;
            this.hidden = hidden;
            this.root = root;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean isHidden() {
            return hidden;
        }

        @Override
        public Collection<String> parentAliasNames() {
            return this.parentAliasNames;
        }

        @Override
        public String parentDataStreamName() {
            return this.parentDataStreamName;
        }

        @Override
        public Meta.DataStream parentDataStream() {
            if (this.parentDataStreamName != null) {
                return (Meta.DataStream) this.root.getIndexOrLike(this.parentDataStreamName);
            } else {
                return null;
            }
        }

        @Override
        public ImmutableSet<Meta.Alias> parentAliases() {
            if (this.parentAliasNames != null && !this.parentAliasNames.isEmpty()) {
                ImmutableSet<Meta.Alias> result = this.cachedParentAliases;

                if (result == null) {
                    result = ImmutableSet.map(this.parentAliasNames, (n) -> (Meta.Alias) this.root.getIndexOrLike(n));
                    this.cachedParentAliases = result;
                }

                return result;
            } else {
                return ImmutableSet.empty();
            }
        }

        @Override
        public ImmutableSet<Meta.IndexOrNonExistent> resolveDeep(Alias.ResolutionMode resolutionMode) {
            ImmutableSet<Meta.IndexOrNonExistent> result = resolutionMode == Alias.ResolutionMode.NORMAL ? this.cachedResolveDeep
                    : this.cachedResolveDeepWrite;

            if (result == null) {
                result = this.resolveDeepImpl(resolutionMode);
                if (resolutionMode == Alias.ResolutionMode.NORMAL) {
                    this.cachedResolveDeep = result;
                } else {
                    this.cachedResolveDeepWrite = result;
                }
            }

            return result;
        }

        @Override
        public ImmutableSet<String> resolveDeepToNames(Alias.ResolutionMode resolutionMode) {
            ImmutableSet<String> result = resolutionMode == Alias.ResolutionMode.NORMAL ? this.cachedResolveDeepToNames
                    : this.cachedResolveDeepToNamesWrite;

            if (result == null) {
                result = this.resolveDeepToNamesImpl(resolutionMode);
                if (resolutionMode == Alias.ResolutionMode.NORMAL) {
                    this.cachedResolveDeepToNames = result;
                } else {
                    this.cachedResolveDeepToNamesWrite = result;
                }
            }

            return result;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public abstract boolean equals(Object obj);

        @Override
        public String toString() {
            return this.name;
        }

        protected abstract AbstractIndexLike<T> withAlias(String alias);

        protected abstract ImmutableSet<Meta.IndexOrNonExistent> resolveDeepImpl(Alias.ResolutionMode resolutionMode);

        protected abstract ImmutableSet<String> resolveDeepToNamesImpl(Alias.ResolutionMode resolutionMode);

        protected abstract T copy();

        public Meta getRoot() {
            return root;
        }

        protected void root(DefaultMetaImpl root) {
            if (this.root != null && !this.root.equals(root)) {
                throw new IllegalStateException("Cannot set root twice");
            }

            this.root = root;
        }

        @Override
        public boolean exists() {
            return true;
        }

    }

    static abstract class AbstractIndexCollection<T> extends AbstractIndexLike<T> implements IndexCollection {
        private final UnmodifiableCollection<IndexLikeObject> members;
        private ImmutableSet<Meta.Index> cachedResolveDeepAsIndex;
        private ImmutableSet<Meta.Index> cachedResolveDeepAsIndexWrite;

        public AbstractIndexCollection(DefaultMetaImpl root, String name, Collection<String> parentAliasNames, String parentDataStreamName,
                UnmodifiableCollection<IndexLikeObject> members, boolean hidden) {
            super(root, name, parentAliasNames, parentDataStreamName, hidden);
            this.members = members;
        }

        @Override
        public UnmodifiableCollection<IndexLikeObject> members() {
            return members;
        }

        @Override
        protected ImmutableSet<Meta.IndexOrNonExistent> resolveDeepImpl(Alias.ResolutionMode resolutionMode) {
            return ImmutableSet.<Meta.IndexOrNonExistent>of(resolveDeepAsIndex(resolutionMode));
        }

        @Override
        public ImmutableSet<Meta.Index> resolveDeepAsIndex(Alias.ResolutionMode resolutionMode) {
            ImmutableSet<Meta.Index> result = resolutionMode == Alias.ResolutionMode.TO_WRITE_TARGET ? this.cachedResolveDeepAsIndexWrite
                    : this.cachedResolveDeepAsIndex;

            if (result == null) {
                ImmutableSet.Builder<Meta.Index> builder = new ImmutableSet.Builder<>(this.members.size());

                for (IndexLikeObject member : this.members) {
                    if (member instanceof Meta.Index) {
                        builder.add((Meta.Index) member);
                    } else if (member instanceof Meta.IndexCollection) {
                        builder.addAll(((Meta.IndexCollection) member).resolveDeepAsIndex(resolutionMode));
                    } else {
                        throw new RuntimeException("Unexpected member " + member + " of " + this);
                    }
                }

                result = builder.build();

                if (resolutionMode == Alias.ResolutionMode.TO_WRITE_TARGET) {
                    this.cachedResolveDeepAsIndexWrite = result;
                } else {
                    this.cachedResolveDeepAsIndex = result;
                }
            }

            return result;
        }

        @Override
        protected ImmutableSet<String> resolveDeepToNamesImpl(Alias.ResolutionMode resolutionMode) {
            ImmutableSet.Builder<String> result = new ImmutableSet.Builder<>(this.members.size());

            for (IndexLikeObject member : this.members) {
                if (member instanceof Meta.Index) {
                    result.add(member.name());
                } else {
                    result.addAll(member.resolveDeepToNames(resolutionMode));
                }
            }

            return result.build();
        }
    }

    static class DefaultMetaImpl extends MetaImpl {
        private final ImmutableSet<Index> indices;
        private final ImmutableSet<Alias> aliases;
        private final ImmutableSet<DataStream> dataStreams;
        private final ImmutableSet<Index> indicesWithoutParents;
        private final ImmutableSet<Index> nonHiddenIndicesWithoutParents;
        private final ImmutableSet<Index> nonHiddenIndices;
        private final ImmutableSet<Index> nonSystemIndices;
        private final ImmutableSet<Index> nonSystemIndicesWithoutParents;
        private final ImmutableSet<IndexCollection> indexCollections;
        private final ImmutableSet<Alias> nonHiddenAliases;
        private final ImmutableSet<DataStream> nonHiddenDataStreams;
        private final ImmutableMap<String, Meta.IndexLikeObject> nameMap;
        private final org.elasticsearch.cluster.metadata.Metadata esMetadata;

        /**
         * For testing only!
         */
        public DefaultMetaImpl(ImmutableSet<Index> indices, ImmutableSet<Alias> aliases, ImmutableSet<DataStream> datastreams,
                ImmutableSet<Index> indicesWithoutParents) {
            indices.forEach((i) -> ((AbstractIndexLike<?>) i).root(this));
            aliases.forEach((i) -> ((AbstractIndexLike<?>) i).root(this));
            datastreams.forEach((i) -> ((AbstractIndexLike<?>) i).root(this));

            this.indices = indices;
            this.aliases = aliases;
            this.dataStreams = datastreams;
            this.indicesWithoutParents = indicesWithoutParents;
            this.indexCollections = ImmutableSet.<IndexCollection>of(aliases).with(datastreams);
            this.nonHiddenIndicesWithoutParents = this.indicesWithoutParents.matching(e -> !e.isHidden());
            this.nonHiddenIndices = this.indices.matching(e -> !e.isHidden());
            this.nonSystemIndices = this.indices.matching(e -> !e.isSystem());
            this.nonSystemIndicesWithoutParents = this.indicesWithoutParents.matching(e -> !e.isSystem());
            this.nonHiddenAliases = this.aliases.matching(e -> !e.isHidden());
            this.nonHiddenDataStreams = this.dataStreams.matching(e -> !e.isHidden());

            this.nameMap = ImmutableMap
                    .<String, IndexLikeObject>of(ImmutableMap.<Index, String, Index>map(indices, e -> ImmutableMap.entry(e.name(), e)))
                    .with(ImmutableMap.<IndexCollection, String, IndexCollection>map(indexCollections, e -> ImmutableMap.entry(e.name(), e)));

            org.elasticsearch.cluster.metadata.Metadata.Builder esMetadataBuilder = org.elasticsearch.cluster.metadata.Metadata.builder();

            Map<String, org.elasticsearch.cluster.metadata.AliasMetadata> esAliasMetadata = new HashMap<>();

            for (String alias : aliases.map(Alias::nameForIndexPatternMatching)) {
                esAliasMetadata.put(alias, org.elasticsearch.cluster.metadata.AliasMetadata.builder(alias).build());
            }

            for (Index index : indices) {
                org.elasticsearch.cluster.metadata.IndexMetadata.Builder esIndex = org.elasticsearch.cluster.metadata.IndexMetadata
                        .builder(index.name())
                        .settings(org.elasticsearch.common.settings.Settings.builder().put(
                                org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(),
                                org.elasticsearch.Version.CURRENT))
                        .numberOfShards(1).numberOfReplicas(1);

                for (String alias : index.parentAliasNames().stream().map(Meta::indexLikeNameWithoutFailuresSuffix).toList()) {
                    esIndex.putAlias(esAliasMetadata.get(alias));
                }

                esMetadataBuilder.put(esIndex);
            }

            for (String dataStreamNameWithoutComponent : datastreams.map(DataStream::nameForIndexPatternMatching)) {
                // Pre ES 8.14 version:
                // esMetadataBuilder.put(new org.elasticsearch.cluster.metadata.DataStream(dataStream.name(),
                //         ImmutableList.of(dataStream.members()).map(i -> new org.elasticsearch.index.Index(i.name(), i.name())), 1L,
                //              ImmutableMap.empty(), false, false, false, false, IndexMode.STANDARD));

                ImmutableSet<DataStream> dataAndFailureDataStreams = dataStreams.matching(ds -> ds.nameForIndexPatternMatching().equals(dataStreamNameWithoutComponent));
                DataStream dataStreamWithDataIndices = dataAndFailureDataStreams.stream().filter(ds -> !ds.name().endsWith(FAILURES_SUFFIX)).findFirst().orElse(null);
                DataStream dataStreamWithFailureIndices = dataAndFailureDataStreams.stream().filter(ds -> ds.name().endsWith(FAILURES_SUFFIX)).findFirst().orElse(null);

                ImmutableList<IndexLikeObject> dataIndices = dataStreamWithDataIndices != null? ImmutableList.of(dataStreamWithDataIndices.members()) : ImmutableList.empty();
                ImmutableList<IndexLikeObject> failureIndices = dataStreamWithFailureIndices != null? ImmutableList.of(dataStreamWithFailureIndices.members()) : ImmutableList.empty();
                DataStreamOptions dataStreamOptions = failureIndices.isEmpty()?  DataStreamOptions.FAILURE_STORE_DISABLED : DataStreamOptions.FAILURE_STORE_ENABLED;

                esMetadataBuilder.put(new org.elasticsearch.cluster.metadata.DataStream(dataStreamNameWithoutComponent,
                        dataIndices.map(i -> new org.elasticsearch.index.Index(i.nameForIndexPatternMatching(), i.nameForIndexPatternMatching())), 1L,
                        ImmutableMap.empty(), false, false, false, false, IndexMode.STANDARD, DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE, dataStreamOptions,
                        failureIndices.map(i -> new org.elasticsearch.index.Index(i.nameForIndexPatternMatching(), i.nameForIndexPatternMatching())),
                        false, null));
            }

            this.esMetadata = esMetadataBuilder.build();
        }

        public DefaultMetaImpl(org.elasticsearch.cluster.metadata.Metadata esMetadata) {
            ProjectMetadata project = esMetadata.getProject();
            ImmutableSet.Builder<Index> indices = new ImmutableSet.Builder<>(project.indices().size());
            ImmutableMap.Builder<String, Meta.IndexLikeObject> nameMap = new ImmutableMap.Builder<>(project.indices().size());
            ImmutableSet.Builder<Index> indicesWithoutParents = new ImmutableSet.Builder<>(project.indices().size());
            ImmutableMap.Builder<org.elasticsearch.cluster.metadata.AliasMetadata, ImmutableList.Builder<IndexLikeObject>> aliasToIndicesMap = new ImmutableMap.Builder<org.elasticsearch.cluster.metadata.AliasMetadata, ImmutableList.Builder<IndexLikeObject>>()
                    .defaultValue((k) -> new ImmutableList.Builder<IndexLikeObject>());
            ImmutableMap.Builder<org.elasticsearch.cluster.metadata.AliasMetadata, IndexLikeObject> aliasToWriteIndexMap = new ImmutableMap.Builder<org.elasticsearch.cluster.metadata.AliasMetadata, IndexLikeObject>();

            ImmutableMap.Builder<org.elasticsearch.cluster.metadata.DataStreamAlias, List<IndexLikeObject>> dataStreamDataComponentAliasToIndicesMap = new ImmutableMap.Builder<org.elasticsearch.cluster.metadata.DataStreamAlias, List<IndexLikeObject>>()
                    .defaultValue((k) -> new ArrayList<IndexLikeObject>());
            ImmutableMap.Builder<org.elasticsearch.cluster.metadata.DataStreamAlias, List<IndexLikeObject>> dataStreamFailureComponentAliasToIndicesMap = new ImmutableMap.Builder<org.elasticsearch.cluster.metadata.DataStreamAlias, List<IndexLikeObject>>()
                    .defaultValue((k) -> new ArrayList<IndexLikeObject>());
            ImmutableSet.Builder<Alias> aliases = new ImmutableSet.Builder<>(64);
            ImmutableSet.Builder<DataStream> datastreams = new ImmutableSet.Builder<>(project.dataStreams().size());
            this.esMetadata = esMetadata;

            Map<String, org.elasticsearch.cluster.metadata.DataStreamAlias> dataStreamsAliases = project.dataStreamAliases();
            Map<String, ImmutableList.Builder<String>> dataStreamAliasReverseLookup = new HashMap<>();

            for (org.elasticsearch.cluster.metadata.DataStreamAlias dataStreamAlias : dataStreamsAliases.values()) {
                for (String dataStream : dataStreamAlias.getDataStreams()) {
                    dataStreamAliasReverseLookup.computeIfAbsent(dataStream, k -> new ImmutableList.Builder<>()).add(dataStreamAlias.getName());
                }
            }

            for (org.elasticsearch.cluster.metadata.DataStream esDataStream : project.dataStreams().values()) {
                ImmutableList<String> parentAliasNames = dataStreamAliasReverseLookup.get(esDataStream.getName()) == null?
                        ImmutableList.empty() : dataStreamAliasReverseLookup.get(esDataStream.getName()).build();

                // backing (data) indices
                List<org.elasticsearch.index.Index> esDataStreamDataIndices = esDataStream.getIndices();
                ImmutableList.Builder<IndexLikeObject> dataMemberIndices = new ImmutableList.Builder<>(esDataStreamDataIndices.size());

                for (org.elasticsearch.index.Index esIndex : esDataStreamDataIndices) {
                    org.elasticsearch.cluster.metadata.IndexMetadata esIndexMetadata = project.index(esIndex.getName());

                    Index index = new IndexImpl(this, esIndex.getName(), ImmutableSet.empty(), esDataStream.getName(), esIndexMetadata.isHidden(),
                            esIndexMetadata.isSystem(), esIndexMetadata.getState());
                    indices.add(index);
                    nameMap.put(index.name(), index);
                    dataMemberIndices.add(index);
                }

                DataStream dataStreamWithDataIndices = new DataStreamImpl(this, esDataStream.getName(),
                        parentAliasNames, dataMemberIndices.build(), esDataStream.isHidden());
                datastreams.add(dataStreamWithDataIndices);
                nameMap.put(dataStreamWithDataIndices.name(), dataStreamWithDataIndices);

                // failure store indices
                List<org.elasticsearch.index.Index> esDataStreamFailureStoreIndices = esDataStream.getFailureIndices();
                ImmutableList.Builder<IndexLikeObject> failureStoreMemberIndices = new ImmutableList.Builder<>(esDataStreamFailureStoreIndices.size());

                String dataStreamNameWithFailuresSuffix = Meta.indexLikeNameWithFailuresSuffix(esDataStream.getName());

                for (org.elasticsearch.index.Index esIndex : esDataStreamFailureStoreIndices) {
                    org.elasticsearch.cluster.metadata.IndexMetadata esIndexMetadata = project.index(esIndex.getName());

                    Index index = new IndexImpl(this, esIndex.getName(), ImmutableSet.empty(), dataStreamNameWithFailuresSuffix, esIndexMetadata.isHidden(),
                            esIndexMetadata.isSystem(), esIndexMetadata.getState());
                    indices.add(index);
                    nameMap.put(index.name(), index);
                    failureStoreMemberIndices.add(index);
                }

                ImmutableList<String> parentAliasNamesWithFailuresSuffix = parentAliasNames.map(Meta::indexLikeNameWithFailuresSuffix);

                DataStream dataStreamWithFailureStoreIndices = new DataStreamImpl(this, dataStreamNameWithFailuresSuffix,
                        parentAliasNamesWithFailuresSuffix, failureStoreMemberIndices.build(), esDataStream.isHidden());
                datastreams.add(dataStreamWithFailureStoreIndices);
                nameMap.put(dataStreamWithFailureStoreIndices.name(), dataStreamWithFailureStoreIndices);

                for (String parentAlias : parentAliasNames) {
                    dataStreamDataComponentAliasToIndicesMap.get(dataStreamsAliases.get(parentAlias)).add(dataStreamWithDataIndices);
                    dataStreamFailureComponentAliasToIndicesMap.get(dataStreamsAliases.get(parentAlias)).add(dataStreamWithFailureStoreIndices);
                }
            }

            for (org.elasticsearch.cluster.metadata.IndexMetadata esIndexMetadata : project.indices().values()) {
                String name = esIndexMetadata.getIndex().getName();

                if (nameMap.contains(name)) {
                    // Index has been already created via a DataStream - it's a backing or a failure store index
                    continue;
                }

                Index index = new IndexImpl(this, name, esIndexMetadata.getAliases().keySet(), null, esIndexMetadata.isHidden(),
                        esIndexMetadata.isSystem(), esIndexMetadata.getState());
                indices.add(index);
                nameMap.put(name, index);

                if (esIndexMetadata.getAliases().isEmpty()) {
                    indicesWithoutParents.add(index);
                }

                for (org.elasticsearch.cluster.metadata.AliasMetadata esAliasMetadata : esIndexMetadata.getAliases().values()) {
                    aliasToIndicesMap.get(esAliasMetadata).add(index);
                    if (esAliasMetadata.writeIndex() != null && esAliasMetadata.writeIndex().booleanValue()) {
                        aliasToWriteIndexMap.put(esAliasMetadata, index);
                    }
                }
            }

            // aliases to indices (by ES semantics alias cannot point to both data streams and indices)
            for (Map.Entry<org.elasticsearch.cluster.metadata.AliasMetadata, ImmutableList.Builder<IndexLikeObject>> entry : aliasToIndicesMap.build()
                    .entrySet()) {
                ImmutableList<IndexLikeObject> members = entry.getValue().build();

                IndexLikeObject writeTarget = aliasToWriteIndexMap.get(entry.getKey());
                if (writeTarget == null && members.size() == 1) {
                    // By ES semantics, if an alias has only one member, this automatically becomes the write index
                    writeTarget = members.only();
                }

                Alias alias = new AliasImpl(this, entry.getKey().alias(), members,
                        entry.getKey().isHidden() != null ? entry.getKey().isHidden() : false, writeTarget);
                aliases.add(alias);
                nameMap.put(alias.name(), alias);
            }

            // aliases to data stream with data component (with backing indices)
            for (Map.Entry<org.elasticsearch.cluster.metadata.DataStreamAlias, List<IndexLikeObject>> entry : dataStreamDataComponentAliasToIndicesMap.build()
                    .entrySet()) {

                IndexLikeObject writeTarget = null;
                if (entry.getKey().getWriteDataStream() != null) {
                    writeTarget = nameMap.get(entry.getKey().getWriteDataStream());
                }
                Alias alias = new AliasImpl(this, entry.getKey().getName(), ImmutableList.of(entry.getValue()), false, writeTarget);
                aliases.add(alias);
                nameMap.put(alias.name(), alias);
            }

            // aliases to data stream with failures component (with failure store indices)
            for (Map.Entry<org.elasticsearch.cluster.metadata.DataStreamAlias, List<IndexLikeObject>> entry : dataStreamFailureComponentAliasToIndicesMap.build()
                    .entrySet()) {

                IndexLikeObject writeTarget = null; // At the moment, write targets for failure stores in data stream aliases are not supported.

                String aliasNameWithFailuresSuffix = Meta.indexLikeNameWithFailuresSuffix(entry.getKey().getName());
                Alias alias = new AliasImpl(this, aliasNameWithFailuresSuffix, ImmutableList.of(entry.getValue()), false, writeTarget);
                aliases.add(alias);
                nameMap.put(alias.name(), alias);
            }

            this.indices = indices.build();
            this.indicesWithoutParents = indicesWithoutParents.build();
            this.aliases = aliases.build();
            this.dataStreams = datastreams.build();
            this.indexCollections = ImmutableSet.<IndexCollection>of(this.aliases).with(this.dataStreams);
            this.nonHiddenIndicesWithoutParents = this.indicesWithoutParents.matching(e -> !e.isHidden());
            this.nonHiddenIndices = this.indices.matching(e -> !e.isHidden());
            this.nonSystemIndices = this.indices.matching(e -> !e.isSystem());
            this.nonSystemIndicesWithoutParents = this.indicesWithoutParents.matching(e -> !e.isSystem());
            this.nonHiddenAliases = this.aliases.matching(e -> !e.isHidden());
            this.nonHiddenDataStreams = this.dataStreams.matching(e -> !e.isHidden());
            this.nameMap = nameMap.build();
        }

        @Override
        public IndexLikeObject getIndexOrLike(String name) {
            return this.nameMap.get(name);
        }

        @Override
        public ImmutableMap<String, IndexLikeObject> indexLikeObjects() {
            return nameMap;
        }

        @Override
        public ImmutableSet<Index> indices() {
            return indices;
        }

        @Override
        public ImmutableSet<Index> indicesWithoutParents() {
            return indicesWithoutParents;
        }

        @Override
        public ImmutableSet<Alias> aliases() {
            return aliases;
        }

        @Override
        public ImmutableSet<DataStream> dataStreams() {
            return dataStreams;
        }

        @Override
        public ImmutableSet<IndexCollection> indexCollections() {
            return indexCollections;
        }

        @Override
        public ImmutableSet<Index> nonHiddenIndices() {
            return nonHiddenIndices;
        }

        @Override
        public ImmutableSet<Index> nonHiddenIndicesWithoutParents() {
            return nonHiddenIndicesWithoutParents;
        }

        @Override
        public ImmutableSet<Index> nonSystemIndices() {
            return nonSystemIndices;
        }

        @Override
        public ImmutableSet<Index> nonSystemIndicesWithoutParents() {
            return this.nonSystemIndicesWithoutParents;
        }

        @Override
        public ImmutableSet<Alias> nonHiddenAliases() {
            return nonHiddenAliases;
        }

        @Override
        public ImmutableSet<DataStream> nonHiddenDataStreams() {
            return nonHiddenDataStreams;
        }

        @Override
        public Meta.Mock.AliasBuilder alias(String aliasName) {
            return new Meta.Mock.AliasBuilder() {

                @Override
                public Meta of(String... indexNames) {
                    validateIndexNames(aliasName, indexNames);

                    ImmutableSet<String> singleAliasSet = ImmutableSet.of(aliasName);
                    ImmutableMap.Builder<String, IndexLikeObject> aliasMembersBuilder = new ImmutableMap.Builder<>(indexNames.length);
                    ImmutableSet.Builder<Index> newIndices = new ImmutableSet.Builder<>(indexNames.length);
                    ImmutableSet.Builder<DataStream> newDataStreams = new ImmutableSet.Builder<>(indexNames.length);
                    IndexLikeObject writeTarget = null;

                    for (String indexName : indexNames) {
                        boolean setWriteTarget = false;

                        if (indexName.startsWith(">")) {
                            setWriteTarget = true;
                            indexName = indexName.substring(1);
                        }

                        AbstractIndexLike<?> indexLikeObject = (AbstractIndexLike<?>) getIndexOrLike(indexName);

                        if (indexLikeObject != null) {
                            indexLikeObject = indexLikeObject.withAlias(aliasName);
                        } else {
                            indexLikeObject = new IndexImpl(null, indexName, singleAliasSet, null);
                        }

                        if (setWriteTarget) {
                            writeTarget = indexLikeObject;
                        }

                        if (indexLikeObject instanceof DataStream) {
                            newDataStreams.add((DataStream) indexLikeObject);
                        } else {
                            newIndices.add((Index) indexLikeObject);
                        }

                        aliasMembersBuilder.put(indexName, indexLikeObject);
                    }

                    for (Index existingIndex : DefaultMetaImpl.this.indices) {
                        if (!newIndices.contains(existingIndex)) {
                            newIndices.add(((IndexImpl) existingIndex).copy());
                        }
                    }

                    for (DataStream existingDataStream : DefaultMetaImpl.this.dataStreams) {
                        if (!newDataStreams.contains(existingDataStream)) {
                            newDataStreams.add(((DataStreamImpl) existingDataStream).copy());
                        }
                    }

                    UnmodifiableCollection<IndexLikeObject> members = aliasMembersBuilder.build().values();

                    if (writeTarget == null && members.size() == 1) {
                        writeTarget = members.iterator().next();
                    }

                    ImmutableSet<Alias> aliases = ImmutableSet.<Alias>of(DefaultMetaImpl.this.aliases.map(i -> ((AliasImpl) i).copy()))
                            .with(new AliasImpl(null, aliasName, members, false, writeTarget));

                    return new DefaultMetaImpl(newIndices.build(), aliases, newDataStreams.build(), DefaultMetaImpl.this.indicesWithoutParents);
                }

            };
        }

        @Override
        public Meta.Mock.DataStreamBuilder dataStream(String dataStreamName) {
            return new Meta.Mock.DataStreamBuilder() {

                @Override
                public Meta of(String... indexNames) {
                    validateIndexNames(dataStreamName, indexNames);

                    ImmutableMap.Builder<String, IndexLikeObject> dataStreamMembersBuilder = new ImmutableMap.Builder<>(indexNames.length);
                    ImmutableSet.Builder<Index> newIndices = new ImmutableSet.Builder<>(indexNames.length);

                    for (String indexName : indexNames) {
                        AbstractIndexLike<?> indexLikeObject = (AbstractIndexLike<?>) getIndexOrLike(indexName);

                        if (indexLikeObject != null) {
                            throw new RuntimeException("Cannot reuse datastream backing or failure index");
                        } else {
                            indexLikeObject = new IndexImpl(null, indexName, ImmutableSet.empty(), dataStreamName);
                        }

                        newIndices.add((Index) indexLikeObject);

                        dataStreamMembersBuilder.put(indexName, indexLikeObject);
                    }

                    for (Index existingIndex : DefaultMetaImpl.this.indices) {
                        if (!newIndices.contains(existingIndex)) {
                            newIndices.add(((IndexImpl) existingIndex).copy());
                        }
                    }

                    ImmutableSet<Index> indices = newIndices.build();
                    ImmutableSet<DataStream> dataStreams = ImmutableSet
                            .<DataStream>of(DefaultMetaImpl.this.dataStreams.map(i -> ((DataStreamImpl) i).copy()))
                            .with(new DataStreamImpl(null, dataStreamName, ImmutableSet.empty(), dataStreamMembersBuilder.build().values(), false));

                    return new DefaultMetaImpl(indices, DefaultMetaImpl.this.aliases, dataStreams, DefaultMetaImpl.this.indicesWithoutParents);
                }

            };
        }

        @Override
        public String toString() {
            return "{indices: " + indices.size() + "; collections: " + indexCollections.size() + "}";
        }

        @Override
        public Object toBasicObject() {
            return DocNode.of("version", version(), "indices", indices, "aliases", aliases, "data_streams", dataStreams, "name_map", nameMap);
        }

        @Override
        public org.elasticsearch.cluster.metadata.Metadata esMetadata() {
            return esMetadata;
        }

        @Override
        public long version() {
            return esMetadata != null ? esMetadata.version() : -1;
        }

        /**
         * For testing and mocking purposes
         */
        static Meta indices(String... indexNames) {
            if (Arrays.stream(indexNames).anyMatch(index -> index.endsWith(FAILURES_SUFFIX))) {
                throw new RuntimeException("Index names must not end with: " + FAILURES_SUFFIX);
            }
            ImmutableSet<Index> indices = ImmutableSet.map(Arrays.asList(indexNames), (k) -> new IndexImpl(null, k, ImmutableSet.empty(), null));

            return new DefaultMetaImpl(indices, ImmutableSet.empty(), ImmutableSet.empty(), indices);
        }

        private static final AtomicReference<DefaultMetaImpl> currentInstance = new AtomicReference<>();

        static Meta from(org.elasticsearch.cluster.service.ClusterService clusterService) {
            DefaultMetaImpl currentInstance = DefaultMetaImpl.currentInstance.get();
            org.elasticsearch.cluster.metadata.Metadata esMetadata = clusterService.state().metadata();

            if (currentInstance == null || currentInstance.esMetadata.version() != esMetadata.version()
                    || !currentInstance.esMetadata.clusterUUID().equals(esMetadata.clusterUUID())) {
                currentInstance = new DefaultMetaImpl(esMetadata);
                DefaultMetaImpl.currentInstance.set(currentInstance);

                if (log.isTraceEnabled()) {
                    log.trace("New Meta:\n{}", currentInstance.toYamlString());
                }
            }

            return currentInstance;
        }
    }

    static class AliasBuilderImpl implements Meta.Mock.AliasBuilder {
        private final String name;

        AliasBuilderImpl(String name) {
            this.name = name;
        }

        @Override
        public Meta of(String... indexNames) {
            validateIndexNames(name, indexNames);

            ImmutableSet<String> aliasSet = ImmutableSet.of(name);
            ImmutableSet<Index> indices = ImmutableSet.map(Arrays.asList(indexNames), (k) -> new IndexImpl(null, k, aliasSet, null));
            ImmutableSet<Alias> aliases = ImmutableSet
                    .of(new AliasImpl(null, name, ImmutableSet.<IndexLikeObject>of(indices), false, indices.size() == 1 ? indices.only() : null));

            return new DefaultMetaImpl(indices, aliases, ImmutableSet.empty(), ImmutableSet.empty());
        }
    }

    static class DataStreamBuilderImpl implements Meta.Mock.DataStreamBuilder {
        private final String name;

        DataStreamBuilderImpl(String name) {
            this.name = name;
        }

        @Override
        public Meta of(String... indexNames) {
            validateIndexNames(name, indexNames);

            ImmutableSet<Index> indices = ImmutableSet.map(Arrays.asList(indexNames), k -> new IndexImpl(null, k, null, name));
            ImmutableSet<DataStream> dataStreams = ImmutableSet
                    .of(new DataStreamImpl(null, name, ImmutableSet.empty(), ImmutableSet.<IndexLikeObject>of(indices), false));

            return new DefaultMetaImpl(indices, ImmutableSet.empty(), dataStreams, ImmutableSet.empty());
        }
    }

    static abstract class AbstractNonExistentImpl implements Meta.IndexLikeObject {
        private final String name;

        public AbstractNonExistentImpl(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Collection<String> parentAliasNames() {
            return ImmutableSet.empty();
        }

        @Override
        public Collection<String> ancestorAliasNames() {
            return ImmutableSet.empty();
        }

        @Override
        public String parentDataStreamName() {
            return null;
        }

        @Override
        public boolean isHidden() {
            return false;
        }

        @Override
        public ImmutableSet<Alias> parentAliases() {
            return ImmutableSet.empty();
        }

        @Override
        public DataStream parentDataStream() {
            return null;
        }

        @Override
        public boolean exists() {
            return false;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            } else if (other instanceof Meta.IndexLikeObject) {
                return ((Meta.IndexLikeObject) other).name().equals(this.name()) && !((Meta.IndexLikeObject) other).exists();
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return this.name;
        }

        @Override
        public Object toBasicObject() {
            return DocNode.of("name", this.name(), "exists", false);
        }
    }

    public static class NonExistentImpl extends AbstractNonExistentImpl implements Meta.NonExistent {
        public NonExistentImpl(String name) {
            super(name);
        }

        @Override
        public ImmutableSet<Meta.IndexOrNonExistent> resolveDeep(Alias.ResolutionMode resolutionMode) {
            return ImmutableSet.of(this);
        }

        @Override
        public ImmutableSet<String> resolveDeepToNames(Alias.ResolutionMode resolutionMode) {
            return ImmutableSet.of(name());
        }

    }

    public static class NonExistentIndexImpl extends AbstractNonExistentImpl implements Meta.Index {
        public NonExistentIndexImpl(String name) {
            super(name);
        }

        @Override
        public ImmutableSet<Meta.IndexOrNonExistent> resolveDeep(Alias.ResolutionMode resolutionMode) {
            return ImmutableSet.empty();
        }

        @Override
        public ImmutableSet<String> resolveDeepToNames(Alias.ResolutionMode resolutionMode) {
            return ImmutableSet.empty();
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public boolean isSystem() {
            return false;
        }

        @Override
        public boolean isDataStreamBackingIndex() {
            return false;
        }
    }

    public static class NonExistentAliasImpl extends AbstractNonExistentImpl implements Meta.Alias {
        public NonExistentAliasImpl(String name) {
            super(name);
        }

        @Override
        public ImmutableSet<Meta.IndexOrNonExistent> resolveDeep(Alias.ResolutionMode resolutionMode) {
            return ImmutableSet.empty();
        }

        @Override
        public ImmutableSet<String> resolveDeepToNames(Alias.ResolutionMode resolutionMode) {
            return ImmutableSet.empty();
        }

        @Override
        public UnmodifiableCollection<IndexLikeObject> members() {
            return ImmutableList.empty();
        }

        @Override
        public ImmutableSet<Index> resolveDeepAsIndex(Alias.ResolutionMode resolutionMode) {
            return ImmutableSet.empty();
        }

        @Override
        public IndexLikeObject writeTarget() {
            return null;
        }

        @Override
        public UnmodifiableCollection<IndexLikeObject> resolve(ResolutionMode resolutionMode) {
            return ImmutableList.empty();
        }

    }

    public static class NonExistentDataStreamImpl extends AbstractNonExistentImpl implements Meta.DataStream {
        public NonExistentDataStreamImpl(String name) {
            super(name);
        }

        @Override
        public ImmutableSet<Meta.IndexOrNonExistent> resolveDeep(Alias.ResolutionMode resolutionMode) {
            return ImmutableSet.empty();
        }

        @Override
        public ImmutableSet<String> resolveDeepToNames(Alias.ResolutionMode resolutionMode) {
            return ImmutableSet.empty();
        }

        @Override
        public UnmodifiableCollection<IndexLikeObject> members() {
            return ImmutableList.empty();
        }

        @Override
        public ImmutableSet<Index> resolveDeepAsIndex(Alias.ResolutionMode resolutionMode) {
            return ImmutableSet.empty();
        }

    }
}
