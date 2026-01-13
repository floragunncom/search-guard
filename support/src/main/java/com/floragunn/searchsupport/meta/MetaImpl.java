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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.fluent.collections.UnmodifiableCollection;

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
            super(root, name, parentAliasNames, parentDataStreamName, hidden, Component.NONE);
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
            return ImmutableSet.of(this.nameWithComponent());
        }

        @Override
        protected AbstractIndexLike<IndexImpl> withAlias(String alias) {
            return new IndexImpl(null, name(), ImmutableSet.of(this.parentAliasNames()).with(component().indexLikeNameWithComponentSuffix(alias)), parentDataStreamName(), isHidden(), system,
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
                return ((Meta.Index) other).nameWithComponent().equals(this.nameWithComponent());
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
            return DocNode.of("name", this.nameWithComponent(), "open", open, "system", isSystem(), "hidden", isHidden());
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
                IndexLikeObject writeTarget, Component component) {
            super(root, name, ImmutableSet.empty(), null, members, hidden, component);
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
                return ((Meta.Alias) other).nameWithComponent().equals(this.nameWithComponent());
            } else {
                return false;
            }
        }

        @Override
        protected AliasImpl copy() {
            return new AliasImpl(null, name(), members(), isHidden(), writeTarget, component());
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
            return DocNode.of("name", this.nameWithComponent(), "members", members(), "hidden", isHidden());
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
                    return ImmutableSet.of(writeTarget.nameWithComponent());
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
                boolean hidden, Component component) {
            super(root, name, parentAliasNames, null, members, hidden, component);
        }

        @Override
        protected AbstractIndexLike<DataStreamImpl> withAlias(String alias) {
            return new DataStreamImpl(null, name(), ImmutableSet.of(this.parentAliasNames()).with(component().indexLikeNameWithComponentSuffix(alias)), members(), isHidden(), component());
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            } else if (other instanceof Meta.DataStream) {
                return ((Meta.DataStream) other).nameWithComponent().equals(this.nameWithComponent());
            } else {
                return false;
            }
        }

        @Override
        protected DataStreamImpl copy() {
            return new DataStreamImpl(null, name(), parentAliasNames(), members(), isHidden(), component());
        }

        @Override
        public Collection<String> ancestorAliasNames() {
            return parentAliasNames();
        }

        @Override
        public Object toBasicObject() {
            return DocNode.of("name", this.nameWithComponent(), "members", members(), "hidden", isHidden());
        }
    }

    static abstract class AbstractIndexLike<T> implements Meta.IndexLikeObject {
        private final String name;
        private final Collection<String> parentAliasNames;
        private final String parentDataStreamName;
        private final boolean hidden;
        private final Component component;
        private DefaultMetaImpl root;
        private ImmutableSet<Meta.IndexOrNonExistent> cachedResolveDeep;
        private ImmutableSet<Meta.IndexOrNonExistent> cachedResolveDeepWrite;
        private ImmutableSet<String> cachedResolveDeepToNames;
        private ImmutableSet<String> cachedResolveDeepToNamesWrite;
        private ImmutableSet<Meta.Alias> cachedParentAliases;

        public AbstractIndexLike(DefaultMetaImpl root, String name, Collection<String> parentAliasNames, String parentDataStreamName,
                boolean hidden, Component component) {
            this.name = Objects.requireNonNull(name);
            this.parentAliasNames = parentAliasNames != null ? parentAliasNames : ImmutableSet.empty();
            this.parentDataStreamName = parentDataStreamName;
            this.hidden = hidden;
            this.root = root;
            this.component = component;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String nameWithComponent() {
            return this.component.indexLikeNameWithComponentSuffix(this.name);
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
        public Component component() {
            return this.component;
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
            int result = name.hashCode();
            result = 31 * result + component.hashCode();
            return result;
        }

        @Override
        public abstract boolean equals(Object obj);

        @Override
        public String toString() {
            return this.nameWithComponent();
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
                UnmodifiableCollection<IndexLikeObject> members, boolean hidden, Component component) {
            super(root, name, parentAliasNames, parentDataStreamName, hidden, component);
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
                    result.add(member.nameWithComponent());
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

            for (Alias alias : aliases) {
                esAliasMetadata.put(alias.name(), org.elasticsearch.cluster.metadata.AliasMetadata.builder(alias.name()).build());
            }

            for (Index index : indices) {
                org.elasticsearch.cluster.metadata.IndexMetadata.Builder esIndex = org.elasticsearch.cluster.metadata.IndexMetadata
                        .builder(index.name())
                        .settings(org.elasticsearch.common.settings.Settings.builder().put(
                                org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(),
                                org.elasticsearch.Version.CURRENT))
                        .numberOfShards(1).numberOfReplicas(1);

                for (String alias : index.parentAliasNames()) {
                    esIndex.putAlias(esAliasMetadata.get(alias));
                }

                esMetadataBuilder.put(esIndex);
            }

            for (DataStream dataStream : datastreams) {
                // Pre ES 8.14 version:
                // esMetadataBuilder.put(new org.elasticsearch.cluster.metadata.DataStream(dataStream.name(),
                //         ImmutableList.of(dataStream.members()).map(i -> new org.elasticsearch.index.Index(i.name(), i.name())), 1L,
                //              ImmutableMap.empty(), false, false, false, false, IndexMode.STANDARD));

                esMetadataBuilder.put(new org.elasticsearch.cluster.metadata.DataStream(dataStream.name(),
                        ImmutableList.of(dataStream.members()).map(i -> new org.elasticsearch.index.Index(i.name(), i.name())), 1L,
                        ImmutableMap.empty(), false, false, false, false, IndexMode.STANDARD, DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE, DataStreamOptions.FAILURE_STORE_DISABLED, ImmutableList.empty(),
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

            ImmutableMap.Builder<Component, ImmutableMap.Builder<org.elasticsearch.cluster.metadata.DataStreamAlias, List<IndexLikeObject>>> componentDataStreamAliasToIndicesMap = //
                    new ImmutableMap.Builder<Component, ImmutableMap.Builder<org.elasticsearch.cluster.metadata.DataStreamAlias, List<IndexLikeObject>>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<org.elasticsearch.cluster.metadata.DataStreamAlias, List<IndexLikeObject>>()
                                    .defaultValue((k2) -> new ArrayList<IndexLikeObject>()));
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
                for (Component component : Component.values()) {
                    //get data stream indices based on component type
                    List<org.elasticsearch.index.Index> esDataStreamIndices = switch (component) {
                        case NONE -> esDataStream.getIndices();
                        case FAILURES -> esDataStream.getFailureIndices();
                    };
                    ImmutableList.Builder<IndexLikeObject> memberIndices = new ImmutableList.Builder<>(esDataStreamIndices.size());
                    String dataStreamNameWithComponent = component.indexLikeNameWithComponentSuffix(esDataStream.getName());

                    for (org.elasticsearch.index.Index esIndex : esDataStreamIndices) {
                        org.elasticsearch.cluster.metadata.IndexMetadata esIndexMetadata = project.index(esIndex.getName());

                        Index index = new IndexImpl(this, esIndex.getName(), ImmutableSet.empty(), dataStreamNameWithComponent, esIndexMetadata.isHidden(),
                                esIndexMetadata.isSystem(), esIndexMetadata.getState());
                        indices.add(index);
                        nameMap.put(index.nameWithComponent(), index);
                        memberIndices.add(index);
                    }

                    ImmutableList<String> parentAliasNames = Optional.ofNullable(dataStreamAliasReverseLookup.get(esDataStream.getName()))
                            .map(ImmutableList.Builder::build)
                            .orElse(ImmutableList.empty());
                    ImmutableList<String> parentAliasNamesWithComponent = parentAliasNames.map(component::indexLikeNameWithComponentSuffix);

                    DataStream dataStream = new DataStreamImpl(this, esDataStream.getName(),
                            parentAliasNamesWithComponent, memberIndices.build(), esDataStream.isHidden(), component);
                    datastreams.add(dataStream);
                    nameMap.put(dataStream.nameWithComponent(), dataStream);

                    for (String parentAlias : parentAliasNames) {
                        componentDataStreamAliasToIndicesMap.get(component).get(dataStreamsAliases.get(parentAlias)).add(dataStream);
                    }
                }
            }

            for (org.elasticsearch.cluster.metadata.IndexMetadata esIndexMetadata : project.indices().values()) {
                String name = esIndexMetadata.getIndex().getName();

                if (nameMap.contains(Component.NONE.indexLikeNameWithComponentSuffix(name))) {
                    // Index has been already created via a DataStream
                    continue;
                }

                Set<String> parentAliases = esIndexMetadata.getAliases().keySet().stream().map(Component.NONE::indexLikeNameWithComponentSuffix)
                        .collect(Collectors.toSet());
                Index index = new IndexImpl(this, name, parentAliases, null, esIndexMetadata.isHidden(),
                        esIndexMetadata.isSystem(), esIndexMetadata.getState());
                indices.add(index);
                nameMap.put(index.nameWithComponent(), index);

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

            //todo do we need to consider data streams in this loop? it looks like ES does not allow to add the same alias to both indices and data streams
            for (Map.Entry<org.elasticsearch.cluster.metadata.AliasMetadata, ImmutableList.Builder<IndexLikeObject>> entry : aliasToIndicesMap.build()
                    .entrySet()) {
                for (Component component : Component.values()) { // related to data stream and aliases for index and ds
                    org.elasticsearch.cluster.metadata.DataStreamAlias dataStreamAlias = dataStreamsAliases.get(entry.getKey().alias());
                    List<IndexLikeObject> dataStreams = dataStreamAlias != null && componentDataStreamAliasToIndicesMap.get(component).contains(dataStreamAlias)
                            ? componentDataStreamAliasToIndicesMap.get(component).get(dataStreamAlias)
                            : ImmutableList.empty();
                    ImmutableList<IndexLikeObject> members = switch (component) {
                        case NONE -> entry.getValue().build().with(dataStreams);
                        case FAILURES -> ImmutableList.of(dataStreams);
                    };

                    //todo this can happen in the case of Component.FAILURES, when we try to find data streams that belong to an alias to which indices are assigned?
                    if (members.isEmpty()) {
                        continue;
                    }

                    IndexLikeObject writeTarget = aliasToWriteIndexMap.get(entry.getKey());
                    if (writeTarget == null && members.size() == 1) {
                        // By ES semantics, if an alias has only one member, this automatically becomes the write index
                        writeTarget = members.only();
                    }

                    Alias alias = new AliasImpl(this, entry.getKey().alias(), members,
                            entry.getKey().isHidden() != null ? entry.getKey().isHidden() : false, writeTarget, component);
                    aliases.add(alias);
                    nameMap.put(alias.nameWithComponent(), alias);
                }
            }

            for (Component component : Component.values()) {
                for (Map.Entry<org.elasticsearch.cluster.metadata.DataStreamAlias, List<IndexLikeObject>> entry : componentDataStreamAliasToIndicesMap.get(component).build()
                        .entrySet()) {
                    if (nameMap.contains(component.indexLikeNameWithComponentSuffix(entry.getKey().getName()))) {
                        //todo it should not happen since alias cannot point to indices and data streams as mentioned in the TODO above
                        // Already created above
                        continue;
                    }

                    Optional<IndexLikeObject> writeTarget = component.extractWriteTargetForDataStreamAlias(project, entry.getKey(), nameMap);
                    Alias alias = new AliasImpl(this, entry.getKey().getName(), ImmutableList.of(entry.getValue()), false, writeTarget.orElse(null), component);
                    aliases.add(alias);
                    nameMap.put(alias.nameWithComponent(), alias);
                }
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
        public Iterable<String> namesOfIndices() {
            // TODO optimize or remove
            return indices.map(e -> e.nameWithComponent());
        }

        @Override
        public Iterable<String> namesOfIndexCollections() {
            // TODO optimize or remove

            return indexCollections.map(e -> e.nameWithComponent());
        }

        @Override
        public Meta.Mock.AliasBuilder alias(String aliasName) {
            return new Meta.Mock.AliasBuilder() {

                @Override
                public Meta of(String... indexNames) {
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
                            .with(new AliasImpl(null, aliasName, members, false, writeTarget, Component.NONE)); //todo add parameter for component?

                    return new DefaultMetaImpl(newIndices.build(), aliases, newDataStreams.build(), DefaultMetaImpl.this.indicesWithoutParents);
                }

            };
        }

        @Override
        public Meta.Mock.DataStreamBuilder dataStream(String dataStreamName) {
            return new Meta.Mock.DataStreamBuilder() {

                @Override
                public Meta of(String... indexNames) {
                    ImmutableMap.Builder<String, IndexLikeObject> dataStreamMembersBuilder = new ImmutableMap.Builder<>(indexNames.length);
                    ImmutableSet.Builder<Index> newIndices = new ImmutableSet.Builder<>(indexNames.length);

                    for (String indexName : indexNames) {
                        AbstractIndexLike<?> indexLikeObject = (AbstractIndexLike<?>) getIndexOrLike(indexName);

                        if (indexLikeObject != null) {
                            throw new RuntimeException("Cannot reuse datastream backing index");
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
                            .with(new DataStreamImpl(null, dataStreamName, ImmutableSet.empty(), dataStreamMembersBuilder.build().values(), false, Component.NONE)); //todo add parameter for component?

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
            ImmutableSet<String> aliasSet = ImmutableSet.of(name);
            ImmutableSet<Index> indices = ImmutableSet.map(Arrays.asList(indexNames), (k) -> new IndexImpl(null, k, aliasSet, null));
            ImmutableSet<Alias> aliases = ImmutableSet
                    .of(new AliasImpl(null, name, ImmutableSet.<IndexLikeObject>of(indices), false, indices.size() == 1 ? indices.only() : null, Component.NONE)); //todo add parameter for component?

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
            ImmutableSet<Index> indices = ImmutableSet.map(Arrays.asList(indexNames), k -> new IndexImpl(null, k, null, name));
            ImmutableSet<DataStream> dataStreams = ImmutableSet
                    .of(new DataStreamImpl(null, name, ImmutableSet.empty(), ImmutableSet.<IndexLikeObject>of(indices), false, Component.NONE)); //todo add parameter for component?

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
        public Component component() {
            return Component.NONE;
        }

        @Override
        public String nameWithComponent() {
            return name;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            } else if (other instanceof Meta.IndexLikeObject) {
                return ((Meta.IndexLikeObject) other).name().equals(this.name()) && !((Meta.IndexLikeObject) other).exists(); //todo name is enough?
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return this.name();
        } //todo name is enough?

        @Override
        public Object toBasicObject() {
            return DocNode.of("name", this.name(), "exists", false); //todo name is enough?
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
