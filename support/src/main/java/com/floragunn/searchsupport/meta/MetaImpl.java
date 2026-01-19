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
import java.util.stream.Stream;

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
                boolean system, org.elasticsearch.cluster.metadata.IndexMetadata.State state, Component component) {
            super(root, name, parentAliasNames, parentDataStreamName, hidden, component);
            this.open = state == org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN;
            this.system = system;
        }

        IndexImpl(DefaultMetaImpl root, String name, Collection<String> parentAliasNames, String parentDataStreamName, Component component) {
            this(root, name, parentAliasNames, parentDataStreamName, false, false, org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN, component);
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
            // TODO CS: do we need support for parent component selector here? Probably not
            return new IndexImpl(null, name(), ImmutableSet.of(this.parentAliasNames()).with(alias), parentDataStreamName(), isHidden(), system,
                    this.open ? org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN
                            : org.elasticsearch.cluster.metadata.IndexMetadata.State.CLOSE, components().toArray(Component[]::new)[0]); // index should have only one component assigned
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
                            : org.elasticsearch.cluster.metadata.IndexMetadata.State.CLOSE, components().toArray(Component[]::new)[0]);
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

    public static class AliasImpl extends AbstractIndexCollection<AliasImpl, IndexLikeObject> implements Meta.Alias {
        private final IndexLikeObject writeTargetData;
        private final IndexLikeObject writeTargetFailures;
        private final ImmutableSet<IndexLikeObject> writeTargetAsSet;

        public AliasImpl(DefaultMetaImpl root, String name, UnmodifiableCollection<IndexLikeObject> members, boolean hidden,
                IndexLikeObject writeTargetData, IndexLikeObject writeTargetFailures) {
            super(root, name, ImmutableSet.empty(), null, members, hidden, determineAliasComponent(members));
            this.writeTargetData = writeTargetData;
            this.writeTargetAsSet = writeTargetData != null ? ImmutableSet.of(writeTargetData) : ImmutableSet.empty();
            this.writeTargetFailures = writeTargetFailures;
        }

        private static Component[] determineAliasComponent(UnmodifiableCollection<IndexLikeObject> members) {
            return members.stream()
                    .flatMap(indexLike -> indexLike.components().stream()) //
                    .collect(Collectors.toSet()) //
                    .toArray(Component[]::new);
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
            return new AliasImpl(null, name(), members(), isHidden(), writeTargetData, writeTargetFailures);
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
        public IndexLikeObject writeTarget(Component component) {
            return switch (component) {
                case NONE ->  writeTargetData;
                case FAILURES -> writeTargetFailures;
            };
        }

        @Override
        public UnmodifiableCollection<IndexLikeObject> resolve(ResolutionMode resolutionMode) {
            if (resolutionMode == ResolutionMode.TO_WRITE_TARGET) {
                ImmutableSet.Builder<IndexLikeObject> resultBuilder = new ImmutableSet.Builder<>();
                for(Component component : components()) {
                    IndexLikeObject target = writeTarget(component);
                    if (target != null) {
                        resultBuilder.add(target);
                    }
                }
                return resultBuilder.build();
            } else {
                return members();
            }
        }

        @Override
        public ImmutableSet<Meta.Index> resolveDeepAsIndex(Alias.ResolutionMode resolutionMode) {
            if (resolutionMode == Alias.ResolutionMode.TO_WRITE_TARGET) {
                    return resolve(resolutionMode).stream() //
                            .flatMap(indexLike -> {
                        if (indexLike instanceof Meta.Index index) {
                            return Stream.of(index);
                        } else if (indexLike instanceof Meta.DataStream dataStream) {
                            return dataStream.resolveDeepAsIndex(resolutionMode).stream();
                        } else {
                            // This probably will never be executed. Alias can point to indices or data streams
                            return super.resolveDeepAsIndex(resolutionMode).stream();
                        }
                    }).collect(ImmutableSet.collector());
            } else {
                return super.resolveDeepAsIndex(resolutionMode);
            }
        }

        @Override
        protected ImmutableSet<String> resolveDeepToNamesImpl(Alias.ResolutionMode resolutionMode) {
            if (resolutionMode == Alias.ResolutionMode.TO_WRITE_TARGET) {
                return resolve(resolutionMode).stream() //
                        .flatMap(indexLike -> {
                            if (indexLike instanceof Meta.Index index) {
                                return Stream.of(index.name());
                            } else if (indexLike instanceof Meta.DataStream dataStream) {
                                return dataStream.resolveDeepToNames(resolutionMode).stream();
                            } else {
                                // This probably will never be executed. Alias can point to indices or data streams
                                return super.resolveDeepToNamesImpl(resolutionMode).stream();
                            }
                        }).collect(ImmutableSet.collector());
            } else {
                return super.resolveDeepToNamesImpl(resolutionMode);
            }
        }
    }

    public static class DataStreamImpl extends AbstractIndexCollection<DataStreamImpl, Index> implements Meta.DataStream {
        private final ImmutableSet<Index> dataMember;
        private final ImmutableSet<Index> failureMember;

        public DataStreamImpl(DefaultMetaImpl root, String name, Collection<String> parentAliasNames,
                ImmutableSet<Index> dataMember, ImmutableSet<Index> failureMember,
                boolean hidden) {
            super(root, name, parentAliasNames, null, dataMember.with(failureMember), hidden, determineComponent(failureMember));
            this.dataMember = dataMember;
            this.failureMember = failureMember;
        }

        private static Component[] determineComponent(UnmodifiableCollection<Index> failureMember) {
            ImmutableSet<Component> components = ImmutableSet.of(Component.NONE);
            if(failureMember.isEmpty()) {
                return components.toArray(Component[]::new);
            }
            return components.with(Component.FAILURES).toArray(Component[]::new);
        }

        @Override
        protected AbstractIndexLike<DataStreamImpl> withAlias(String alias) {
            // TODO: CS do we need information about component selector in parent alias? Probably we don't
            return new DataStreamImpl(null, name(), ImmutableSet.of(this.parentAliasNames()).with(alias), dataMember, failureMember, isHidden());
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
            return new DataStreamImpl(null, name(), parentAliasNames(), dataMember, failureMember, isHidden());
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
        private final ImmutableSet<Component> components;
        private DefaultMetaImpl root;
        private ImmutableSet<Meta.IndexOrNonExistent> cachedResolveDeep;
        private ImmutableSet<Meta.IndexOrNonExistent> cachedResolveDeepWrite;
        private ImmutableSet<String> cachedResolveDeepToNames;
        private ImmutableSet<String> cachedResolveDeepToNamesWrite;
        private ImmutableSet<Meta.Alias> cachedParentAliases;

        public AbstractIndexLike(DefaultMetaImpl root, String name, Collection<String> parentAliasNames, String parentDataStreamName,
                boolean hidden, Component... components) {
            this.name = Objects.requireNonNull(name);
            this.parentAliasNames = parentAliasNames != null ? parentAliasNames : ImmutableSet.empty();
            this.parentDataStreamName = parentDataStreamName;
            this.hidden = hidden;
            this.root = root;
            this.components = ImmutableSet.ofArray(components);
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
        public ImmutableSet<Component> components() {
            return this.components;
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
            return name();
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

    // TODO CS: introduce type parameter for memebers
    static abstract class AbstractIndexCollection<T, Member extends IndexLikeObject> extends AbstractIndexLike<T> implements IndexCollection<Member> {
        private final UnmodifiableCollection<Member> members;
        private ImmutableSet<Meta.Index> cachedResolveDeepAsIndex;
        private ImmutableSet<Meta.Index> cachedResolveDeepAsIndexWrite;

        public AbstractIndexCollection(DefaultMetaImpl root, String name, Collection<String> parentAliasNames, String parentDataStreamName,
                UnmodifiableCollection<Member> members, boolean hidden, Component... component) {
            super(root, name, parentAliasNames, parentDataStreamName, hidden, component);
            this.members = members;
        }

        @Override
        public UnmodifiableCollection<Member> members() {
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

        public DefaultMetaImpl(org.elasticsearch.cluster.metadata.Metadata esMetadata) { // TODO CS building meta
            ProjectMetadata project = esMetadata.getProject();
            ImmutableSet.Builder<Index> indices = new ImmutableSet.Builder<>(project.indices().size());
            ImmutableMap.Builder<String, Meta.IndexLikeObject> nameMap = new ImmutableMap.Builder<>(project.indices().size());
            ImmutableSet.Builder<Index> indicesWithoutParents = new ImmutableSet.Builder<>(project.indices().size());
            ImmutableMap.Builder<org.elasticsearch.cluster.metadata.AliasMetadata, ImmutableList.Builder<IndexLikeObject>> aliasToIndicesMap = new ImmutableMap.Builder<org.elasticsearch.cluster.metadata.AliasMetadata, ImmutableList.Builder<IndexLikeObject>>()
                    .defaultValue((k) -> new ImmutableList.Builder<IndexLikeObject>());
            ImmutableMap.Builder<org.elasticsearch.cluster.metadata.AliasMetadata, IndexLikeObject> aliasToWriteIndexMap = new ImmutableMap.Builder<org.elasticsearch.cluster.metadata.AliasMetadata, IndexLikeObject>();

            ImmutableMap.Builder<org.elasticsearch.cluster.metadata.DataStreamAlias, List<IndexLikeObject>> dataStreamAliasToIndicesMap = new ImmutableMap.Builder<org.elasticsearch.cluster.metadata.DataStreamAlias, List<IndexLikeObject>>()
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

                ImmutableList<String> parentAliasNames = Optional.ofNullable(dataStreamAliasReverseLookup.get(esDataStream.getName()))
                        .map(ImmutableList.Builder::build)
                        .orElse(ImmutableList.empty());

                Map<Component, ImmutableSet.Builder<Index>> membersByComponent = new HashMap<>(2);

                for (Component component : Component.values()) {
                    //get data stream indices based on component type
                    List<org.elasticsearch.index.Index> esDataStreamIndices = switch (component) {
                        case NONE -> esDataStream.getIndices();
                        case FAILURES -> esDataStream.getFailureIndices();
                    };
                    ImmutableSet.Builder<Index> memberIndices = new ImmutableSet.Builder<>(esDataStreamIndices.size());
                    membersByComponent.put(component, memberIndices);

                    for (org.elasticsearch.index.Index esIndex : esDataStreamIndices) {
                        org.elasticsearch.cluster.metadata.IndexMetadata esIndexMetadata = project.index(esIndex.getName());

                        Index index = new IndexImpl(this, esIndex.getName(), ImmutableSet.empty(), esDataStream.getName(), esIndexMetadata.isHidden(),
                                esIndexMetadata.isSystem(), esIndexMetadata.getState(), component);
                        indices.add(index);
                        nameMap.put(index.name(), index);
                        memberIndices.add(index);
                    }
                }
                DataStream dataStream = new DataStreamImpl(this, esDataStream.getName(), parentAliasNames, membersByComponent.get(Component.NONE).build(), membersByComponent.get(Component.FAILURES).build(), esDataStream.isHidden());  //TODO the CS : two data streams are created here for each component. One should be created instead
                for (String parentAlias : parentAliasNames) {
                    dataStreamAliasToIndicesMap.get(dataStreamsAliases.get(parentAlias)).add(dataStream);
                }
                datastreams.add(dataStream);
                nameMap.put(dataStream.name(), dataStream);
            }

            for (org.elasticsearch.cluster.metadata.IndexMetadata esIndexMetadata : project.indices().values()) {
                String name = esIndexMetadata.getIndex().getName();

                if (nameMap.contains(name)) {
                    // Index has been already created via a DataStream
                    continue;
                }

                Set<String> parentAliases = ImmutableSet.of(esIndexMetadata.getAliases().keySet());
                Index index = new IndexImpl(this, name, parentAliases, null, esIndexMetadata.isHidden(),
                        esIndexMetadata.isSystem(), esIndexMetadata.getState(), Component.NONE);
                indices.add(index);
                nameMap.put(index.name(), index);

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
                org.elasticsearch.cluster.metadata.DataStreamAlias dataStreamAlias = dataStreamsAliases.get(entry.getKey().alias());
                List<IndexLikeObject> dataStreams = dataStreamAlias != null && dataStreamAliasToIndicesMap.contains(dataStreamAlias)
                        ? dataStreamAliasToIndicesMap.get(dataStreamAlias)
                        : ImmutableList.empty();

                ImmutableList<IndexLikeObject> members = entry.getValue().build().with(dataStreams);

                IndexLikeObject writeTarget = aliasToWriteIndexMap.get(entry.getKey());
                if (writeTarget == null && members.size() == 1) {
                    // By ES semantics, if an alias has only one member, this automatically becomes the write index
                    writeTarget = members.only();
                }

                Alias alias = new AliasImpl(this, entry.getKey().alias(), members,
                        entry.getKey().isHidden() != null ? entry.getKey().isHidden() : false, writeTarget, null);
                aliases.add(alias);
                nameMap.put(alias.name(), alias);
            }

            for (Map.Entry<org.elasticsearch.cluster.metadata.DataStreamAlias, List<IndexLikeObject>> entry : dataStreamAliasToIndicesMap.build()
                    .entrySet()) {
                if (nameMap.contains(entry.getKey().getName())) {
                    // Already created above
                    continue;
                }

                // write targets
                String writeTargetName = entry.getKey().getWriteDataStream();
                IndexLikeObject writeTargetData = writeTargetName != null ? nameMap.get(writeTargetName) : null;
                IndexLikeObject writeTargetFailure = extractWriteTargetForDataStreamAlias(project, entry.getKey(), nameMap).orElse(null);

                Alias alias = new AliasImpl(this, entry.getKey().getName(), ImmutableList.of(entry.getValue()), false, writeTargetData, writeTargetFailure);
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

        public static Optional<Meta.IndexLikeObject> extractWriteTargetForDataStreamAlias(
                ProjectMetadata project,
                DataStreamAlias dataStreamAlias,
                ImmutableMap.Builder<String, Meta.IndexLikeObject> nameMap) {
            return Optional.ofNullable(project.getIndicesLookup().get(dataStreamAlias.getAlias())) //
                    .map(esAlias -> esAlias.getWriteFailureIndex(project)) //
                    .map(org.elasticsearch.index.Index::getName) //
                    .map(nameMap::get);
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
            return indices.map(e -> e.name());
        }

        @Override
        public Iterable<String> namesOfIndexCollections() {
            // TODO optimize or remove

            return indexCollections.map(e -> e.name());
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
                            indexLikeObject = new IndexImpl(null, indexName, singleAliasSet, null, Component.NONE);
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
                            .with(new AliasImpl(null, aliasName, members, false, writeTarget, null));

                    return new DefaultMetaImpl(newIndices.build(), aliases, newDataStreams.build(), DefaultMetaImpl.this.indicesWithoutParents);
                }

            };
        }

        @Override
        public Meta.Mock.DataStreamBuilder dataStream(String dataStreamName) {
            return new Meta.Mock.DataStreamBuilder() {

                @Override
                public Meta of(String... indexNames) {
                    ImmutableMap.Builder<String, Index> dataStreamMembersBuilder = new ImmutableMap.Builder<>(indexNames.length);
                    ImmutableSet.Builder<Index> newIndices = new ImmutableSet.Builder<>(indexNames.length);

                    for (String indexName : indexNames) {
                        Index index = null;
                        if (getIndexOrLike(indexName) != null) {
                            throw new RuntimeException("Cannot reuse datastream backing index");
                        } else {
                            index = new IndexImpl(null, indexName, ImmutableSet.empty(), dataStreamName, Component.NONE);
                        }

                        newIndices.add((Index) index);

                        dataStreamMembersBuilder.put(indexName, index);
                    }

                    for (Index existingIndex : DefaultMetaImpl.this.indices) {
                        if (!newIndices.contains(existingIndex)) {
                            newIndices.add(((IndexImpl) existingIndex).copy());
                        }
                    }

                    ImmutableSet<Index> indices = newIndices.build();
                    UnmodifiableCollection<Index> dataMembers = dataStreamMembersBuilder.build().values();
                    ImmutableSet<DataStream> dataStreams = ImmutableSet
                            .<DataStream>of(DefaultMetaImpl.this.dataStreams.map(i -> ((DataStreamImpl) i).copy()))
                            .with(new DataStreamImpl(null, dataStreamName, ImmutableSet.empty(), ImmutableSet.of(dataMembers), ImmutableSet.empty(), false)); //todo add parameter for component?

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
            ImmutableSet<Index> indices = ImmutableSet.map(Arrays.asList(indexNames), (k) -> new IndexImpl(null, k, ImmutableSet.empty(), null, Component.NONE));

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
            ImmutableSet<Index> indices = ImmutableSet.map(Arrays.asList(indexNames), (k) -> new IndexImpl(null, k, aliasSet, null, Component.NONE));
            ImmutableSet<Alias> aliases = ImmutableSet
                    .of(new AliasImpl(null, name, ImmutableSet.<IndexLikeObject>of(indices), false, indices.size() == 1 ? indices.only() : null, null));

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
            ImmutableSet<Index> indices = ImmutableSet.map(Arrays.asList(indexNames), k -> new IndexImpl(null, k, null, name, Component.NONE));
            ImmutableSet<DataStream> dataStreams = ImmutableSet
                    .of(new DataStreamImpl(null, name, ImmutableSet.empty(), ImmutableSet.of(indices), ImmutableSet.empty(), false));

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
        public ImmutableSet<Component> components() {
            return ImmutableSet.of(Component.NONE);
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
        public IndexLikeObject writeTarget(Component component) {
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
        public UnmodifiableCollection<Index> members() {
            return ImmutableList.empty();
        }

        @Override
        public ImmutableSet<Index> resolveDeepAsIndex(Alias.ResolutionMode resolutionMode) {
            return ImmutableSet.empty();
        }

    }
}
