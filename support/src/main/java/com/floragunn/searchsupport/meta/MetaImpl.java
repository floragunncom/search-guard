package com.floragunn.searchsupport.meta;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.fluent.collections.UnmodifiableCollection;

public abstract class MetaImpl implements Meta {

    public static class IndexImpl extends AbstractIndexLike<IndexImpl> implements Meta.Index {
        private final boolean open;

        public IndexImpl(DefaultMetaImpl root, String name, Collection<String> parentAliasNames, String parentDataStreamName, boolean hidden,
                org.elasticsearch.cluster.metadata.IndexMetadata.State state) {
            super(root, name, parentAliasNames, parentDataStreamName, hidden);
            this.open = state == org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN;
        }

        @Override
        protected ImmutableSet<Meta.IndexOrNonExistent> resolveDeepImpl() {
            return ImmutableSet.of(this);
        }

        @Override
        protected ImmutableSet<String> resolveDeepToNamesImpl() {
            return ImmutableSet.of(this.name());
        }

        @Override
        protected AbstractIndexLike<IndexImpl> withAlias(String alias) {
            return new IndexImpl(null, name(), ImmutableSet.of(this.parentAliasNames()).with(alias), parentDataStreamName(),
                    isHidden(), this.open ? org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN
                            : org.elasticsearch.cluster.metadata.IndexMetadata.State.CLOSE);
        }

        @Override
        public boolean isOpen() {
            return open;
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
            return new IndexImpl(null, name(), parentAliasNames(), parentDataStreamName(), isHidden(),
                    open ? org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN
                            : org.elasticsearch.cluster.metadata.IndexMetadata.State.CLOSE);
        }
    }

    public static class AliasImpl extends AbstractIndexCollection<AliasImpl> implements Meta.Alias {
        public AliasImpl(DefaultMetaImpl root, String name, UnmodifiableCollection<IndexLikeObject> members, boolean hidden) {
            super(root, name, ImmutableSet.empty(), null, members, hidden);
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
            return new AliasImpl(null, name(), members(), isHidden());
        }
    }

    public static class DataStreamImpl extends AbstractIndexCollection<DataStreamImpl> implements Meta.DataStream {
        public DataStreamImpl(DefaultMetaImpl root, String name, Collection<String> parentAliasNames, UnmodifiableCollection<IndexLikeObject> members,
                boolean hidden) {
            super(root, name, parentAliasNames, null, members, hidden);
        }

        @Override
        protected AbstractIndexLike<DataStreamImpl> withAlias(String alias) {
            return new DataStreamImpl(null, name(), ImmutableSet.of(this.parentAliasNames()).with(alias), members(),
                    isHidden());
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
    }

    public static class NonExistentImpl implements Meta.NonExistent {
        private final String name;

        public NonExistentImpl(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public ImmutableSet<Meta.IndexOrNonExistent> resolveDeep() {
            return ImmutableSet.of(this);
        }

        @Override
        public ImmutableSet<String> resolveDeepToNames() {
            return ImmutableSet.of(name);
        }

        @Override
        public Collection<String> parentAliasNames() {
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
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            } else if (other instanceof Meta.NonExistent) {
                return ((Meta.NonExistent) other).name().equals(this.name());
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public ImmutableSet<Alias> parentAliases() {
            return ImmutableSet.empty();
        }

        @Override
        public DataStream parentDataStream() {
            return null;
        }
    }

    static abstract class AbstractIndexLike<T> implements Meta.IndexLikeObject {
        private final String name;
        private final Collection<String> parentAliasNames;
        private final String parentDataStreamName;
        private final boolean hidden;
        private DefaultMetaImpl root;
        private ImmutableSet<Meta.IndexOrNonExistent> cachedResolveDeep;
        private ImmutableSet<String> cachedResolveDeepToNames;
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
        public ImmutableSet<Meta.IndexOrNonExistent> resolveDeep() {
            ImmutableSet<Meta.IndexOrNonExistent> result = this.cachedResolveDeep;

            if (result == null) {
                result = this.resolveDeepImpl();
                this.cachedResolveDeep = result;
            }

            return result;
        }

        @Override
        public ImmutableSet<String> resolveDeepToNames() {
            ImmutableSet<String> result = this.cachedResolveDeepToNames;

            if (result == null) {
                result = this.resolveDeepToNamesImpl();
                this.cachedResolveDeepToNames = result;
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

        protected abstract ImmutableSet<Meta.IndexOrNonExistent> resolveDeepImpl();

        protected abstract ImmutableSet<String> resolveDeepToNamesImpl();

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

    }

    static abstract class AbstractIndexCollection<T> extends AbstractIndexLike<T> implements IndexCollection {
        private final UnmodifiableCollection<IndexLikeObject> members;

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
        protected ImmutableSet<Meta.IndexOrNonExistent> resolveDeepImpl() {
            ImmutableSet.Builder<Meta.IndexOrNonExistent> result = new ImmutableSet.Builder<>(this.members.size());

            for (IndexLikeObject member : this.members) {
                if (member instanceof Meta.Index) {
                    result.add((Meta.Index) member);
                } else {
                    result.addAll(member.resolveDeep());
                }
            }

            return result.build();
        }

        @Override
        protected ImmutableSet<String> resolveDeepToNamesImpl() {
            ImmutableSet.Builder<String> result = new ImmutableSet.Builder<>(this.members.size());

            for (IndexLikeObject member : this.members) {
                if (member instanceof Meta.Index) {
                    result.add(member.name());
                } else {
                    result.addAll(member.resolveDeepToNames());
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
        private final ImmutableSet<IndexCollection> indexCollections;
        private final ImmutableMap<String, Meta.IndexLikeObject> nameMap;
        private final org.elasticsearch.cluster.metadata.Metadata esMetadata;

        /**
         * For testing only!
         */
        public DefaultMetaImpl(ImmutableSet<Index> indices, ImmutableSet<Alias> aliases, ImmutableSet<DataStream> datastreams,
                ImmutableSet<Index> indicesWithoutParents) {
            indices.forEach((i) -> ((AbstractIndexLike) i).root(this));
            aliases.forEach((i) -> ((AbstractIndexLike) i).root(this));
            datastreams.forEach((i) -> ((AbstractIndexLike) i).root(this));

            this.indices = indices;
            this.aliases = aliases;
            this.dataStreams = datastreams;
            this.indicesWithoutParents = indicesWithoutParents;
            this.indexCollections = ImmutableSet.<IndexCollection>of(aliases).with(datastreams);
            this.nonHiddenIndicesWithoutParents = this.indicesWithoutParents.matching(e -> !e.isHidden());
            this.nonHiddenIndices = this.indices.matching(e -> !e.isHidden());
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
                esMetadataBuilder.put(new org.elasticsearch.cluster.metadata.DataStream(dataStream.name(),
                        new org.elasticsearch.cluster.metadata.DataStream.TimestampField("ts"),
                        ImmutableList.of(dataStream.members()).map(i -> new org.elasticsearch.index.Index(i.name(), i.name())), 1,
                        ImmutableMap.empty(), false, false, false));
            }

            this.esMetadata = esMetadataBuilder.build();
        }

        public DefaultMetaImpl(org.elasticsearch.cluster.metadata.Metadata esMetadata) {
            ImmutableSet.Builder<Index> indices = new ImmutableSet.Builder<>(esMetadata.getIndices().size());
            ImmutableMap.Builder<String, Meta.IndexLikeObject> nameMap = new ImmutableMap.Builder<>(esMetadata.getIndices().size());
            ImmutableSet.Builder<Index> indicesWithoutParents = new ImmutableSet.Builder<>(esMetadata.getIndices().size());
            ImmutableMap.Builder<org.elasticsearch.cluster.metadata.AliasMetadata, ImmutableList.Builder<IndexLikeObject>> aliasToIndicesMap = new ImmutableMap.Builder<org.elasticsearch.cluster.metadata.AliasMetadata, ImmutableList.Builder<IndexLikeObject>>()
                    .defaultValue((k) -> new ImmutableList.Builder<IndexLikeObject>());
            ImmutableSet.Builder<Alias> aliases = new ImmutableSet.Builder<>(64);
            ImmutableSet.Builder<DataStream> datastreams = new ImmutableSet.Builder<>(64);
            this.esMetadata = esMetadata;

            // TODO data stream aliases
            Map<String, org.elasticsearch.cluster.metadata.DataStreamAlias> dataStreamsAliases = esMetadata.dataStreamAliases();

            for (org.elasticsearch.cluster.metadata.DataStream esDataStream : esMetadata.dataStreams().values()) {
                ImmutableList.Builder<IndexLikeObject> memberIndices = new ImmutableList.Builder<>(esDataStream.getIndices().size());

                for (org.elasticsearch.index.Index esIndex : esDataStream.getIndices()) {
                    org.elasticsearch.cluster.metadata.IndexMetadata esIndexMetadata = esMetadata.index(esIndex.getName());

                    Index index = new IndexImpl(this, esIndex.getName(), ImmutableSet.empty(), esDataStream.getName(), esIndexMetadata.isHidden(),
                            esIndexMetadata.getState());
                    indices.add(index);
                    nameMap.put(index.name(), index);
                    memberIndices.add(index);
                }

                DataStream dataStream = new DataStreamImpl(this, esDataStream.getName(), null, memberIndices.build(), esDataStream.isHidden());
                datastreams.add(dataStream);
                nameMap.put(dataStream.name(), dataStream);

            }

            for (org.elasticsearch.cluster.metadata.IndexMetadata esIndexMetadata : esMetadata.getIndices().values()) {
                String name = esIndexMetadata.getIndex().getName();

                if (nameMap.contains(name)) {
                    // Index has been already created via a DataStream
                    continue;
                }

                Index index = new IndexImpl(this, name, esIndexMetadata.getAliases().keySet(), null, esIndexMetadata.isHidden(),
                        esIndexMetadata.getState());
                indices.add(index);
                nameMap.put(name, index);

                if (esIndexMetadata.getAliases().isEmpty()) {
                    indicesWithoutParents.add(index);
                }

                for (org.elasticsearch.cluster.metadata.AliasMetadata esAliasMetadata : esIndexMetadata.getAliases().values()) {
                    aliasToIndicesMap.get(esAliasMetadata).add(index);
                }
            }

            for (Map.Entry<org.elasticsearch.cluster.metadata.AliasMetadata, ImmutableList.Builder<IndexLikeObject>> entry : aliasToIndicesMap.build()
                    .entrySet()) {
                Alias alias = new AliasImpl(this, entry.getKey().alias(), entry.getValue().build(), entry.getKey().isHidden());
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
            this.nameMap = nameMap.build();
        }

        @Override
        public IndexLikeObject getIndexOrLike(String name) {
            return this.nameMap.get(name);
        }

        @Override
        public ImmutableMap<String, IndexLikeObject> indexLikeObjects() {
            // TODO
            //return ImmutableSet.of((UnmodifiableCollection<IndexLikeObject>) (UnmodifiableCollection) indices.values())
            //        .with((UnmodifiableCollection<IndexLikeObject>) (UnmodifiableCollection) indexCollections.values());
            throw new RuntimeException("not implemented");
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

                    for (String indexName : indexNames) {
                        AbstractIndexLike<?> indexLikeObject = (AbstractIndexLike<?>) getIndexOrLike(indexName);

                        if (indexLikeObject != null) {
                            indexLikeObject = indexLikeObject.withAlias(aliasName);
                        } else {
                            indexLikeObject = new IndexImpl(null, indexName, singleAliasSet, null, false,
                                    org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN);
                        }

                        newIndices.add((Index) indexLikeObject);

                        aliasMembersBuilder.put(indexName, indexLikeObject);
                    }

                    for (Index existingIndex : DefaultMetaImpl.this.indices) {
                        if (!newIndices.contains(existingIndex)) {
                            newIndices.add(((IndexImpl) existingIndex).copy());
                        }
                    }

                    ImmutableSet<Index> indices = newIndices.build();
                    ImmutableSet<Alias> aliases = ImmutableSet.<Alias>of(DefaultMetaImpl.this.aliases.map(i -> ((AliasImpl) i).copy()))
                            .with(new AliasImpl(null, aliasName, aliasMembersBuilder.build().values(), false));

                    return new DefaultMetaImpl(indices, aliases, DefaultMetaImpl.this.dataStreams, DefaultMetaImpl.this.indicesWithoutParents);
                }

            };
        }

        @Override
        public Meta.Mock.DataStreamBuilder dataStream(String dataStreamName) {
            return new Meta.Mock.DataStreamBuilder() {

                @Override
                public Meta of(String... indexNames) {
                    ImmutableSet<String> dataStreamSet = ImmutableSet.of(dataStreamName);
                    ImmutableMap.Builder<String, IndexLikeObject> dataStreamMembersBuilder = new ImmutableMap.Builder<>(indexNames.length);
                    ImmutableSet.Builder<Index> newIndices = new ImmutableSet.Builder<>(indexNames.length);

                    for (String indexName : indexNames) {
                        AbstractIndexLike<?> indexLikeObject = (AbstractIndexLike<?>) getIndexOrLike(indexName);

                        if (indexLikeObject != null) {
                            indexLikeObject = indexLikeObject.withAlias(dataStreamName);
                        } else {
                            indexLikeObject = new IndexImpl(null, indexName, dataStreamSet, null, false,
                                    org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN);
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
            ImmutableSet<Index> indices = ImmutableSet.map(Arrays.asList(indexNames),
                    (k) -> new IndexImpl(null, k, ImmutableSet.empty(), null, false, org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN));

            return new DefaultMetaImpl(indices, ImmutableSet.empty(), ImmutableSet.empty(), indices);
        }

        private static final AtomicReference<DefaultMetaImpl> currentInstance = new AtomicReference<>();

        static Meta from(org.elasticsearch.cluster.service.ClusterService clusterService) {
            DefaultMetaImpl currentInstance = DefaultMetaImpl.currentInstance.get();
            org.elasticsearch.cluster.metadata.Metadata esMetadata = clusterService.state().metadata();

            if (currentInstance == null || currentInstance.esMetadata.version() != esMetadata.version()) {
                currentInstance = new DefaultMetaImpl(esMetadata);
                DefaultMetaImpl.currentInstance.set(currentInstance);
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
            ImmutableSet<Index> indices = ImmutableSet.map(Arrays.asList(indexNames),
                    (k) -> new IndexImpl(null, k, aliasSet, null, false, org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN));
            ImmutableSet<Alias> aliases = ImmutableSet.of(new AliasImpl(null, name, ImmutableSet.<IndexLikeObject>of(indices), false));

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
            ImmutableSet<Index> indices = ImmutableSet.map(Arrays.asList(indexNames),
                    k -> new IndexImpl(null, k, null, name, false, org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN));
            ImmutableSet<DataStream> dataStreams = ImmutableSet
                    .of(new DataStreamImpl(null, name, ImmutableSet.empty(), ImmutableSet.<IndexLikeObject>of(indices), false));

            return new DefaultMetaImpl(indices, ImmutableSet.empty(), dataStreams, ImmutableSet.empty());
        }
    }
}
