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

    public static class IndexImpl extends AbstractIndexLike implements Meta.Index {
        private final boolean open;

        public IndexImpl(String name, Collection<String> parentAliasNames, String parentDataStreamName, boolean hidden,
                org.elasticsearch.cluster.metadata.IndexMetadata.State state) {
            super(name, parentAliasNames, parentDataStreamName, hidden);
            this.open = state == org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN;
        }

        @Override
        protected ImmutableSet<Index> resolveDeepImpl() {
            return ImmutableSet.of(this);
        }

        @Override
        protected ImmutableSet<String> resolveDeepToNamesImpl() {
            return ImmutableSet.of(this.name());
        }

        @Override
        protected AbstractIndexLike withAlias(String alias) {
            return new IndexImpl(name(), ImmutableSet.of(this.parentAliasNames()).with(alias), parentDataStreamName(), isHidden(),
                    this.open ? org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN
                            : org.elasticsearch.cluster.metadata.IndexMetadata.State.CLOSE);
        }

        @Override
        public boolean isOpen() {
            return open;
        }

    }

    public static class AliasImpl extends AbstractIndexCollection implements Meta.Alias {
        public AliasImpl(String name, UnmodifiableCollection<IndexLikeObject> members, boolean hidden) {
            super(name, ImmutableSet.empty(), null, members, hidden);
        }

        @Override
        protected AbstractIndexLike withAlias(String alias) {
            throw new RuntimeException("Aliases cannot point to aliases");
        }
    }

    public static class DataStreamImpl extends AbstractIndexCollection implements Meta.DataStream {
        public DataStreamImpl(String name, Collection<String> parentAliasNames, UnmodifiableCollection<IndexLikeObject> members, boolean hidden) {
            super(name, parentAliasNames, null, members, hidden);
        }

        @Override
        protected AbstractIndexLike withAlias(String alias) {
            return new DataStreamImpl(name(), ImmutableSet.of(this.parentAliasNames()).with(alias), members(), isHidden());
        }
    }

    static abstract class AbstractIndexLike implements Meta.IndexLikeObject {
        private final String name;
        private final Collection<String> parentAliasNames;
        private final String parentDataStreamName;
        private final boolean hidden;
        private ImmutableSet<Meta.Index> cachedResolveDeep;
        private ImmutableSet<String> cachedResolveDeepToNames;

        public AbstractIndexLike(String name, Collection<String> parentAliasNames, String parentDataStreamName, boolean hidden) {
            this.name = Objects.requireNonNull(name);
            this.parentAliasNames = parentAliasNames != null ? parentAliasNames : ImmutableSet.empty();
            this.parentDataStreamName = parentDataStreamName;
            this.hidden = hidden;
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
        public ImmutableSet<Meta.Index> resolveDeep() {
            ImmutableSet<Meta.Index> result = this.cachedResolveDeep;

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
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Meta.IndexLikeObject)) {
                return false;
            }
            Meta.IndexLikeObject other = (Meta.IndexLikeObject) obj;

            return other.name().equals(this.name);
        }

        @Override
        public String toString() {
            return this.name;
        }

        protected abstract AbstractIndexLike withAlias(String alias);

        protected abstract ImmutableSet<Meta.Index> resolveDeepImpl();

        protected abstract ImmutableSet<String> resolveDeepToNamesImpl();

    }

    static abstract class AbstractIndexCollection extends AbstractIndexLike implements IndexCollection {
        private final UnmodifiableCollection<IndexLikeObject> members;

        public AbstractIndexCollection(String name, Collection<String> parentAliasNames, String parentDataStreamName,
                UnmodifiableCollection<IndexLikeObject> members, boolean hidden) {
            super(name, parentAliasNames, parentDataStreamName, hidden);
            this.members = members;
        }

        @Override
        public UnmodifiableCollection<IndexLikeObject> members() {
            return members;
        }

        @Override
        protected ImmutableSet<Meta.Index> resolveDeepImpl() {
            ImmutableSet.Builder<Meta.Index> result = new ImmutableSet.Builder<>(this.members.size());

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
        private final ImmutableMap<String, Index> indices;
        private final ImmutableMap<String, Alias> aliases;
        private final ImmutableMap<String, DataStream> dataStreams;
        private final ImmutableMap<String, Index> indicesWithoutParents;
        private final ImmutableMap<String, Index> nonHiddenIndicesWithoutParents;
        private final ImmutableMap<String, Index> nonHiddenIndices;
        private final ImmutableMap<String, IndexCollection> indexCollections;
        private final org.elasticsearch.cluster.metadata.Metadata esMetadata;

        /**
         * For testing only!
         */
        public DefaultMetaImpl(ImmutableMap<String, Index> indices, ImmutableMap<String, Alias> aliases, ImmutableMap<String, DataStream> datastreams,
                ImmutableMap<String, Index> indicesWithoutParents) {
            this.indices = indices;
            this.aliases = aliases;
            this.dataStreams = datastreams;
            this.indicesWithoutParents = indicesWithoutParents;
            this.indexCollections = aliases.assertElementType(String.class, IndexCollection.class)
                    .with(datastreams.assertElementType(String.class, IndexCollection.class));
            this.nonHiddenIndicesWithoutParents = this.indicesWithoutParents.matching((k, v) -> !v.isHidden());
            this.nonHiddenIndices = this.indices.matching((k, v) -> !v.isHidden());

            org.elasticsearch.cluster.metadata.Metadata.Builder esMetadataBuilder = org.elasticsearch.cluster.metadata.Metadata.builder();

            Map<String, org.elasticsearch.cluster.metadata.AliasMetadata> esAliasMetadata = new HashMap<>();

            for (Alias alias : aliases.values()) {
                esAliasMetadata.put(alias.name(), org.elasticsearch.cluster.metadata.AliasMetadata.builder(alias.name()).build());
            }

            for (Index index : indices.values()) {
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

            for (DataStream dataStream : datastreams.values()) {
                esMetadataBuilder.put(new org.elasticsearch.cluster.metadata.DataStream(dataStream.name(),
                        new org.elasticsearch.cluster.metadata.DataStream.TimestampField("ts"),
                        ImmutableList.of(dataStream.members()).map(i -> new org.elasticsearch.index.Index(i.name(), i.name())), 1,
                        ImmutableMap.empty(), false, false, false));
            }

            this.esMetadata = esMetadataBuilder.build();
        }

        public DefaultMetaImpl(org.elasticsearch.cluster.metadata.Metadata esMetadata) {
            ImmutableMap.Builder<String, Index> indices = new ImmutableMap.Builder<>(esMetadata.getIndices().size());
            ImmutableMap.Builder<String, Index> indicesWithoutParents = new ImmutableMap.Builder<>(esMetadata.getIndices().size());
            ImmutableMap.Builder<org.elasticsearch.cluster.metadata.AliasMetadata, ImmutableList.Builder<IndexLikeObject>> aliasToIndicesMap = new ImmutableMap.Builder<org.elasticsearch.cluster.metadata.AliasMetadata, ImmutableList.Builder<IndexLikeObject>>()
                    .defaultValue((k) -> new ImmutableList.Builder<IndexLikeObject>());
            ImmutableMap.Builder<String, Alias> aliases = new ImmutableMap.Builder<>();
            ImmutableMap.Builder<String, DataStream> datastreams = new ImmutableMap.Builder<>();
            this.esMetadata = esMetadata;

            // TODO data stream aliases
            Map<String, org.elasticsearch.cluster.metadata.DataStreamAlias> dataStreamsAliases = esMetadata.dataStreamAliases();

            for (org.elasticsearch.cluster.metadata.DataStream esDataStream : esMetadata.dataStreams().values()) {
                ImmutableList.Builder<IndexLikeObject> memberIndices = new ImmutableList.Builder<>(esDataStream.getIndices().size());

                for (org.elasticsearch.index.Index esIndex : esDataStream.getIndices()) {
                    org.elasticsearch.cluster.metadata.IndexMetadata esIndexMetadata = esMetadata.index(esIndex.getName());

                    Index index = new IndexImpl(esIndex.getName(), ImmutableSet.empty(), esDataStream.getName(), esIndexMetadata.isHidden(),
                            esIndexMetadata.getState());
                    indices.put(esIndex.getName(), index);
                    memberIndices.add(index);
                }

                datastreams.put(esDataStream.getName(),
                        new DataStreamImpl(esDataStream.getName(), null, memberIndices.build(), esDataStream.isHidden()));

            }

            for (org.elasticsearch.cluster.metadata.IndexMetadata esIndexMetadata : esMetadata.getIndices().values()) {
                String name = esIndexMetadata.getIndex().getName();

                if (indices.contains(name)) {
                    // Index has been already created via a DataStream
                    continue;
                }

                Index index = new IndexImpl(name, esIndexMetadata.getAliases().keySet(), null, esIndexMetadata.isHidden(),
                        esIndexMetadata.getState());
                indices.put(name, index);

                if (esIndexMetadata.getAliases().isEmpty()) {
                    indicesWithoutParents.put(name, index);
                }

                for (org.elasticsearch.cluster.metadata.AliasMetadata esAliasMetadata : esIndexMetadata.getAliases().values()) {
                    aliasToIndicesMap.get(esAliasMetadata).add(index);
                }
            }

            for (Map.Entry<org.elasticsearch.cluster.metadata.AliasMetadata, ImmutableList.Builder<IndexLikeObject>> entry : aliasToIndicesMap.build()
                    .entrySet()) {
                aliases.put(entry.getKey().alias(), new AliasImpl(entry.getKey().alias(), entry.getValue().build(), entry.getKey().isHidden()));
            }

            this.indices = indices.build();
            this.indicesWithoutParents = indicesWithoutParents.build();
            this.aliases = aliases.build();
            this.dataStreams = datastreams.build();
            this.indexCollections = this.aliases.assertElementType(String.class, IndexCollection.class)
                    .with(this.dataStreams.assertElementType(String.class, IndexCollection.class));
            this.nonHiddenIndicesWithoutParents = this.indicesWithoutParents.matching((k, v) -> !v.isHidden());
            this.nonHiddenIndices = this.indices.matching((k, v) -> !v.isHidden());
        }

        @Override
        public IndexLikeObject getIndexOrLike(String name) {
            Index index = this.indices.get(name);
            if (index != null) {
                return index;
            }

            IndexCollection indexCollection = this.indexCollections.get(name);
            if (indexCollection != null) {
                return indexCollection;
            }

            return null;
        }

        @Override
        public ImmutableMap<String, IndexLikeObject> indexLikeObjects() {
            // TODO
            //return ImmutableSet.of((UnmodifiableCollection<IndexLikeObject>) (UnmodifiableCollection) indices.values())
            //        .with((UnmodifiableCollection<IndexLikeObject>) (UnmodifiableCollection) indexCollections.values());
            throw new RuntimeException("not implemented");
        }

        @Override
        public ImmutableMap<String, Index> indices() {
            return indices;
        }

        @Override
        public ImmutableMap<String, Index> indicesWithoutParents() {
            return indicesWithoutParents;
        }

        @Override
        public ImmutableMap<String, Alias> aliases() {
            return aliases;
        }

        @Override
        public ImmutableMap<String, DataStream> dataStreams() {
            return dataStreams;
        }

        @Override
        public ImmutableMap<String, IndexCollection> indexCollections() {
            return indexCollections;
        }

        @Override
        public ImmutableMap<String, Index> nonHiddenIndices() {
            return nonHiddenIndices;
        }

        @Override
        public ImmutableMap<String, Index> nonHiddenIndicesWithoutParents() {
            return nonHiddenIndicesWithoutParents;
        }

        @Override
        public Iterable<String> namesOfIndices() {
            return indices.keySet();
        }

        @Override
        public Iterable<String> namesOfIndexCollections() {
            return indexCollections.keySet();
        }

        @Override
        public Meta.Mock.AliasBuilder alias(String aliasName) {
            return new Meta.Mock.AliasBuilder() {

                @Override
                public Meta of(String... indexNames) {
                    ImmutableSet<String> singleAliasSet = ImmutableSet.of(aliasName);
                    ImmutableMap.Builder<String, IndexLikeObject> aliasMembersBuilder = new ImmutableMap.Builder<>(indexNames.length);
                    ImmutableMap.Builder<String, Index> newIndices = new ImmutableMap.Builder<>(indexNames.length);

                    for (String indexName : indexNames) {
                        AbstractIndexLike indexLikeObject = (AbstractIndexLike) getIndexOrLike(indexName);

                        if (indexLikeObject != null) {
                            indexLikeObject = indexLikeObject.withAlias(aliasName);
                        } else {
                            indexLikeObject = new IndexImpl(indexName, singleAliasSet, null, false,
                                    org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN);
                            newIndices.put(indexName, (Index) indexLikeObject);
                        }

                        aliasMembersBuilder.put(indexName, indexLikeObject);
                    }

                    ImmutableMap<String, Index> indices = ImmutableMap.of(DefaultMetaImpl.this.indices).with(newIndices.build());
                    ImmutableMap<String, Alias> aliases = ImmutableMap.of(DefaultMetaImpl.this.aliases).with(aliasName,
                            new AliasImpl(aliasName, aliasMembersBuilder.build().values(), false));

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
                    ImmutableMap.Builder<String, Index> newIndices = new ImmutableMap.Builder<>(indexNames.length);

                    for (String indexName : indexNames) {
                        AbstractIndexLike indexLikeObject = (AbstractIndexLike) getIndexOrLike(indexName);

                        if (indexLikeObject != null) {
                            indexLikeObject = indexLikeObject.withAlias(dataStreamName);
                        } else {
                            indexLikeObject = new IndexImpl(indexName, dataStreamSet, null, false,
                                    org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN);
                            newIndices.put(indexName, (Index) indexLikeObject);
                        }

                        dataStreamMembersBuilder.put(indexName, indexLikeObject);
                    }

                    ImmutableMap<String, Index> indices = ImmutableMap.of(DefaultMetaImpl.this.indices).with(newIndices.build());
                    ImmutableMap<String, DataStream> dataStreams = ImmutableMap.of(DefaultMetaImpl.this.dataStreams).with(dataStreamName,
                            new DataStreamImpl(dataStreamName, ImmutableSet.empty(), dataStreamMembersBuilder.build().values(), false));

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

        /**
         * For testing and mocking purposes
         */
        static Meta indices(String... indexNames) {
            ImmutableMap<String, Index> indices = ImmutableMap.map(Arrays.asList(indexNames), (k) -> ImmutableMap.entry(k,
                    new IndexImpl(k, ImmutableSet.empty(), null, false, org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN)));

            return new DefaultMetaImpl(indices, ImmutableMap.empty(), ImmutableMap.empty(), indices);
        }

        private static final AtomicReference<DefaultMetaImpl> currentInstance = new AtomicReference<>();

        static Meta from(org.elasticsearch.cluster.service.ClusterService clusterService) {
            DefaultMetaImpl currentInstance = DefaultMetaImpl.currentInstance.get();
            org.elasticsearch.cluster.metadata.Metadata esMetadata = clusterService.state().metadata();

            if (currentInstance == null || currentInstance.esMetadata != esMetadata) {
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
            ImmutableMap<String, Index> indices = ImmutableMap.map(Arrays.asList(indexNames), (k) -> ImmutableMap.entry(k,
                    new IndexImpl(k, aliasSet, null, false, org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN)));
            ImmutableMap<String, Alias> aliases = ImmutableMap.of(name,
                    new AliasImpl(name, (UnmodifiableCollection<IndexLikeObject>) (UnmodifiableCollection) indices.values(), false)); // TODO

            return new DefaultMetaImpl(indices, aliases, ImmutableMap.empty(), ImmutableMap.empty());
        }
    }

    static class DataStreamBuilderImpl implements Meta.Mock.DataStreamBuilder {
        private final String name;

        DataStreamBuilderImpl(String name) {
            this.name = name;
        }

        @Override
        public Meta of(String... indexNames) {
            ImmutableSet<String> aliasSet = ImmutableSet.of(name);
            ImmutableMap<String, Index> indices = ImmutableMap.map(Arrays.asList(indexNames), (k) -> ImmutableMap.entry(k,
                    new IndexImpl(k, aliasSet, null, false, org.elasticsearch.cluster.metadata.IndexMetadata.State.OPEN)));
            ImmutableMap<String, DataStream> dataStreams = ImmutableMap.of(name, new DataStreamImpl(name, ImmutableSet.empty(),
                    (UnmodifiableCollection<IndexLikeObject>) (UnmodifiableCollection) indices.values(), false)); // TODO

            return new DefaultMetaImpl(indices, ImmutableMap.empty(), dataStreams, ImmutableMap.empty());
        }
    }
}
