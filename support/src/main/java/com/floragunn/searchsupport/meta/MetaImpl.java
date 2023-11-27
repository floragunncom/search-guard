package com.floragunn.searchsupport.meta;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.fluent.collections.UnmodifiableCollection;
import com.floragunn.fluent.collections.UnmodifiableMap;

public abstract class MetaImpl implements Meta {

    public static class IndexImpl extends AbstractIndexLike implements Meta.Index {

        public IndexImpl(String name, Collection<String> parentAliasNames, String parentDataStreamName) {
            super(name, parentAliasNames, parentDataStreamName);
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
            return new IndexImpl(name(), ImmutableSet.of(this.parentAliasNames()).with(alias), parentDataStreamName());
        }

    }

    public static class AliasImpl extends AbstractIndexCollection implements Meta.Alias {
        public AliasImpl(String name, UnmodifiableCollection<IndexLikeObject> members) {
            super(name, ImmutableSet.empty(), null, members);
        }

        @Override
        protected AbstractIndexLike withAlias(String alias) {
            throw new RuntimeException("Aliases cannot point to aliases");
        }
    }

    public static class DataStreamImpl extends AbstractIndexCollection implements Meta.DataStream {
        public DataStreamImpl(String name, Collection<String> parentAliasNames, UnmodifiableCollection<IndexLikeObject> members) {
            super(name, parentAliasNames, null, members);
        }

        @Override
        protected AbstractIndexLike withAlias(String alias) {
            return new DataStreamImpl(name(), ImmutableSet.of(this.parentAliasNames()).with(alias), members());
        }
    }

    static abstract class AbstractIndexLike implements Meta.IndexLikeObject {
        private final String name;
        private final Collection<String> parentAliasNames;
        private final String parentDataStreamName;
        private ImmutableSet<Meta.Index> cachedResolveDeep;
        private ImmutableSet<String> cachedResolveDeepToNames;

        public AbstractIndexLike(String name, Collection<String> parentAliasNames, String parentDataStreamName) {
            this.name = Objects.requireNonNull(name);
            this.parentAliasNames = parentAliasNames != null ? parentAliasNames : ImmutableSet.empty();
            this.parentDataStreamName = parentDataStreamName;
        }

        @Override
        public String name() {
            return name;
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
                UnmodifiableCollection<IndexLikeObject> members) {
            super(name, parentAliasNames, parentDataStreamName);
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
        private final UnmodifiableMap<String, Index> indices;
        private final UnmodifiableMap<String, IndexCollection> indexCollections;

        public DefaultMetaImpl(UnmodifiableMap<String, Index> indices, UnmodifiableMap<String, IndexCollection> indexCollections) {
            this.indices = indices;
            this.indexCollections = indexCollections;
        }

        public DefaultMetaImpl(org.elasticsearch.cluster.metadata.Metadata esMetadata) {
            ImmutableMap.Builder<String, Index> indices = new ImmutableMap.Builder<>(esMetadata.getIndices().size());
            ImmutableMap.Builder<String, ImmutableList.Builder<IndexLikeObject>> aliasToIndicesMap = new ImmutableMap.Builder<String, ImmutableList.Builder<IndexLikeObject>>()
                    .defaultValue((k) -> new ImmutableList.Builder<IndexLikeObject>());
            ImmutableMap.Builder<String, IndexCollection> indexCollections = new ImmutableMap.Builder<>();

            Map<String, org.elasticsearch.cluster.metadata.DataStream> dataStreams = esMetadata.dataStreams();
            // TODO data stream aliases

            Map<String, org.elasticsearch.cluster.metadata.DataStreamAlias> dataStreamsAliases = esMetadata.dataStreamAliases();

            for (org.elasticsearch.cluster.metadata.DataStream esDataStream : dataStreams.values()) {
                ImmutableList.Builder<IndexLikeObject> memberIndices = new ImmutableList.Builder<>(esDataStream.getIndices().size());

                for (org.elasticsearch.index.Index esIndex : esDataStream.getIndices()) {
                    Index index = new IndexImpl(esIndex.getName(), ImmutableSet.empty(), esDataStream.getName());
                    indices.put(esIndex.getName(), index);
                    memberIndices.add(index);
                }

                indexCollections.put(esDataStream.getName(), new DataStreamImpl(esDataStream.getName(), null, memberIndices.build()));
            }

            for (org.elasticsearch.cluster.metadata.IndexMetadata esIndexMetadata : esMetadata.getIndices().values()) {
                String name = esIndexMetadata.getIndex().getName();

                if (indices.contains(name)) {
                    // Index has been already created via a DataStream
                    continue;
                }

                Index index = new IndexImpl(name, esIndexMetadata.getAliases().keySet(), null);

                indices.put(name, index);
                for (org.elasticsearch.cluster.metadata.AliasMetadata esAliasMetadata : esIndexMetadata.getAliases().values()) {
                    aliasToIndicesMap.get(esAliasMetadata.alias()).add(index);
                }
            }

            for (Map.Entry<String, ImmutableList.Builder<IndexLikeObject>> entry : aliasToIndicesMap.build().entrySet()) {
                indexCollections.put(entry.getKey(), new AliasImpl(entry.getKey(), entry.getValue().build()));
            }

            this.indices = indices.build();
            this.indexCollections = indexCollections.build();
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
        public Iterable<IndexLikeObject> indexLikeObjects() {
            // TODO
            return ImmutableSet.of((UnmodifiableCollection<IndexLikeObject>) (UnmodifiableCollection) indices.values())
                    .with((UnmodifiableCollection<IndexLikeObject>) (UnmodifiableCollection) indexCollections.values());
        }

        @Override
        public Iterable<Index> indices() {
            return indices.values();
        }

        @Override
        public Iterable<IndexCollection> indexCollections() {
            return indexCollections.values();
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
                            indexLikeObject = new IndexImpl(indexName, singleAliasSet, null);
                            newIndices.put(indexName, (Index) indexLikeObject);
                        }

                        aliasMembersBuilder.put(indexName, indexLikeObject);
                    }

                    ImmutableMap<String, Index> indices = ImmutableMap.of(DefaultMetaImpl.this.indices).with(newIndices.build());
                    ImmutableMap<String, IndexCollection> aliases = ImmutableMap.of(DefaultMetaImpl.this.indexCollections).with(aliasName,
                            new AliasImpl(aliasName, aliasMembersBuilder.build().values()));

                    return new DefaultMetaImpl(indices, aliases);
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
                            indexLikeObject = new IndexImpl(indexName, dataStreamSet, null);
                            newIndices.put(indexName, (Index) indexLikeObject);
                        }

                        dataStreamMembersBuilder.put(indexName, indexLikeObject);
                    }

                    ImmutableMap<String, Index> indices = ImmutableMap.of(DefaultMetaImpl.this.indices).with(newIndices.build());
                    ImmutableMap<String, IndexCollection> aliases = ImmutableMap.of(dataStreamName,
                            new DataStreamImpl(dataStreamName, ImmutableSet.empty(), dataStreamMembersBuilder.build().values()));

                    return new DefaultMetaImpl(indices, aliases);
                }

            };
        }

        @Override
        public String toString() {
            return "{indices: " + indices.size() + "; collections: " + indexCollections.size() + "}";
        }

        /**
         * For testing and mocking purposes
         */
        static Meta indices(String... indexNames) {
            return new DefaultMetaImpl(
                    ImmutableMap.map(Arrays.asList(indexNames), (k) -> ImmutableMap.entry(k, new IndexImpl(k, ImmutableSet.empty(), null))),
                    ImmutableMap.empty());
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
            ImmutableMap<String, Index> indices = ImmutableMap.map(Arrays.asList(indexNames),
                    (k) -> ImmutableMap.entry(k, new IndexImpl(k, aliasSet, null)));
            ImmutableMap<String, IndexCollection> aliases = ImmutableMap.of(name,
                    new AliasImpl(name, (UnmodifiableCollection<IndexLikeObject>) (UnmodifiableCollection) indices.values())); // TODO

            return new DefaultMetaImpl(indices, aliases);
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
            ImmutableMap<String, Index> indices = ImmutableMap.map(Arrays.asList(indexNames),
                    (k) -> ImmutableMap.entry(k, new IndexImpl(k, aliasSet, null)));
            ImmutableMap<String, IndexCollection> aliases = ImmutableMap.of(name, new DataStreamImpl(name, ImmutableSet.empty(),
                    (UnmodifiableCollection<IndexLikeObject>) (UnmodifiableCollection) indices.values())); // TODO

            return new DefaultMetaImpl(indices, aliases);
        }
    }
}
