package com.floragunn.searchsupport.meta;

import java.util.Collection;

import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.fluent.collections.UnmodifiableCollection;
import com.floragunn.searchsupport.meta.MetaImpl.DefaultMetaImpl;

/**
 * Abstraction for the ES index space metadata. 
 * 
 * Provides unified, controlled and uncluttered interfaces to the ES metadata.
 */
public interface Meta extends Document<Meta> {
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
    
    
    /**
     * Returns indices that are not hidden 
     */
    ImmutableSet<Index> nonHiddenIndices();

    /**
     * Returns indices that are not hidden and which are not contained in an alias or data stream
     */
    ImmutableSet<Index> nonHiddenIndicesWithoutParents();

    /**
     * Returns indices that are not system indices (but may be possibly hidden indices)
     */
    ImmutableSet<Index> nonSystemIndices();

    /**
     * Returns indices that are not contained in an alias or data stream and that are not system indices (but may be possibly hidden indices)
     */
    ImmutableSet<Index> nonSystemIndicesWithoutParents();

    Iterable<String> namesOfIndices();

    Iterable<String> namesOfIndexCollections();

    IndexLikeObject getIndexOrLike(String name);

    boolean equals(Object other);

    int hashCode();

    Mock.AliasBuilder alias(String aliasName);

    Mock.DataStreamBuilder dataStream(String dataStreamName);

    org.elasticsearch.cluster.metadata.Metadata esMetadata();

    long version();

    interface IndexLikeObject extends Document<IndexLikeObject> {
        String name();

        ImmutableSet<IndexOrNonExistent> resolveDeep();

        ImmutableSet<String> resolveDeepToNames();

        ImmutableSet<Alias> parentAliases();

        DataStream parentDataStream();

        String parentDataStreamName();

        /**
         * Returns the names of the aliases containing this index. 
         */
        Collection<String> parentAliasNames();

        /**
         * Returns the names of the aliases containing this index. Additionally, if this is a data stream backing index, this also returns any aliases containing the data stream.
         */
        Collection<String> ancestorAliasNames();

        boolean equals(Object other);

        int hashCode();

        boolean isHidden();

        boolean exists();
    }

    interface Index extends IndexOrNonExistent {
        boolean isOpen();

        boolean isSystem();
    }

    interface IndexCollection extends IndexLikeObject {
        UnmodifiableCollection<IndexLikeObject> members();

        ImmutableSet<Index> resolveDeepAsIndex();

        static ImmutableSet<Index> resolveDeep(ImmutableSet<? extends Meta.IndexCollection> aliasesAndDataStreams) {
            if (aliasesAndDataStreams.size() == 0) {
                return ImmutableSet.empty();
            }

            if (aliasesAndDataStreams.size() == 1) {
                return aliasesAndDataStreams.only().resolveDeepAsIndex();
            }

            ImmutableSet.Builder<Index> result = new ImmutableSet.Builder<>(aliasesAndDataStreams.size() * 20);

            for (Meta.IndexCollection object : aliasesAndDataStreams) {
                result.addAll(object.resolveDeepAsIndex());
            }

            return result.build();
        }
    }

    interface Alias extends IndexCollection {
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
            return DefaultMetaImpl.indices(indexNames);
        }

        static AliasBuilder alias(String aliasName) {
            return new DefaultMetaImpl.AliasBuilderImpl(aliasName);
        }

        static DataStreamBuilder dataStream(String dataStreamName) {
            return new DefaultMetaImpl.DataStreamBuilderImpl(dataStreamName);
        }

        /**
         * For testing and mocking purposes
         */
        static interface AliasBuilder {
            Meta of(String... indexNames);
        }

        /**
         * For testing and mocking purposes
         */
        static interface DataStreamBuilder {
            Meta of(String... indexNames);
        }
    }
}