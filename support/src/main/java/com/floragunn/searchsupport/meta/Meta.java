package com.floragunn.searchsupport.meta;

import java.util.Collection;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.fluent.collections.UnmodifiableCollection;
import com.floragunn.searchsupport.meta.MetaImpl.DefaultMetaImpl;

/**
 * Abstraction for the ES index space metadata. 
 * 
 * Provides unified, controlled and uncluttered interfaces to the ES metadata.
 */
public interface Meta {
    Iterable<IndexLikeObject> indexLikeObjects();

    Iterable<Index> indices();

    Iterable<IndexCollection> indexCollections();

    Iterable<String> namesOfIndices();

    Iterable<String> namesOfIndexCollections();

    IndexLikeObject getIndexOrLike(String name);

    boolean equals(Object other);

    int hashCode();

    Mock.AliasBuilder alias(String aliasName);

    Mock.DataStreamBuilder dataStream(String dataStreamName);

    interface IndexLikeObject {
        String name();

        ImmutableSet<Index> resolveDeep();

        ImmutableSet<String> resolveDeepToNames();

        Collection<String> parentAliasNames();

        String parentDataStreamName();

        boolean equals(Object other);

        int hashCode();

    }

    interface Index extends IndexLikeObject {

    }

    interface IndexCollection extends IndexLikeObject {
        UnmodifiableCollection<IndexLikeObject> members();
    }

    interface Alias extends IndexCollection {

    }

    interface DataStream extends IndexCollection {

    }

    static Meta from(org.elasticsearch.cluster.metadata.Metadata esMetadata) {
        return new DefaultMetaImpl(esMetadata);
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