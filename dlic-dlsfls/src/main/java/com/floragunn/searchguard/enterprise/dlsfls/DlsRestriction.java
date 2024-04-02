/*
 * Copyright 2016-2022 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */
package com.floragunn.searchguard.enterprise.dlsfls;

import java.util.function.Function;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermsQueryBuilder;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.queries.QueryBuilderTraverser;

public class DlsRestriction {

    public static final DlsRestriction NONE = new DlsRestriction(ImmutableList.empty());
    public static final DlsRestriction FULL = new DlsRestriction(ImmutableList.of(com.floragunn.searchsupport.queries.Query.MATCH_NONE));

    private static final Query NON_NESTED_QUERY;

    static {
        //Match all documents but not the nested ones
        //Nested document types start with __ 
        //https://discuss.elastic.co/t/whats-nested-documents-layout-inside-the-lucene/59944/9
        NON_NESTED_QUERY = new BooleanQuery.Builder().add(new MatchAllDocsQuery(), Occur.FILTER)
                .add(new PrefixQuery(new Term("_type", "__")), Occur.MUST_NOT).build();
    }

    private final ImmutableList<com.floragunn.searchsupport.queries.Query> queries;

    DlsRestriction(ImmutableList<com.floragunn.searchsupport.queries.Query> queries) {
        this.queries = queries;
    }

    public boolean isUnrestricted() {
        return this.queries.isEmpty();
    }

    public BooleanQuery.Builder toBooleanQueryBuilder(SearchExecutionContext searchExecutionContext, Function<Query, Query> queryMapFunction) {
        if (this.queries.isEmpty()) {
            return null;
        }

        boolean hasNestedMapping = searchExecutionContext.hasNested();
        
        if (!hasNestedMapping && this.queries.size() == 1) {
            
        }

        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        queryBuilder.setMinimumNumberShouldMatch(1);

        for (com.floragunn.searchsupport.queries.Query dlsQuery : this.queries) {
            Query luceneQuery = searchExecutionContext.toQuery(dlsQuery.getQueryBuilder()).query();

            if (queryMapFunction != null) {
                luceneQuery = queryMapFunction.apply(luceneQuery);
            }

            queryBuilder.add(luceneQuery, Occur.SHOULD);

            if (hasNestedMapping) {
                handleNested(searchExecutionContext, queryBuilder, luceneQuery);
            }
        }

        return queryBuilder;
    }

    public Query toQuery(SearchExecutionContext searchExecutionContext, Function<Query, Query> queryMapFunction) {
        if (this.queries.isEmpty()) {
            return null;
        }

        boolean hasNestedMapping = searchExecutionContext.hasNested();
        
        if (!hasNestedMapping && this.queries.size() == 1) {
            Query luceneQuery = searchExecutionContext.toQuery(this.queries.get(0).getQueryBuilder()).query();

            if (queryMapFunction != null) {
                luceneQuery = queryMapFunction.apply(luceneQuery);
            }

            return luceneQuery;
        }

        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        queryBuilder.setMinimumNumberShouldMatch(1);

        for (com.floragunn.searchsupport.queries.Query dlsQuery : this.queries) {
            Query luceneQuery = searchExecutionContext.toQuery(dlsQuery.getQueryBuilder()).query();

            if (queryMapFunction != null) {
                luceneQuery = queryMapFunction.apply(luceneQuery);
            }

            queryBuilder.add(luceneQuery, Occur.SHOULD);

            if (hasNestedMapping) {
                handleNested(searchExecutionContext, queryBuilder, luceneQuery);
            }
        }

        return queryBuilder.build();
    }

    
    boolean containsTermLookupQuery() {
        for (com.floragunn.searchsupport.queries.Query query : this.queries) {
            if (QueryBuilderTraverser.exists(query.getQueryBuilder(),
                    (q) -> (q instanceof TermsQueryBuilder) && ((TermsQueryBuilder) q).termsLookup() != null)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public String toString() {
        if (isUnrestricted()) {
            return "DLS:<none>";
        } else {
            return "DLS:" + queries;
        }
    }

    private static void handleNested(SearchExecutionContext searchExecutionContext, BooleanQuery.Builder dlsQueryBuilder, Query parentQuery) {
        BitSetProducer parentDocumentsFilter = searchExecutionContext.bitsetFilter(NON_NESTED_QUERY);
        dlsQueryBuilder.add(new ToChildBlockJoinQuery(parentQuery, parentDocumentsFilter), Occur.SHOULD);
    }

    public static class IndexMap {
        public static final DlsRestriction.IndexMap NONE = new DlsRestriction.IndexMap(null);

        private final ImmutableMap<String, DlsRestriction> indexMap;

        IndexMap(ImmutableMap<String, DlsRestriction> indexMap) {
            this.indexMap = indexMap;
        }

        public boolean isUnrestricted() {
            return this.indexMap == null;
        }

        public ImmutableMap<String, DlsRestriction> getIndexMap() {
            return indexMap;
        }

        boolean containsTermLookupQuery() {
            if (indexMap == null) {
                return false;
            }

            for (DlsRestriction restriction : this.indexMap.values()) {
                if (restriction.containsTermLookupQuery()) {
                    return true;
                }
            }

            return false;
        }
    }

    public ImmutableList<com.floragunn.searchsupport.queries.Query> getQueries() {
        return queries;
    }
}
