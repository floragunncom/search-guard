/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.configuration;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;


final class DlsQueryParser {
    
    private static final Query NON_NESTED_QUERY;
    
    static {
        //Match all documents but not the nested ones
        //Nested document types start with __ 
        //https://discuss.elastic.co/t/whats-nested-documents-layout-inside-the-lucene/59944/9
        NON_NESTED_QUERY = new BooleanQuery.Builder()
        .add(new MatchAllDocsQuery(), Occur.FILTER)
        .add(new PrefixQuery(new Term("_type", "__")), Occur.MUST_NOT)
        .build();
    }


    private static Cache<String, QueryBuilder> queries = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(4, TimeUnit.HOURS)
            .build();

    private DlsQueryParser() {

    }

    static Query parse(final Set<String> unparsedDlsQueries, final QueryShardContext queryShardContext,
            final NamedXContentRegistry namedXContentRegistry) throws IOException {

        if (unparsedDlsQueries == null || unparsedDlsQueries.isEmpty()) {
            return null;
        }
        
        final boolean hasNestedMapping = queryShardContext.hasNested();

        BooleanQuery.Builder dlsQueryBuilder = new BooleanQuery.Builder();
        dlsQueryBuilder.setMinimumNumberShouldMatch(1);

        for (final String unparsedDlsQuery : unparsedDlsQueries) {
            try {

                final QueryBuilder qb = queries.get(unparsedDlsQuery, new Callable<QueryBuilder>() {

                    @Override
                    public QueryBuilder call() throws Exception {
                        final XContentParser parser = JsonXContent.jsonXContent.createParser(namedXContentRegistry, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, unparsedDlsQuery);                
                        final QueryBuilder qb = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
                        return qb;
                    }

                });
                final ParsedQuery parsedQuery = queryShardContext.toQuery(qb);
                final Query dlsQuery = parsedQuery.query();
                dlsQueryBuilder.add(dlsQuery, Occur.SHOULD);
                
                if (hasNestedMapping) {
                    handleNested(queryShardContext, dlsQueryBuilder, dlsQuery);
                }  

            } catch (ExecutionException e) {
                throw new IOException(e);
            }
        }

        // no need for scoring here, so its possible to wrap this in a
        // ConstantScoreQuery
        return new ConstantScoreQuery(dlsQueryBuilder.build());

    }
    
    static ParsedQuery parse(final Set<String> unparsedDlsQueries, ParsedQuery originalQuery, final QueryShardContext queryShardContext,
            final NamedXContentRegistry namedXContentRegistry) throws IOException {
        if (unparsedDlsQueries == null || unparsedDlsQueries.isEmpty()) {
            return null;
        }
        
        final boolean hasNestedMapping = queryShardContext.hasNested();

        BooleanQuery.Builder dlsQueryBuilder = new BooleanQuery.Builder();
        dlsQueryBuilder.setMinimumNumberShouldMatch(1);

        for (final String unparsedDlsQuery : unparsedDlsQueries) {
            try {

                final QueryBuilder qb = queries.get(unparsedDlsQuery, new Callable<QueryBuilder>() {

                    @Override
                    public QueryBuilder call() throws Exception {
                        final XContentParser parser = JsonXContent.jsonXContent.createParser(namedXContentRegistry, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, unparsedDlsQuery);                
                        final QueryBuilder qb = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
                        return qb;
                    }

                });
                final ParsedQuery parsedQuery = queryShardContext.toQuery(qb);
                
                // no need for scoring here, so its possible to wrap this in a
                // ConstantScoreQuery
                final Query dlsQuery = new ConstantScoreQuery(parsedQuery.query());
                dlsQueryBuilder.add(dlsQuery, Occur.SHOULD);
                
                if (hasNestedMapping) {
                    handleNested(queryShardContext, dlsQueryBuilder, dlsQuery);
                }  

            } catch (ExecutionException e) {
                throw new IOException(e);
            }
        }

        dlsQueryBuilder.add(originalQuery.query(), Occur.MUST);
        return new ParsedQuery(dlsQueryBuilder.build());
    }
    
    private static void handleNested(final QueryShardContext queryShardContext, 
            final BooleanQuery.Builder dlsQueryBuilder, 
            final Query parentQuery) {      
        final BitSetProducer parentDocumentsFilter = queryShardContext.bitsetFilter(NON_NESTED_QUERY);
        dlsQueryBuilder.add(new ToChildBlockJoinQuery(parentQuery, parentDocumentsFilter), Occur.SHOULD);
    }


}
