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
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.TermsQueryBuilder;

import com.floragunn.searchguard.support.ConfigConstants;
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
            final NamedXContentRegistry namedXContentRegistry, final ThreadContext threadContext) throws IOException {

        if (unparsedDlsQueries == null || unparsedDlsQueries.isEmpty()) {
            return null;
        }
        
        if(skip(unparsedDlsQueries, namedXContentRegistry, threadContext)) {
        	System.out.println("skipped2");
        	return null;
        }
        
        final boolean hasNestedMapping = queryShardContext.getMapperService().hasNested();

        BooleanQuery.Builder dlsQueryBuilder = new BooleanQuery.Builder();
        dlsQueryBuilder.setMinimumNumberShouldMatch(1);

        for (final String unparsedDlsQuery : unparsedDlsQueries) {

            	final QueryBuilder qb = parse(unparsedDlsQuery, namedXContentRegistry);
                final ParsedQuery parsedQuery = parseSafely(queryShardContext, qb);
                final Query dlsQuery = parsedQuery.query();
                dlsQueryBuilder.add(dlsQuery, Occur.SHOULD);
                
                if (hasNestedMapping) {
                    handleNested(queryShardContext, dlsQueryBuilder, dlsQuery);
                }
        }

        // no need for scoring here, so its possible to wrap this in a
        // ConstantScoreQuery
        return new ConstantScoreQuery(dlsQueryBuilder.build());

    }
    
    static ParsedQuery parse(final Set<String> unparsedDlsQueries, ParsedQuery originalQuery, final QueryShardContext queryShardContext,
            final NamedXContentRegistry namedXContentRegistry, final ThreadContext threadContext) throws IOException {
        if (unparsedDlsQueries == null || unparsedDlsQueries.isEmpty()) {
            return null;
        }
        
        if(skip(unparsedDlsQueries, namedXContentRegistry, threadContext)) {
        	System.out.println("skipped1");
        	return null;
        }
        
        final boolean hasNestedMapping = queryShardContext.getMapperService().hasNested();

        BooleanQuery.Builder dlsQueryBuilder = new BooleanQuery.Builder();
        dlsQueryBuilder.setMinimumNumberShouldMatch(1);

        for (final String unparsedDlsQuery : unparsedDlsQueries) {
         

            	final QueryBuilder qb = parse(unparsedDlsQuery, namedXContentRegistry);
                final ParsedQuery parsedQuery = parseSafely(queryShardContext, qb);
                
                // no need for scoring here, so its possible to wrap this in a
                // ConstantScoreQuery
                final Query dlsQuery = new ConstantScoreQuery(parsedQuery.query());
                dlsQueryBuilder.add(dlsQuery, Occur.SHOULD);
                
                if (hasNestedMapping) {
                    handleNested(queryShardContext, dlsQueryBuilder, dlsQuery);
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

    private static boolean skip(final Set<String> unparsedDlsQueries, final NamedXContentRegistry namedXContentRegistry, final ThreadContext threadContext) throws IOException {
    	
    	if(unparsedDlsQueries.size() == 1) {
    		final String unparsedDlsQuery = unparsedDlsQueries.iterator().next();
    		final QueryBuilder qb = parse(unparsedDlsQuery, namedXContentRegistry);
    		
    		if (qb.getClass() == TermsQueryBuilder.class && ((TermsQueryBuilder) qb).termsLookup() != null) {
    			
    			final String actionName = (String) threadContext.getTransient(ConfigConstants.SG_ACTION_NAME);
    	    	
    			if(actionName.startsWith("indices:data/read/search") && threadContext.getTransient("_sg_issuggest") != Boolean.TRUE) {
    				//we skip dls here because its handled in the valve
        			return true;
    			}
    		}
    		
    	} else {
    		for (final String unparsedDlsQuery : unparsedDlsQueries) {
    			final QueryBuilder qb = parse(unparsedDlsQuery, namedXContentRegistry);
        		
        		if (qb.getClass() == TermsQueryBuilder.class) { 
        			throw new ElasticsearchSecurityException("Terms lookup queries are not supported as dls queries alongside with other queries");
        		}
    		}
    	}
    	
    	return false;
    }
    
    private static QueryBuilder parse(final String unparsedDlsQuery, final NamedXContentRegistry namedXContentRegistry) throws IOException {
    	try {
			final QueryBuilder qb = queries.get(unparsedDlsQuery, new Callable<QueryBuilder>() {

			    @Override
			    public QueryBuilder call() throws Exception {
			        final XContentParser parser = JsonXContent.jsonXContent.createParser(namedXContentRegistry, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, unparsedDlsQuery);                
			        final QueryBuilder qb = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
			        return qb;
			    }

			});
			
			return qb;
		} catch (ExecutionException e) {
			if(e.getCause() instanceof IOException) {
				throw (IOException) e.getCause();
			} else {
				throw new IOException(e.getCause());
			}
		}
    }
    
    private static ParsedQuery parseSafely(final QueryShardContext queryShardContext, QueryBuilder qb) throws IOException {
        try {
			return queryShardContext.toQuery(qb);
		} catch (Exception e) {
			//https://forum.search-guard.com/t/terms-lookup-in-dls-query/1479			
			throw new IOException("Terms lookup queries, geo shape queries with indexed shapes and percolate queries are not supported as DLS queries", e);
		}

    }

}
