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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
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
    
	private static final Logger log = LogManager.getLogger(DlsQueryParser.class);
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

    //return null means the wrapper is not doing dls
    //the wrapper does dls slow, its only for get and suggest
    //can not handle tl queries
    static Query parseForWrapper(final Set<String> unparsedDlsQueries, final QueryShardContext queryShardContext,
            final NamedXContentRegistry namedXContentRegistry, final ThreadContext threadContext) throws IOException {

        if (unparsedDlsQueries == null || unparsedDlsQueries.isEmpty()) {
            return null;
        }
        
        if(isSearchAndNoSuggest(threadContext)) {
        	stripTermsLookupQueries(unparsedDlsQueries, namedXContentRegistry);
        }
        
        final boolean hasNestedMapping = queryShardContext.getMapperService().hasNested();

        BooleanQuery.Builder dlsQueryBuilder = new BooleanQuery.Builder();
        dlsQueryBuilder.setMinimumNumberShouldMatch(1);

        for (final String unparsedDlsQuery : unparsedDlsQueries) {

            	final QueryBuilder qb = parseRaw(unparsedDlsQuery, namedXContentRegistry);
                final ParsedQuery parsedQuery = parseSafely(queryShardContext, qb);
                final Query dlsQuery = parsedQuery.query();
                dlsQueryBuilder.add(dlsQuery, Occur.SHOULD);
                
                if (hasNestedMapping) {
                    handleNested(queryShardContext, dlsQueryBuilder, dlsQuery);
                }
        }
        
        //to make check for "now" in date math queries we can not perform this check earlier
        if(isSearchAndNoSuggest(threadContext)) {
        	//we handle this in the valve
        	return null;
        }

        // no need for scoring here, so its possible to wrap this in a
        // ConstantScoreQuery
        return new ConstantScoreQuery(dlsQueryBuilder.build());

    }
    
    //return null means the valve is not doing dls
    //the valve does dls fast, its for normal search request
    //can not handle tl queries
    static ParsedQuery parseForValve(final Set<String> unparsedDlsQueries, ParsedQuery originalQuery, final QueryShardContext queryShardContext,
            final NamedXContentRegistry namedXContentRegistry, final ThreadContext threadContext) throws IOException {
        if (unparsedDlsQueries == null || unparsedDlsQueries.isEmpty()) {
            return null;
        }
        
        if(isNoSearchOrSuggest(threadContext)) {
        	//we handle this in the wrapper
        	return null;
        }
        
        //if(threadContext.getTransient("_sg_tl_handled") == Boolean.TRUE) {
        	//ignore TL quietly
            stripTermsLookupQueries(unparsedDlsQueries, namedXContentRegistry);

            if(unparsedDlsQueries.isEmpty()) {
            	return null;
            }
        //}
        
        final boolean hasNestedMapping = queryShardContext.getMapperService().hasNested();

        BooleanQuery.Builder dlsQueryBuilder = new BooleanQuery.Builder();
        dlsQueryBuilder.setMinimumNumberShouldMatch(1);

        for (final String unparsedDlsQuery : unparsedDlsQueries) {

            	final QueryBuilder qb = parseRaw(unparsedDlsQuery, namedXContentRegistry);
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
    
    static QueryBuilder parseRaw(final String unparsedDlsQuery, final NamedXContentRegistry namedXContentRegistry) throws IOException {
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
		} catch (RuntimeException e) {
			//https://forum.search-guard.com/t/terms-lookup-in-dls-query/1479			
			log.warn("Geo shape queries with indexed shapes and percolate queries are not supported as DLS queries. For Terms lookup queries special rules apply.", e);
			throw e;
		}
    }
    
    static Set<String> getTermsLookupQueries(Set<String> unparsedDlsQueries, final NamedXContentRegistry namedXContentRegistry) throws IOException {
    	Set<String> ret = new HashSet<>();
    	for (final String unparsedDlsQuery : unparsedDlsQueries) {
    		if(isTermsLookupQuery(parseRaw(unparsedDlsQuery, namedXContentRegistry))) {
    			ret.add(unparsedDlsQuery);
    		}
    	}
    	
    	return Collections.unmodifiableSet(ret);
    }
    
    private static boolean stripTermsLookupQueries(Set<String> unparsedDlsQueries, final NamedXContentRegistry namedXContentRegistry) throws IOException {
    	Set<String> ret = new HashSet<>();
    	for (final String unparsedDlsQuery : unparsedDlsQueries) {
    		if(isTermsLookupQuery(parseRaw(unparsedDlsQuery, namedXContentRegistry))) {
    			ret.add(unparsedDlsQuery);
    		}
    	}
    	
    	return unparsedDlsQueries.removeAll(ret);
    }
    
    private static boolean isTermsLookupQuery(QueryBuilder qb) {
    	return qb != null && qb.getClass() == TermsQueryBuilder.class && ((TermsQueryBuilder) qb).termsLookup() != null;	
    }
    
    private static boolean isSearchAndNoSuggest(ThreadContext threadContext) {
    	return !isNoSearchOrSuggest(threadContext);
    }
    
    private static boolean isNoSearchOrSuggest(ThreadContext threadContext) {
        if(threadContext.getTransient("_sg_issuggest") == Boolean.TRUE) {
            //we need to apply it here
            return true;
        }
        
        
        final String action = (String) threadContext.getTransient(ConfigConstants.SG_ACTION_NAME);
        assert action != null;
        //we need to apply here if it is not a search request
        //(a get for example)
        return !action.startsWith("indices:data/read/search");
    }

}
