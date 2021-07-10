/*
 * Copyright 2016-2021 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.dlsfls;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
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
import org.elasticsearch.index.query.TermsQueryBuilder;

import com.floragunn.searchguard.queries.QueryBuilderTraverser;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class DlsQueryParser {

    private static final Logger log = LogManager.getLogger(DlsQueryParser.class);
    private static final Query NON_NESTED_QUERY;

    static {
        //Match all documents but not the nested ones
        //Nested document types start with __ 
        //https://discuss.elastic.co/t/whats-nested-documents-layout-inside-the-lucene/59944/9
        NON_NESTED_QUERY = new BooleanQuery.Builder().add(new MatchAllDocsQuery(), Occur.FILTER)
                .add(new PrefixQuery(new Term("_type", "__")), Occur.MUST_NOT).build();
    }

    private static Cache<String, QueryBuilder> parsedQueryCache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(4, TimeUnit.HOURS)
            .build();
    private static Cache<String, Boolean> queryContainsTlqCache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(4, TimeUnit.HOURS)
            .build();

    private final NamedXContentRegistry namedXContentRegistry;

    public DlsQueryParser(NamedXContentRegistry namedXContentRegistry) {
        this.namedXContentRegistry = namedXContentRegistry;
    }

    public BooleanQuery.Builder parse(Set<String> unparsedDlsQueries, QueryShardContext queryShardContext) {
        return parse(unparsedDlsQueries, queryShardContext, null);
    }

    public BooleanQuery.Builder parse(Set<String> unparsedDlsQueries, QueryShardContext queryShardContext,
            Function<Query, Query> queryMapFunction) {

        if (unparsedDlsQueries == null || unparsedDlsQueries.isEmpty()) {
            return null;
        }
        
        boolean hasNestedMapping = queryShardContext.getMapperService().hasNested();

        BooleanQuery.Builder dlsQueryBuilder = new BooleanQuery.Builder();
        dlsQueryBuilder.setMinimumNumberShouldMatch(1);

        for (String unparsedDlsQuery : unparsedDlsQueries) {
            ParsedQuery parsedQuery = queryShardContext.toQuery(parse(unparsedDlsQuery));
            Query dlsQuery = parsedQuery.query();

            if (queryMapFunction != null) {
                dlsQuery = queryMapFunction.apply(dlsQuery);
            }

            dlsQueryBuilder.add(dlsQuery, Occur.SHOULD);

            if (hasNestedMapping) {
                handleNested(queryShardContext, dlsQueryBuilder, dlsQuery);
            }
        }

        return dlsQueryBuilder;
    }
    
    private static void handleNested(final QueryShardContext queryShardContext, final BooleanQuery.Builder dlsQueryBuilder,
            final Query parentQuery) {
        final BitSetProducer parentDocumentsFilter = queryShardContext.bitsetFilter(NON_NESTED_QUERY);
        dlsQueryBuilder.add(new ToChildBlockJoinQuery(parentQuery, parentDocumentsFilter), Occur.SHOULD);
    }

    public QueryBuilder parse(String unparsedDlsQuery) {
        try {
            final QueryBuilder qb = parsedQueryCache.get(unparsedDlsQuery, new Callable<QueryBuilder>() {

                @Override
                public QueryBuilder call() throws Exception {
                    final XContentParser parser = JsonXContent.jsonXContent.createParser(namedXContentRegistry,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, unparsedDlsQuery);
                    final QueryBuilder qb = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
                    return qb;
                }

            });

            return qb;
        } catch (ExecutionException e) {
            throw new RuntimeException("Error while parsing " + unparsedDlsQuery, e.getCause());
        }
    }

    boolean containsTermLookupQuery(Set<String> unparsedQueries) {
        for (String query : unparsedQueries) {
            if (containsTermLookupQuery(query)) {
                if (log.isDebugEnabled()) {
                    log.debug("containsTermLookupQuery() returns true due to " + query + "\nqueries: " + unparsedQueries);
                }
                
                return true;
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("containsTermLookupQuery() returns false\nqueries: " + unparsedQueries);
        }
        
        return false;
    }

    boolean containsTermLookupQuery(String query)  {
        try {
            return queryContainsTlqCache.get(query, () -> {
                QueryBuilder queryBuilder = parse(query);

                return QueryBuilderTraverser.exists(queryBuilder,
                        (q) -> (q instanceof TermsQueryBuilder) && ((TermsQueryBuilder) q).termsLookup() != null);
            });
        } catch (ExecutionException e) {
            throw new RuntimeException("Error handling parsing " + query, e.getCause());
        }
    }

  
}
