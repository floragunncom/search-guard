/*
 * Copyright 2021 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.dlsfls.filter;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;

import com.floragunn.searchguard.configuration.DlsRequestHandlerValve;
import com.floragunn.searchguard.support.ConfigConstants;

public class DlsFilterLevelRequestHandler implements DlsRequestHandlerValve {
    private static final Logger log = LogManager.getLogger(DlsFilterLevelRequestHandler.class);

    private final NamedXContentRegistry namedXContentRegistry;
    private final ThreadContext threadContext;

    public DlsFilterLevelRequestHandler(NamedXContentRegistry namedXContentRegistry, ThreadContext threadContext) {
        this.namedXContentRegistry = namedXContentRegistry;
        this.threadContext = threadContext;
    }

    @Override
    public <T extends TransportRequest> T handle(T request, TransportChannel channel, Task task) throws ElasticsearchSecurityException {
        String filterLevelDlsQuery = threadContext.getHeader(ConfigConstants.SG_DLS_FILTER_LEVEL_QUERY_HEADER);

        System.out.println("X " + threadContext.getHeaders().keySet());
        
        if (filterLevelDlsQuery == null) {
            return request;
        }
        
        if (threadContext.getHeader(ConfigConstants.SG_FILTER_LEVEL_DLS_DONE) != null) {
            if (log.isTraceEnabled()) {
                log.trace("Not handling " + request + " because DLS has been done for " + threadContext.getHeader(ConfigConstants.SG_FILTER_LEVEL_DLS_DONE));
            }
            return request;
        }
        
        if (request instanceof ActionRequest) {
            // will be handled by DlsFlsValue
            return request;
        }
        
        if (log.isDebugEnabled()) {
            log.debug("Applying filter level DLS query to " + request + ":\n" + filterLevelDlsQuery);
        }

        threadContext.putHeader(ConfigConstants.SG_FILTER_LEVEL_DLS_DONE, request.toString());
        
        BoolQueryBuilder filterLevelQueryBuilder = getParsedQuery(request, filterLevelDlsQuery);
        
        if (request instanceof ShardSearchRequest) {
            @SuppressWarnings("unchecked")
            T result = (T) handle((ShardSearchRequest) request, filterLevelQueryBuilder);
            return result;
        } else if (request instanceof ShardFetchSearchRequest) {
            System.out.println(request);
            return request;

        } else {
            log.error("Unsupported request type for filter level DLS: " + request);
            throw new ElasticsearchSecurityException("Unsupported request type for filter level DLS: " + request.getClass().getName());
        }
    }

    private ShardSearchRequest handle(ShardSearchRequest request, BoolQueryBuilder filterLevelQueryBuilder) {

        if (request.getClusterAlias() != null && request.getClusterAlias().length() > 0) {
            filterLevelQueryBuilder = addLocalClusterPrefix(filterLevelQueryBuilder, request.getClusterAlias());
        }
        
        if (request.source().query() != null) {
            filterLevelQueryBuilder.must(request.source().query());
        }

        request.source().query(filterLevelQueryBuilder);

        return request;
    }

    private BoolQueryBuilder getParsedQuery(TransportRequest request, String filterLevelDlsQuery) {
        try {
            XContentParser parser = JsonXContent.jsonXContent.createParser(namedXContentRegistry, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    filterLevelDlsQuery);
            QueryBuilder queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(parser);

            if (!(queryBuilder instanceof BoolQueryBuilder)) {
                throw new Exception("DLS query must be a boolean query: " + filterLevelDlsQuery + "; " + queryBuilder);
            }

            return (BoolQueryBuilder) queryBuilder;

        } catch (Exception e) {
            log.error("Error while handling DLS for " + request + "\n" + filterLevelDlsQuery, e);
            throw new ElasticsearchSecurityException("Error while handling DLS", e);
        }
    }

    private BoolQueryBuilder addLocalClusterPrefix(BoolQueryBuilder boolQueryBuilder, String localClusterPrefix) {
        if (boolQueryBuilder.must().size() != 0 || boolQueryBuilder.mustNot().size() != 0 || boolQueryBuilder.filter().size() != 0) {
            if (log.isDebugEnabled()) {
                log.debug("According to the structure, this is not a query containing several index-scoped DLS queryies. Possibly, index) scoping was not necessary. Returning unmodified query: " + boolQueryBuilder);
            }
            
            return boolQueryBuilder;
        }
        
        BoolQueryBuilder result = QueryBuilders.boolQuery().minimumShouldMatch(1);

        for (QueryBuilder subQueryBuilder : boolQueryBuilder.should()) {
            if (subQueryBuilder instanceof TermQueryBuilder && ((TermQueryBuilder) subQueryBuilder).fieldName().equals("_index")) {
                TermQueryBuilder termQueryBuilder = (TermQueryBuilder) subQueryBuilder;
                result.should(QueryBuilders.termQuery("_index", localClusterPrefix + ":" + termQueryBuilder.value()));
            } else if (subQueryBuilder instanceof BoolQueryBuilder && isIndexScopedQuery((BoolQueryBuilder) subQueryBuilder)) {
                List<QueryBuilder> subQueryBuilders = ((BoolQueryBuilder) subQueryBuilder).must();
                TermQueryBuilder termQueryBuilder = (TermQueryBuilder) subQueryBuilders.get(0);

                result.should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("_index", localClusterPrefix + ":" + termQueryBuilder.value()))
                        .must(subQueryBuilders.get(1)));
            } else {
                result.should(subQueryBuilder);
            }
        }
        
        return result;
    }

    private boolean isIndexScopedQuery(BoolQueryBuilder boolQueryBuilder) {
        List<QueryBuilder> subQueryBuilders = boolQueryBuilder.must();

        if (subQueryBuilders.size() != 2) {
            return false;
        }

        if (boolQueryBuilder.should().size() != 0 || boolQueryBuilder.mustNot().size() != 0 || boolQueryBuilder.filter().size() != 0) {
            return false;
        }

        QueryBuilder firstSubQueryBuilder = subQueryBuilders.get(0);

        if (!(firstSubQueryBuilder instanceof TermQueryBuilder)) {
            return false;
        }

        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) firstSubQueryBuilder;

        return "_index".equals(termQueryBuilder.fieldName());
    }

}
