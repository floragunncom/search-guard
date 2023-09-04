/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.femt.request.handler;

import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.SyncAuthorizationFilter;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import static com.floragunn.searchguard.enterprise.femt.PrivilegesInterceptorImpl.SG_FILTER_LEVEL_FEMT_DONE;

public class SearchRequestHandler extends RequestHandler<SearchRequest> {

    private final Client nodeClient;
    private final ThreadContext threadContext;
    public SearchRequestHandler(Logger log, Client nodeClient, ThreadContext threadContext) {
        super(log);
        this.nodeClient = nodeClient;
        this.threadContext = threadContext;
    }

    @Override
    public SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, SearchRequest request, ActionListener<?> listener) {
        threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, request.toString());

        try (ThreadContext.StoredContext storedContext = threadContext.newStoredContext()) {
            BoolQueryBuilder queryBuilder = createQueryExtension(requestedTenant, null);

            if (request.source().query() != null) {
                queryBuilder.must(request.source().query());
            }

            if (log.isTraceEnabled()) {
                log.trace("handling search request: {}", queryBuilder);
            }

            request.source().query(queryBuilder);
            if(log.isDebugEnabled()) {
                log.debug(
                        "Query to indices '{}' was intercepted to limit access only to tenant '{}', extended query version '{}'",
                        String.join(", ", request.indices()),
                        requestedTenant,
                        queryBuilder);
            }

            nodeClient.search(request, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {
                    try {
                        storedContext.restore();

                        @SuppressWarnings("unchecked")
                        ActionListener<SearchResponse> searchListener = (ActionListener<SearchResponse>) listener;

                        searchListener.onResponse(unscopeIds(response));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });

            return SyncAuthorizationFilter.Result.INTERCEPTED;
        }
    }

    private SearchResponse unscopeIds(SearchResponse searchResponse) {
        SearchResponseSections originalSections = searchResponse.getInternalResponse();
        SearchHits originalSearchHits = originalSections.hits();
        SearchHit[] originalSearchHitArray = originalSearchHits.getHits();
        SearchHit [] rewrittenSearchHitArray = new  SearchHit [originalSearchHitArray.length];

        for (int i = 0; i < originalSearchHitArray.length; i++) {
            rewrittenSearchHitArray[i] = new SearchHit(originalSearchHitArray[i].docId(), unscopedId(originalSearchHitArray[i].getId()), originalSearchHitArray[i].getNestedIdentity());
            rewrittenSearchHitArray[i].sourceRef(originalSearchHitArray[i].getSourceRef());
            rewrittenSearchHitArray[i].addDocumentFields(originalSearchHitArray[i].getDocumentFields(), originalSearchHitArray[i].getMetadataFields());
            rewrittenSearchHitArray[i].setPrimaryTerm(originalSearchHitArray[i].getPrimaryTerm());
            rewrittenSearchHitArray[i].setSeqNo(originalSearchHitArray[i].getSeqNo());
            rewrittenSearchHitArray[i].setRank(originalSearchHitArray[i].getRank());
            rewrittenSearchHitArray[i].shard(originalSearchHitArray[i].getShard());
            rewrittenSearchHitArray[i].version(originalSearchHitArray[i].getVersion());
        }

        SearchHits rewrittenSearchHits = new SearchHits(rewrittenSearchHitArray, originalSearchHits.getTotalHits(), originalSearchHits.getMaxScore());
        SearchResponseSections rewrittenSections = new SearchResponseSections(rewrittenSearchHits, originalSections.aggregations(), originalSections.suggest(),
                originalSections.timedOut(), originalSections.terminatedEarly(), null, originalSections.getNumReducePhases());

        return new SearchResponse(rewrittenSections, searchResponse.getScrollId(), searchResponse.getTotalShards(),
                searchResponse.getSuccessfulShards(), searchResponse.getSkippedShards(), searchResponse.getTook().millis(),
                searchResponse.getShardFailures(), searchResponse.getClusters(), searchResponse.pointInTimeId());

    }
}
