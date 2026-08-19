package com.floragunn.searchguard.enterprise.femt.request.mapper;

import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.HashMap;

public class SearchMapper implements Unscoper<SearchResponse> {

    private final static Logger log = LogManager.getLogger(SearchMapper.class);

    public SearchRequest toScopedSearchRequest(SearchRequest request, String tenant) {
        log.debug("Rewriting search request - adding tenant scope");
        BoolQueryBuilder queryBuilder = RequestResponseTenantData.sgTenantFieldQuery(tenant);

        if (request.source().query() != null) {
            queryBuilder.must(request.source().query());
        }

        log.trace("handling search request: {}", queryBuilder);

        request.source().query(queryBuilder);
        if (log.isDebugEnabled()) {
            log.debug(
                    "Query to indices '{}' was intercepted to limit access only to tenant '{}', extended query version '{}'",
                    String.join(", ", request.indices()),
                    tenant,
                    queryBuilder
            );
        }
        return request;
    }

    @Override
    public SearchResponse unscopeResponse(SearchResponse response) {
        log.debug("Rewriting search response - removing tenant scope");
        SearchHits originalSearchHits = response.getHits();
        SearchHit[] originalSearchHitArray = originalSearchHits.getHits();
        SearchHit [] rewrittenSearchHitArray = new SearchHit[originalSearchHitArray.length];

        for (int i = 0; i < originalSearchHitArray.length; i++) {
            SearchHit originalSearchHit = originalSearchHitArray[i];
            SearchHit rewrittenSearchHit = SearchHit.unpooled(originalSearchHit.docId(),
                    RequestResponseTenantData.unscopedId(originalSearchHit.getId()), originalSearchHit.getNestedIdentity());
            BytesReference source = originalSearchHit.getSourceRef();
            rewrittenSearchHit.sourceRef(source == null ? null : new BytesArray(source.toBytesRef(), true));
            rewrittenSearchHit.addDocumentFields(new HashMap<>(originalSearchHit.getDocumentFields()),
                    new HashMap<>(originalSearchHit.getMetadataFields()));
            rewrittenSearchHit.setPrimaryTerm(originalSearchHit.getPrimaryTerm());
            rewrittenSearchHit.setSeqNo(originalSearchHit.getSeqNo());
            rewrittenSearchHit.setRank(originalSearchHit.getRank());
            rewrittenSearchHit.shard(originalSearchHit.getShard());
            rewrittenSearchHit.version(originalSearchHit.getVersion());
            rewrittenSearchHit.score(originalSearchHit.getScore());
            rewrittenSearchHit.explanation(originalSearchHit.getExplanation());
            rewrittenSearchHitArray[i] = rewrittenSearchHit;
        }

        SearchHits rewrittenSearchHits = new SearchHits(rewrittenSearchHitArray, originalSearchHits.getTotalHits(),
                originalSearchHits.getMaxScore(), originalSearchHits.getSortFields(), originalSearchHits.getCollapseField(),
                originalSearchHits.getCollapseValues());
        SearchResponseSections rewrittenSections = new SearchResponseSections(rewrittenSearchHits, response.getAggregations(), response.getSuggest(),
                response.isTimedOut(), response.isTerminatedEarly(), null, response.getNumReducePhases(), response.getTimeRangeFilterFromMillis());

        SearchResponse pooledSearchResponse;
        try {
            pooledSearchResponse = new SearchResponse(rewrittenSections,
                response.getScrollId(),
                response.getTotalShards(),
                response.getSuccessfulShards(),
                response.getSkippedShards(),
                response.getTook().millis(),
                response.getShardFailures(),
                response.getClusters(),
                response.pointInTimeId(),
                null,
                null);
        } finally {
            rewrittenSearchHits.decRef();
        }
        pooledSearchResponse.setDirectoryMetrics(response.getDirectoryMetrics());
        return pooledSearchResponse;
    }

}
