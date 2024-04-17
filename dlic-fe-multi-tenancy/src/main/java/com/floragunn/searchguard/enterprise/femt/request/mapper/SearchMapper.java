package com.floragunn.searchguard.enterprise.femt.request.mapper;

import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class SearchMapper {

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

    public SearchResponse toUnscopedSearchResponse(SearchResponse response) {
        log.debug("Rewriting search response - removing tenant scope");
        SearchResponseSections originalSections = response.getInternalResponse();
        SearchHits originalSearchHits = originalSections.hits();
        SearchHit[] originalSearchHitArray = originalSearchHits.getHits();
        SearchHit [] rewrittenSearchHitArray = new  SearchHit [originalSearchHitArray.length];

        for (int i = 0; i < originalSearchHitArray.length; i++) {
            rewrittenSearchHitArray[i] = new SearchHit(originalSearchHitArray[i].docId(), RequestResponseTenantData.unscopedId(originalSearchHitArray[i].getId()), originalSearchHitArray[i].getNestedIdentity());
            rewrittenSearchHitArray[i].sourceRef(originalSearchHitArray[i].getSourceRef());
            rewrittenSearchHitArray[i].addDocumentFields(originalSearchHitArray[i].getDocumentFields(), originalSearchHitArray[i].getMetadataFields());
            rewrittenSearchHitArray[i].setPrimaryTerm(originalSearchHitArray[i].getPrimaryTerm());
            rewrittenSearchHitArray[i].setSeqNo(originalSearchHitArray[i].getSeqNo());
            rewrittenSearchHitArray[i].setRank(originalSearchHitArray[i].getRank());
            rewrittenSearchHitArray[i].shard(originalSearchHitArray[i].getShard());
            rewrittenSearchHitArray[i].version(originalSearchHitArray[i].getVersion());
            rewrittenSearchHitArray[i].score(originalSearchHitArray[i].getScore());
            rewrittenSearchHitArray[i].explanation(originalSearchHitArray[i].getExplanation());
        }

        SearchHits rewrittenSearchHits = new SearchHits(rewrittenSearchHitArray, originalSearchHits.getTotalHits(), originalSearchHits.getMaxScore());
        SearchResponseSections rewrittenSections = new SearchResponseSections(rewrittenSearchHits, originalSections.aggregations(), originalSections.suggest(),
                originalSections.timedOut(), originalSections.terminatedEarly(), null, originalSections.getNumReducePhases());

        return new SearchResponse(rewrittenSections, response.getScrollId(), response.getTotalShards(),
                response.getSuccessfulShards(), response.getSkippedShards(), response.getTook().millis(),
                response.getShardFailures(), response.getClusters(), response.pointInTimeId());
    }

}
