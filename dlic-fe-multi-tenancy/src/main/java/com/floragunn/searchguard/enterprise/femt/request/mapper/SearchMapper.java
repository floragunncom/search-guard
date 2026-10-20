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
            rewrittenSearchHitArray[i] = SearchHit.unpooled(originalSearchHitArray[i].docId(), RequestResponseTenantData.unscopedId(originalSearchHitArray[i].getId()), originalSearchHitArray[i].getNestedIdentity());
            // The source of the original hit may be a pooled buffer which is released together with the original response.
            // Thus, we need to create an unpooled copy of the source (this is what SearchHits.asUnpooled() did before ES 9.5).
            rewrittenSearchHitArray[i].sourceRef(unpooledCopy(originalSearchHitArray[i].getSourceRef()));
            rewrittenSearchHitArray[i].addDocumentFields(originalSearchHitArray[i].getDocumentFields(), originalSearchHitArray[i].getMetadataFields());
            rewrittenSearchHitArray[i].setPrimaryTerm(originalSearchHitArray[i].getPrimaryTerm());
            rewrittenSearchHitArray[i].setSeqNo(originalSearchHitArray[i].getSeqNo());
            rewrittenSearchHitArray[i].setRank(originalSearchHitArray[i].getRank());
            rewrittenSearchHitArray[i].shard(originalSearchHitArray[i].getShard());
            rewrittenSearchHitArray[i].version(originalSearchHitArray[i].getVersion());
            rewrittenSearchHitArray[i].score(originalSearchHitArray[i].getScore());
            rewrittenSearchHitArray[i].explanation(originalSearchHitArray[i].getExplanation());
        }

        // The SearchHits object is pooled (ref counted). Its ownership is transferred to the SearchResponse created below, which releases it on decRef().
        // The contained SearchHit objects are unpooled, so releasing the SearchHits object does not touch them.
        SearchHits rewrittenSearchHits = new SearchHits(rewrittenSearchHitArray, originalSearchHits.getTotalHits(), originalSearchHits.getMaxScore(),
                originalSearchHits.getSortFields(), originalSearchHits.getCollapseField(), originalSearchHits.getCollapseValues());
        // SearchResponseSections is still pooled
        SearchResponseSections rewrittenSections = new SearchResponseSections(rewrittenSearchHits, response.getAggregations(), response.getSuggest(),
                response.isTimedOut(), response.isTerminatedEarly(), null, response.getNumReducePhases(), response.getTimeRangeFilterFromMillis());

        // SearchResponse is still pooled. It is not possible to create unpooled SearchResponse.
        SearchResponse pooledSearchResponse = new SearchResponse(rewrittenSections,
            response.getScrollId(),
            response.getTotalShards(),
            response.getSuccessfulShards(),
            response.getSkippedShards(),
            response.getTook().millis(),
            response.getShardFailures(),
            response.getClusters(),
            response.pointInTimeId(),
            null, // original search source; only used for profile results, which are not passed on
            null); // request indices; only used for profile results, which are not passed on
        return pooledSearchResponse;
    }

    private static BytesReference unpooledCopy(BytesReference source) {
        if (source == null) {
            return null;
        }
        return new BytesArray(BytesReference.toBytes(source));
    }

}
