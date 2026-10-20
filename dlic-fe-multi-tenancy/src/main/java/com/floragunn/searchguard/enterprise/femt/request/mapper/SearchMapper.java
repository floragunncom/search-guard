package com.floragunn.searchguard.enterprise.femt.request.mapper;

import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.RefCounted;
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

        // Since ES 9.5 there is no public way to create an unpooled SearchHits container. The container is ref counted,
        // but the hits inside are unpooled, so releasing the container is a no-op for them.
        SearchHits rewrittenSearchHits = new SearchHits(rewrittenSearchHitArray, originalSearchHits.getTotalHits(), originalSearchHits.getMaxScore());
        try {
            // SearchResponseSections is still pooled
            SearchResponseSections rewrittenSections = new SearchResponseSections(rewrittenSearchHits, response.getAggregations(), response.getSuggest(),
                    response.isTimedOut(), response.isTerminatedEarly(), null, response.getNumReducePhases(), response.getTimeRangeFilterFromMillis());

            // SearchResponse is still pooled. It is not possible to create unpooled SearchResponse.
            // The SearchResponse constructor increments the ref count of the hits; the response then becomes the sole owner.
            // Profile results are not carried over (see above), so original source and indices are not needed.
            SearchResponse pooledSearchResponse = new SearchResponse(rewrittenSections,
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
            return pooledSearchResponse;
        } finally {
            rewrittenSearchHits.decRef();
        }
    }

    /**
     * The source of a pooled SearchHit may be backed by pooled memory which is released together with the original response.
     * This mirrors what SearchHit.asUnpooled() did before ES 9.5: create a deep copy in that case.
     */
    private static BytesReference unpooledCopy(BytesReference source) {
        if (source instanceof RefCounted) {
            return new BytesArray(source.toBytesRef(), true);
        } else {
            return source;
        }
    }

}
