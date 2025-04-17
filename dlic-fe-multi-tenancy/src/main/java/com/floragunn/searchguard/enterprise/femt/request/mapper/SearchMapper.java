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
        // the below line creates unpooled source object in SearchHit by intermediate invocation
        // of org.elasticsearch.search.SearchHit.asUnpooled
        SearchHits originalSearchHits = response.getHits().asUnpooled();
        SearchHit[] originalSearchHitArray = originalSearchHits.getHits();
        SearchHit [] rewrittenSearchHitArray = new SearchHit[originalSearchHitArray.length];

        for (int i = 0; i < originalSearchHitArray.length; i++) {
            rewrittenSearchHitArray[i] = SearchHit.unpooled(originalSearchHitArray[i].docId(), RequestResponseTenantData.unscopedId(originalSearchHitArray[i].getId()), originalSearchHitArray[i].getNestedIdentity());
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

        // Creates an unpooled version
        SearchHits rewrittenSearchHits = SearchHits.unpooled(rewrittenSearchHitArray, originalSearchHits.getTotalHits(), originalSearchHits.getMaxScore());
        // SearchResponseSections is still pooled
        SearchResponseSections rewrittenSections = new SearchResponseSections(rewrittenSearchHits, response.getAggregations(), response.getSuggest(),
                response.isTimedOut(), response.isTerminatedEarly(), null, response.getNumReducePhases());
         // Why is "decRef" method invoked here?
        // 1. decRef should be invoked when resources are pooled to release the pooled resource.
        // 2. SearchResponseSections contains only the SearchHits which can be pooled or unpooled.
        // 3. Therefore, invocation rewrittenSections.decRef() can possibly released resources in SearchHits stored inside SearchResponseSections.
        // 4. But SearchHits provided to SearchResponseSections constructor are already unpooled.
        // 5. Therefore, invocation rewrittenSections.decRef() is not necessary.
        // 6. However, in ES 8.13.4, SearchResponseSections object is always created as pooled objects by the constructor unless the
        // search hits are empty
        // 7. Therefore, invocation rewrittenSections.decRef() is necessary to avoid false positive warnings related to the memory leaks and
        // class SearchResponseSections.

        // SearchResponse is still pooled. It is not possible to create unpooled SearchResponse.
        SearchResponse pooledSearchResponse = new SearchResponse(rewrittenSections,
            response.getScrollId(),
            response.getTotalShards(),
            response.getSuccessfulShards(),
            response.getSkippedShards(),
            response.getTook().millis(),
            response.getShardFailures(),
            response.getClusters(),
            response.pointInTimeId());
        return pooledSearchResponse;
    }

}
