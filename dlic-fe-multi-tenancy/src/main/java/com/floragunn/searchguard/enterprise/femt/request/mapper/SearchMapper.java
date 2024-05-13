package com.floragunn.searchguard.enterprise.femt.request.mapper;

import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import com.floragunn.searchsupport.PrivilegedCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.lang.reflect.Field;
import java.util.function.Supplier;

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

        SearchHits rewrittenSearchHits = new SearchHits(rewrittenSearchHitArray, originalSearchHits.getTotalHits(), originalSearchHits.getMaxScore())
            // Creates an unpooled version. This invocation creates a deep copy of source which is present is <code>SearchHit<code>s
            .asUnpooled();
        // SearchResponseSections is still pooled
        SearchResponseSections rewrittenSections = new SearchResponseSections(rewrittenSearchHits, response.getAggregations(), response.getSuggest(),
                response.isTimedOut(), response.isTerminatedEarly(), null, response.getNumReducePhases());

        // SearchResponse is still pooled. It is not possible to create unpooled SearchResponse.
        SearchResponse pooledSearchResponse = new SearchResponse(rewrittenSections,
            response.getScrollId(),
            response.getTotalShards(),
            response.getSuccessfulShards(),
            response.getSkippedShards(),
            response.getTook().millis(),
            response.getShardFailures(),
            response.getClusters(),
            response.pointInTimeId()) {
            @Override
            public void incRef() {
                super.incRef();
                log.debug("Unscoped search response '{}' incRef", System.identityHashCode(this), referenceCount());
            }

            private RuntimeException referenceCount() {
                return new RuntimeException("To trace ref counting, currentValue " + getReferencesCount());
            }

            @Override
            public boolean tryIncRef() {
                boolean result = super.tryIncRef();
                log.debug("Unscoped search response '{}' tryIncRef", System.identityHashCode(this), referenceCount());
                return result;

            }

            @Override
            public boolean decRef() {
                boolean result = super.decRef();
                log.debug("Unscoped search response '{}' decRef", System.identityHashCode(this), referenceCount());
                return result;
            }

            @Override
            public void mustIncRef() {
                log.debug("Unscoped search response '{}' mustIncRef", System.identityHashCode(this), referenceCount());
                super.mustIncRef();
            }

            @Override
            public boolean hasReferences() {
                boolean hasReferences = super.hasReferences();
                log.debug("Unscoped search response '{}' hasReferences '{}'", System.identityHashCode(this), hasReferences, referenceCount());
                return hasReferences;
            }

            @Override
            protected void finalize() {
                hasReferences();
            }

            private int getReferencesCount() {
                SearchResponse that = this;
                Supplier<Integer> supplier = () -> {
                    try {
                        Field refCountedField = that.getClass().getSuperclass().getDeclaredField("refCounted");
                        refCountedField.setAccessible(true);
                        RefCounted refCounted = (RefCounted) refCountedField.get(that);
                        refCountedField = refCounted.getClass().getDeclaredField("val$refCounted");
                        refCountedField.setAccessible(true);
                        refCounted = (RefCounted) refCountedField.get(refCounted);
                        Field valueField = refCounted.getClass().getSuperclass().getDeclaredField("refCount");
                        valueField.setAccessible(true);
                        return (Integer) valueField.get(refCounted);
                    } catch (NoSuchFieldException | IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                };
                return PrivilegedCode.execute(supplier);
            }
        };
        log.debug("Unscoped search response created '{}', has references '{}'.", System.identityHashCode(pooledSearchResponse), pooledSearchResponse.hasReferences());
        return pooledSearchResponse;
    }

}
