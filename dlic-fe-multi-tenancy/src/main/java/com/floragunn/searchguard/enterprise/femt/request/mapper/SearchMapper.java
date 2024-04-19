package com.floragunn.searchguard.enterprise.femt.request.mapper;

import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.KeyStore;
import java.security.PrivilegedAction;

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
        SearchHits originalSearchHits = response.getHits();
        SearchHit[] originalSearchHitArray = originalSearchHits.getHits();

        for (int i = 0; i < originalSearchHitArray.length; i++) {
            SearchHit searchHit = originalSearchHitArray[i];
            String scopedId = RequestResponseTenantData.unscopedId(searchHit.getId());
            AccessController.doPrivileged(new PrivilegedAction<Object>() {
                @Override
                public Object run() {
                    try {
                        Field idField = searchHit.getClass().getDeclaredField("id");
                        idField.setAccessible(true);
                        idField.set(searchHit, new Text(scopedId));
                    } catch (NoSuchFieldException | IllegalAccessException e) {
                        throw new RuntimeException("Cannot unscope id in search response");
                    }
                    return null;
                }
            });
        }
        return response;
    }

}
