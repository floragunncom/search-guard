package com.floragunn.signals.actions.summary;

import com.floragunn.searchguard.support.PrivilegedConfigClient;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

class WatchRepository {

    private final String watchIndexName;
    private final PrivilegedConfigClient privilegedConfigClient;

    WatchRepository(String watchIndexName, PrivilegedConfigClient privilegedConfigClient) {
        this.watchIndexName = requireNonNull(watchIndexName);
        this.privilegedConfigClient = requireNonNull(privilegedConfigClient);
    }

    /**
     * @param tenant required valid tenant name
     * @param namePrefix can be empty or null to disable filtering by name
     * @return list contains ids with tenant prefix
     *
     */
    public List<String> searchWatchIdsWithSeverityAndNames(String tenant, String namePrefix, int size) {
        Objects.requireNonNull(tenant, "tenant is required");
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.size(size);

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery() //
                .filter(tenantIs(tenant)) //
                .filter(withNamePrefix(namePrefix)) //
                .filter(severityLevelExistsQuery());
        sourceBuilder.query(boolQuery);

        SearchRequest request = new SearchRequest(watchIndexName).source(sourceBuilder);
        SearchResponse searchResponse = privilegedConfigClient.search(request).actionGet();
        try {
            return Arrays.stream(searchResponse.getHits().getHits()) //
                    .map(SearchHit::getId) //
                    .toList();
        } finally {
            searchResponse.decRef();
        }
    }

    private static @NotNull AbstractQueryBuilder<?> withNamePrefix(String namePrefix) {
        if( namePrefix == null || namePrefix.isEmpty()) {
            return QueryBuilders.matchAllQuery();
        }
        return QueryBuilders.prefixQuery("_name", namePrefix);
    }

    private static @NotNull TermQueryBuilder tenantIs(String tenant) {
        return QueryBuilders.termQuery("_tenant", tenant);
    }

    private static @NotNull ExistsQueryBuilder severityLevelExistsQuery() {
        return QueryBuilders.existsQuery("severity.mapping.level");
    }
}
