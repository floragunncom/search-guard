package com.floragunn.signals.actions.summary;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.signals.watch.Watch;
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
    public List<WatchActionNames> searchWatchIdsWithSeverityAndIdPrefix(String tenant, String namePrefix, int size) {
        Objects.requireNonNull(tenant, "tenant is required");
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.size(size);
        sourceBuilder.fetchSource(new String[]{"active", "actions.name"}, new String[0]);

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery() //
                .filter(tenantIs(tenant)) //
                .filter(withNamePrefix(namePrefix)) //
                .filter(severityLevelExistsQuery());
        sourceBuilder.query(boolQuery);

        SearchRequest request = new SearchRequest(watchIndexName).source(sourceBuilder);


        SearchResponse searchResponse = privilegedConfigClient.search(request).actionGet();
        try {
            return Arrays.stream(searchResponse.getHits().getHits()) //
                    .map(this::convertHitToWatchActionNames) //
                    .toList();
        } finally {
            searchResponse.decRef();
        }
    }

    private WatchActionNames convertHitToWatchActionNames(SearchHit hit) {
        String watchId = hit.getId();
        DocNode documentSources = DocNode.wrap(hit.getSourceAsMap());
        ImmutableList<DocNode> actions = documentSources.getAsListOfNodes("actions");
        Boolean active = isWatchActive(documentSources);
        if (actions == null || actions.isEmpty()) {
            return new WatchActionNames(watchId, ImmutableList.empty(), active);
        }
        List<String> actionNames = actions.stream() //
                .map(docNode -> docNode.getAsString("name")) //
                .filter(Objects::nonNull) //
                .toList();
        return new WatchActionNames(watchId, actionNames, active);
    }

    private static Boolean isWatchActive(DocNode documentSources) {
        try {
            Boolean active = documentSources.getBoolean("active");
            return active == null ? Watch.DEFAULT_ACTIVE : active;
        } catch (ConfigValidationException e) {
            return null;
        }
    }

    private static @NotNull AbstractQueryBuilder<?> withNamePrefix(String namePrefix) {
        if( namePrefix == null || namePrefix.isEmpty()) {
            return QueryBuilders.matchAllQuery();
        }
        return QueryBuilders.prefixQuery("_name", namePrefix.toLowerCase());
    }

    private static @NotNull TermQueryBuilder tenantIs(String tenant) {
        return QueryBuilders.termQuery("_tenant", tenant);
    }

    private static @NotNull ExistsQueryBuilder severityLevelExistsQuery() {
        return QueryBuilders.existsQuery("severity.mapping.level");
    }
}
