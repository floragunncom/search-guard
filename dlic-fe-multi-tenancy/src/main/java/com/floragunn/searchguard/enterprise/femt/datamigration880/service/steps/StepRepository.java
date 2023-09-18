package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.searchguard.support.PrivilegedConfigClient;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.Objects;
import java.util.Optional;

/**
 * The only purpose of this repository is to simplify testing of steps, so that steps does not depend on final class
 * {@link com.floragunn.searchguard.support.PrivilegedConfigClient} which cannot be simply mock with Mockito library.
 */
class StepRepository {

    private final PrivilegedConfigClient client;

    StepRepository(PrivilegedConfigClient client) {
        this.client = Objects.requireNonNull(client, "Client is required");
    }

    public IndicesStatsResponse findIndexState(String...dataIndices) {
        return client.admin().indices().stats(new IndicesStatsRequest().indices(dataIndices)).actionGet();
    }

    public Optional<GetIndexResponse> findIndexByName(String indexNameOrAlias) {
        try {
            GetIndexRequest request = new GetIndexRequest();
            request.indices(indexNameOrAlias);
            GetIndexResponse response = client.admin().indices().getIndex(request).actionGet();
            return Optional.ofNullable(response);
        } catch (IndexNotFoundException e) {
            return Optional.empty();
        }
    }

    public GetIndexResponse findAllIndicesIncludingHidden() {
        GetIndexRequest request = new GetIndexRequest().indices("*").indicesOptions(IndicesOptions.strictExpandHidden());
        return client.admin().indices().getIndex(request).actionGet();
    }

    public GetSettingsResponse getIndexSettings(String...indices) {
        GetSettingsRequest request = new GetSettingsRequest().indices(indices);
        return client.admin().indices().getSettings(request).actionGet();
    }

}
