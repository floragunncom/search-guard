package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.CANNOT_RETRIEVE_INDICES_STATE_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.WRITE_BLOCK_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.WRITE_UNBLOCK_ERROR;
import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING;

/**
 * The only purpose of this repository is to simplify testing of steps, so that steps does not depend on final class
 * {@link com.floragunn.searchguard.support.PrivilegedConfigClient} which cannot be simply mock with Mockito library.
 */
class StepRepository {

    private final PrivilegedConfigClient client;

    StepRepository(PrivilegedConfigClient client) {
        this.client = Objects.requireNonNull(client, "Client is required");
    }

    public IndicesStatsResponse findIndexState(String...indexNames) {
        IndicesStatsResponse response = client.admin().indices().stats(new IndicesStatsRequest().indices(indexNames)).actionGet();
        if(response.getFailedShards() > 0) {
            throw new StepException("Cannot load current indices state", CANNOT_RETRIEVE_INDICES_STATE_ERROR, null);
        }
        return response;
    }

    public Optional<GetIndexResponse> findIndexByNameOrAlias(String indexNameOrAlias) {
        Strings.requireNonEmpty(indexNameOrAlias, "Index or alias name is required");
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

    public void writeBlockIndices(ImmutableList<String> indices) {
        AddIndexBlockResponse response = client.admin() //
            .indices() //
            .prepareAddBlock(IndexMetadata.APIBlock.WRITE, indices.toArray(String[]::new)) //
            .get();
        if(!response.isAcknowledged()) {
            String details = "Indices to block " + indices.stream().map(name -> "'" + name + "'").collect(Collectors.joining(", "));
            throw new StepException("Cannot block indices", WRITE_BLOCK_ERROR, details);
        }
    }

    public void releaseWriteLock(ImmutableList<String> indices) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.APIBlock.WRITE.settingName(), false)
            .build();
        UpdateSettingsRequest request = new UpdateSettingsRequest(indices.toArray(String[]::new)).settings(settings);
        AcknowledgedResponse acknowledgedResponse = client.admin().indices().updateSettings(request).actionGet();
        if(!acknowledgedResponse.isAcknowledged()) {
            String details = "Indices to unblock " + indices.stream() //
                .map(name -> "'" + name + "'") //
                .collect(Collectors.joining(", "));
            throw new StepException("Cannot unblock indices", WRITE_UNBLOCK_ERROR, details);
        }
    }

    public GetMappingsResponse findIndexMappings(String indexName) {
        Strings.requireNonEmpty(indexName, "Index name is required");
        return client.admin().indices().getMappings(new GetMappingsRequest().indices(indexName)).actionGet();
    }

    public void createIndex(String name, int numberOfShards, int numberOfReplicas, long mappingsTotalFieldsLimit,
        Map<String, Object> mappingSources) {
        Strings.requireNonEmpty(name, "Index name is required");
        Objects.requireNonNull(mappingSources, "Mappings for index creation are required");
        Settings settings = Settings.builder() //
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards) //
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas) //
                .put(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), mappingsTotalFieldsLimit)
                .build();
        CreateIndexRequest request = new CreateIndexRequest() //
            .index(name) //
            .settings(settings) //
            .mapping(mappingSources);
        CreateIndexResponse response = client.admin().indices().create(request).actionGet();
        if(!response.isAcknowledged()) {
            throw new StepException("Cannot create index " + name, StepExecutionStatus.CANNOT_CREATE_INDEX_ERROR, null);
        }
    }
}
