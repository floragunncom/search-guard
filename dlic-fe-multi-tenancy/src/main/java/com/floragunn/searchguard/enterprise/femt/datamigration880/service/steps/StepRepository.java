/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.Constants;
import com.floragunn.searchsupport.client.SearchScroller;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.CANNOT_BULK_CREATE_DOCUMENT_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.CANNOT_COUNT_DOCUMENTS;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.CANNOT_DELETE_INDEX_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.CANNOT_REFRESH_INDEX_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.CANNOT_RETRIEVE_INDICES_STATE_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.CANNOT_UPDATE_MAPPINGS_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.DELETE_ALL_BULK_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.DELETE_ALL_SEARCH_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.DELETE_ALL_TIMEOUT_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.REINDEX_BULK_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.REINDEX_SEARCH_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.REINDEX_TIMEOUT_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.WRITE_BLOCK_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.WRITE_UNBLOCK_ERROR;
import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING;

/**
 * The only purpose of this repository is to simplify testing of steps, so that steps does not depend on final class
 * {@link com.floragunn.searchguard.support.PrivilegedConfigClient} which cannot be simply mock with Mockito library.
 */
class StepRepository {

    private static final Logger log = LogManager.getLogger(StepRepository.class);
    public static final int BATCH_SIZE = 100;

    private final PrivilegedConfigClient client;

    StepRepository(PrivilegedConfigClient client) {
        this.client = Objects.requireNonNull(client, "Client is required");
    }

    public IndicesStatsResponse findIndexState(String...indexNames) {
        Objects.requireNonNull(indexNames, "Indices are required");
        IndicesStatsResponse response = client.admin().indices().stats(new IndicesStatsRequest().indices(indexNames)).actionGet();
        if(response.getFailedShards() > 0) {
            throw new StepException("Cannot load current indices state", CANNOT_RETRIEVE_INDICES_STATE_ERROR, null);
        }
        if(isFailure(response.getStatus())) {
            throw new StepException("Unsuccessful index state response", CANNOT_RETRIEVE_INDICES_STATE_ERROR, null);
        }
        return response;
    }

    public Optional<GetIndexResponse> findIndexByNameOrAlias(String indexNameOrAlias) {
        Strings.requireNonEmpty(indexNameOrAlias, "Index or alias name is required");
        try {
            GetIndexRequest request = new GetIndexRequest(Constants.DEFAULT_MASTER_TIMEOUT);
            request.indices(indexNameOrAlias);
            GetIndexResponse response = client.admin().indices().getIndex(request).actionGet();
            return Optional.ofNullable(response);
        } catch (IndexNotFoundException e) {
            return Optional.empty();
        }
    }

    public GetIndexResponse findAllIndicesIncludingHidden() {
        GetIndexRequest request = new GetIndexRequest(Constants.DEFAULT_MASTER_TIMEOUT).indices("*").indicesOptions(IndicesOptions.strictExpandHidden());
        return client.admin().indices().getIndex(request).actionGet();
    }

    public GetSettingsResponse getIndexSettings(String...indices) {
        Objects.requireNonNull(indices, "Indices are required");
        GetSettingsRequest request = new GetSettingsRequest().indices(indices);
        return client.admin().indices().getSettings(request).actionGet();
    }

    public void writeBlockIndices(ImmutableList<String> indices) {
        Objects.requireNonNull(indices, "Indices list is required");
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
        Objects.requireNonNull(indices, "Indices list is required");
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
        return client.admin().indices().getMappings(new GetMappingsRequest(Constants.DEFAULT_MASTER_TIMEOUT).indices(indexName)).actionGet();
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

    public void forEachDocumentInIndex(String indexName, int batchSize, Consumer<ImmutableList<SearchHit>> consumer) {
        Strings.requireNonEmpty(indexName, "Index name is required.");
        Objects.requireNonNull(consumer, "Search hits consumer is required.");
        SearchScroller searchScroller = new SearchScroller(client);
        SearchRequest request = new SearchRequest(indexName);
        request.source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()).size(batchSize));
        searchScroller.scroll(request, TimeValue.timeValueMinutes(5), Function.identity(), consumer);
    }

    public void bulkCreate(String indexName, Map<String, String> documents) {
        Strings.requireNonEmpty(indexName, "Index name is required");
        Objects.requireNonNull(documents, "Documents to create are required.");
        String sortedDocumentsIds = documents.keySet().stream().sorted().map(id -> "'" + id + "'").collect(Collectors.joining(", "));
        log.info("Index '{}', bulk create documents {}", indexName, sortedDocumentsIds);
        BulkRequest bulkRequest = new BulkRequest(indexName);
        for(Map.Entry<String, String> currentDocument : documents.entrySet()) {
            IndexRequest indexRequest = new IndexRequest(indexName);
            indexRequest.create(true);
            indexRequest.id(currentDocument.getKey());
            indexRequest.source(currentDocument.getValue(), XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        BulkResponse response = client.bulk(bulkRequest).actionGet();
        if(response.hasFailures()) {
            String details = "Index name '" + indexName + "', document ids " + sortedDocumentsIds +  ", error details "
                + response.buildFailureMessage();
            throw new StepException("Cannot create document in index", CANNOT_BULK_CREATE_DOCUMENT_ERROR, details);
        }
    }

    public void flushIndex(String indexName) {
        Strings.requireNonEmpty(indexName, "Index name is required");
        FlushRequest request = new FlushRequest(indexName);
        BroadcastResponse flushResponse = client.admin().indices().flush(request).actionGet();
        if((flushResponse.getFailedShards() > 0) || isFailure(flushResponse.getStatus())) {
            throw new StepException("Cannot flush index '" + indexName + "'.", CANNOT_REFRESH_INDEX_ERROR, null);
        }
    }

    public void refreshIndex(String indexName) {
        Strings.requireNonEmpty(indexName, "Index name is required");
        BroadcastResponse refreshResponse = client.admin().indices().refresh(new RefreshRequest(indexName)).actionGet();
        if((refreshResponse.getFailedShards() > 0) || isFailure(refreshResponse.getStatus())) {
            throw new StepException("Cannot refresh index '" + indexName + "'.", StepExecutionStatus.CANNOT_REFRESH_INDEX_ERROR, null);
        }
    }

    public void deleteIndices(String...indices) {
        AcknowledgedResponse acknowledgedResponse = client.admin().indices().delete(new DeleteIndexRequest(indices)).actionGet();
        if(!acknowledgedResponse.isAcknowledged()) {
            String details = "Indices: " + Arrays.stream(indices).map(name -> "'" + name + "'").collect(Collectors.joining(", "));
            throw new StepException("Cannot delete indices " , CANNOT_DELETE_INDEX_ERROR, details);
        }
    }

    public BulkByScrollResponse reindexData(String sourceIndexName, String destinationIndexName) {
        Strings.requireNonEmpty(sourceIndexName, "Source index name is required");
        Strings.requireNonEmpty(destinationIndexName, "Destination index name is required");
        log.info("Try to reindex data from '{}' to '{}'", sourceIndexName, destinationIndexName);
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceBatchSize(BATCH_SIZE);
        reindexRequest.setSourceIndices(sourceIndexName);
        reindexRequest.setDestIndex(destinationIndexName);
        reindexRequest.setDestOpType("create");
        reindexRequest.setRefresh(true);
        reindexRequest.setAbortOnVersionConflict(true);
        reindexRequest.setScroll(TimeValue.timeValueMinutes(5));
        BulkByScrollResponse response = client.execute(ReindexAction.INSTANCE, reindexRequest).actionGet();
        log.debug("Reindex from '{}' to '{}' response '{}'",sourceIndexName, destinationIndexName, response);
        if(!response.getBulkFailures().isEmpty()) {
            String message = "Cannot reindex data from '" + sourceIndexName + "' to '" + destinationIndexName + "' due to bulk failures";
            throw new StepException(message, REINDEX_BULK_ERROR, null);
        }
        if(! response.getSearchFailures().isEmpty()) {
            String message = "Cannot reindex data from '" + sourceIndexName + "' to '" + destinationIndexName + "' due to search failures";
            throw new StepException(message, REINDEX_SEARCH_ERROR, null);
        }
        if(response.isTimedOut()) {
            String message = "Cannot reindex data from '" + sourceIndexName + "' to '" + destinationIndexName + "' due to timeout";
            throw new StepException(message, REINDEX_TIMEOUT_ERROR, null);
        }
        return response;
    }

    public long countDocuments(String indexName) {
        Strings.requireNonEmpty(indexName, "Index name is required");
        SearchRequest request = new SearchRequest(indexName);
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource()
            .size(0)
            .trackTotalHits(true) // <-- this is extramural important line
            .query(QueryBuilders.matchAllQuery());
        request.source(sourceBuilder);
        SearchResponse response = client.search(request).actionGet();
        try {
            if ((response.getFailedShards() > 0) || (isFailure(response.status()))) {
                throw new StepException("Cannot count documents in index '" + indexName + "'", CANNOT_COUNT_DOCUMENTS, null);
            }

            return response.getHits().getTotalHits().value();
        } finally {
            response.decRef();
        }
    }

    public void updateMappings(String indexName, Map<String, ?> sources) {
        Strings.requireNonEmpty(indexName, "Index name is required");
        PutMappingRequest request = new PutMappingRequest().indices(indexName).source(sources);
        AcknowledgedResponse acknowledgedResponse = client.admin().indices().putMapping(request).actionGet();
        if(!acknowledgedResponse.isAcknowledged()) {
            String details = "Cannot update mappings of index '" + indexName + "'";
            throw new StepException("Cannot delete indices " , CANNOT_UPDATE_MAPPINGS_ERROR, details);
        }
    }

    public BulkByScrollResponse deleteAllDocuments(String indexName) {
        Strings.requireNonEmpty(indexName, "Index name is required");
        DeleteByQueryRequest request = new DeleteByQueryRequest(indexName);
        request.setQuery(QueryBuilders.matchAllQuery());
        request.setRefresh(true);
        request.setBatchSize(BATCH_SIZE);
        request.setScroll(TimeValue.timeValueMinutes(5));
        BulkByScrollResponse response = client.execute(DeleteByQueryAction.INSTANCE, request).actionGet();
        if(!response.getBulkFailures().isEmpty()) {
            String message = "Cannot delete all documents from index '" + indexName + "' due to bulk error";
            throw new StepException(message, DELETE_ALL_BULK_ERROR, null);
        }
        if(! response.getSearchFailures().isEmpty()) {
            String message = "Cannot delete all documents from index '" + indexName + "' due to search error";
            throw new StepException(message, DELETE_ALL_SEARCH_ERROR, null);
        }
        if(response.isTimedOut()) {
            String message = "Cannot delete all documents from index '" + indexName + "' due to timeout";
            throw new StepException(message, DELETE_ALL_TIMEOUT_ERROR, null);
        }
        return response;
    }

    private boolean isSuccess(RestStatus restStatus) {
        return (restStatus.getStatus() >= 200) && (restStatus.getStatus() < 300);
    }

    private boolean isFailure(RestStatus restStatus) {
        return ! isSuccess(restStatus);
    }

}
