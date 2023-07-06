package com.floragunn.signals.actions.watch.generic.service.persistence;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.client.SearchScroller;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import static com.floragunn.signals.actions.watch.generic.service.persistence.WatchParametersData.FIELD_TENANT_ID;
import static com.floragunn.signals.actions.watch.generic.service.persistence.WatchParametersData.FIELD_WATCH_ID;
import static com.floragunn.signals.settings.SignalsSettings.SignalsStaticSettings.IndexNames.WATCHES_INSTANCE_PARAMETERS;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;

public class WatchParametersRepository {

    private static final Logger log = LogManager.getLogger(WatchParametersRepository.class);
    public static final int WATCH_PARAMETER_DATA_PAGE_SIZE = 100;

    private final PrivilegedConfigClient client;

    public WatchParametersRepository(PrivilegedConfigClient client) {
        this.client = Objects.requireNonNull(client, "Client is required.");
    }

    public void store(WatchParametersData...data) throws ConfigValidationException {
        Objects.requireNonNull(data, "Watch parameters data is required.");
        BulkRequest bulkRequest = new BulkRequest(WATCHES_INSTANCE_PARAMETERS).setRefreshPolicy(IMMEDIATE);
        Arrays.stream(data)//
            .map(watchParameters -> new IndexRequest(WATCHES_INSTANCE_PARAMETERS).id(watchParameters.getId())//
                .source(watchParameters.toJsonString(), XContentType.JSON)) //
            .forEach(bulkRequest::add);
        BulkResponse bulkItemResponses = client.bulk(bulkRequest).actionGet();
        if(bulkItemResponses.hasFailures()) {
            throw new ConfigValidationException(new ValidationError(null, bulkItemResponses.buildFailureMessage()));
        }
    }

    public Optional<WatchParametersData> findOneById(String tenantId, String watchId, String instanceId) {
        String parametersId = WatchParametersData.createId(tenantId, watchId, instanceId);
        GetResponse response = client.get(new GetRequest(WATCHES_INSTANCE_PARAMETERS, parametersId)).actionGet();
        return getResponseToWatchParametersData(response);
    }

    public ImmutableList<WatchParametersData> findByWatchId(String tenantId, String watchId) {
        Objects.requireNonNull(tenantId, "Tenant id is required");
        Objects.requireNonNull(watchId, "Watch id is required");
        BoolQueryBuilder boolQuery = parametersByTenantIdAndWatchIdQuery(tenantId, watchId);
        SearchRequest request = new SearchRequest(WATCHES_INSTANCE_PARAMETERS);
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource().query(boolQuery).version(true);
        sourceBuilder.size(WATCH_PARAMETER_DATA_PAGE_SIZE);
        request.source(sourceBuilder);
        SearchScroller searchScroller = new SearchScroller(client);
        ImmutableList<WatchParametersData> result = searchScroller.scrollAndLoadAll(request, this::searchHitToWatchParameterData);
        log.info("Found '{}' watch instances for generic watch '{}' and tenant '{}'", result.size(), watchId, tenantId);
        return result;
    }

    private static BoolQueryBuilder parametersByTenantIdAndWatchIdQuery(String tenantId, String watchId) {
        TermQueryBuilder queryTenant = QueryBuilders.termQuery(FIELD_TENANT_ID, tenantId);
        TermQueryBuilder queryWatchId = QueryBuilders.termQuery(FIELD_WATCH_ID, watchId);
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must().addAll(Arrays.asList(queryTenant, queryWatchId));
        return boolQuery;
    }

    public boolean delete(String tenantId, String watchId, String instanceId) {
        String parametersId = WatchParametersData.createId(tenantId, watchId, instanceId);
        DeleteResponse response = client.delete(new DeleteRequest(WATCHES_INSTANCE_PARAMETERS, parametersId).setRefreshPolicy(IMMEDIATE))//
            .actionGet();
        return response.status() == RestStatus.OK;
    }

    public void deleteByWatchId(String tenantId, String watchId) {
        Objects.requireNonNull(tenantId, "Tenant id is required");
        Objects.requireNonNull(watchId, "Watch id is required");
        ImmutableList<WatchParametersData> parametersForDeletion = findByWatchId(tenantId, watchId);
        if (parametersForDeletion.isEmpty()) {
            log.debug("Nothing to delete, parameters for watch '{}' defined for tenant '{}' does not exist.", watchId, tenantId);
            return;
        }
        BulkResponse response = delete(parametersForDeletion);
        if (response.hasFailures()) {
            String message = "Cannot delete watch parameters for tenant '" + tenantId + "' and watch '" + watchId + ". " //
                + response.buildFailureMessage();
            throw new RuntimeException(message);
        }
        log.debug("Deleted '{}' instance parameters", parametersForDeletion.size());
    }

    private BulkResponse delete(ImmutableList<WatchParametersData> parametersToBeDeleted) {
        BulkRequest bulkDeleteRequest = new BulkRequest(WATCHES_INSTANCE_PARAMETERS).setRefreshPolicy(IMMEDIATE);
        parametersToBeDeleted.map(WatchParametersData::getId) //
            .map(documentId -> new DeleteRequest(WATCHES_INSTANCE_PARAMETERS).id(documentId)) //
            .forEach(bulkDeleteRequest::add);
        return client.bulk(bulkDeleteRequest).actionGet();
    }

    private Optional<WatchParametersData> getResponseToWatchParametersData(GetResponse response) {
        return Optional.ofNullable(response) //
            .filter(GetResponse::isExists) //
            .map(existingResponse -> documentToWatchParameterData(existingResponse.getSourceAsString(), existingResponse.getVersion()));
    }

    private WatchParametersData searchHitToWatchParameterData(SearchHit searchHit) {
        return documentToWatchParameterData(searchHit.getSourceAsString(), searchHit.getVersion());
    }

    private static WatchParametersData documentToWatchParameterData(String jsonDocument, long version) {
        try {
            return new WatchParametersData(DocNode.parse(Format.JSON).from(jsonDocument), version);
        } catch (DocumentParseException e) {
            throw new RuntimeException("Database contain watch parameters which are not valid json document", e);
        }
    }
}
