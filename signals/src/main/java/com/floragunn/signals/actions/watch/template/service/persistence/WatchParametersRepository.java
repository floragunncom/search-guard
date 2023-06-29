package com.floragunn.signals.actions.watch.template.service.persistence;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.floragunn.signals.actions.watch.template.service.persistence.WatchParametersData.FIELD_TENANT_ID;
import static com.floragunn.signals.actions.watch.template.service.persistence.WatchParametersData.FIELD_WATCH_ID;
import static com.floragunn.signals.settings.SignalsSettings.SignalsStaticSettings.IndexNames.WATCHES_INSTANCE_PARAMETERS;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;

public class WatchParametersRepository {

    private static final Logger log = LogManager.getLogger(WatchParametersRepository.class);

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

    // support scrolling
    public ImmutableList<WatchParametersData> findByWatchId(String tenantId, String watchId) {
        Objects.requireNonNull(tenantId, "Tenant id is required");
        Objects.requireNonNull(watchId, "Watch id is required");
        BoolQueryBuilder boolQuery = parametersByTenantIdAndWatchIdQuery(tenantId, watchId);
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest request = new SearchRequest(WATCHES_INSTANCE_PARAMETERS);
        request.scroll(scroll);
        request.source(SearchSourceBuilder.searchSource().query(boolQuery));
        SearchResponse searchResponse = client.search(request).actionGet();
        if(! RestStatus.OK.equals(searchResponse.status())) {
            throw new RuntimeException("Cannot search for watch with id " + tenantId + ", which belongs to tenant " + tenantId);
        }
        List<WatchParametersData> mutableList = scroll(searchResponse, scroll, this::jsonToWatchParametersData);
        ImmutableList<WatchParametersData> result = ImmutableList.of(mutableList);
        log.info("Found '{}' watch instances for generic watch '{}' and tenant '{}'", result.size(), watchId, tenantId);
        return result;
    }

    private <T> ImmutableList<T> scroll(SearchResponse searchResponse, Scroll scroll, Function<String, T> resultMapper) {
        List<T> mutableList = new ArrayList<>();
        try {
            SearchHit[] hits = searchResponse.getHits().getHits();
            while ((hits != null) && (hits.length > 0)) {
                String scrollId = searchResponse.getScrollId();
                log.debug("'{}' elements were gained due to scrolling with id '{}'.", hits.length, scrollId);
                List<T> currentPage = Arrays.stream(hits)//
                    .map(SearchHit::getSourceAsString)//
                    .map(resultMapper)//
                    .collect(Collectors.toList());
                mutableList.addAll(currentPage);
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = client.searchScroll(scrollRequest).actionGet();
                hits = searchResponse.getHits().getHits();
            }
        } finally {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(searchResponse.getScrollId());
            client.clearScroll(clearScrollRequest).actionGet();
            log.debug("Cleared scroll request with id '{}'", searchResponse.getScrollId());
        }
        return ImmutableList.of(mutableList);
    }

    //without scroll
//    public ImmutableList<WatchParametersData> findByWatchId(String tenantId, String watchId) {
//        Objects.requireNonNull(tenantId, "Tenant id is required");
//        Objects.requireNonNull(watchId, "Watch id is required");
//        BoolQueryBuilder boolQuery = parametersByTenantIdAndWatchIdQuery(tenantId, watchId);
//        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
//        SearchRequest request = new SearchRequest(WATCHES_INSTANCE_PARAMETERS);
//        request.scroll(scroll);
//        request.source(SearchSourceBuilder.searchSource().query(boolQuery));
//        SearchResponse searchResponse = client.search(request).actionGet();
//        if(! RestStatus.OK.equals(searchResponse.status())) {
//            throw new RuntimeException("Cannot search for watch with id " + tenantId + ", which belongs to tenant " + tenantId);
//        }
//        List<WatchParametersData> mutableList = Arrays.stream(searchResponse.getHits().getHits())//
//            .map(SearchHit::getSourceAsString)//
//            .map(this::jsonToWatchParametersData)//
//            .collect(Collectors.toList());
//        log.info("Found '{}' watch instances for generic watch '{}' and tenant '{}'", mutableList.size(), watchId, tenantId);
//        return ImmutableList.of(mutableList);
//    }

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
        return response.isExists() ? Optional.ofNullable(jsonToWatchParametersData(response.getSourceAsString())) : Optional.empty();
    }

    private WatchParametersData jsonToWatchParametersData(String json) {
        try {
            return new WatchParametersData(DocNode.parse(Format.JSON).from(json));
        } catch (DocumentParseException e) {
            throw new RuntimeException("Database contain watch parameters which are not valid json document", e);
        }
    }

}
