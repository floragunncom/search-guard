package com.floragunn.signals.actions.watch.template.service.persistence;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.floragunn.signals.actions.watch.template.service.persistence.WatchParametersData.FIELD_TENANT_ID;
import static com.floragunn.signals.actions.watch.template.service.persistence.WatchParametersData.FIELD_WATCH_ID;
import static com.floragunn.signals.settings.SignalsSettings.SignalsStaticSettings.IndexNames.WATCHES_INSTANCE_PARAMETERS;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;

public class WatchParametersRepository {

    private final PrivilegedConfigClient client;

    public WatchParametersRepository(PrivilegedConfigClient client) {
        this.client = Objects.requireNonNull(client, "Client is required.");
    }

    public void store(WatchParametersData...data) throws ConfigValidationException {
        BulkRequest bulkRequest = new BulkRequest(WATCHES_INSTANCE_PARAMETERS).setRefreshPolicy(IMMEDIATE);
        Arrays.stream(data)//
            .map(watchParameters -> new IndexRequest(WATCHES_INSTANCE_PARAMETERS).id(watchParameters.id())//
                .source(watchParameters.toJsonString(), XContentType.JSON)) //
            .forEach(indexRequest -> bulkRequest.add(indexRequest));
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
        BoolQueryBuilder boolQuery = parametersByTenantIdAndWatchIdQuery(tenantId, watchId);
        SearchRequest request = new SearchRequest(WATCHES_INSTANCE_PARAMETERS);
        request.source(SearchSourceBuilder.searchSource().query(boolQuery));
        SearchResponse searchResponse = client.search(request).actionGet();
        if(! RestStatus.OK.equals(searchResponse.status())) {
            throw new RuntimeException("Cannot search for watch with id " + tenantId + ", which belongs to tenant " + tenantId);
        }
        List<WatchParametersData> mutableList = Arrays.stream(searchResponse.getHits().getHits())//
            .map(SearchHit::getSourceAsString)//
            .map(this::jsonToWatchParametersData)//
            .collect(Collectors.toList());
        return ImmutableList.of(mutableList);
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
