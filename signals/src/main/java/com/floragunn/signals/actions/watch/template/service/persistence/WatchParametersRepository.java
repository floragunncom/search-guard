package com.floragunn.signals.actions.watch.template.service.persistence;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;

import java.util.Objects;
import java.util.Optional;

import static com.floragunn.signals.settings.SignalsSettings.SignalsStaticSettings.IndexNames.WATCHES_INSTANCE_PARAMETERS;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;

public class WatchParametersRepository {

    private final PrivilegedConfigClient client;

    public WatchParametersRepository(PrivilegedConfigClient client) {
        this.client = Objects.requireNonNull(client, "Client is required.");
    }

    public IndexResponse store(WatchParametersData data) {
        String json = data.toJsonString();
        IndexRequest request = new IndexRequest(WATCHES_INSTANCE_PARAMETERS).id(data.id()).setRefreshPolicy(IMMEDIATE)
            .source(json, XContentType.JSON);
        return client.index(request).actionGet();
    }

    public Optional<WatchParametersData> findOneById(String tenantId, String watchId, String instanceId) {
        String parametersId = WatchParametersData.createId(tenantId, watchId, instanceId);
        GetResponse response = client.get(new GetRequest(WATCHES_INSTANCE_PARAMETERS, parametersId)).actionGet();
        return getResponseToWatchParametersData(response);
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
