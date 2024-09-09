package com.floragunn.aim.policy.instance;

import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.api.internal.InternalPolicyInstanceAPI;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.rest.RestStatus;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PolicyInstanceService {
    private static final Logger LOG = LogManager.getLogger(PolicyInstanceService.class);

    private final PrivilegedConfigClient client;
    private PolicyInstanceStateLogHandler policyInstanceStateLogHandler;

    public PolicyInstanceService(Client client) {
        this.client = PrivilegedConfigClient.adapt(client);
    }

    protected synchronized void setPolicyInstanceStateLogHandler(PolicyInstanceStateLogHandler policyInstanceStateLogHandler) {
        this.policyInstanceStateLogHandler = policyInstanceStateLogHandler;
    }

    public void updateState(String index, PolicyInstanceState state) {
        try {
            DocWriteResponse response = client.prepareIndex(AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_STATES_NAME).setId(index)
                    .setSource(state.toDocNode()).get();
            if (RestStatus.CREATED != response.status() && RestStatus.OK != response.status()) {
                LOG.warn("Could not update policy instance state: {}", response);
            }
        } catch (Exception e) {
            LOG.warn("Could not update policy instance state", e);
        } finally {
            if (policyInstanceStateLogHandler != null) {
                policyInstanceStateLogHandler.putStateLogEntry(index, state);
            }
        }
    }

    public CompletableFuture<GetResponse> getStateAsync(String indexName) {
        CompletableFuture<GetResponse> result = new CompletableFuture<>();
        client.prepareGet(AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_STATES_NAME, indexName).execute(new ActionListener<>() {
            @Override
            public void onResponse(GetResponse documentFields) {
                result.complete(documentFields);
            }

            @Override
            public void onFailure(Exception e) {
                result.completeExceptionally(e);
            }
        });
        return result;
    }

    public Map<String, PolicyInstanceState> getStates(Collection<String> indexNames) {
        Map<String, PolicyInstanceState> result = new HashMap<>();
        try {
            MultiGetResponse response = client.prepareMultiGet()
                    .addIds(AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_STATES_NAME, indexNames).get();
            for (MultiGetItemResponse item : response.getResponses()) {
                try {
                    if (item.isFailed()) {
                        LOG.debug("Failed to get policy instance state for index '{}'\n{}", item.getId(), item.getFailure());
                    } else if (item.getResponse().isExists()) {
                        result.put(item.getId(),
                                new PolicyInstanceState(DocNode.parse(Format.JSON).from(item.getResponse().getSourceAsBytesRef().utf8ToString())));
                    }
                } catch (DocumentParseException dpe) {
                    LOG.debug("Failed to parse policy instance state for index '{}'\n{}", item.getId(), dpe);
                }
            }
        } catch (Exception e) {
            LOG.warn("Error while executing multi get operation for policy instance states", e);
        }
        return result;
    }

    public void deleteCreateStates(List<String> delete, Map<String, PolicyInstanceState> create) {
        try {
            if (delete.isEmpty() && create.isEmpty()) {
                return;
            }
            BulkRequest request = new BulkRequest(AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_STATES_NAME);
            for (String index : delete) {
                request.add(new DeleteRequest().id(index));
            }
            for (Map.Entry<String, PolicyInstanceState> entry : create.entrySet()) {
                LOG.trace("Creating state for index '{}'", entry.getKey());
                request.add(new IndexRequest().id(entry.getKey()).source(entry.getValue().toDocNode()));
            }
            BulkResponse response = client.bulk(request).actionGet();
            if (response.hasFailures()) {
                LOG.warn("Error while creating and deleting policy instance states: {}", response.buildFailureMessage());
            }
        } catch (Exception e) {
            LOG.warn("Error while creating policy instance states", e);
        }
    }

    public CompletableFuture<InternalPolicyInstanceAPI.PostExecuteRetry.Response> postExecuteRetryAsync(String index, boolean execute,
            boolean retry) {
        InternalPolicyInstanceAPI.PostExecuteRetry.Request request = new InternalPolicyInstanceAPI.PostExecuteRetry.Request(index, execute, retry);
        CompletableFuture<InternalPolicyInstanceAPI.PostExecuteRetry.Response> result = new CompletableFuture<>();
        client.execute(InternalPolicyInstanceAPI.PostExecuteRetry.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(InternalPolicyInstanceAPI.PostExecuteRetry.Response response) {
                result.complete(response);
            }

            @Override
            public void onFailure(Exception e) {
                result.completeExceptionally(e);
            }
        });
        return result;
    }
}
