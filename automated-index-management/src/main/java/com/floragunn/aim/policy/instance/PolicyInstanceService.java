package com.floragunn.aim.policy.instance;

import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.api.internal.InternalPolicyInstanceAPI;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.rest.RestStatus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class PolicyInstanceService {
    private static final Logger LOG = LogManager.getLogger(PolicyInstanceService.class);

    private final AutomatedIndexManagementSettings settings;
    private final PrivilegedConfigClient client;
    private PolicyInstanceStateLogHandler policyInstanceStateLogHandler;

    public PolicyInstanceService(AutomatedIndexManagementSettings settings, Client client) {
        this.settings = settings;
        this.client = PrivilegedConfigClient.adapt(client);
    }

    protected void setPolicyInstanceStateLogHandler(PolicyInstanceStateLogHandler policyInstanceStateLogHandler) {
        this.policyInstanceStateLogHandler = policyInstanceStateLogHandler;
    }

    public void deleteState(String indexName) {
        DeleteRequest request = new DeleteRequest(settings.getStatic().configIndices().getStatesName()).id(indexName);
        try {
            DeleteResponse response = client.delete(request).actionGet();
            if (!RestStatus.OK.equals(response.status()) && !RestStatus.NOT_FOUND.equals(response.status())) {
                LOG.warn("Error while deleting policy instance state for index '" + indexName + "'");
            }
        } catch (Exception e) {
            LOG.warn("Error while deleting policy instance state for index '" + indexName + "'");
        }
    }

    public void updateState(String index, PolicyInstanceState state) {
        IndexRequest request = new IndexRequest(settings.getStatic().configIndices().getStatesName()).id(index).source(state.toDocNode());
        try {
            IndexResponse response = client.index(request).actionGet();
            if (RestStatus.CREATED != response.status() && RestStatus.OK != response.status()) {
                LOG.warn("Could not update policy instance state: " + response);
            }
        } catch (Exception e) {
            LOG.warn("Could not update policy instance state", e);
        } finally {
            if (policyInstanceStateLogHandler != null) {
                policyInstanceStateLogHandler.putStateLogEntry(index, state);
            }
        }
    }

    public PolicyInstanceState getState(String indexName) {
        GetRequest request = new GetRequest(settings.getStatic().configIndices().getStatesName()).id(indexName);
        try {
            GetResponse response = client.get(request).actionGet();
            if (response.isExists()) {
                return new PolicyInstanceState(DocNode.parse(Format.JSON).from(response.getSourceAsBytesRef().utf8ToString()));
            } else {
                LOG.trace("State not found for index '" + indexName + "'");
            }
        } catch (ConfigValidationException e) {
            LOG.warn("Error while parsing policy instance state from index", e);
        } catch (Exception e) {
            LOG.warn("Error while retrieving policy instance state from index", e);
        }
        return null;
    }

    public CompletableFuture<GetResponse> getStateAsync(String indexName) {
        CompletableFuture<GetResponse> result = new CompletableFuture<>();
        client.execute(GetAction.INSTANCE, new GetRequest(settings.getStatic().configIndices().getStatesName(), indexName),
                new ActionListener<GetResponse>() {
                    @Override
                    public void onResponse(GetResponse response) {
                        result.complete(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        result.completeExceptionally(e);
                    }
                });
        return result;
    }

    public void deleteStates(String... indexNames) {
        BulkRequest request = new BulkRequest(settings.getStatic().configIndices().getStatesName());
        for (String name : indexNames) {
            request.add(new DeleteRequest().id(name));
        }
        try {
            BulkResponse response = client.bulk(request).actionGet();
            if (response.hasFailures()) {
                LOG.warn("Error while deleting policy instance states: " + response.buildFailureMessage());
            }
        } catch (Exception e) {
            LOG.warn("Error while executing delete operation for policy instance states");
        }
    }

    public Map<String, PolicyInstanceState> getStates(Set<String> indexNames) {
        MultiGetRequest request = new MultiGetRequest();
        for (String indexName : indexNames) {
            request.add(settings.getStatic().configIndices().getStatesName(), indexName);
        }
        Map<String, PolicyInstanceState> result = new HashMap<>();
        try {
            MultiGetResponse response = client.multiGet(request).actionGet();
            for (MultiGetItemResponse item : response.getResponses()) {
                if (item.isFailed()) {
                    LOG.debug("Failed to get policy instance state for index '" + item.getId() + "'", item.getFailure());
                } else if (item.getResponse().isExists()) {
                    result.put(item.getId(),
                            new PolicyInstanceState(DocNode.parse(Format.JSON).from(item.getResponse().getSourceAsBytesRef().utf8ToString())));
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
            BulkRequest request = new BulkRequest(settings.getStatic().configIndices().getStatesName());
            for (String index : delete) {
                request.add(new DeleteRequest().id(index));
            }
            for (Map.Entry<String, PolicyInstanceState> entry : create.entrySet()) {
                LOG.trace("Creating state for index '" + entry.getKey() + "'");
                request.add(new IndexRequest().id(entry.getKey()).source(entry.getValue().toDocNode()));
            }
            BulkResponse response = client.bulk(request).actionGet();
            if (response.hasFailures()) {
                LOG.warn("Error while creating and deleting policy instance states: " + response.buildFailureMessage());
            }
        } catch (Exception e) {
            LOG.warn("Error while creating policy instance states", e);
        }
    }

    public CompletableFuture<InternalPolicyInstanceAPI.PostExecuteRetry.Response> postExecuteRetryAsync(String index, boolean execute,
            boolean retry) {
        InternalPolicyInstanceAPI.PostExecuteRetry.Request request = new InternalPolicyInstanceAPI.PostExecuteRetry.Request(index, execute, retry);
        CompletableFuture<InternalPolicyInstanceAPI.PostExecuteRetry.Response> result = new CompletableFuture<>();
        client.execute(InternalPolicyInstanceAPI.PostExecuteRetry.INSTANCE, request,
                new ActionListener<InternalPolicyInstanceAPI.PostExecuteRetry.Response>() {
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
