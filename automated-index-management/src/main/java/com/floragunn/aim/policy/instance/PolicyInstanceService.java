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
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PolicyInstanceService {
    private static final Logger LOG = LogManager.getLogger(PolicyInstanceService.class);

    private final PrivilegedConfigClient client;
    private final List<StateUpdateListener> stateUpdateListeners;

    public PolicyInstanceService(Client client) {
        this.client = PrivilegedConfigClient.adapt(client);
        stateUpdateListeners = new ArrayList<>();
    }

    public synchronized void addStateUpdateListener(StateUpdateListener listener) {
        stateUpdateListeners.add(listener);
    }

    public synchronized void removeStateUpdateListener(StateUpdateListener listener) {
        stateUpdateListeners.remove(listener);
    }

    public boolean activeStateExistsForPolicy(String policy) {
        try {
            SearchResponse response = client.prepareSearch(AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_STATES_NAME)
                    .setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery(PolicyInstanceState.POLICY_NAME_FIELD, policy))
                            .mustNot(QueryBuilders.termsQuery(PolicyInstanceState.STATUS_FIELD, PolicyInstanceState.Status.DELETED.name(),
                                    PolicyInstanceState.Status.NOT_STARTED.name())))
                    .get();
            boolean res = response.getHits().getTotalHits() == null || response.getHits().getTotalHits().value > 0;
            LOG.trace("Active states for policy '{}' search response:\n{}", policy, response.toString());
            response.decRef();
            return res;
        } catch (Exception e) {
            LOG.warn("Could not retrieve policy instance state for policy {}", policy, e);
            return false;
        }
    }

    public void updateState(String index, PolicyInstanceState state) {
        LOG.trace("Updating policy instance state for index '{}':\n{}", index, state.toPrettyJsonString());
        try {
            DocWriteResponse response = client.prepareIndex(AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_STATES_NAME).setId(index)
                    .setSource(state.toDocNode()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
            if (RestStatus.CREATED != response.status() && RestStatus.OK != response.status()) {
                LOG.warn("Could not update policy instance state: {}", response);
            }
        } catch (Exception e) {
            LOG.warn("Could not update policy instance state", e);
        } finally {
            synchronized (this) {
                for (StateUpdateListener listener : stateUpdateListeners) {
                    listener.onStateUpdate(index, state);
                }
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

    public PolicyInstanceState getState(String index) {
        try {
            GetResponse response = client.prepareGet(AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_STATES_NAME, index).get();
            if (response.isExists()) {
                return new PolicyInstanceState(DocNode.parse(Format.JSON).from(response.getSourceAsBytesRef().utf8ToString()));
            }
        } catch (ConfigValidationException e) {
            LOG.warn("Failed to parse policy instance state for index '{}'. State is invalid", index, e);
        } catch (Exception e) {
            LOG.error("Error while retrieving policy instance state for index '{}'", index, e);
        }
        return null;
    }

    public Map<String, PolicyInstanceState> getStates(Collection<String> indexNames) {
        Map<String, PolicyInstanceState> result = new HashMap<>(indexNames.size());
        try {
            MultiGetResponse response = client.prepareMultiGet()
                    .addIds(AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_STATES_NAME, indexNames).get();
            for (MultiGetItemResponse item : response) {
                if (item.isFailed()) {
                    LOG.warn("Failed to retrieve policy instance state for index '{}'", item.getIndex());
                } else if (item.getResponse().isExists()) {
                    try {
                        result.put(item.getResponse().getIndex(),
                                new PolicyInstanceState(DocNode.parse(Format.JSON).from(item.getResponse().getSourceAsBytesRef().utf8ToString())));
                    } catch (ConfigValidationException e) {
                        LOG.warn("Failed to parse policy instance state for index '{}'. State is invalid", item.getIndex(), e);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Error while retrieving policy instance state for indices '{}'", indexNames, e);
        }
        return result;
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

    @FunctionalInterface
    public interface StateUpdateListener {
        void onStateUpdate(String index, PolicyInstanceState state);
    }
}
