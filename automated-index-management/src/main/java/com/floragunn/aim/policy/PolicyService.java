package com.floragunn.aim.policy;

import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.api.internal.InternalPolicyAPI;
import com.floragunn.aim.policy.actions.Action;
import com.floragunn.aim.policy.conditions.Condition;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.internal.Client;

import java.util.concurrent.CompletableFuture;

public class PolicyService {
    private static final Logger LOG = LogManager.getLogger(PolicyService.class);

    private final PrivilegedConfigClient client;
    private final Condition.Factory conditionFactory;
    private final Action.Factory actionFactory;

    public PolicyService(Client client, Condition.Factory conditionFactory, Action.Factory actionFactory) {
        this.client = PrivilegedConfigClient.adapt(client);
        this.conditionFactory = conditionFactory;
        this.actionFactory = actionFactory;
    }

    public GetResponse getPolicy(String policyName) {
        return client.get(new GetRequest().index(AutomatedIndexManagementSettings.ConfigIndices.POLICIES_NAME).id(policyName)).actionGet();
    }

    public Policy getPolicyNew(String policyName) {
        try {
            GetResponse response = client.get(new GetRequest().index(AutomatedIndexManagementSettings.ConfigIndices.POLICIES_NAME).id(policyName))
                    .actionGet();
            if (response.isExists()) {
                DocNode node = DocNode.parse(Format.JSON).from(response.getSourceAsBytesRef().utf8ToString());
                return Policy.parse(node, Policy.ParsingContext.lenient(conditionFactory, actionFactory));
            }
        } catch (ConfigValidationException e) {
            LOG.warn("Failed to parse policy '{}'. Policy is invalid", policyName, e);
        } catch (Exception e) {
            LOG.error("Failed to retrieve policy '{}' from index", policyName, e);
        }
        return null;
    }

    public CompletableFuture<GetResponse> getPolicyAsync(String policyName) {
        CompletableFuture<GetResponse> result = new CompletableFuture<>();
        client.prepareGet(AutomatedIndexManagementSettings.ConfigIndices.POLICIES_NAME, policyName).execute(new ActionListener<>() {
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

    public InternalPolicyAPI.StatusResponse putPolicy(String policyName, Policy policy) {
        return putPolicy(policyName, policy, false);
    }

    public InternalPolicyAPI.StatusResponse putPolicy(String policyName, Policy policy, boolean force) {
        return client.execute(InternalPolicyAPI.Put.INSTANCE, new InternalPolicyAPI.Put.Request(policyName, policy, force)).actionGet();
    }

    public CompletableFuture<InternalPolicyAPI.StatusResponse> putPolicyAsync(String policyName, Policy policy) {
        return putPolicyAsync(policyName, policy, false);
    }

    public CompletableFuture<InternalPolicyAPI.StatusResponse> putPolicyAsync(String policyName, Policy policy, boolean force) {
        CompletableFuture<InternalPolicyAPI.StatusResponse> result = new CompletableFuture<>();
        client.execute(InternalPolicyAPI.Put.INSTANCE, new InternalPolicyAPI.Put.Request(policyName, policy, force), new ActionListener<>() {
            @Override
            public void onResponse(InternalPolicyAPI.StatusResponse statusResponse) {
                result.complete(statusResponse);
            }

            @Override
            public void onFailure(Exception e) {
                result.completeExceptionally(e);
            }
        });
        return result;
    }

    public CompletableFuture<InternalPolicyAPI.StatusResponse> deletePolicyAsync(String policyName) {
        return deletePolicyAsync(policyName, false);
    }

    public CompletableFuture<InternalPolicyAPI.StatusResponse> deletePolicyAsync(String policyName, boolean force) {
        CompletableFuture<InternalPolicyAPI.StatusResponse> result = new CompletableFuture<>();
        client.execute(InternalPolicyAPI.Delete.INSTANCE, new InternalPolicyAPI.Delete.Request(policyName, force), new ActionListener<>() {
            @Override
            public void onResponse(InternalPolicyAPI.StatusResponse statusResponse) {
                result.complete(statusResponse);
            }

            @Override
            public void onFailure(Exception e) {
                result.completeExceptionally(e);
            }
        });
        return result;
    }
}
