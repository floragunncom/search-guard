package com.floragunn.aim.policy;

import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.api.internal.InternalPolicyAPI;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.internal.Client;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public class PolicyService {
    private final PrivilegedConfigClient client;

    public PolicyService(Client client) {
        this.client = PrivilegedConfigClient.adapt(client);
    }

    public GetResponse getPolicy(String policyName) {
        return client.get(new GetRequest().index(AutomatedIndexManagementSettings.ConfigIndices.POLICIES_NAME).id(policyName)).actionGet();
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

    public MultiGetResponse multiGetPolicy(Collection<String> policyNames) {
        return client.prepareMultiGet().addIds(AutomatedIndexManagementSettings.ConfigIndices.POLICIES_NAME, policyNames).get();
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
