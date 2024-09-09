package com.floragunn.aim.integration.support;

import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.api.internal.InternalPolicyAPI;
import com.floragunn.aim.api.internal.InternalPolicyInstanceAPI;
import com.floragunn.aim.api.internal.InternalSettingsAPI;
import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.awaitility.Awaitility;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClusterHelper {
    public static final Header DEFAULT_AUTH = basicAuth("uhura", "uhura");
    public static final Header AIM_ALL_AUTH = basicAuth("cara", "redshirt");
    public static final Header AIM_POLICY_READ_AUTH = basicAuth("lucy", "redshirt");
    public static final Header AIM_POLICY_MANAGE_AUTH = basicAuth("maren", "redshirt");
    public static final Header AIM_POLICY_INSTANCE_READ_AUTH = basicAuth("leni", "redshirt");
    public static final Header AIM_POLICY_INSTANCE_MANAGE_AUTH = basicAuth("lisa", "redshirt");

    private static Header basicAuth(String username, String password) {
        return new BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((username + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));
    }

    public static class Rest {
        public static GenericRestClient.HttpResponse deletePolicy(LocalCluster cluster, Header auth, String policyName) throws Exception {
            return cluster.getRestClient(auth).delete("/_aim/policy/" + policyName);
        }

        public static GenericRestClient.HttpResponse deletePolicy(LocalCluster cluster, String policyName) throws Exception {
            return deletePolicy(cluster, DEFAULT_AUTH, policyName);
        }

        public static GenericRestClient.HttpResponse getPolicy(LocalCluster cluster, Header auth, String policyName) throws Exception {
            return cluster.getRestClient(auth).get("/_aim/policy/" + policyName);
        }

        public static GenericRestClient.HttpResponse getPolicy(LocalCluster cluster, String policyName) throws Exception {
            return getPolicy(cluster, DEFAULT_AUTH, policyName);
        }

        public static GenericRestClient.HttpResponse getPolicyInternal(LocalCluster cluster, Header auth, String policyName) throws Exception {
            return cluster.getRestClient(auth).get("/_aim/policy/" + policyName + "/internal");
        }

        public static GenericRestClient.HttpResponse getPolicyInternal(LocalCluster cluster, String policyName) throws Exception {
            return getPolicyInternal(cluster, DEFAULT_AUTH, policyName);
        }

        public static GenericRestClient.HttpResponse putPolicy(LocalCluster cluster, Header auth, String policyName, Policy policy) throws Exception {
            return cluster.getRestClient(auth).putJson("/_aim/policy/" + policyName, policy);
        }

        public static GenericRestClient.HttpResponse putPolicy(LocalCluster cluster, String policyName, Policy policy) throws Exception {
            return putPolicy(cluster, DEFAULT_AUTH, policyName, policy);
        }

        public static GenericRestClient.HttpResponse getPolicyInstanceStatus(LocalCluster cluster, Header auth, String indexName) throws Exception {
            return cluster.getRestClient(auth).get("/_aim/state/" + indexName);
        }

        public static GenericRestClient.HttpResponse getPolicyInstanceStatus(LocalCluster cluster, String indexName) throws Exception {
            return getPolicyInstanceStatus(cluster, DEFAULT_AUTH, indexName);
        }

        public static GenericRestClient.HttpResponse postPolicyInstanceExecute(LocalCluster cluster, Header auth, String indexName) throws Exception {
            return cluster.getRestClient(auth).post("/_aim/execute/" + indexName);
        }

        public static GenericRestClient.HttpResponse postPolicyInstanceExecute(LocalCluster cluster, String indexName) throws Exception {
            return postPolicyInstanceExecute(cluster, DEFAULT_AUTH, indexName);
        }

        public static GenericRestClient.HttpResponse postPolicyInstanceExecuteRetry(LocalCluster cluster, Header auth, String indexName)
                throws Exception {
            return cluster.getRestClient(auth).post("/_aim/execute/" + indexName + "/true");
        }

        public static GenericRestClient.HttpResponse postPolicyInstanceExecuteRetry(LocalCluster cluster, String indexName) throws Exception {
            return postPolicyInstanceExecuteRetry(cluster, DEFAULT_AUTH, indexName);
        }

        public static GenericRestClient.HttpResponse postPolicyInstanceRetry(LocalCluster cluster, Header auth, String indexName) throws Exception {
            return cluster.getRestClient(auth).post("/_aim/retry/" + indexName);
        }

        public static GenericRestClient.HttpResponse postPolicyInstanceRetry(LocalCluster cluster, String indexName) throws Exception {
            return postPolicyInstanceRetry(cluster, DEFAULT_AUTH, indexName);
        }

        public static GenericRestClient.HttpResponse deleteSetting(LocalCluster cluster, Header auth, String key) throws Exception {
            return cluster.getRestClient(auth).delete("/_aim/settings/" + key);
        }

        public static GenericRestClient.HttpResponse deleteSetting(LocalCluster cluster, String key) throws Exception {
            return deleteSetting(cluster, DEFAULT_AUTH, key);
        }

        public static GenericRestClient.HttpResponse getSetting(LocalCluster cluster, Header auth, String key) throws Exception {
            return cluster.getRestClient(auth).get("/_aim/settings/" + key);
        }

        public static GenericRestClient.HttpResponse getSetting(LocalCluster cluster, String key) throws Exception {
            return getSetting(cluster, DEFAULT_AUTH, key);
        }

        public static GenericRestClient.HttpResponse putSetting(LocalCluster cluster, Header auth, String key, String value) throws Exception {
            return cluster.getRestClient(auth).putJson("/_aim/settings/" + key, value);
        }

        public static GenericRestClient.HttpResponse putSetting(LocalCluster cluster, String key, String value) throws Exception {
            return putSetting(cluster, DEFAULT_AUTH, key, value);
        }
    }

    public static class Internal {
        public static InternalPolicyAPI.StatusResponse putPolicy(LocalCluster.Embedded cluster, String policyName, Policy policy) {
            return cluster.getInternalNodeClient().admin().indices()
                    .execute(InternalPolicyAPI.Put.INSTANCE, new InternalPolicyAPI.Put.Request(policyName, policy, false)).actionGet();
        }

        public static InternalPolicyInstanceAPI.PostExecuteRetry.Response postPolicyInstanceExecuteRetry(LocalCluster.Embedded cluster,
                String indexName, boolean execute, boolean retry) {
            return cluster.getInternalNodeClient().admin().indices().execute(InternalPolicyInstanceAPI.PostExecuteRetry.INSTANCE,
                    new InternalPolicyInstanceAPI.PostExecuteRetry.Request(indexName, execute, retry)).actionGet();
        }

        public static InternalPolicyInstanceAPI.PostExecuteRetry.Response postPolicyInstanceRetry(LocalCluster.Embedded cluster, String indexName) {
            return postPolicyInstanceExecuteRetry(cluster, indexName, false, true);
        }

        public static InternalPolicyInstanceAPI.PostExecuteRetry.Response postPolicyInstanceExecute(LocalCluster.Embedded cluster, String indexName) {
            return postPolicyInstanceExecuteRetry(cluster, indexName, true, false);
        }

        public static InternalSettingsAPI.Update.Response postSettingsUpdate(LocalCluster.Embedded cluster,
                AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?> key, Object value) {
            return cluster.getInternalNodeClient().admin().indices().execute(InternalSettingsAPI.Update.INSTANCE,
                    new InternalSettingsAPI.Update.Request(ImmutableMap.of(key, value), ImmutableList.empty())).actionGet();
        }

        public static InternalSettingsAPI.Update.Response postSettingsDelete(LocalCluster.Embedded cluster,
                AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?> key) {
            return cluster.getInternalNodeClient().admin().indices()
                    .execute(InternalSettingsAPI.Update.INSTANCE, new InternalSettingsAPI.Update.Request(ImmutableMap.empty(), ImmutableList.of(key)))
                    .actionGet();
        }
    }

    public static class Index {
        private static GetResponse get(LocalCluster.Embedded cluster, String index, String doc) {
            return cluster.getPrivilegedInternalNodeClient().get(new GetRequest(index).id(doc)).actionGet();
        }

        public static GetResponse getPolicy(LocalCluster.Embedded cluster, String policyName) {
            return get(cluster, AutomatedIndexManagementSettings.ConfigIndices.POLICIES_NAME, policyName);
        }

        public static boolean isPolicyExists(LocalCluster.Embedded cluster, String policyName) {
            return get(cluster, AutomatedIndexManagementSettings.ConfigIndices.POLICIES_NAME, policyName).isExists();
        }

        public static void awaitPolicyExists(LocalCluster.Embedded cluster, String policyName) {
            Awaitility.await().until(() -> get(cluster, AutomatedIndexManagementSettings.ConfigIndices.POLICIES_NAME, policyName),
                    GetResponse::isExists);
        }

        public static CreateIndexResponse createManagedIndex(LocalCluster.Embedded cluster, String indexName, String policyName) {
            Settings.Builder builder = Settings.builder().put(AutomatedIndexManagementSettings.Static.POLICY_NAME_FIELD.name(), policyName);
            Settings indexSettings = builder.build();
            CreateIndexRequest request = new CreateIndexRequest(indexName, indexSettings);
            return cluster.getInternalNodeClient().admin().indices().create(request).actionGet();
        }

        public static CreateIndexResponse createManagedIndex(LocalCluster.Embedded cluster, String indexName, String policyName, Settings settings) {
            Settings.Builder builder = Settings.builder().put(AutomatedIndexManagementSettings.Static.POLICY_NAME_FIELD.name(), policyName)
                    .put(settings);
            CreateIndexRequest request = new CreateIndexRequest(indexName, builder.build());
            return cluster.getInternalNodeClient().admin().indices().create(request).actionGet();
        }

        public static CreateIndexResponse createManagedIndex(LocalCluster.Embedded cluster, String indexName, String policyName, String alias,
                Settings settings) {
            Settings.Builder builder = Settings.builder().put(AutomatedIndexManagementSettings.Static.POLICY_NAME_FIELD.name(), policyName)
                    .put(settings);
            CreateIndexRequest request = new CreateIndexRequest(indexName, builder.build()).alias(new Alias(alias));
            return cluster.getInternalNodeClient().admin().indices().create(request).actionGet();
        }

        public static GetResponse getPolicyInstanceStatus(LocalCluster.Embedded cluster, String indexName) {
            return get(cluster, AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_STATES_NAME, indexName);
        }

        public static boolean isPolicyInstanceStatusExists(LocalCluster.Embedded cluster, String indexName) {
            return get(cluster, AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_STATES_NAME, indexName).isExists();
        }

        public static boolean isPolicyInstanceStatusEqual(LocalCluster.Embedded cluster, String indexName, PolicyInstanceState.Status status) {
            GetResponse response = get(cluster, AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_STATES_NAME, indexName);
            return response.isExists() && status.name().equals(response.getSource().get(PolicyInstanceState.STATUS_FIELD));
        }

        public static void awaitPolicyInstanceStatusExists(LocalCluster.Embedded cluster, String indexName) {
            Awaitility.await().until(() -> get(cluster, AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_STATES_NAME, indexName),
                    GetResponse::isExists);
        }

        public static void awaitPolicyInstanceStatusEqual(LocalCluster.Embedded cluster, String indexName, PolicyInstanceState.Status status,
                Runnable task) {
            Awaitility.await().until(() -> {
                task.run();
                return getPolicyInstanceStatus(cluster, indexName);
            }, s -> status.name().equals(s.getSource().get(PolicyInstanceState.STATUS_FIELD)));
        }

        public static void awaitPolicyInstanceStatusEqual(LocalCluster.Embedded cluster, String indexName, PolicyInstanceState.Status status) {
            awaitPolicyInstanceStatusEqual(cluster, indexName, status, () -> {
            });
        }

        public static void assertStatus(LocalCluster.Embedded cluster, String indexName, PolicyInstanceState.Status status) {
            GetResponse getResponse = getPolicyInstanceStatus(cluster, indexName);
            assertTrue(getResponse.isExists(), Strings.toString(getResponse));
            assertEquals(status.name(), getResponse.getSource().get(PolicyInstanceState.STATUS_FIELD));
        }

        public static void awaitSegmentCount(LocalCluster.Embedded cluster, String indexName, Integer min, Integer max) {
            Awaitility.await().until(() -> {
                IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest().indices(indexName).clear().segments(true);
                return cluster.getInternalNodeClient().admin().indices().stats(indicesStatsRequest).actionGet();
            }, indicesStatsResponse -> {
                for (IndexShardStats indexShardStats : indicesStatsResponse.getIndex(indexName)) {
                    for (ShardStats shardStats : indexShardStats) {
                        if (shardStats.getShardRouting().primary()) {
                            if (!shardStats.getShardRouting().started()) {
                                return false;
                            }
                            if (min != null && shardStats.getStats().getSegments().getCount() < min) {
                                return false;
                            }
                            if (max != null && shardStats.getStats().getSegments().getCount() > max) {
                                return false;
                            }
                        }
                    }
                }
                return true;
            });
        }

        public static void awaitSearchHitCount(LocalCluster.Embedded cluster, SearchRequest searchRequest, int count) {
            Awaitility.await().until(() -> {
                SearchResponse response = null;
                try {
                    response = cluster.getInternalNodeClient().search(searchRequest).actionGet();
                    return Objects.requireNonNull(response.getHits().getTotalHits()).value == count;
                } catch (NullPointerException e) {
                    return false;
                } finally {
                    if (response != null) {
                        response.decRef();
                    }
                }
            });
        }
    }
}
