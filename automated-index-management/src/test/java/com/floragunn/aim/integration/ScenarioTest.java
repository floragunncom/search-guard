package com.floragunn.aim.integration;

import com.floragunn.aim.AutomatedIndexManagementModule;
import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.MockSupport;
import com.floragunn.aim.integration.support.ClusterHelper;
import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.actions.SetReadOnlyAction;
import com.floragunn.aim.policy.conditions.SizeCondition;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.awaitility.Awaitility;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("IntegrationTest")
@Execution(ExecutionMode.SAME_THREAD)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ScenarioTest {
    private static LocalCluster.Embedded CLUSTER;

    @BeforeAll
    public static void setup() {
        MockSupport.init();
        CLUSTER = new LocalCluster.Builder().sslEnabled().resources("sg_config").enableModule(AutomatedIndexManagementModule.class)
                .clusterConfiguration(ClusterConfiguration.THREE_MASTERS)
                .nodeSettings(AutomatedIndexManagementSettings.Static.StateLog.ENABLED.name(), false).waitForComponents("aim").embedded().start();
        ClusterHelper.Internal.postSettingsUpdate(CLUSTER, AutomatedIndexManagementSettings.Dynamic.EXECUTION_DELAY_RANDOM_ENABLED, false);
        ClusterHelper.Internal.postSettingsUpdate(CLUSTER, AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE, false);
        Awaitility.setDefaultTimeout(60, TimeUnit.SECONDS);
    }

    @Order(1)
    @Test
    public void testPenetration() throws Exception {
        String policyName = "penetration_test_policy";
        String indexPrefix = "penetration_test_index_";

        MockSupport.MockCondition mockCondition = new MockSupport.MockCondition().setResult(true).setFail(false);
        MockSupport.MockAction mockAction = new MockSupport.MockAction().setFail(false).setRunnable(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        Policy policy = new Policy(new Policy.Step("first", ImmutableList.of(mockCondition), ImmutableList.of(mockAction)));
        int testIndexCount = AutomatedIndexManagementSettings.Static.DEFAULT_THREAD_POOL_SIZE * 2;
        for (int i = 0; i < testIndexCount; i++) {
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexPrefix + i, policyName);
        }
        List<String> indexNames = new ArrayList<>(testIndexCount);
        for (int i = 0; i < testIndexCount; i++) {
            indexNames.add(indexPrefix + i);
        }
        Awaitility.await()
                .until(() -> CLUSTER.getInternalNodeClient().admin().indices()
                        .getIndex(new GetIndexRequest().indices(indexNames.toArray(new String[] {}))).actionGet(),
                        getIndexResponse -> Arrays.asList(getIndexResponse.indices()).containsAll(indexNames));
        ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
        Awaitility.await().until(() -> mockAction.getExecutionCount() == testIndexCount);
        for (int i = 0; i < testIndexCount; i++) {
            assertEquals(1, mockCondition.getExecutionCount(indexPrefix + i),
                    "Expected every mock condition to be executed once. Failed for index '" + indexPrefix + i + "'");
            assertEquals(1, mockAction.getExecutionCount(indexPrefix + i),
                    "Expected every mock action to be executed once. Failed for index '" + indexPrefix + i + "'");
        }

        for (int i = 0; i < testIndexCount; i++) {
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexPrefix + i, PolicyInstanceState.Status.FINISHED);
        }
    }

    @Order(2)
    @Test
    public void testDeactivateActivate() throws Exception {
        String policyName = "deactivate_activate_test_policy";
        String indexName = "deactivate_activate_test_index";

        Policy policy = new Policy(
                new Policy.Step("first", ImmutableList.of(new SizeCondition(ByteSizeValue.ofGb(4))), ImmutableList.of(new SetReadOnlyAction())));
        ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
        ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
        ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

        ClusterHelper.Internal.postSettingsUpdate(CLUSTER, AutomatedIndexManagementSettings.Dynamic.ACTIVE, false);
        Awaitility.await().until(() -> ClusterHelper.Rest.getSetting(CLUSTER, AutomatedIndexManagementSettings.Dynamic.ACTIVE.getName()),
                httpResponse -> {
                    try {
                        return Boolean.FALSE.equals(DocNode.wrap(DocReader.json().read(httpResponse.getBody())).get("data"));
                    } catch (DocumentParseException e) {
                        throw new RuntimeException(e);
                    }
                });

        GenericRestClient.HttpResponse response = ClusterHelper.Rest.putPolicy(CLUSTER, "second_" + policyName, policy);
        assertEquals(503, response.getStatusCode(), response.getBody());

        response = ClusterHelper.Rest.getPolicy(CLUSTER, policyName);
        assertEquals(200, response.getStatusCode(), response.getBody());

        response = ClusterHelper.Rest.deletePolicy(CLUSTER, policyName);
        assertEquals(503, response.getStatusCode(), response.getBody());

        response = ClusterHelper.Rest.postPolicyInstanceExecute(CLUSTER, indexName);
        assertEquals(503, response.getStatusCode(), response.getBody());

        response = ClusterHelper.Rest.postPolicyInstanceRetry(CLUSTER, indexName);
        assertEquals(503, response.getStatusCode(), response.getBody());

        response = ClusterHelper.Rest.getPolicyInstanceStatus(CLUSTER, indexName);
        assertEquals(200, response.getStatusCode(), response.getBody());

        response = ClusterHelper.Rest.putSetting(CLUSTER, AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE.getName(), "false");
        assertEquals(200, response.getStatusCode(), response.getBody());

        response = ClusterHelper.Rest.deleteSetting(CLUSTER, AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE.getName());
        assertEquals(200, response.getStatusCode(), response.getBody());

        response = ClusterHelper.Rest.putSetting(CLUSTER, AutomatedIndexManagementSettings.Dynamic.ACTIVE.getName(), "true");
        assertEquals(200, response.getStatusCode(), response.getBody());

        Awaitility.await().until(() -> ClusterHelper.Rest.getSetting(CLUSTER, AutomatedIndexManagementSettings.Dynamic.ACTIVE.getName()),
                httpResponse -> {
                    try {
                        return Boolean.TRUE.equals(DocNode.wrap(DocReader.json().read(httpResponse.getBody())).get("data"));
                    } catch (DocumentParseException e) {
                        throw new RuntimeException(e);
                    }
                });

        response = ClusterHelper.Rest.postPolicyInstanceExecute(CLUSTER, indexName);
        assertEquals(200, response.getStatusCode());
    }

    @Order(3)
    @Test
    public void testMasterNodeShutdown() throws Exception {
        String policyName = "master_node_shutdown_test_policy";
        String indexName = "master_node_shutdown_test_index";

        Policy policy = new Policy(new Policy.Step("first", ImmutableList.of(new SizeCondition(ByteSizeValue.ofGb(4))), ImmutableList.empty()));
        ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
        ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
        ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

        ClusterService clusterService = CLUSTER.getInjectable(ClusterService.class);
        String masterName = Objects.requireNonNull(clusterService.state().getNodes().getMasterNode()).getName();
        CLUSTER.getNodeByName(masterName).stop();
        Thread.sleep(1000);

        ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.WAITING);
    }
}