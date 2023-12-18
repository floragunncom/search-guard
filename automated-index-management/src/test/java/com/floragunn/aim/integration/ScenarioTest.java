package com.floragunn.aim.integration;

import com.floragunn.aim.AutomatedIndexManagementModule;
import com.floragunn.aim.AutomatedIndexManagementSettings;
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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("IntegrationTest")
@Execution(ExecutionMode.SAME_THREAD)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ScenarioTest {
    private static LocalCluster CLUSTER;

    @BeforeAll
    public static void setup() throws Exception {
        CLUSTER = new LocalCluster.Builder().sslEnabled().resources("sg_config").enableModule(AutomatedIndexManagementModule.class)
                .clusterConfiguration(ClusterConfiguration.THREE_MASTERS)
                .nodeSettings(AutomatedIndexManagementSettings.Static.StateLog.ENABLED.name(), false).start();
        ClusterHelper.Internal.postSettingsUpdate(CLUSTER, AutomatedIndexManagementSettings.Dynamic.EXECUTION_DELAY_RANDOM_ENABLED, false);
        ClusterHelper.Internal.postSettingsUpdate(CLUSTER, AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE, false);
        Awaitility.setDefaultTimeout(60, TimeUnit.SECONDS);
    }

    @Order(1)
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

    @Order(2)
    @Test
    public void testMasterNodeShutdown() {
        String policyName = "master_node_shutdown_test_policy";
        String indexName = "master_node_shutdown_test_index";

        Policy policy = new Policy(new Policy.Step("first", ImmutableList.of(new SizeCondition(ByteSizeValue.ofGb(4))), ImmutableList.empty()));
        ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
        ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
        ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

        ClusterService clusterService = CLUSTER.getInjectable(ClusterService.class);
        String masterName = clusterService.state().getNodes().getMasterNode().getName();
        CLUSTER.getNodeByName(masterName).stop();

        ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.WAITING);
    }
}
