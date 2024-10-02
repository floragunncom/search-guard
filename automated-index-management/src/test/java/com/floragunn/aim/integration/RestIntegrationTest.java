package com.floragunn.aim.integration;

import com.floragunn.aim.AutomatedIndexManagementModule;
import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.MockSupport;
import com.floragunn.aim.api.internal.InternalSettingsAPI;
import com.floragunn.aim.integration.support.ClusterHelper;
import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.actions.SnapshotAsyncAction;
import com.floragunn.aim.policy.conditions.SizeCondition;
import com.floragunn.aim.policy.conditions.SnapshotCreatedCondition;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.http.HttpStatus;
import org.awaitility.Awaitility;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.concurrent.TimeUnit;

import static com.floragunn.aim.integration.support.ClusterHelper.*;
import static org.apache.http.HttpStatus.*;
import static org.junit.jupiter.api.Assertions.*;

@Tag("IntegrationTest")
@Execution(ExecutionMode.SAME_THREAD)
public class RestIntegrationTest {
    private static final Policy VALID_INTERNAL_POLICY = new Policy(
            new Policy.Step("first", ImmutableList.of(new SizeCondition(ByteSizeValue.ofGb(4))),
                    ImmutableList.of(new SnapshotAsyncAction("test", "test"))),
            new Policy.Step(SnapshotCreatedCondition.STEP_NAME, ImmutableList.of(new SnapshotCreatedCondition("test")), ImmutableList.empty()));
    private static final Policy VALID_POLICY = new Policy(new Policy.Step("test", ImmutableList.of(new SizeCondition(ByteSizeValue.ofGb(4))),
            ImmutableList.of(new SnapshotAsyncAction("test", "test"))));
    private static final Policy INVALID_POLICY = new Policy();
    private static final String POLICY_INSTANCE_TEST_POLICY_NAME = "test_policy";
    private static final String POLICY_INSTANCE_TEST_INDEX_NAME = "test_index";
    private static LocalCluster.Embedded CLUSTER;

    @BeforeAll
    public static void setup() throws Exception {
        MockSupport.init();
        CLUSTER = new LocalCluster.Builder().sslEnabled().resources("sg_config").enableModule(AutomatedIndexManagementModule.class)
                .waitForComponents("aim").embedded().start();
        Awaitility.setDefaultTimeout(1, TimeUnit.MINUTES);

        ClusterHelper.Internal.putPolicy(CLUSTER, POLICY_INSTANCE_TEST_POLICY_NAME, VALID_INTERNAL_POLICY);
        ClusterHelper.Index.createManagedIndex(CLUSTER, POLICY_INSTANCE_TEST_INDEX_NAME, POLICY_INSTANCE_TEST_POLICY_NAME);
        ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, POLICY_INSTANCE_TEST_INDEX_NAME);
    }

    @Execution(ExecutionMode.CONCURRENT)
    @Nested
    public class RestPolicyTest {
        @Test
        public void testDeletePolicy() throws Exception {
            String policyName = "delete_test_policy";
            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, VALID_INTERNAL_POLICY);
            ClusterHelper.Index.awaitPolicyExists(CLUSTER, policyName);

            GenericRestClient.HttpResponse response = ClusterHelper.Rest.deletePolicy(CLUSTER, policyName);
            assertEquals(HttpStatus.SC_OK, response.getStatusCode(), response.getBody());
        }

        @Test
        public void testDeleteNonExistingPolicy() throws Exception {
            String policyName = "nonexistent_test_policy";

            GenericRestClient.HttpResponse response = ClusterHelper.Rest.deletePolicy(CLUSTER, policyName);
            assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode(), response.getBody());
        }

        @Test
        public void testGetPolicy() throws Exception {
            String policyName = "get_test_policy";
            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, VALID_INTERNAL_POLICY);
            ClusterHelper.Index.awaitPolicyExists(CLUSTER, policyName);

            GenericRestClient.HttpResponse response = ClusterHelper.Rest.getPolicy(CLUSTER, policyName);
            assertEquals(HttpStatus.SC_OK, response.getStatusCode(), response.getBody());
            assertFalse(response.getBody().contains("test_1"), response.getBody());
        }

        @Test
        public void testGetPolicyWithInternalStates() throws Exception {
            String policyName = "get_internal_test_policy";
            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, VALID_INTERNAL_POLICY);
            ClusterHelper.Index.awaitPolicyExists(CLUSTER, policyName);

            GenericRestClient.HttpResponse response = ClusterHelper.Rest.getPolicyInternal(CLUSTER, policyName);
            assertEquals(HttpStatus.SC_OK, response.getStatusCode(), response.getBody());
            assertTrue(response.getBody().contains(SnapshotCreatedCondition.STEP_NAME), response.getBody());
        }

        @Test
        public void testGetNonExistingPolicy() throws Exception {
            String policyName = "nonexistent_test_policy";

            GenericRestClient.HttpResponse response = ClusterHelper.Rest.getPolicy(CLUSTER, policyName);
            assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode(), response.getBody());
        }

        @Test
        public void testPutPolicy() throws Exception {
            String policyName = "put_test_policy";

            GenericRestClient.HttpResponse response = ClusterHelper.Rest.putPolicy(CLUSTER, policyName, VALID_POLICY);
            assertEquals(HttpStatus.SC_CREATED, response.getStatusCode(), response.getBody());
        }

        @Test
        public void testPutInvalidPolicy() throws Exception {
            String policyName = "invalid_test_policy";

            GenericRestClient.HttpResponse response = ClusterHelper.Rest.putPolicy(CLUSTER, policyName, INVALID_POLICY);
            assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode(), response.getBody());
        }
    }

    @Execution(ExecutionMode.CONCURRENT)
    @Nested
    public class RestPolicyInstanceTest {
        @Test
        public void testGetPolicyInstanceStatus() throws Exception {
            GenericRestClient.HttpResponse response = ClusterHelper.Rest.getPolicyInstanceStatus(CLUSTER, POLICY_INSTANCE_TEST_INDEX_NAME);
            assertEquals(HttpStatus.SC_OK, response.getStatusCode(), response.getBody());
        }

        @Test
        public void testGetPolicyInstanceStatusNonExistingInstance() throws Exception {
            GenericRestClient.HttpResponse response = ClusterHelper.Rest.getPolicyInstanceStatus(CLUSTER, "bla");
            assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode(), response.getBody());
        }

        @Test
        public void testPostPolicyInstanceExecute() throws Exception {
            GenericRestClient.HttpResponse response = ClusterHelper.Rest.postPolicyInstanceExecute(CLUSTER, POLICY_INSTANCE_TEST_INDEX_NAME);
            assertEquals(HttpStatus.SC_OK, response.getStatusCode(), response.getBody());
        }

        @Test
        public void testPostPolicyInstanceExecuteRetry() throws Exception {
            GenericRestClient.HttpResponse response = ClusterHelper.Rest.postPolicyInstanceExecuteRetry(CLUSTER, POLICY_INSTANCE_TEST_INDEX_NAME);
            assertEquals(HttpStatus.SC_OK, response.getStatusCode(), response.getBody());
        }

        @Test
        public void testPostPolicyInstanceExecuteNonExistingInstance() throws Exception {
            GenericRestClient.HttpResponse response = ClusterHelper.Rest.postPolicyInstanceExecute(CLUSTER, "bla");
            assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode(), response.getBody());
        }
    }

    @Execution(ExecutionMode.SAME_THREAD)
    @Nested
    public class RestSettingsTest {
        @Test
        public void testDeleteUnknownSetting() throws Exception {
            GenericRestClient.HttpResponse response = ClusterHelper.Rest.deleteSetting(CLUSTER, "unknown");
            assertEquals(SC_NOT_FOUND, response.getStatusCode(), response.getBody());
            assertEquals("Unknown setting", response.getBodyAsDocNode().get("message"), response.getBody());
        }

        @Test
        public void testGetUnknownSetting() throws Exception {
            GenericRestClient.HttpResponse response = ClusterHelper.Rest.getSetting(CLUSTER, "unknown");
            assertEquals(SC_NOT_FOUND, response.getStatusCode(), response.getBody());
            assertEquals("Unknown setting", response.getBodyAsDocNode().get("message"), response.getBody());
        }

        @Test
        public void testPutUnknownSetting() throws Exception {
            GenericRestClient.HttpResponse response = ClusterHelper.Rest.putSetting(CLUSTER, "unknown", "\"1s\"");
            assertEquals(SC_NOT_FOUND, response.getStatusCode(), response.getBody());
            assertEquals("Unknown setting", response.getBodyAsDocNode().get("message"), response.getBody());
        }

        @Test
        public void testDeleteSetting() throws Exception {
            ClusterHelper.Internal.postSettingsUpdate(CLUSTER, AutomatedIndexManagementSettings.Dynamic.EXECUTION_DELAY,
                    TimeValue.timeValueSeconds(1));

            GenericRestClient.HttpResponse response = ClusterHelper.Rest.deleteSetting(CLUSTER, "execution.delay");
            assertEquals(SC_OK, response.getStatusCode(), response.getBody());
        }

        @Test
        public void testGetSetting() throws Exception {
            ClusterHelper.Internal.postSettingsUpdate(CLUSTER, AutomatedIndexManagementSettings.Dynamic.EXECUTION_DELAY,
                    TimeValue.timeValueSeconds(1));

            GenericRestClient.HttpResponse response = ClusterHelper.Rest.getSetting(CLUSTER, "execution.delay");
            assertEquals(SC_OK, response.getStatusCode(), response.getBody());
            assertEquals("1s", response.getBodyAsDocNode().get("data"));

            InternalSettingsAPI.Update.Response deleteResponse = ClusterHelper.Internal.postSettingsDelete(CLUSTER,
                    AutomatedIndexManagementSettings.Dynamic.EXECUTION_DELAY);
            assertFalse(deleteResponse.hasFailedAttributes(), "Attribute delete failed");
            assertFalse(deleteResponse.hasRefreshFailures(), "Refresh failed");
        }

        @Test
        public void testPutSetting() throws Exception {
            GenericRestClient.HttpResponse response = ClusterHelper.Rest.putSetting(CLUSTER, "execution.delay", "\"1s\"");
            assertEquals(SC_OK, response.getStatusCode(), response.getBody());

            InternalSettingsAPI.Update.Response deleteResponse = ClusterHelper.Internal.postSettingsDelete(CLUSTER,
                    AutomatedIndexManagementSettings.Dynamic.EXECUTION_DELAY);
            assertFalse(deleteResponse.hasFailedAttributes(), "Attribute delete failed");
            assertFalse(deleteResponse.hasRefreshFailures(), "Refresh failed");
        }
    }

    @Execution(ExecutionMode.CONCURRENT)
    @Nested
    public class RestStaticActionGroupPermissionsTest {
        @Test
        public void testSGS_AIM_ALL() throws Exception {
            String policyName = "aim-all";
            String policyPath = "/_aim/policy/" + policyName;
            String settingPath = "/_aim/settings/execution.delay";

            GenericRestClient.HttpResponse response = CLUSTER.getRestClient(AIM_ALL_AUTH).putJson(policyPath, VALID_POLICY);
            assertEquals(SC_CREATED, response.getStatusCode(), response.getBody());
            ClusterHelper.Index.awaitPolicyExists(CLUSTER, policyName);

            response = CLUSTER.getRestClient(AIM_ALL_AUTH).get(policyPath);
            assertEquals(SC_OK, response.getStatusCode(), response.getBody());

            response = CLUSTER.getRestClient(AIM_ALL_AUTH).get(policyPath + "/internal");
            assertEquals(SC_OK, response.getStatusCode(), response.getBody());
            assertTrue(response.getBody().contains(SnapshotCreatedCondition.STEP_NAME), response.getBody());

            response = CLUSTER.getRestClient(AIM_ALL_AUTH).delete(policyPath);
            assertEquals(SC_OK, response.getStatusCode(), response.getBody());
            Awaitility.await().until(() -> !ClusterHelper.Index.isPolicyExists(CLUSTER, policyName));

            response = CLUSTER.getRestClient(AIM_ALL_AUTH).get(settingPath);
            assertEquals(SC_OK, response.getStatusCode(), response.getBody());
        }

        @Test
        public void testSGS_AIM_POLICY_READ() throws Exception {
            String policyName = "aim-policy-read";
            String policyPath = "/_aim/policy/" + policyName;
            String settingPath = "/_aim/settings/execution.delay";

            GenericRestClient.HttpResponse response = CLUSTER.getRestClient(AIM_POLICY_READ_AUTH).putJson(policyPath, VALID_POLICY);
            assertEquals(SC_FORBIDDEN, response.getStatusCode(), response.getBody());

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, VALID_INTERNAL_POLICY);
            ClusterHelper.Index.awaitPolicyExists(CLUSTER, policyName);

            response = CLUSTER.getRestClient(AIM_POLICY_READ_AUTH).get(policyPath);
            assertEquals(SC_OK, response.getStatusCode(), response.getBody());

            response = CLUSTER.getRestClient(AIM_POLICY_READ_AUTH).get(policyPath + "/internal");
            assertEquals(SC_OK, response.getStatusCode(), response.getBody());
            assertTrue(response.getBody().contains(SnapshotCreatedCondition.STEP_NAME), response.getBody());

            response = CLUSTER.getRestClient(AIM_POLICY_READ_AUTH).delete(policyPath);
            assertEquals(SC_FORBIDDEN, response.getStatusCode(), response.getBody());

            response = CLUSTER.getRestClient(AIM_POLICY_READ_AUTH).get(settingPath);
            assertEquals(SC_FORBIDDEN, response.getStatusCode(), response.getBody());
        }

        @Test
        public void testSGS_AIM_POLICY_MANAGE() throws Exception {
            String policyName = "aim-policy-manage";
            String policyPath = "/_aim/policy/" + policyName;
            String settingPath = "/_aim/settings/execution.delay";

            GenericRestClient.HttpResponse response = CLUSTER.getRestClient(AIM_POLICY_MANAGE_AUTH).putJson(policyPath, VALID_POLICY);
            assertEquals(SC_CREATED, response.getStatusCode(), response.getBody());
            ClusterHelper.Index.awaitPolicyExists(CLUSTER, policyName);

            response = CLUSTER.getRestClient(AIM_POLICY_MANAGE_AUTH).get(policyPath);
            assertEquals(SC_OK, response.getStatusCode(), response.getBody());

            response = CLUSTER.getRestClient(AIM_POLICY_MANAGE_AUTH).get(policyPath + "/internal");
            assertEquals(SC_OK, response.getStatusCode(), response.getBody());
            assertTrue(response.getBody().contains(SnapshotCreatedCondition.STEP_NAME), response.getBody());

            response = CLUSTER.getRestClient(AIM_POLICY_MANAGE_AUTH).delete(policyPath);
            assertEquals(SC_OK, response.getStatusCode(), response.getBody());
            Awaitility.await().until(() -> !ClusterHelper.Index.isPolicyExists(CLUSTER, policyName));

            response = CLUSTER.getRestClient(AIM_POLICY_MANAGE_AUTH).get(settingPath);
            assertEquals(SC_FORBIDDEN, response.getStatusCode(), response.getBody());
        }

        @Test
        public void testSGS_AIM_POLICY_INSTANCE_READ() throws Exception {
            String policyName = "aim-policy-instance-read";
            String policyPath = "/_aim/policy/" + policyName;
            String indexName = "aim-policy-instance-read";
            String statusPath = "/_aim/state/" + indexName;
            String settingPath = "/_aim/settings/execution.delay";

            GenericRestClient.HttpResponse response = CLUSTER.getRestClient(AIM_POLICY_INSTANCE_READ_AUTH).putJson(policyPath, VALID_POLICY);
            assertEquals(SC_FORBIDDEN, response.getStatusCode());

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, VALID_POLICY);
            ClusterHelper.Index.awaitPolicyExists(CLUSTER, policyName);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            response = CLUSTER.getRestClient(AIM_POLICY_INSTANCE_READ_AUTH).get(statusPath);
            assertEquals(SC_OK, response.getStatusCode());

            response = CLUSTER.getRestClient(AIM_POLICY_INSTANCE_READ_AUTH).post("/_aim/execute/" + indexName);
            assertEquals(SC_FORBIDDEN, response.getStatusCode());

            response = CLUSTER.getRestClient(AIM_POLICY_INSTANCE_READ_AUTH).post("/_aim/execute/" + indexName + "/true");
            assertEquals(SC_FORBIDDEN, response.getStatusCode());

            response = CLUSTER.getRestClient(AIM_POLICY_INSTANCE_READ_AUTH).post("/_aim/retry/" + indexName);
            assertEquals(SC_FORBIDDEN, response.getStatusCode());

            response = CLUSTER.getRestClient(AIM_POLICY_INSTANCE_READ_AUTH).get(settingPath);
            assertEquals(SC_FORBIDDEN, response.getStatusCode(), response.getBody());
        }

        @Test
        public void testSGS_AIM_POLICY_INSTANCE_MANAGE() throws Exception {
            String policyName = "aim-policy-instance-manage";
            String policyPath = "/_aim/policy/" + policyName;
            String indexName = "aim-policy-instance-manage";
            String statusPath = "/_aim/state/" + indexName;
            String executePath = "/_aim/execute/" + indexName;
            String retryPath = "/_aim/retry/" + indexName;
            String settingPath = "/_aim/settings/execution.delay";

            GenericRestClient.HttpResponse response = CLUSTER.getRestClient(AIM_POLICY_INSTANCE_MANAGE_AUTH).putJson(policyPath, VALID_POLICY);
            assertEquals(SC_FORBIDDEN, response.getStatusCode(), response.getBody());

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, VALID_POLICY);
            ClusterHelper.Index.awaitPolicyExists(CLUSTER, policyName);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            response = CLUSTER.getRestClient(AIM_POLICY_INSTANCE_MANAGE_AUTH).get(statusPath);
            assertEquals(SC_OK, response.getStatusCode());

            response = CLUSTER.getRestClient(AIM_POLICY_INSTANCE_MANAGE_AUTH).post(executePath);
            assertEquals(SC_OK, response.getStatusCode());

            response = CLUSTER.getRestClient(AIM_POLICY_INSTANCE_MANAGE_AUTH).post(executePath + "/true");
            assertEquals(SC_OK, response.getStatusCode());

            response = CLUSTER.getRestClient(AIM_POLICY_INSTANCE_MANAGE_AUTH).post(retryPath);
            assertEquals(SC_OK, response.getStatusCode());

            response = CLUSTER.getRestClient(AIM_POLICY_INSTANCE_MANAGE_AUTH).get(settingPath);
            assertEquals(SC_FORBIDDEN, response.getStatusCode(), response.getBody());
        }
    }
}
