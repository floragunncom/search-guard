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
import com.floragunn.searchguard.test.RestMatchers;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.awaitility.Awaitility;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.concurrent.TimeUnit;

import static com.floragunn.aim.integration.support.ClusterHelper.*;
import static com.floragunn.searchguard.test.RestMatchers.distinctNodesAt;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
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
            assertThat(response, RestMatchers.isOk());
        }

        @Test
        public void testDeleteNonExistingPolicy() throws Exception {
            String policyName = "nonexistent_test_policy";

            GenericRestClient.HttpResponse response = ClusterHelper.Rest.deletePolicy(CLUSTER, policyName);
            assertThat(response, RestMatchers.isNotFound());
        }

        @Test
        public void testGetPolicy() throws Exception {
            String policyName = "get_test_policy";
            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, VALID_INTERNAL_POLICY);
            ClusterHelper.Index.awaitPolicyExists(CLUSTER, policyName);

            GenericRestClient.HttpResponse response = ClusterHelper.Rest.getPolicy(CLUSTER, policyName);
            assertThat(response, RestMatchers.isOk());
            assertFalse(response.getBody().contains("test_1"), response.getBody());
        }

        @Test
        public void testGetPolicyWithInternalStates() throws Exception {
            String policyName = "get_internal_test_policy";
            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, VALID_INTERNAL_POLICY);
            ClusterHelper.Index.awaitPolicyExists(CLUSTER, policyName);

            GenericRestClient.HttpResponse response = ClusterHelper.Rest.getPolicyInternal(CLUSTER, policyName);
            assertThat(response, RestMatchers.isOk());
            assertTrue(response.getBody().contains(SnapshotCreatedCondition.STEP_NAME), response.getBody());
        }

        @Test
        public void testGetNonExistingPolicy() throws Exception {
            String policyName = "nonexistent_test_policy";

            GenericRestClient.HttpResponse response = ClusterHelper.Rest.getPolicy(CLUSTER, policyName);
            assertThat(response, RestMatchers.isNotFound());
        }

        @Test
        public void testPutPolicy() throws Exception {
            String policyName = "put_test_policy";

            GenericRestClient.HttpResponse response = Rest.putPolicy(CLUSTER, policyName, VALID_POLICY);
            assertThat(response, RestMatchers.isCreated());
        }

        @Test
        public void testPutInvalidPolicy() throws Exception {
            String policyName = "invalid_test_policy";

            GenericRestClient.HttpResponse response = Rest.putPolicy(CLUSTER, policyName, INVALID_POLICY);
            assertThat(response, RestMatchers.isBadRequest());
        }
    }

    @Execution(ExecutionMode.CONCURRENT)
    @Nested
    public class RestPolicyInstanceTest {
        @Test
        public void testGetPolicyInstanceStatus() throws Exception {
            GenericRestClient.HttpResponse response = ClusterHelper.Rest.getPolicyInstanceStatus(CLUSTER, POLICY_INSTANCE_TEST_INDEX_NAME);
            assertThat(response, RestMatchers.isOk());
        }

        @Test
        public void testGetPolicyInstanceStatusNonExistingInstance() throws Exception {
            GenericRestClient.HttpResponse response = ClusterHelper.Rest.getPolicyInstanceStatus(CLUSTER, "bla");
            assertThat(response, RestMatchers.isNotFound());
        }

        @Test
        public void testPostPolicyInstanceExecute() throws Exception {
            GenericRestClient.HttpResponse response = ClusterHelper.Rest.postPolicyInstanceExecute(CLUSTER, POLICY_INSTANCE_TEST_INDEX_NAME);
            assertThat(response, RestMatchers.isOk());
        }

        @Test
        public void testPostPolicyInstanceExecuteRetry() throws Exception {
            GenericRestClient.HttpResponse response = ClusterHelper.Rest.postPolicyInstanceExecuteRetry(CLUSTER, POLICY_INSTANCE_TEST_INDEX_NAME);
            assertThat(response, RestMatchers.isOk());
        }

        @Test
        public void testPostPolicyInstanceExecuteNonExistingInstance() throws Exception {
            GenericRestClient.HttpResponse response = ClusterHelper.Rest.postPolicyInstanceExecute(CLUSTER, "bla");
            assertThat(response, RestMatchers.isNotFound());
        }
    }

    @Execution(ExecutionMode.SAME_THREAD)
    @Nested
    public class RestSettingsTest {
        @Test
        public void testDeleteUnknownSetting() throws Exception {
            GenericRestClient.HttpResponse response = ClusterHelper.Rest.deleteSetting(CLUSTER, "unknown");
            assertThat(response, RestMatchers.isNotFound());
            assertThat(response, json(distinctNodesAt("message", is("Unknown setting"))));
        }

        @Test
        public void testGetUnknownSetting() throws Exception {
            GenericRestClient.HttpResponse response = ClusterHelper.Rest.getSetting(CLUSTER, "unknown");
            assertThat(response, RestMatchers.isNotFound());
            assertThat(response, json(distinctNodesAt("message", is("Unknown setting"))));
        }

        @Test
        public void testPutUnknownSetting() throws Exception {
            GenericRestClient.HttpResponse response = ClusterHelper.Rest.putSetting(CLUSTER, "unknown", "\"1s\"");
            assertThat(response, RestMatchers.isNotFound());
            assertThat(response, json(distinctNodesAt("message", is("Unknown setting"))));
        }

        @Test
        public void testDeleteSetting() throws Exception {
            ClusterHelper.Internal.postSettingsUpdate(CLUSTER, AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE, false);

            GenericRestClient.HttpResponse response = ClusterHelper.Rest.deleteSetting(CLUSTER,
                    AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE.getName());
            assertThat(response, RestMatchers.isOk());
        }

        @Test
        public void testGetSetting() throws Exception {
            ClusterHelper.Internal.postSettingsUpdate(CLUSTER, AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE, true);

            GenericRestClient.HttpResponse response = ClusterHelper.Rest.getSetting(CLUSTER,
                    AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE.getName());
            assertThat(response, RestMatchers.isOk());
            assertThat(response, json(distinctNodesAt("data", is(true))));

            InternalSettingsAPI.Update.Response deleteResponse = ClusterHelper.Internal.postSettingsDelete(CLUSTER,
                    AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE);
            assertFalse(deleteResponse.hasFailedAttributes(), "Attribute delete failed");
            assertFalse(deleteResponse.hasRefreshFailures(), "Refresh failed");
        }

        @Test
        public void testPutSetting() throws Exception {
            GenericRestClient.HttpResponse response = ClusterHelper.Rest.putSetting(CLUSTER,
                    AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE.getName(), "true");
            assertThat(response, RestMatchers.isOk());

            InternalSettingsAPI.Update.Response deleteResponse = ClusterHelper.Internal.postSettingsDelete(CLUSTER,
                    AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE);
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

            GenericRestClient.HttpResponse response = Rest.putPolicy(CLUSTER, AIM_ALL_AUTH, policyName, VALID_POLICY);
            assertThat(response, RestMatchers.isCreated());
            ClusterHelper.Index.awaitPolicyExists(CLUSTER, policyName);

            response = Rest.getPolicy(CLUSTER, AIM_ALL_AUTH, policyName);
            assertThat(response, RestMatchers.isOk());

            response = Rest.getPolicyInternal(CLUSTER, AIM_ALL_AUTH, policyName);
            assertThat(response, RestMatchers.isOk());
            assertTrue(response.getBody().contains(SnapshotCreatedCondition.STEP_NAME), response.getBody());

            response = Rest.deletePolicy(CLUSTER, AIM_ALL_AUTH, policyName);
            assertThat(response, RestMatchers.isOk());
            Awaitility.await().until(() -> !ClusterHelper.Index.isPolicyExists(CLUSTER, policyName));

            response = Rest.getSetting(CLUSTER, AIM_ALL_AUTH, AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE.getName());
            assertThat(response, RestMatchers.isOk());
        }

        @Test
        public void testSGS_AIM_POLICY_READ() throws Exception {
            String policyName = "aim-policy-read";

            GenericRestClient.HttpResponse response = Rest.putPolicy(CLUSTER, AIM_POLICY_READ_AUTH, policyName, VALID_POLICY);
            assertThat(response, RestMatchers.isForbidden());

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, VALID_INTERNAL_POLICY);
            ClusterHelper.Index.awaitPolicyExists(CLUSTER, policyName);

            response = Rest.getPolicy(CLUSTER, AIM_POLICY_READ_AUTH, policyName);
            assertThat(response, RestMatchers.isOk());

            response = Rest.getPolicyInternal(CLUSTER, AIM_POLICY_READ_AUTH, policyName);
            assertThat(response, RestMatchers.isOk());
            assertTrue(response.getBody().contains(SnapshotCreatedCondition.STEP_NAME), response.getBody());

            response = Rest.deletePolicy(CLUSTER, AIM_POLICY_READ_AUTH, policyName);
            assertThat(response, RestMatchers.isForbidden());

            response = Rest.getSetting(CLUSTER, AIM_POLICY_READ_AUTH, AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE.getName());
            assertThat(response, RestMatchers.isForbidden());
        }

        @Test
        public void testSGS_AIM_POLICY_MANAGE() throws Exception {
            String policyName = "aim-policy-manage";

            GenericRestClient.HttpResponse response = Rest.putPolicy(CLUSTER, AIM_POLICY_MANAGE_AUTH, policyName, VALID_POLICY);
            assertThat(response, RestMatchers.isCreated());
            ClusterHelper.Index.awaitPolicyExists(CLUSTER, policyName);

            response = Rest.getPolicy(CLUSTER, AIM_POLICY_MANAGE_AUTH, policyName);
            assertThat(response, RestMatchers.isOk());

            response = Rest.getPolicyInternal(CLUSTER, AIM_POLICY_MANAGE_AUTH, policyName);
            assertThat(response, RestMatchers.isOk());
            assertTrue(response.getBody().contains(SnapshotCreatedCondition.STEP_NAME), response.getBody());

            response = Rest.deletePolicy(CLUSTER, AIM_POLICY_MANAGE_AUTH, policyName);
            assertThat(response, RestMatchers.isOk());
            Awaitility.await().until(() -> !ClusterHelper.Index.isPolicyExists(CLUSTER, policyName));

            response = Rest.getSetting(CLUSTER, AIM_POLICY_MANAGE_AUTH, AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE.getName());
            assertThat(response, RestMatchers.isForbidden());
        }

        @Test
        public void testSGS_AIM_POLICY_INSTANCE_READ() throws Exception {
            String policyName = "aim-policy-instance-read";
            String indexName = "aim-policy-instance-read";

            GenericRestClient.HttpResponse response = Rest.putPolicy(CLUSTER, AIM_POLICY_INSTANCE_READ_AUTH, policyName, VALID_POLICY);
            assertThat(response, RestMatchers.isForbidden());

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, VALID_POLICY);
            ClusterHelper.Index.awaitPolicyExists(CLUSTER, policyName);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            response = Rest.getPolicyInstanceStatus(CLUSTER, AIM_POLICY_INSTANCE_READ_AUTH, indexName);
            assertThat(response, RestMatchers.isOk());

            response = Rest.postPolicyInstanceExecute(CLUSTER, AIM_POLICY_INSTANCE_READ_AUTH, indexName);
            assertThat(response, RestMatchers.isForbidden());

            response = Rest.postPolicyInstanceExecuteRetry(CLUSTER, AIM_POLICY_INSTANCE_READ_AUTH, indexName);
            assertThat(response, RestMatchers.isForbidden());

            response = Rest.postPolicyInstanceRetry(CLUSTER, AIM_POLICY_INSTANCE_READ_AUTH, indexName);
            assertThat(response, RestMatchers.isForbidden());

            response = Rest.getSetting(CLUSTER, AIM_POLICY_INSTANCE_READ_AUTH, AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE.getName());
            assertThat(response, RestMatchers.isForbidden());
        }

        @Test
        public void testSGS_AIM_POLICY_INSTANCE_MANAGE() throws Exception {
            String policyName = "aim-policy-instance-manage";
            String indexName = "aim-policy-instance-manage";

            GenericRestClient.HttpResponse response = Rest.putPolicy(CLUSTER, AIM_POLICY_INSTANCE_MANAGE_AUTH, policyName, VALID_POLICY);
            assertThat(response, RestMatchers.isForbidden());

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, VALID_POLICY);
            ClusterHelper.Index.awaitPolicyExists(CLUSTER, policyName);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            response = Rest.getPolicyInstanceStatus(CLUSTER, AIM_POLICY_INSTANCE_MANAGE_AUTH, indexName);
            assertThat(response, RestMatchers.isOk());

            response = Rest.postPolicyInstanceExecute(CLUSTER, AIM_POLICY_INSTANCE_MANAGE_AUTH, indexName);
            assertThat(response, RestMatchers.isOk());

            response = Rest.postPolicyInstanceExecuteRetry(CLUSTER, AIM_POLICY_INSTANCE_MANAGE_AUTH, indexName);
            assertThat(response, RestMatchers.isOk());

            response = Rest.postPolicyInstanceRetry(CLUSTER, AIM_POLICY_INSTANCE_MANAGE_AUTH, indexName);
            assertThat(response, RestMatchers.isOk());

            response = Rest.getSetting(CLUSTER, AIM_POLICY_INSTANCE_MANAGE_AUTH, AutomatedIndexManagementSettings.Dynamic.STATE_LOG_ACTIVE.getName());
            assertThat(response, RestMatchers.isForbidden());
        }
    }
}
