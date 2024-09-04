package com.floragunn.aim.integration;

import com.floragunn.aim.AutomatedIndexManagementModule;
import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.MockSupport;
import com.floragunn.aim.api.internal.InternalPolicyInstanceAPI;
import com.floragunn.aim.integration.support.ClusterHelper;
import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.actions.*;
import com.floragunn.aim.policy.conditions.*;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.aim.policy.instance.PolicyInstanceStateLogHandler;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.Format;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.http.HttpStatus;
import org.awaitility.Awaitility;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.floragunn.aim.integration.support.ClusterHelper.DEFAULT_AUTH;
import static org.elasticsearch.cluster.metadata.IndexMetadata.*;
import static org.junit.jupiter.api.Assertions.*;

@Tag("IntegrationTest")
@Execution(ExecutionMode.SAME_THREAD)
public class PolicyInstanceIntegrationTest {
    @TempDir
    protected static Path SNAPSHOT_REPO_PATH;
    private static LocalCluster CLUSTER;

    private static void setupRepository(String repositoryName) {
        Settings.Builder builder = Settings.builder().put("location", SNAPSHOT_REPO_PATH.toAbsolutePath());
        PutRepositoryRequest request = new PutRepositoryRequest(repositoryName).type("fs").settings(builder);
        AcknowledgedResponse response = CLUSTER.getInternalNodeClient().admin().cluster().execute(PutRepositoryAction.INSTANCE, request).actionGet();
        assertTrue(response.isAcknowledged(), Strings.toString(response, true, true));
    }

    private static void deleteRepository(String repositoryName) {
        DeleteRepositoryRequest deleteRepositoryRequest = new DeleteRepositoryRequest(repositoryName);
        AcknowledgedResponse deleteRepositoryResponse = CLUSTER.getInternalNodeClient().admin().cluster()
                .execute(DeleteRepositoryAction.INSTANCE, deleteRepositoryRequest).actionGet();
        assertTrue(deleteRepositoryResponse.isAcknowledged(), Strings.toString(deleteRepositoryResponse, true, true));
    }

    @BeforeAll
    public static void setup() {
        CLUSTER = new LocalCluster.Builder().sslEnabled().resources("sg_config").enableModule(AutomatedIndexManagementModule.class)
                .nodeSettings("path.repo", SNAPSHOT_REPO_PATH.toAbsolutePath()).start();
        MockSupport.init(CLUSTER);
        Awaitility.setDefaultTimeout(30, TimeUnit.SECONDS);
        ClusterHelper.Internal.postSettingsUpdate(CLUSTER, AutomatedIndexManagementSettings.Dynamic.EXECUTION_DELAY, TimeValue.timeValueHours(1));
    }

    @Execution(ExecutionMode.CONCURRENT)
    @Test
    public void testInstanceCreation() {
        String policyName = "instance_creation_test_policy";
        String indexName = "instance_creation_test_index";
        Policy policy = new Policy(
                new Policy.Step("first", ImmutableList.of(new MockSupport.MockCondition().setResult(false)), ImmutableList.empty()));

        ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
        ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
        ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);
        ClusterHelper.Index.assertStatus(CLUSTER, indexName, PolicyInstanceState.Status.NOT_STARTED);

        ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
        ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.WAITING);
    }

    @Test
    public void testInstanceCreationNoPolicy() {
        String policyName = "instance_creation_no_policy_test_policy";
        String indexName = "instance_creation_no_policy_test_index";
        Policy policy = new Policy(
                new Policy.Step("first", ImmutableList.of(new MockSupport.MockCondition().setResult(false)), ImmutableList.empty()));

        CreateIndexResponse response = ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
        assertTrue(response.isAcknowledged(), response.toString());
        Awaitility.await().until(
                () -> CLUSTER.getInternalNodeClient().admin().indices().getIndex(new GetIndexRequest().indices(indexName)).actionGet(),
                getIndexResponse -> Arrays.asList(getIndexResponse.indices()).contains(indexName));
        assertFalse(ClusterHelper.Index.isPolicyInstanceStatusExists(CLUSTER, indexName));

        ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
        ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);
    }

    @Test
    public void testExecuteRetry() throws Exception {
        String policyName = "execute_retry_test_policy";
        String indexName = "execute_retry_test_index";

        MockSupport.MockAction mockAction = new MockSupport.MockAction().setFail(true);
        Policy policy = new Policy(new Policy.Step("first", ImmutableList.empty(), ImmutableList.of(mockAction)));

        ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
        ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
        ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

        ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
        ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FAILED);
        assertEquals(1, mockAction.getExecutionCount(), "Expected one execution for action");

        ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
        ClusterHelper.Index.assertStatus(CLUSTER, indexName, PolicyInstanceState.Status.FAILED);

        mockAction.setFail(false);

        InternalPolicyInstanceAPI.PostExecuteRetry.Response response = ClusterHelper.Internal.postPolicyInstanceExecuteRetry(CLUSTER, indexName, true,
                true);
        assertTrue(response.isExists());

        ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FINISHED);
        assertEquals(2, mockAction.getExecutionCount());
    }

    @Test
    public void testRetry() throws Exception {
        String policyName = "retry_test_policy";
        String indexName = "retry_test_index";

        MockSupport.MockAction mockAction = new MockSupport.MockAction().setFail(true);
        Policy policy = new Policy(new Policy.Step("first", ImmutableList.empty(), ImmutableList.of(mockAction)));

        ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
        ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
        ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

        ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
        ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FAILED);
        assertEquals(1, mockAction.getExecutionCount());

        ClusterHelper.Internal.postPolicyInstanceExecuteRetry(CLUSTER, indexName, true, true);
        Awaitility.await().until(mockAction::getExecutionCount, integer -> integer.equals(2));

        ClusterHelper.Index.assertStatus(CLUSTER, indexName, PolicyInstanceState.Status.FAILED);
        GetResponse statusResponse = ClusterHelper.Index.getPolicyInstanceStatus(CLUSTER, indexName);
        DocNode statusNode = DocNode.wrap(DocReader.json().readObject(statusResponse.getSourceAsString()));
        assertEquals(1, statusNode.get(PolicyInstanceState.LAST_EXECUTED_ACTION_FIELD, PolicyInstanceState.ActionState.RETRIES_FIELD),
                "Expected one retry: " + statusNode.toPrettyJsonString());

        ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
        assertEquals(2, mockAction.getExecutionCount());

        mockAction.setFail(false);

        InternalPolicyInstanceAPI.PostExecuteRetry.Response response = ClusterHelper.Internal.postPolicyInstanceExecuteRetry(CLUSTER, indexName,
                false, true);
        assertTrue(response.isExists());

        ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);

        ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FINISHED);
        statusResponse = ClusterHelper.Index.getPolicyInstanceStatus(CLUSTER, indexName);
        statusNode = DocNode.wrap(DocReader.json().readObject(statusResponse.getSourceAsString()));
        assertEquals(2, statusNode.get(PolicyInstanceState.LAST_EXECUTED_STEP_FIELD, PolicyInstanceState.StepState.RETRY_COUNT_FIELD),
                "Expected two retries");
        assertEquals(3, mockAction.getExecutionCount());
    }

    @Execution(ExecutionMode.SAME_THREAD)
    @Nested
    public class StateLogTest {
        @Test
        public void testStateLogEntry() throws Exception {
            String policyName = "state_log_entry_test_policy";
            String indexName = "state_log_entry_test_index";
            MockSupport.MockAction mockAction = new MockSupport.MockAction().setFail(true);
            MockSupport.MockCondition mockCondition = new MockSupport.MockCondition().setResult(false);
            Policy policy = new Policy(new Policy.Step("first", ImmutableList.empty(), ImmutableList.of(mockAction)),
                    new Policy.Step("second", ImmutableList.of(mockCondition), ImmutableList.of(new DeleteAction())));

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            SearchRequest statesLogSearchRequest = new SearchRequest(AutomatedIndexManagementSettings.Static.StateLog.DEFAULT_ALIAS_NAME).source(
                    new SearchSourceBuilder().query(QueryBuilders.termQuery(PolicyInstanceStateLogHandler.StateLogEntry.INDEX_FIELD, indexName)));
            SearchResponse searchResponse = CLUSTER.getInternalNodeClient().search(statesLogSearchRequest).actionGet();
            assertEquals(0, searchResponse.getHits().getTotalHits().value, Strings.toString(searchResponse, true, true));

            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FAILED);

            Awaitility.await().until(() -> CLUSTER.getInternalNodeClient().search(statesLogSearchRequest).actionGet(),
                    searchResponse1 -> Long.valueOf(1).equals(searchResponse1.getHits().getTotalHits().value));

            mockAction.setFail(false);
            ClusterHelper.Internal.postPolicyInstanceExecuteRetry(CLUSTER, indexName, true, true);

            Thread.sleep(1000);
            searchResponse = CLUSTER.getInternalNodeClient().search(statesLogSearchRequest).actionGet();
            assertEquals(1, searchResponse.getHits().getTotalHits().value, Strings.toString(searchResponse, true, true));

            mockCondition.setResult(true);
            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            Awaitility.await().until(() -> CLUSTER.getInternalNodeClient().search(statesLogSearchRequest).actionGet(),
                    searchResponse1 -> Long.valueOf(3).equals(searchResponse1.getHits().getTotalHits().value));
        }

        @Test
        public void testStateLogPolicy() throws Exception {
            String indexPrefix = AutomatedIndexManagementSettings.Static.StateLog.DEFAULT_INDEX_NAME_PREFIX;
            String alias = AutomatedIndexManagementSettings.Static.StateLog.DEFAULT_ALIAS_NAME;
            String writeAlias = PolicyInstanceStateLogHandler.getWriteAliasName(alias);

            String indexName = indexPrefix + "-000001";
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);
            IndexResponse response = CLUSTER.getInternalNodeClient().index(new IndexRequest(writeAlias).source(ImmutableMap.of("first", "entry")))
                    .actionGet();
            assertEquals(RestStatus.CREATED, response.status(), Strings.toString(response, true, true));

            Awaitility
                    .await().until(
                            () -> CLUSTER.getInternalNodeClient()
                                    .search(new SearchRequest(alias)
                                            .source(new SearchSourceBuilder().query(QueryBuilders.termQuery("first", "entry"))))
                                    .actionGet(),
                            searchResponse -> searchResponse.getHits().getTotalHits().value == 1);

            MockSupport.STATE_LOG_ROLLOVER_DOC_COUNT.setResult(indexName, true);
            InternalPolicyInstanceAPI.PostExecuteRetry.Response executeResponse = ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER,
                    indexName);
            assertTrue(executeResponse.isExists());

            Awaitility.await().until(() -> ClusterHelper.Index.getPolicyInstanceStatus(CLUSTER, indexName),
                    statusResponse -> "delete".equals(statusResponse.getSource().get(PolicyInstanceState.CURRENT_STEP_FIELD)));

            String newIndexName = indexPrefix + "-000002";
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, newIndexName);

            response = CLUSTER.getInternalNodeClient().prepareIndex(writeAlias).setSource(ImmutableMap.of("second", "entry")).execute().actionGet();
            assertEquals(RestStatus.CREATED, response.status(), Strings.toString(response, true, true));

            Awaitility
                    .await().until(
                            () -> CLUSTER.getInternalNodeClient()
                                    .search(new SearchRequest(alias)
                                            .source(new SearchSourceBuilder().query(QueryBuilders.termQuery("second", "entry"))))
                                    .actionGet(),
                            searchResponse -> searchResponse.getHits().getTotalHits().value == 1);
        }
    }

    @Execution(ExecutionMode.CONCURRENT)
    @Nested
    public class ConditionTest {
        @Test
        public void testForceMergeDoneConditionExecution() throws Exception {
            ForceMergeDoneCondition condition = new ForceMergeDoneCondition(1);
            Policy policy = new Policy(new Policy.Step("first", ImmutableList.of(condition), ImmutableList.empty()));
            String policyName = condition.getType() + "_condition_test_policy";
            String indexName = condition.getType() + "_condition_test_index";

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName, Settings.builder().put("index.number_of_shards", 1).build());
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            for (int i = 0; i < 2; i++) {
                CLUSTER.getInternalNodeClient().index(
                        new IndexRequest(indexName).source(ImmutableMap.of("key", "value")).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
                        .actionGet();
                Thread.sleep(1000);
            }
            ClusterHelper.Index.awaitSegmentCount(CLUSTER, indexName, 2, null);

            ForceMergeRequest request = new ForceMergeRequest(indexName).maxNumSegments(1);
            ForceMergeResponse response = CLUSTER.getInternalNodeClient().admin().indices().forceMerge(request).actionGet();
            assertEquals(RestStatus.OK, response.getStatus(), Strings.toString(response, true, true));

            ClusterHelper.Index.awaitSegmentCount(CLUSTER, indexName, 1, 1);
            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FINISHED);
        }

        @Test
        public void testMaxAgeConditionExecution() throws Exception {
            AgeCondition condition = new AgeCondition(TimeValue.timeValueSeconds(2));
            Policy policy = new Policy(new Policy.Step("first", ImmutableList.of(condition), ImmutableList.empty()));
            String policyName = condition.getType() + "_condition_test_policy";
            String indexName = condition.getType() + "_condition_test_index";

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            Thread.sleep(2000);

            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FINISHED);
        }

        @Test
        public void testMaxDocCountConditionExecution() throws Exception {
            DocCountCondition condition = new DocCountCondition(0);
            Policy policy = new Policy(new Policy.Step("first", ImmutableList.of(condition), ImmutableList.empty()));
            String policyName = condition.getType() + "_condition_test_policy";
            String indexName = condition.getType() + "_condition_test_index";

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.WAITING);

            GenericRestClient.HttpResponse response = CLUSTER.getRestClient(DEFAULT_AUTH).putJson("/" + indexName + "/_create/test",
                    "{\"test_val\": 2}");
            assertEquals(HttpStatus.SC_CREATED, response.getStatusCode(), response.getBody());

            Awaitility.await().until(() -> {
                GenericRestClient.HttpResponse httpResponse = CLUSTER.getRestClient(DEFAULT_AUTH).get("/" + indexName + "/_stats/docs");
                return DocNode.parse(Format.JSON).from(httpResponse.getBody());
            }, node -> Integer.valueOf(1).equals(node.get("indices", indexName, "primaries", "docs", "count")));

            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FINISHED);
        }

        @Test
        public void testMaxSizeConditionExecution() throws Exception {
            SizeCondition condition = new SizeCondition(ByteSizeValue.ofBytes(5));
            Policy policy = new Policy(new Policy.Step("first", ImmutableList.of(condition), ImmutableList.empty()));
            String policyName = condition.getType() + "_condition_test_policy";
            String indexName = condition.getType() + "_condition_test_index";

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.WAITING);

            GenericRestClient.HttpResponse response = CLUSTER.getRestClient(DEFAULT_AUTH).putJson("/" + indexName + "/_create/test",
                    "{\"test_val\":2}");
            assertEquals(HttpStatus.SC_CREATED, response.getStatusCode(), response.getBody());

            Awaitility.await().until(() -> {
                GenericRestClient.HttpResponse httpResponse = CLUSTER.getRestClient(DEFAULT_AUTH).get("/" + indexName + "/_stats/store");
                return DocNode.parse(Format.JSON).from(httpResponse.getBody());
            }, node -> 10 < (int) node.get("indices", indexName, "primaries", "store", "size_in_bytes"));
            Thread.sleep(1000);

            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FINISHED);
        }

        @Test
        public void testSnapshotCreatedConditionExecution() throws Exception {
            String snapshotName = "snapshot_condition_test_snap";
            String repositoryName = "snapshot_condition_test_repo";
            SnapshotCreatedCondition condition = new SnapshotCreatedCondition(repositoryName);
            Policy policy = new Policy(new Policy.Step("first", ImmutableList.of(condition), ImmutableList.empty()));
            String policyName = condition.getType() + "_condition_test_policy";
            String indexName = condition.getType() + "_condition_test_index";

            setupRepository(repositoryName);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);

            CreateSnapshotRequest request = new CreateSnapshotRequest(repositoryName, snapshotName).indices(indexName).waitForCompletion(true);
            CreateSnapshotResponse createSnapshotResponse = CLUSTER.getInternalNodeClient().admin().cluster()
                    .execute(CreateSnapshotAction.INSTANCE, request).actionGet();
            assertSame(RestStatus.OK, createSnapshotResponse.status(), Strings.toString(createSnapshotResponse, true, true));

            Awaitility.await().until(
                    () -> CLUSTER.getInternalNodeClient().admin().cluster().prepareSnapshotStatus().setRepository(repositoryName)
                            .setSnapshots(snapshotName).execute().actionGet(),
                    snapshotsStatusResponse -> SnapshotsInProgress.State.SUCCESS.equals(snapshotsStatusResponse.getSnapshots().get(0).getState()));

            PolicyInstanceState mockState = new PolicyInstanceState(policyName);
            mockState.setSnapshotName(snapshotName);
            IndexResponse indexResponse = CLUSTER.getPrivilegedInternalNodeClient()
                    .index(new IndexRequest(AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_STATES_NAME).id(indexName)
                            .source(mockState.toDocNode()))
                    .actionGet();
            assertEquals(RestStatus.CREATED, indexResponse.status(), Strings.toString(indexResponse, true, true));
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
            ClusterHelper.Index.awaitPolicyExists(CLUSTER, policyName);

            Awaitility.await().until(() -> ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName),
                    InternalPolicyInstanceAPI.PostExecuteRetry.Response::isExists);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FINISHED);
        }
    }

    @Execution(ExecutionMode.CONCURRENT)
    @Nested
    public class ActionTest {
        @Test
        public void testAllocationActionExecution() throws Exception {
            AllocationAction action = new AllocationAction(ImmutableMap.of("_name", "some_node_name"), ImmutableMap.empty(), ImmutableMap.empty());
            Policy policy = new Policy(new Policy.Step("first", ImmutableList.empty(), ImmutableList.of(action)));
            String policyName = action.getType() + "_action_test_policy";
            String indexName = action.getType() + "_action_test_index";

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FINISHED);

            GetSettingsResponse getResponse = CLUSTER.getInternalNodeClient()
                    .execute(GetSettingsAction.INSTANCE, new GetSettingsRequest().indices(indexName)).actionGet();
            assertEquals("some_node_name", getResponse.getSetting(indexName, "index.routing.allocation.require._name"), "Expected 'some_node_name'");
        }

        @Test
        public void testCloseActionExecution() throws Exception {
            CloseAction action = new CloseAction();
            Policy policy = new Policy(new Policy.Step("first", ImmutableList.empty(), ImmutableList.of(action)));
            String policyName = action.getType() + "_action_test_policy";
            String indexName = action.getType() + "_action_test_index";

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FINISHED);

            ClusterStateResponse clusterStateResponse = CLUSTER.getInternalNodeClient()
                    .execute(ClusterStateAction.INSTANCE, new ClusterStateRequest()).actionGet();
            assertEquals(IndexMetadata.State.CLOSE, clusterStateResponse.getState().metadata().index(indexName).getState());
        }

        @Test
        public void testDeleteActionExecution() throws Exception {
            DeleteAction action = new DeleteAction();
            Policy policy = new Policy(new Policy.Step("first", ImmutableList.empty(), ImmutableList.of(action)));
            String policyName = action.getType() + "_action_test_policy";
            String indexName = action.getType() + "_action_test_index";

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);

            Awaitility.await().until(() -> CLUSTER.getRestClient(DEFAULT_AUTH).head(indexName), httpResponse -> httpResponse.getStatusCode() == 404);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.DELETED);
        }

        @Test
        public void testForceMergeActionExecution() throws Exception {
            ForceMergeAsyncAction action = new ForceMergeAsyncAction(1);
            Policy policy = new Policy(new Policy.Step("first", ImmutableList.empty(), ImmutableList.of(action)));
            String policyName = action.getType() + "_action_test_policy";
            String indexName = action.getType() + "_action_test_index";

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName, Settings.builder().put("index.number_of_shards", 1).build());
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            for (int i = 0; i < 2; i++) {
                CLUSTER.getInternalNodeClient().index(
                        new IndexRequest(indexName).source(ImmutableMap.of("key", "value")).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
                        .actionGet();
                Thread.sleep(1000);
            }

            ClusterHelper.Index.awaitSegmentCount(CLUSTER, indexName, 2, null);
            CLUSTER.getInternalNodeClient().admin().indices()
                    .updateSettings(new UpdateSettingsRequest(indexName).settings(Settings.builder().put(SETTING_BLOCKS_WRITE, true))).actionGet();

            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FINISHED);

            ClusterHelper.Index.awaitSegmentCount(CLUSTER, indexName, 1, 1);
        }

        @Test
        public void testRolloverActionExecution() {
            RolloverAction action = new RolloverAction();
            Policy policy = new Policy(new Policy.Step("first", ImmutableList.empty(), ImmutableList.of(action)));
            String policyName = action.getType() + "_action_test_policy";
            String indexName = action.getType() + "_action_test_index-000001";
            String aliasName = action.getType() + "_action_test_alias";

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
            Settings settings = Settings.builder().put(AutomatedIndexManagementSettings.Static.ROLLOVER_ALIAS_FIELD.name(), aliasName).build();
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName, aliasName, settings);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FINISHED);

            String newIndexName = action.getType() + "_action_test_index-000002";

            GetIndexResponse response = CLUSTER.getInternalNodeClient().admin().indices().getIndex(new GetIndexRequest().indices(newIndexName))
                    .actionGet();
            assertTrue(Arrays.asList(response.indices()).contains(newIndexName), Arrays.toString(response.indices()));
        }

        @Test
        public void testRolloverActionNoAlias() throws Exception {
            RolloverAction action = new RolloverAction();
            Policy policy = new Policy(new Policy.Step("first", ImmutableList.empty(), ImmutableList.of(action)));
            String policyName = action.getType() + "_action_no_alias_test_policy";
            String indexName = action.getType() + "_action_no_alias_test_index-1";

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FAILED);

            GetResponse statusResponse = ClusterHelper.Index.getPolicyInstanceStatus(CLUSTER, indexName);
            DocNode statusNode = DocNode.wrap(DocReader.json().readObject(statusResponse.getSourceAsString()));
            assertEquals("No rollover alias configured in index settings", statusNode.get(PolicyInstanceState.LAST_EXECUTED_ACTION_FIELD,
                    PolicyInstanceState.ActionState.ERROR_FIELD, PolicyInstanceState.Error.MESSAGE_FIELD));
        }

        @Test
        public void testRolloverActionAlreadyRolledOver() throws Exception {
            RolloverAction action = new RolloverAction();
            Policy policy = new Policy(new Policy.Step("first", ImmutableList.empty(), ImmutableList.of(action)));
            String policyName = action.getType() + "_action_rolled_test_policy";
            String indexName = action.getType() + "_action_rolled_test_index-1";
            String aliasName = action.getType() + "_action_rolled_test_alias";

            Settings settings = Settings.builder().put(AutomatedIndexManagementSettings.Static.ROLLOVER_ALIAS_FIELD.name(), aliasName).build();
            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName, aliasName, settings);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            RolloverResponse response = CLUSTER.getInternalNodeClient().admin().indices()
                    .execute(org.elasticsearch.action.admin.indices.rollover.RolloverAction.INSTANCE, new RolloverRequest(aliasName, null))
                    .actionGet();
            assertTrue(response.isRolledOver(), Strings.toString(response));

            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FAILED);

            GetResponse statusResponse = ClusterHelper.Index.getPolicyInstanceStatus(CLUSTER, indexName);
            DocNode statusNode = DocNode.wrap(DocReader.json().readObject(statusResponse.getSourceAsString()));
            assertEquals("Index does not have the rollover alias assigned. Index might be already rolled over",
                    statusNode.get(PolicyInstanceState.LAST_EXECUTED_ACTION_FIELD, PolicyInstanceState.ActionState.ERROR_FIELD,
                            PolicyInstanceState.Error.MESSAGE_FIELD));
        }

        @Test
        public void testSetPriorityActionExecution() {
            SetPriorityAction action = new SetPriorityAction(50);
            Policy policy = new Policy(new Policy.Step("first", ImmutableList.empty(), ImmutableList.of(action)));
            String policyName = action.getType() + "_action_test_policy";
            String indexName = action.getType() + "_action_test_index";

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FINISHED);

            GetSettingsRequest request = new GetSettingsRequest().indices(indexName);
            GetSettingsResponse response = CLUSTER.getInternalNodeClient().admin().indices().execute(GetSettingsAction.INSTANCE, request).actionGet();
            assertEquals(50, response.getIndexToSettings().get(indexName).getAsInt(SETTING_PRIORITY, 0));
        }

        @Test
        public void testSetReadOnlyActionExecution() {
            SetReadOnlyAction action = new SetReadOnlyAction();
            Policy policy = new Policy(new Policy.Step("first", ImmutableList.empty(), ImmutableList.of(action)));
            String policyName = action.getType() + "_action_test_policy";
            String indexName = action.getType() + "_action_test_index";

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FINISHED);

            GetSettingsRequest request = new GetSettingsRequest().indices(indexName);
            GetSettingsResponse response = CLUSTER.getInternalNodeClient().admin().indices().execute(GetSettingsAction.INSTANCE, request).actionGet();
            assertTrue(response.getIndexToSettings().get(indexName).getAsBoolean(SETTING_BLOCKS_WRITE, false), Strings.toString(response));
        }

        @Test
        public void testSetReplicaCountActionExecution() {
            SetReplicaCountAction action = new SetReplicaCountAction(2);
            Policy policy = new Policy(new Policy.Step("first", ImmutableList.empty(), ImmutableList.of(action)));
            String policyName = action.getType() + "_action_test_policy";
            String indexName = action.getType() + "_action_test_index";

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FINISHED);

            GetSettingsRequest request = new GetSettingsRequest().indices(indexName);
            GetSettingsResponse response = CLUSTER.getInternalNodeClient().admin().indices().execute(GetSettingsAction.INSTANCE, request).actionGet();
            assertEquals(2, response.getIndexToSettings().get(indexName).getAsInt(SETTING_NUMBER_OF_REPLICAS, 0));
        }

        @Test
        public void testSnapshotActionExecution() throws Exception {
            String snapshotNamePrefix = "snapshot_action_test_snap";
            String repositoryName = "snapshot_action_test_repo";
            SnapshotAsyncAction action = new SnapshotAsyncAction(snapshotNamePrefix, repositoryName);
            Policy policy = new Policy(new Policy.Step("first", ImmutableList.empty(), ImmutableList.of(action)));
            String policyName = action.getType() + "_action_test_policy";
            String indexName = action.getType() + "_action_test_index";

            setupRepository(repositoryName);

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FINISHED);

            GetSnapshotsResponse snapshotsResponse = CLUSTER.getInternalNodeClient().admin().cluster()
                    .execute(GetSnapshotsAction.INSTANCE, new GetSnapshotsRequest(repositoryName)).actionGet();
            assertTrue(
                    snapshotsResponse.getSnapshots().stream()
                            .anyMatch(snapshotInfo -> snapshotInfo.snapshotId().getName().startsWith(snapshotNamePrefix)),
                    Strings.toString(snapshotsResponse, true, true));

            GetResponse statusResponse = ClusterHelper.Index.getPolicyInstanceStatus(CLUSTER, indexName);
            DocNode statusNode = DocNode.wrap(DocReader.json().read(statusResponse.getSourceAsString()));
            String concreteSnapshotName = (String) statusNode.get(PolicyInstanceState.SNAPSHOT_NAME);
            Awaitility.await().until(
                    () -> CLUSTER.getInternalNodeClient().admin().cluster().prepareSnapshotStatus().setRepository(repositoryName)
                            .setSnapshots(concreteSnapshotName).execute().actionGet(),
                    snapshotsStatusResponse -> SnapshotsInProgress.State.SUCCESS.equals(snapshotsStatusResponse.getSnapshots().get(0).getState()));
        }

        @Test
        public void testSnapshotActionNonExistentRepo() throws Exception {
            String snapshotName = "snapshot_action_non_existing_repo_test_snap";
            String repositoryName = "snapshot_action_non_existing_repo_test_repo";

            SnapshotAsyncAction action = new SnapshotAsyncAction(snapshotName, repositoryName);
            Policy policy = new Policy(new Policy.Step("action_test_state", ImmutableList.empty(), ImmutableList.of(action)));
            String policyName = action.getType() + "_action_non_existing_repo_test_policy";
            String indexName = action.getType() + "_action_non_existing_repo_test_index";

            ClusterHelper.Internal.putPolicy(CLUSTER, policyName, policy);
            ClusterHelper.Index.createManagedIndex(CLUSTER, indexName, policyName);
            ClusterHelper.Index.awaitPolicyInstanceStatusExists(CLUSTER, indexName);

            ClusterHelper.Internal.postPolicyInstanceExecute(CLUSTER, indexName);
            ClusterHelper.Index.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FAILED);

            GetResponse statusResponse = ClusterHelper.Index.getPolicyInstanceStatus(CLUSTER, indexName);
            DocNode statusNode = DocNode.wrap(DocReader.json().readObject(statusResponse.getSourceAsString()));
            assertEquals("[" + repositoryName + "] missing", statusNode.get(PolicyInstanceState.LAST_EXECUTED_ACTION_FIELD,
                    PolicyInstanceState.ActionState.ERROR_FIELD, PolicyInstanceState.Error.MESSAGE_FIELD));
        }
    }
}