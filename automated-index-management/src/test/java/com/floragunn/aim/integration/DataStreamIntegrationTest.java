package com.floragunn.aim.integration;

import com.floragunn.aim.AutomatedIndexManagementModule;
import com.floragunn.aim.integration.support.ClusterHelper;
import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.actions.RolloverAction;
import com.floragunn.aim.policy.conditions.DocCountCondition;
import com.floragunn.aim.policy.conditions.IndexCountCondition;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.aim.policy.schedule.IntervalSchedule;
import com.floragunn.aim.policy.schedule.Schedule;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.RestMatchers;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.floragunn.searchguard.test.RestMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class DataStreamIntegrationTest {
    private static LocalCluster CLUSTER;

    @BeforeAll
    public static void setup() {
        CLUSTER = new LocalCluster.Builder().singleNode().sslEnabled().enableModule(AutomatedIndexManagementModule.class).useExternalProcessCluster()
                .start();
    }

    @Test
    public void testRolloverActionExecutionDataStream() throws Exception {
        RolloverAction action = new RolloverAction();
        DocCountCondition condition = new DocCountCondition(0);
        Policy policy = new Policy(new IntervalSchedule(Duration.ofMinutes(60), false, Schedule.Scope.POLICY), ImmutableList.of(new Policy.Step("first", ImmutableList.of(condition), ImmutableList.of(action))));
        String policyName = action.getType() + "_action_datastream_test_policy";
        String dataStreamName = action.getType() + "_action_datastream_test_datastream";

        GenericRestClient.HttpResponse response;

        response = ClusterHelper.Rest.putPolicy(CLUSTER, policyName, policy);
        assertThat(response, RestMatchers.isCreated());

        response = ClusterHelper.Rest.createManagedDataStream(CLUSTER, dataStreamName, policyName);
        assertThat(response, RestMatchers.isOk());

        response = CLUSTER.getAdminCertRestClient().get("/_data_stream/" + dataStreamName);
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("data_streams", hasSize(1))));
        assertThat(response, json(nodeAt("data_streams[0].name", is(dataStreamName))));
        assertThat(response, json(nodeAt("data_streams[0].indices", hasSize(1))));
        String indexName = response.getBodyAsDocNode().findSingleValueByJsonPath("data_streams[0].indices[0].index_name", String.class);

        ClusterHelper.Rest.postPolicyInstanceExecute(CLUSTER, indexName);
        ClusterHelper.Rest.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.WAITING);

        response = CLUSTER.getAdminCertRestClient().postJson(dataStreamName + "/_doc", ImmutableMap.of("@timestamp", "2025-01-01T00:00:00Z", "message", "test"));
        assertThat(response, isCreated());

        Awaitility.await().until(() -> CLUSTER.getAdminCertRestClient().get("/" + indexName + "/_stats/docs"), statsResponse -> json(nodeAt("_all.primaries.docs.count", is(1))).matches(statsResponse));

        ClusterHelper.Rest.postPolicyInstanceExecute(CLUSTER, indexName);
        ClusterHelper.Rest.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FINISHED);

        response = CLUSTER.getAdminCertRestClient().get("_data_stream/" + dataStreamName);
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("data_streams", hasSize(1))));
        assertThat(response, json(nodeAt("data_streams[0].name", is(dataStreamName))));
        assertThat(response, json(nodeAt("data_streams[0].indices", hasSize(2))));
    }

    @Test
    public void testIndexCountConditionExecutionDataStream() throws Exception {
        IndexCountCondition condition = new IndexCountCondition("all_alias", 1);
        Policy policy = new Policy(new Policy.Step("first", ImmutableList.of(condition), ImmutableList.empty()));
        String policyName = condition.getType() + "_condition_datastream_test_policy";
        String dataStreamName = condition.getType() + "_action_datastream_test_datastream";

        GenericRestClient.HttpResponse response;

        response = ClusterHelper.Rest.putPolicy(CLUSTER, policyName, policy);
        assertThat(response, RestMatchers.isCreated());

        response = ClusterHelper.Rest.createManagedDataStream(CLUSTER, dataStreamName, policyName);
        assertThat(response, RestMatchers.isOk());

        response = CLUSTER.getAdminCertRestClient().get("/_data_stream/" + dataStreamName);
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("data_streams", hasSize(1))));
        assertThat(response, json(nodeAt("data_streams[0].name", is(dataStreamName))));
        assertThat(response, json(nodeAt("data_streams[0].indices", hasSize(1))));
        String indexName = response.getBodyAsDocNode().findSingleValueByJsonPath("data_streams[0].indices[0].index_name", String.class);

        ClusterHelper.Rest.postPolicyInstanceExecute(CLUSTER, indexName);
        ClusterHelper.Rest.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.WAITING);

        response = CLUSTER.getAdminCertRestClient().post(dataStreamName + "/_rollover");
        assertThat(response, isOk());
        String newIndexName = response.getBodyAsDocNode().getAsString("new_index");

        response = CLUSTER.getAdminCertRestClient().get("/_data_stream/" + dataStreamName);
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("data_streams", hasSize(1))));
        assertThat(response, json(nodeAt("data_streams[0].name", is(dataStreamName))));
        assertThat(response, json(nodeAt("data_streams[0].indices", hasSize(2))));

        ClusterHelper.Rest.awaitPolicyInstanceStatusExists(CLUSTER, newIndexName);

        ClusterHelper.Rest.postPolicyInstanceExecute(CLUSTER, indexName);
        ClusterHelper.Rest.awaitPolicyInstanceStatusEqual(CLUSTER, indexName, PolicyInstanceState.Status.FINISHED);
    }
}
