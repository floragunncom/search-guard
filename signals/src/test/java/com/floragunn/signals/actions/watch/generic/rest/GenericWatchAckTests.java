package com.floragunn.signals.actions.watch.generic.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.SignalsModule;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.xcontent.XContentType;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static com.floragunn.searchguard.test.TestSgConfig.TenantPermission.ALL_TENANTS_AND_ACCESS;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.docNodeSizeEqualTo;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_OK;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

public class GenericWatchAckTests extends AbstractGenericWatchTest {

    private static final Logger log = LogManager.getLogger(GenericWatchAckTests.class);
    private static final String INDEX_SOURCE = "test_source_index";

    private final static TestSgConfig.User USER_ADMIN = new TestSgConfig.User("admin").roles(new TestSgConfig.Role("signals_master")//
        .clusterPermissions("*")//
        .indexPermissions("*").on("*")//
        .tenantPermission(ALL_TENANTS_AND_ACCESS));
    public static final String TEST_ACTION_NAME = "testsink";

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()//
        .user(USER_ADMIN).enableModule(SignalsModule.class)//
        .nodeSettings("signals.enabled", true) //
        .build();

    @BeforeClass
    public static void setupTestData() {
        try (Client client = cluster.getInternalNodeClient()) {
            client.index(new IndexRequest(INDEX_SOURCE).source(XContentType.JSON, "key1", "1", "key2", "2")).actionGet();
            client.index(new IndexRequest(INDEX_SOURCE).setRefreshPolicy(IMMEDIATE).source(XContentType.JSON, "key1", "3", "key2", "4")) //
                .actionGet();
            client.index(new IndexRequest(INDEX_SOURCE).setRefreshPolicy(IMMEDIATE).source(XContentType.JSON, "key1", "5", "key2", "6")) //
                .actionGet();
        }
    }


    @Test
    public void shouldAckOnlyOneWatchInstance() throws Exception {
        String watchId = "watch-id-should-ack-only-one-watch-instance";
        String watchPath = watchPath(watchId);
        String instancesPath = allInstancesPath(watchId);
        String instanceId1 = "instance_id_1";
        String instanceId2 = "instance_id_2";
        String instanceId3 = "instance_id_3";
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "instance_parameter")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['created_by':instance.id]")//
                .throttledFor("1ms").name("testsink").build();
            GenericRestClient.HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of(instanceId1, DocNode.of("instance_parameter", 0), instanceId2, DocNode.of("instance_parameter", 1),
                instanceId3, DocNode.of("instance_parameter", 2));
            response = restClient.putJson(instancesPath, node.toJsonString());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId1) > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId2) > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId3) > 0);

            response = restClient.put(instancePath(watchId, instanceId2) + "/_ack");

            log.info("Ack watch instance response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Thread.sleep(1000);
            long previousNumberOfExecutionInstance1 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId1);
            long previousNumberOfExecutionInstance2 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId2);
            long previousNumberOfExecutionInstance3 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId3);
            Thread.sleep(3000);
            //make sure that only acked watch action is not executed
            long currentNumberOfExecutionInstance1 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId1);
            long currentNumberOfExecutionInstance2 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId2);
            long currentNumberOfExecutionInstance3 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId3);
            assertThat(currentNumberOfExecutionInstance1, greaterThan(previousNumberOfExecutionInstance1)); //is still executed
            assertThat(currentNumberOfExecutionInstance2, equalTo(previousNumberOfExecutionInstance2)); // is not executed because is acked
            assertThat(currentNumberOfExecutionInstance3, greaterThan(previousNumberOfExecutionInstance3));//is still executed
            response = restClient.get(instancePath(watchId, instanceId2) + "/_state");
            log.debug("Get watch state response status code '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.actions.testsink.last_status.code", "ACKED"));
        }
    }

    @Test
    public void shouldAckOnlyOneWatchInstanceAction() throws Exception {
        String watchId = "watch-id-should-ack-only-one-watch-instance-action";
        String watchPath = watchPath(watchId);
        String instanceId = "instance_id_ack_one";
        String instancePath = instancePath(watchId, instanceId);
        String actionName1 = "first-action";
        String actionName2 = "second-action";
        String actionName3 = "third-action";
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()) {
            final String destinationIndex1 = "destination-index-for-" + watchId + "-" + actionName1;
            final String destinationIndex2 = "destination-index-for-" + watchId + "-" + actionName2;
            final String destinationIndex3 = "destination-index-for-" + watchId + "-" + actionName3;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex1)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(destinationIndex2)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(destinationIndex3)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "instance_parameter")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex1).throttledFor("1ms").name(actionName1) //
                .and().index(destinationIndex2).throttledFor("1ms").name(actionName2) //
                .and().index(destinationIndex3).throttledFor("1ms").name(actionName3) //
                .build();
            GenericRestClient.HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of("instance_parameter", 0);
            response = restClient.putJson(instancePath, node.toJsonString());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex1) > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex2) > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex3) > 0);

            response = restClient.put(instancePath(watchId, instanceId) + "/_ack/" + actionName2);

            log.info("Ack watch instance action response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Thread.sleep(1000);
            long previousNumberOfExecutionAction1 = countDocumentInIndex(client, destinationIndex1);
            long previousNumberOfExecutionAction2 = countDocumentInIndex(client, destinationIndex2);
            long previousNumberOfExecutionAction3 = countDocumentInIndex(client, destinationIndex3);
            Thread.sleep(3000);
            //make sure that only acked watch action is not executed
            long currentNumberOfExecutionAction1 = countDocumentInIndex(client, destinationIndex1);
            long currentNumberOfExecutionAction2 = countDocumentInIndex(client, destinationIndex2);
            long currentNumberOfExecutionAction3 = countDocumentInIndex(client, destinationIndex3);
            assertThat(currentNumberOfExecutionAction1, greaterThan(previousNumberOfExecutionAction1)); //is still executed
            assertThat(currentNumberOfExecutionAction2, equalTo(previousNumberOfExecutionAction2)); // is not executed because is acked
            assertThat(currentNumberOfExecutionAction3, greaterThan(previousNumberOfExecutionAction3));//is still executed
            response = restClient.get(instancePath(watchId, instanceId) + "/_state");
            log.debug("Get watch state response status code '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.actions." + actionName2 + ".last_status.code", "ACKED"));
        }
    }

    @Test
    public void shouldUnAckOnlyOneWatchInstance() throws Exception {
        String watchId = "watch-id-should-un-ack-only-one-watch-instance";
        String watchPath = watchPath(watchId);
        String instancesPath = allInstancesPath(watchId);
        String instanceId1 = "instance_id_1";
        String instanceId2 = "instance_id_2";
        String instanceId3 = "instance_id_3";
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "instance_parameter")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['created_by':instance.id]")//
                .throttledFor("1ms").name("testsink").build();
            GenericRestClient.HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of(instanceId1, DocNode.of("instance_parameter", 0), instanceId2, DocNode.of("instance_parameter", 1),
                instanceId3, DocNode.of("instance_parameter", 2));
            response = restClient.putJson(instancesPath, node.toJsonString());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId1) > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId2) > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId3) > 0);
            response = restClient.put(instancePath(watchId, instanceId2) + "/_ack");
            log.info("Ack watch instance response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Thread.sleep(1000);
            long previousNumberOfExecutionInstance1 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId1);
            long previousNumberOfExecutionInstance2 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId2);
            long previousNumberOfExecutionInstance3 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId3);
            Thread.sleep(3000);
            //make sure that only acked watch action is not executed
            long currentNumberOfExecutionInstance1 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId1);
            long currentNumberOfExecutionInstance2 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId2);
            long currentNumberOfExecutionInstance3 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId3);
            assertThat(currentNumberOfExecutionInstance1, greaterThan(previousNumberOfExecutionInstance1)); //is still executed
            assertThat(currentNumberOfExecutionInstance2, equalTo(previousNumberOfExecutionInstance2)); // is not executed because is acked
            assertThat(currentNumberOfExecutionInstance3, greaterThan(previousNumberOfExecutionInstance3));//is still executed
            response = restClient.get(instancePath(watchId, instanceId2) + "/_state");
            log.debug("Get watch state response status code '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.actions.testsink.last_status.code", "ACKED"));

            response = restClient.delete(instancePath(watchId, instanceId2) + "/_ack");

            log.debug("Un-ack response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId2) > currentNumberOfExecutionInstance2);
            response = restClient.get(instancePath(watchId, instanceId2) + "/_state");
            log.debug("Get watch state response status code '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            body = response.getBodyAsDocNode();
            assertThat(body, not(containsValue("$.actions.testsink.last_status.code", "ACKED")));
        }
    }

    @Test
    public void shouldUnAckOnlyOneWatchInstanceAction() throws Exception {
        String watchId = "watch-id-should-un-ack-only-one-watch-instance-action";
        String watchPath = watchPath(watchId);
        String instanceId = "instance_id";
        String instancePath = instancePath(watchId, instanceId);
        String actionName1 = "first-action";
        String actionName2 = "second-action";
        String actionName3 = "third-action";
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()) {
            final String destinationIndex1 = "destination-index-for-" + watchId + "-" + actionName1;
            final String destinationIndex2 = "destination-index-for-" + watchId + "-" + actionName2;
            final String destinationIndex3 = "destination-index-for-" + watchId + "-" + actionName3;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex1)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(destinationIndex2)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(destinationIndex3)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "instance_parameter")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex1).throttledFor("1ms").name(actionName1) //
                .and().index(destinationIndex2).throttledFor("1ms").name(actionName2) //
                .and().index(destinationIndex3).throttledFor("1ms").name(actionName3) //
                .build();
            GenericRestClient.HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of("instance_parameter", 0);
            response = restClient.putJson(instancePath, node.toJsonString());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex1) > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex2) > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex3) > 0);
            //all action has been executed so far
            response = restClient.put(instancePath(watchId, instanceId) + "/_ack"); //ack all actions
            log.info("Ack watch instance action response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Thread.sleep(1000);
            long previousNumberOfExecutionAction1 = countDocumentInIndex(client, destinationIndex1);
            long previousNumberOfExecutionAction2 = countDocumentInIndex(client, destinationIndex2);
            long previousNumberOfExecutionAction3 = countDocumentInIndex(client, destinationIndex3);
            Thread.sleep(3000);
            //All watch actions are not executed, because actions were acked
            long currentNumberOfExecutionAction1 = countDocumentInIndex(client, destinationIndex1);
            long currentNumberOfExecutionAction2 = countDocumentInIndex(client, destinationIndex2);
            long currentNumberOfExecutionAction3 = countDocumentInIndex(client, destinationIndex3);
            assertThat(currentNumberOfExecutionAction1, equalTo(previousNumberOfExecutionAction1)); //is not executed because is acked
            assertThat(currentNumberOfExecutionAction2, equalTo(previousNumberOfExecutionAction2)); //is not executed because is acked
            assertThat(currentNumberOfExecutionAction3, equalTo(previousNumberOfExecutionAction3));//is not executed because is acked
            response = restClient.delete(instancePath(watchId, instanceId) + "/_ack/" + actionName2); //un-ack action2
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex2) > currentNumberOfExecutionAction2);
            response = restClient.get(instancePath(watchId, instanceId) + "/_state");
            log.debug("Get watch state response status code '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.actions." + actionName1 + ".last_status.code", "ACKED"));
            assertThat(body, not(containsValue("$.actions." + actionName2 + ".last_status.code", "ACKED")));// action to is NOT acked
            assertThat(body, containsValue("$.actions." + actionName3 + ".last_status.code", "ACKED"));
        }
    }

    @Test
    public void shouldAckAndGetOnlyOneWatchInstance() throws Exception {
        String watchId = "watch-id-should-ack-and-get-only-one-watch-instance";
        String watchPath = watchPath(watchId);
        String instancesPath = allInstancesPath(watchId);
        String instanceId1 = "instance_id_1";
        String instanceId2 = "instance_id_2";
        String instanceId3 = "instance_id_3";
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "instance_parameter")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['created_by':instance.id]")//
                .throttledFor("1ms").name(TEST_ACTION_NAME).build();
            GenericRestClient.HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of(instanceId1, DocNode.of("instance_parameter", 0), instanceId2, DocNode.of("instance_parameter", 1),
                instanceId3, DocNode.of("instance_parameter", 2));
            response = restClient.putJson(instancesPath, node.toJsonString());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId1) > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId2) > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId3) > 0);

            response = restClient.put(instancePath(watchId, instanceId2) + "/_ack_and_get");

            log.info("Ack and get watch instance response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.acked[0].action_id", TEST_ACTION_NAME));
            Thread.sleep(1000);
            long previousNumberOfExecutionInstance1 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId1);
            long previousNumberOfExecutionInstance2 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId2);
            long previousNumberOfExecutionInstance3 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId3);
            Thread.sleep(3000);
            //make sure that only acked watch action is not executed
            long currentNumberOfExecutionInstance1 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId1);
            long currentNumberOfExecutionInstance2 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId2);
            long currentNumberOfExecutionInstance3 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId3);
            assertThat(currentNumberOfExecutionInstance1, greaterThan(previousNumberOfExecutionInstance1)); //is still executed
            assertThat(currentNumberOfExecutionInstance2, equalTo(previousNumberOfExecutionInstance2)); // is not executed because is acked
            assertThat(currentNumberOfExecutionInstance3, greaterThan(previousNumberOfExecutionInstance3));//is still executed
            response = restClient.get(instancePath(watchId, instanceId2) + "/_state");
            log.debug("Get watch state response status code '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.actions.testsink.last_status.code", "ACKED"));
        }
    }

    @Test
    public void shouldAckAndGetOnlyOneWatchInstanceAction() throws Exception {
        String watchId = "watch-id-should-ack-and-get-only-one-watch-instance-action";
        String watchPath = watchPath(watchId);
        String instanceId = "instance_id_ack_and_get_one";
        String instancePath = instancePath(watchId, instanceId);
        String actionName1 = "first-action";
        String actionName2 = "second-action";
        String actionName3 = "third-action";
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()) {
            final String destinationIndex1 = "destination-index-for-" + watchId + "-" + actionName1;
            final String destinationIndex2 = "destination-index-for-" + watchId + "-" + actionName2;
            final String destinationIndex3 = "destination-index-for-" + watchId + "-" + actionName3;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex1)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(destinationIndex2)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(destinationIndex3)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "instance_parameter")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex1).throttledFor("1ms").name(actionName1) //
                .and().index(destinationIndex2).throttledFor("1ms").name(actionName2) //
                .and().index(destinationIndex3).throttledFor("1ms").name(actionName3) //
                .build();
            GenericRestClient.HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of("instance_parameter", 0);
            response = restClient.putJson(instancePath, node.toJsonString());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex1) > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex2) > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex3) > 0);

            response = restClient.put(instancePath(watchId, instanceId) + "/_ack_and_get/" + actionName2);

            log.info("Ack and get watch instance action response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.acked[0].action_id", actionName2));
            Thread.sleep(1000);
            long previousNumberOfExecutionAction1 = countDocumentInIndex(client, destinationIndex1);
            long previousNumberOfExecutionAction2 = countDocumentInIndex(client, destinationIndex2);
            long previousNumberOfExecutionAction3 = countDocumentInIndex(client, destinationIndex3);
            Thread.sleep(3000);
            //make sure that only acked watch action is not executed
            long currentNumberOfExecutionAction1 = countDocumentInIndex(client, destinationIndex1);
            long currentNumberOfExecutionAction2 = countDocumentInIndex(client, destinationIndex2);
            long currentNumberOfExecutionAction3 = countDocumentInIndex(client, destinationIndex3);
            assertThat(currentNumberOfExecutionAction1, greaterThan(previousNumberOfExecutionAction1)); //is still executed
            assertThat(currentNumberOfExecutionAction2, equalTo(previousNumberOfExecutionAction2)); // is not executed because is acked
            assertThat(currentNumberOfExecutionAction3, greaterThan(previousNumberOfExecutionAction3));//is still executed
            response = restClient.get(instancePath(watchId, instanceId) + "/_state");
            log.debug("Get watch state response status code '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.actions." + actionName2 + ".last_status.code", "ACKED"));
        }
    }

    @Test
    public void shouldUnAckAndGetOnlyOneWatchInstance() throws Exception {
        String watchId = "watch-id-should-un-ack-and-get-only-one-watch-instance";
        String watchPath = watchPath(watchId);
        String instancesPath = allInstancesPath(watchId);
        String instanceId1 = "instance_id_1";
        String instanceId2 = "instance_id_2";
        String instanceId3 = "instance_id_3";
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "instance_parameter")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['created_by':instance.id]")//
                .throttledFor("1ms").name(TEST_ACTION_NAME).build();
            GenericRestClient.HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of(instanceId1, DocNode.of("instance_parameter", 0), instanceId2, DocNode.of("instance_parameter", 1),
                instanceId3, DocNode.of("instance_parameter", 2));
            response = restClient.putJson(instancesPath, node.toJsonString());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId1) > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId2) > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId3) > 0);
            response = restClient.put(instancePath(watchId, instanceId2) + "/_ack_and_get");
            log.info("Ack watch and get instance response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.acked[0].action_id", TEST_ACTION_NAME));
            Thread.sleep(1000);
            long previousNumberOfExecutionInstance1 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId1);
            long previousNumberOfExecutionInstance2 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId2);
            long previousNumberOfExecutionInstance3 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId3);
            Thread.sleep(3000);
            //make sure that only acked watch action is not executed
            long currentNumberOfExecutionInstance1 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId1);
            long currentNumberOfExecutionInstance2 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId2);
            long currentNumberOfExecutionInstance3 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId3);
            assertThat(currentNumberOfExecutionInstance1, greaterThan(previousNumberOfExecutionInstance1)); //is still executed
            assertThat(currentNumberOfExecutionInstance2, equalTo(previousNumberOfExecutionInstance2)); // is not executed because is acked
            assertThat(currentNumberOfExecutionInstance3, greaterThan(previousNumberOfExecutionInstance3));//is still executed
            response = restClient.get(instancePath(watchId, instanceId2) + "/_state");
            log.debug("Get watch state response status code '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.actions.testsink.last_status.code", "ACKED"));

            response = restClient.delete(instancePath(watchId, instanceId2) + "/_ack_and_get");

            log.debug("Un-ack and get response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.unacked_action_ids[0]", TEST_ACTION_NAME));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId2) > currentNumberOfExecutionInstance2);
            response = restClient.get(instancePath(watchId, instanceId2) + "/_state");
            log.debug("Get watch state response status code '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            body = response.getBodyAsDocNode();
            assertThat(body, not(containsValue("$.actions.testsink.last_status.code", "ACKED")));
        }
    }

    @Test
    public void shouldUnAckAndGetOnlyOneWatchInstanceAction() throws Exception {
        String watchId = "watch-id-should-un-ack-and-get-only-one-watch-instance-action";
        String watchPath = watchPath(watchId);
        String instanceId = "instance_id";
        String instancePath = instancePath(watchId, instanceId);
        String actionName1 = "first-action";
        String actionName2 = "second-action";
        String actionName3 = "third-action";
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()) {
            final String destinationIndex1 = "destination-index-for-" + watchId + "-" + actionName1;
            final String destinationIndex2 = "destination-index-for-" + watchId + "-" + actionName2;
            final String destinationIndex3 = "destination-index-for-" + watchId + "-" + actionName3;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex1)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(destinationIndex2)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(destinationIndex3)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "instance_parameter")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex1).throttledFor("1ms").name(actionName1) //
                .and().index(destinationIndex2).throttledFor("1ms").name(actionName2) //
                .and().index(destinationIndex3).throttledFor("1ms").name(actionName3) //
                .build();
            GenericRestClient.HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of("instance_parameter", 0);
            response = restClient.putJson(instancePath, node.toJsonString());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex1) > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex2) > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex3) > 0);
            //all action has been executed so far
            response = restClient.put(instancePath(watchId, instanceId) + "/_ack_and_get"); //ack all actions
            log.info("Ack watch instance action response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("$.acked", 3));

            Thread.sleep(1000);
            long previousNumberOfExecutionAction1 = countDocumentInIndex(client, destinationIndex1);
            long previousNumberOfExecutionAction2 = countDocumentInIndex(client, destinationIndex2);
            long previousNumberOfExecutionAction3 = countDocumentInIndex(client, destinationIndex3);
            Thread.sleep(3000);
            //All watch actions are not executed, because actions were acked
            long currentNumberOfExecutionAction1 = countDocumentInIndex(client, destinationIndex1);
            long currentNumberOfExecutionAction2 = countDocumentInIndex(client, destinationIndex2);
            long currentNumberOfExecutionAction3 = countDocumentInIndex(client, destinationIndex3);
            assertThat(currentNumberOfExecutionAction1, equalTo(previousNumberOfExecutionAction1)); //is not executed because is acked
            assertThat(currentNumberOfExecutionAction2, equalTo(previousNumberOfExecutionAction2)); //is not executed because is acked
            assertThat(currentNumberOfExecutionAction3, equalTo(previousNumberOfExecutionAction3));//is not executed because is acked
            response = restClient.delete(instancePath(watchId, instanceId) + "/_ack_and_get/" + actionName2); //un-ack action2
            log.debug("Un-ack and get response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.unacked_action_ids[0]", actionName2));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex2) > currentNumberOfExecutionAction2);
            response = restClient.get(instancePath(watchId, instanceId) + "/_state");
            log.debug("Get watch state response status code '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.actions." + actionName1 + ".last_status.code", "ACKED"));
            assertThat(body, not(containsValue("$.actions." + actionName2 + ".last_status.code", "ACKED")));// action to is NOT acked
            assertThat(body, containsValue("$.actions." + actionName3 + ".last_status.code", "ACKED"));
        }
    }

    @Override
    protected GenericRestClient getAdminRestClient() {
        return cluster.getRestClient(USER_ADMIN);
    }
}
