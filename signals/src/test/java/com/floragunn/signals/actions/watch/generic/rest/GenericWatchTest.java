/*
 * Copyright 2019-2023 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.floragunn.signals.actions.watch.generic.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.MockWebserviceProvider;
import com.floragunn.signals.SignalsModule;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstancesRepository;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.xcontent.XContentType;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static com.floragunn.searchguard.test.TestSgConfig.TenantPermission.ALL_TENANTS_AND_ACCESS;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containSubstring;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.http.HttpStatus.SC_BAD_REQUEST;
import static org.apache.http.HttpStatus.SC_CONFLICT;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

public class GenericWatchTest extends AbstractGenericWatchTest {

    private static final Logger log = LogManager.getLogger(GenericWatchTest.class);
    private static final String INDEX_SOURCE = "test_source_index";

    private final static User USER_ADMIN = new User("admin").roles(new Role("signals_master")//
        .clusterPermissions("*")//
        .indexPermissions("*").on("*")//
        .tenantPermission(ALL_TENANTS_AND_ACCESS));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()//
        .user(USER_ADMIN).enableModule(SignalsModule.class)//
        .nodeSettings("signals.enabled", true) //
        .build();

    @BeforeClass
    public static void setupTestData() {
        try (Client client = cluster.getInternalNodeClient()) {
            BulkRequest request = new BulkRequest();
            request.add(new IndexRequest(INDEX_SOURCE).source(XContentType.JSON, "key1", "1", "key2", "2"));
            request.add(new IndexRequest(INDEX_SOURCE).source(XContentType.JSON, "key1", "3", "key2", "4"));
            request.add(new IndexRequest(INDEX_SOURCE).source(XContentType.JSON, "key1", "5", "key2", "6"));
            request.setRefreshPolicy(IMMEDIATE);
            client.bulk(request).actionGet();
        }
    }

    @Test
    public void shouldCreateGenericWatchParameters() throws Exception {
        String watchId = "my-watch-create-generic-watch-parameters";
        String instanceId = "instance_id_should_create_generic_watch_parameters";
        String path = instancePath(watchId, instanceId);
        String watchInstanceStatePath = path + "/_state";
        String watchPath = createGenericWatch(DEFAULT_TENANT, watchId, "vm_id", "name", "time", "int", "long", "double", "bool");
        DocNode parameterValues = DocNode.of("vm_id", 258, "name", "kirk", "time", "2023-05-15T13:07:52.000Z") //
                .with(DocNode.of("int", Integer.MAX_VALUE, "long", Long.MIN_VALUE, "double", Math.PI, "bool", false));
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {

            HttpResponse response = client.putJson(path, parameterValues);

            log.debug("Create generic watch instance response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            response = client.get(path);
            log.debug("Create watch parameters retrieved by REST request '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.data.vm_id", 258));
            assertThat(body, containsValue("data.name", "kirk"));
            assertThat(body, containsValue("data.time", "2023-05-15T13:07:52.000Z"));
            assertThat(body, containsValue("data.int", Integer.MAX_VALUE));
            assertThat(body, containsValue("data.long", Long.MIN_VALUE));
            assertThat(body, containsValue("data.double", Math.PI));
            assertThat(body, containsValue("data.bool", false));

            Awaitility.await("watch instance state").timeout(3, SECONDS) //
                .until(() -> client.get(watchInstanceStatePath).getStatusCode(), equalTo(SC_OK));
            response = client.get(watchInstanceStatePath);
            log.debug("Watch state response code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.instance_id", instanceId));
            assertThat(body, containsValue("$.parent_generic_watch_id", watchId));

            // state for generic watch ("template") should not be created
            response = client.get(watchPath + "/_state");
            assertThat(response.getStatusCode(), equalTo(SC_NOT_FOUND));


            // and .signals_watch_instances should be hidden
            HttpResponse indicesResponse = client.get("/_cat/indices?expand_wildcards=all");

            log.debug("Indices response status code '{}' and body '{}'", indicesResponse.getStatusCode(), indicesResponse.getBody());
            assertThat(indicesResponse.getBody(), not(containsString(".signals_watch_instances")));
        }
    }

    @Test
    public void shouldDeleteWatchInstance() throws Exception {
        String watchId = "my-watch-to-be-deleted";
        String instanceId = "instance_id_should_use_nested_values_in_map";
        String path = instancePath(watchId, instanceId);
        String watchInstanceStatePath = path + "/_state";
        createGenericWatch(DEFAULT_TENANT, watchId, "message");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            HttpResponse response = client.putJson(path, DocNode.of("message", "please do not delete me!" ));
            log.debug("Create watch instance response '{}'.", response.getBody());
            response = client.get(path);
            log.debug("Get generic watch instance parameters status code '{}'response '{}'.", response.getStatusCode(),  response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Awaitility.await("watch instance state") //
                .timeout(3, SECONDS) //
                .until(() -> client.get(watchInstanceStatePath).getStatusCode(), equalTo(SC_OK));

            response = client.delete(path);

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            response = client.get(path);
            assertThat(response.getStatusCode(), equalTo(SC_NOT_FOUND));
            // should delete state with instance
            Awaitility.await("watch instance state deletion") //
                .timeout(3, SECONDS) //
                .until(() -> client.get(watchInstanceStatePath).getStatusCode(), equalTo(SC_NOT_FOUND));
        }
    }

    @Test
    public void shouldCreateThreeWatchInstanceWithUsageOfBulkRequest() throws Exception {
        String watchId = "watch-with-many-instances-create-three";
        String path = allInstancesPath(watchId);
        createGenericWatch(DEFAULT_TENANT, watchId, "param_name_1", "param_name_2");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            final String instanceIdOne = "instance_id_one";
            final String instanceIdTwo = "instance_id_two";
            final String instanceIdThree = "instance_id_three";
            DocNode requestBody = DocNode.of(
                instanceIdOne, DocNode.of("param_name_1", "param-value-1", "param_name_2", 1),
                instanceIdTwo, DocNode.of("param_name_1", "param-value-2", "param_name_2", 2),
                instanceIdThree, DocNode.of("param_name_1", "param-value-3", "param_name_2", 3));

            HttpResponse response = client.putJson(path, requestBody);

            log.debug("Create multiple watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            response = client.get(path + "/" + instanceIdOne);
            log.debug("Stored watch instance '{}' parameters '{}'.", instanceIdOne, response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThat(response.getBodyAsDocNode(), containsValue("data.param_name_1", "param-value-1"));
            assertThat(response.getBodyAsDocNode(), containsValue("data.param_name_2", 1));
            response = client.get(path + "/" + instanceIdTwo);
            log.debug("Stored watch instance '{}' parameters '{}'.", instanceIdTwo, response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThat(response.getBodyAsDocNode(), containsValue("data.param_name_1", "param-value-2"));
            assertThat(response.getBodyAsDocNode(), containsValue("data.param_name_2", 2));
            response = client.get(path + "/" + instanceIdThree);
            log.debug("Stored watch instance '{}' parameters '{}'.", instanceIdThree, response.getBody());
            assertThat(response.getBodyAsDocNode(), containsValue("data.param_name_1", "param-value-3"));
            assertThat(response.getBodyAsDocNode(), containsValue("data.param_name_2", 3));

            // verify watch states of each created instance
            String watchInstanceStatePathOne = path + "/" + instanceIdOne + "/_state";
            Awaitility.await("first watch instance state").timeout(3, SECONDS) //
                .until(() -> client.get(watchInstanceStatePathOne).getStatusCode(), equalTo(SC_OK));
            response = client.get(watchInstanceStatePathOne);
            assertThat(response.getBodyAsDocNode(), containsValue("$.instance_id", instanceIdOne));
            assertThat(response.getBodyAsDocNode(), containsValue("$.parent_generic_watch_id", watchId));
            String watchInstanceStatePathTwo = path + "/" + instanceIdTwo + "/_state";
            Awaitility.await("second watch instance state").timeout(3, SECONDS) //
                .until(() -> client.get(watchInstanceStatePathTwo).getStatusCode(), equalTo(SC_OK));
            response = client.get(watchInstanceStatePathTwo);
            assertThat(response.getBodyAsDocNode(), containsValue("$.instance_id", instanceIdTwo));
            assertThat(response.getBodyAsDocNode(), containsValue("$.parent_generic_watch_id", watchId));
            String watchInstanceStatePathThree = path + "/" + instanceIdThree + "/_state";
            Awaitility.await("third watch instance state").timeout(3, SECONDS) //
                .until(() -> client.get(watchInstanceStatePathThree).getStatusCode(), equalTo(SC_OK));
            response = client.get(watchInstanceStatePathThree);
            assertThat(response.getBodyAsDocNode(), containsValue("$.instance_id", instanceIdThree));
            assertThat(response.getBodyAsDocNode(), containsValue("$.parent_generic_watch_id", watchId));
        }
    }

    @Test
    public void shouldLoadExistingWatchInstances() throws Exception {
        String watchId = "watch-for-load-all-instance-test";
        String instanceId = "zero_instance";
        String path = allInstancesPath(watchId);
        createGenericWatch(DEFAULT_TENANT, watchId, "vm_id");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode requestBody = DocNode.of("vm_id", "param-value-0");
            client.putJson(path + "/" + instanceId , requestBody);
            requestBody = DocNode.of("first_instance", DocNode.of("vm_id", 1), "second_instance", DocNode.of("vm_id", 2));
            client.putJson(path , requestBody);

            HttpResponse response = client.get(path);

            log.debug("Get all watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("data.zero_instance.vm_id", "param-value-0"));
            assertThat(body, containsValue("data.first_instance.vm_id", 1));
            assertThat(body, containsValue("data.second_instance.vm_id", 2));
        }
    }

    @Test
    public void shouldNotExecuteGenericWatch() throws Exception {
        String watchId = "watch-generic-watch-should-not-be-executed";
        String watchPath = watchPath(watchId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources(); Client client = cluster.getInternalNodeClient()) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true).cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).throttledFor("1h").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));

            Thread.sleep(1200);
            assertThat(countDocumentInIndex(client, destinationIndex), equalTo(0L));
        }
    }

    @Test
    public void shouldExecuteGenericWatchInstance() throws Exception {
        String watchId = "watch-id-generic-watch-instance-should-be-executed";
        String watchPath = watchPath(watchId);
        String parametersPath = allInstancesPath(watchId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()) {

            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "instance_parameter")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).throttledFor("1h").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));

            response = restClient.putJson(parametersPath, DocNode.of("watch_instance_id", DocNode.of("instance_parameter", 7)));

            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS).until(() -> countDocumentInIndex(client, destinationIndex) > 0);
            // instance id should be present in watch state after execution
            String watchInstanceStatePath = parametersPath + "/watch_instance_id/_state";
            Awaitility.await("watch instance state").timeout(3, SECONDS) //
                .until(() -> restClient.get(watchInstanceStatePath).getStatusCode(), equalTo(SC_OK));
            response = restClient.get(watchInstanceStatePath);
            log.debug("Watch state response code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.instance_id", "watch_instance_id"));
            assertThat(body, containsValue("$.parent_generic_watch_id", watchId));
        }
    }

    @Test
    public void shouldUseItsOwnInstanceParameterValueAndStoreWatchExecutionState() throws Exception {
        String watchId = "watch-should-use-own-generic-watch-instance-parameter-value";
        String watchPath = watchPath(watchId);
        String parametersPath = allInstancesPath(watchId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources(); //
            Client client = cluster.getInternalNodeClient(); //
            MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook")
        ){
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            DocNode webhookBody = DocNode.of("Color","{{instance.color}}", "Shp", "{{instance.shape}}",
                "Opacity","{{instance.transparency}}", "CreatorInstanceId", "{{instance.id}}");
            Watch watch = new WatchBuilder(watchId).instances(true, "value", "color", "shape", "transparency")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex)
                .transform(null, "['name':instance.value, 'my_instance_id':instance.id, 'Color':instance.color, 'Shp':instance.shape, 'Opacity':instance.transparency]")
                .throttledFor("1h").name("testsink")
                .and().postWebhook(webhookProvider.getUri()).body(webhookBody).throttledFor("1h").name("webhook-action-name")
                .build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            String instanceIdOne = "first_instance_id";
            String instanceIdTwo = "second_instance_id";
            DocNode node = DocNode.of(instanceIdOne, DocNode.of("value", "one", "color", "black", "shape", "rectangular", "transparency", "minimal"),
                instanceIdTwo, DocNode.of("value", "two", "color", "blue", "shape", "round", "transparency", "full"));

            // create two instances
            response = restClient.putJson(parametersPath, node);

            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await("first watch execution").atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "one") > 0);
            Awaitility.await("second watch execution").atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "two") > 0);
            // both watches create documents, so wait is not needed for further checks on documents
            long documentCount = countDocumentWithTerm(client, destinationIndex, "my_instance_id.keyword", instanceIdOne);
            assertThat(documentCount, greaterThan(0L));
            documentCount = countDocumentWithTerm(client, destinationIndex, "my_instance_id.keyword", instanceIdTwo);
            assertThat(documentCount, greaterThan(0L));
            documentCount = countDocumentWithTerm(client, destinationIndex, "Color.keyword", "black");
            assertThat(documentCount, greaterThan(0L));
            documentCount = countDocumentWithTerm(client, destinationIndex, "Color.keyword", "blue");
            assertThat(documentCount, greaterThan(0L));
            documentCount = countDocumentWithTerm(client, destinationIndex, "Shp.keyword", "rectangular");
            assertThat(documentCount, greaterThan(0L));
            documentCount = countDocumentWithTerm(client, destinationIndex, "Shp.keyword", "round");
            assertThat(documentCount, greaterThan(0L));
            documentCount = countDocumentWithTerm(client, destinationIndex, "Opacity.keyword", "minimal");
            assertThat(documentCount, greaterThan(0L));
            documentCount = countDocumentWithTerm(client, destinationIndex, "Opacity.keyword", "full");
            assertThat(documentCount, greaterThan(0L));
            Awaitility.await("webhook action") //
                .atMost(3, SECONDS) //
                .until(() -> (webhookProvider.containRecentRequestWithBody(instanceIdOne) && webhookProvider.containRecentRequestWithBody(
                    instanceIdTwo)));
            String webhookRequestBody = webhookProvider.getRecentRequestBodies() //
                .stream().filter(body -> body.contains(instanceIdOne)) //
                .findFirst() //
                .orElseThrow(() -> new IllegalStateException("Missing first watch web hook body"));
            DocNode webhookDocNode = DocNode.parse(Format.JSON).from(webhookRequestBody);
            assertThat(webhookDocNode.size(), equalTo(4));
            assertThat(webhookDocNode, containsValue("Color", "black"));
            assertThat(webhookDocNode, containsValue("Shp", "rectangular"));
            assertThat(webhookDocNode, containsValue("Opacity", "minimal"));
            assertThat(webhookDocNode, containsValue("CreatorInstanceId", instanceIdOne));
            webhookRequestBody = webhookProvider.getRecentRequestBodies() //
                .stream().filter(body -> body.contains(instanceIdTwo)) //
                .findFirst() //
                .orElseThrow(() -> new IllegalStateException("Missing first watch web hook body"));
            webhookDocNode = DocNode.parse(Format.JSON).from(webhookRequestBody);
            assertThat(webhookDocNode.size(), equalTo(4));
            assertThat(webhookDocNode, containsValue("Color", "blue"));
            assertThat(webhookDocNode, containsValue("Shp", "round"));
            assertThat(webhookDocNode, containsValue("Opacity", "full"));
            assertThat(webhookDocNode, containsValue("CreatorInstanceId", instanceIdTwo));

            response = restClient.get(String.format("/_signals/watch/%s/%s/instances/%s/_state", DEFAULT_TENANT, watchId, instanceIdOne));
            log.debug("Watch state response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            final String watchInstanceIdSeparator = "/instances/";
            assertThat(body, containsValue("last_execution.watch.id", watchId + watchInstanceIdSeparator + instanceIdOne));
            assertThat(body, containsValue("last_execution.data.testsearch.hits.total.value", 3));
            assertThat(body, containsValue("$.instance_id", instanceIdOne));
            assertThat(body, containsValue("$.parent_generic_watch_id", watchId));

            response = restClient.get(String.format("/_signals/watch/%s/%s/instances/%s/_state", DEFAULT_TENANT, watchId, instanceIdTwo));
            log.debug("Watch state response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            body = response.getBodyAsDocNode();
            assertThat(body, containsValue("last_execution.watch.id", watchId + watchInstanceIdSeparator + instanceIdTwo));
            assertThat(body, containsValue("last_execution.data.testsearch.hits.total.value", 3));
            assertThat(body, containsValue("$.instance_id", instanceIdTwo));
            assertThat(body, containsValue("$.parent_generic_watch_id", watchId));
        }
    }

    @Test
    public void shouldDeleteGenericWatchWithManyInstances() throws Exception {
        String watchId = "my-watch-delete-generic-watch-with-many-instances";
        String watchPath = createGenericWatch(DEFAULT_TENANT, watchId, "parameter");
        String parametersPath = allInstancesPath(watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN);
            Client nodeClient = cluster.getPrivilegedInternalNodeClient()) {
            DocNode node = DocNode.EMPTY;
            final int numberOfInstances = 5;
            for(int i = 0; i < numberOfInstances; ++i) {
                node = node.with(DocNode.of("instance_id_" + i, DocNode.of("parameter", "value")));
            }
            HttpResponse response = client.putJson(parametersPath, node);
            log.debug("Create generic watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await("store instances state in db") //
                .atMost(3, SECONDS) //
                .until(() -> countDocumentWithTerm(nodeClient, ".signals_watches_state", "parent_generic_watch_id", watchId) == numberOfInstances);

            response = client.delete(watchPath);

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Awaitility.await().atMost(3, SECONDS).until(() -> {
                HttpResponse instancesResponse = client.get(parametersPath);
                return SC_NOT_FOUND == instancesResponse.getStatusCode();
            });
            Awaitility.await("instances state deletion") //
                .atMost(3, SECONDS) //
                .until(() -> countDocumentWithTerm(nodeClient, ".signals_watches_state", "parent_generic_watch_id", watchId) == 0);
        }
    }

    @Test
    public void shouldDeleteGenericWatchWithoutInstances() throws Exception {
        String watchId = "my-watch-delete-generic-watch-without-instances";
        String watchPath = createGenericWatch(DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {

            HttpResponse response = client.delete(watchPath);

            assertThat(response.getStatusCode(), equalTo(SC_OK));
        }
    }

    /**
     * The test check if it is possible to run more watch instances then the default ES page size which is equal to 10.
     * The ensure that loading watch parameters works correctly. The test consumes a lot of resources.
     */
    @Test
    public void shouldRunManyWatchInstances() throws Exception {
        String watchId = "my-watch-should-run-many-watch-instances";
        String watchPath = String.format("/_signals/watch/%s/%s", DEFAULT_TENANT, watchId);
        String parametersPath = allInstancesPath(watchId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()
        ) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "name").atMsInterval(5000).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['name':instance.name]")//
                .throttledFor("1h").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            List<String> nameParameterValues = new ArrayList<>();
            DocNode node = DocNode.EMPTY;
            for(int i = 0; i < (WatchInstancesRepository.WATCH_PARAMETER_DATA_PAGE_SIZE + 5); ++i) {
                String name = "my_name_is_" + i;
                nameParameterValues.add(name);
                node = node.with(DocNode.of("instance_id_" + i, DocNode.of("name", name)));
            }
            //this line should run watch instances
            response = restClient.putJson(parametersPath, node);

            log.debug("Create watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            for(String parameterValue : nameParameterValues) {
                Awaitility.await().atMost(3, SECONDS)
                    .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", parameterValue) > 0);
            }
        }
    }

    @Test
    public void shouldLoadManyInstanceParameters() throws Exception {
        String watchId = "my-watch-should-create-significant-number-of-instances";
        String watchPath = createGenericWatch(DEFAULT_TENANT, watchId, "parameter");
        String parametersPath = allInstancesPath(watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            try {
                DocNode node = DocNode.EMPTY;
                final int numberOfInstances = WatchInstancesRepository.WATCH_PARAMETER_DATA_PAGE_SIZE * 2;
                Function<Integer, String> createInstanceId = i -> "instance_id_" + i;
                for (int i = 0; i < numberOfInstances; ++i) {
                    String instanceId = createInstanceId.apply(i);
                    node = node.with(DocNode.of(instanceId, DocNode.of("parameter", "value_" + i)));
                }
                HttpResponse response = client.putJson(parametersPath, node);
                log.debug("Create generic watch instance response '{}'.", response.getBody());
                assertThat(response.getStatusCode(), equalTo(SC_CREATED));

                response = client.get(parametersPath);

                log.debug("Get many parameters response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
                assertThat(response.getStatusCode(), equalTo(SC_OK));
                DocNode body = response.getBodyAsDocNode();
                Set<String> existingInstancesIds = body.getAsNode("data").keySet();
                for (int i = 0; i < numberOfInstances; ++i) {
                    String instanceId = createInstanceId.apply(i);
                    assertThat(existingInstancesIds.contains(instanceId), equalTo(true));
                }
            } finally {
                client.delete(watchPath);
            }
        }
    }

    @Test
    public void shouldUpdateParameterValue() throws Exception {
        String watchId = "my-watch-should-update-instance-parameter-value";
        String watchPath = String.format("/_signals/watch/%s/%s", DEFAULT_TENANT, watchId);
        String instanceId = "update_my_parameters_id";
        String instancePath = instancePath(watchId, instanceId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN);
            Client client = cluster.getInternalNodeClient()
        ) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "name")//
                .atMsInterval(500).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['name':instance.name]")//
                .throttledFor("1s").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            response = restClient.putJson(instancePath, DocNode.of("name", "initial_value"));
            log.debug("Create watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "initial_value") > 0);

            response = restClient.putJson(instancePath, DocNode.of("name", "updated_value"));


            log.debug("Update instances response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            response = restClient.get(watchPath + "/instances");
            log.debug("Getting instance parameters after update, status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Awaitility.await().atMost(3, SECONDS) //
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "updated_value") > 0);
        }
    }

    @Test
    public void shouldUpdateGenericWatchDefinition() throws Exception {
        String watchId = "my-watch-should-update-generic-watch";
        String watchPath = String.format("/_signals/watch/%s/%s", DEFAULT_TENANT, watchId);
        String instanceId = "update_my_parameters_id";
        String instancePath = instancePath(watchId, instanceId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN);
            Client client = cluster.getInternalNodeClient()
        ) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "name")//
                .atMsInterval(500).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['name':instance.name]")//
                .throttledFor("1s").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            response = restClient.putJson(instancePath, DocNode.of("name", "parameter_value"));
            log.debug("Create watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "parameter_value") > 0);
            watch = new WatchBuilder(watchId).instances(true, "name").atMsInterval(500).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['surname':instance.name]")//
                .throttledFor("1s").name("testsink").build();


            response = restClient.putJson(watchPath, watch);
            log.debug("Update generic watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Awaitility.await().atMost(3, SECONDS)//
                .until(() -> countDocumentWithTerm(client, destinationIndex, "surname.keyword", "parameter_value") > 0);
        }
    }

    @Test
    public void shouldUpdateMultipleInstanceParametersWithBulkRequest() throws Exception {
        String watchId = "my-watch-should-update-multiple-parameters-with-bulk-request";
        String watchPath = String.format("/_signals/watch/%s/%s", DEFAULT_TENANT, watchId);
        String instancesPath = watchPath + "/instances";
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()
        ) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "name").atMsInterval(500).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['name':instance.name]")//
                .throttledFor("1s").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of("first_instance", DocNode.of("name", "Dave"), "second_instance", DocNode.of("name", "Dan"));
            response = restClient.putJson(instancesPath, node);
            log.debug("Create watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "Dave") > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "Dan") > 0);
            node = DocNode.of("first_instance", DocNode.of("name", "Angela"), "second_instance", DocNode.of("name", "Daisy"));

            response = restClient.putJson(instancesPath, node);

            log.debug("Update watch instance parameters response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "Angela") > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "Daisy") > 0);
        }
    }

    @Test
    public void shouldUpdateAndCreateNewInstanceViaBulkRequest() throws Exception {
        String watchId = "my-watch-should-update-and-create-new-instance-with-bulk-request";
        String watchPath = String.format("/_signals/watch/%s/%s", DEFAULT_TENANT, watchId);
        String instancesPath = watchPath + "/instances";
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()
        ) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "name")//
                .atMsInterval(100).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['name':instance.name, 'watch':instance.id]")//
                .name("testsink").throttledFor("50ms").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of("first_instance", DocNode.of("name", "Dave"), "second_instance", DocNode.of("name", "Dan"));
            response = restClient.putJson(instancesPath, node);
            log.debug("Create watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "Dave") > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "Dan") > 0);
            node = DocNode.of("new_instance", DocNode.of("name", "Angela"), "second_instance", DocNode.of("name", "Daisy"));

            response = restClient.putJson(instancesPath, node);

            log.debug("Update watch instance parameters response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            long numberOfDanDocuments = countDocumentWithTerm(client, destinationIndex, "name.keyword", "Dan");
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "Angela") > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "Daisy") > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "Dan") > numberOfDanDocuments);
        }
    }

    @Test
    @Ignore // the test requires 12s sleep time
    public void shouldAssignThrottleSettingsToWatchInstance() throws Exception {
        String watchId = "my-watch-should-update-and-create-new-instance-with-bulk-request";
        String watchPath = String.format("/_signals/watch/%s/%s", DEFAULT_TENANT, watchId);
        String instancesPath = watchPath + "/instances";
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()
        ) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "name") //
                .atMsInterval(100).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['name':instance.name, 'watch':instance.id]")//
                .name("testsink").throttledFor("2000ms").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of("first_instance", DocNode.of("name", "Dave"), "second_instance", DocNode.of("name", "2"),
                "third_instance", DocNode.of("name", "3"), "fourth_instance", DocNode.of("name", "4"), "fivth_instance", DocNode.of("name", "5"));

            response = restClient.putJson(instancesPath, node);

            log.debug("Create watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Thread.sleep(12000);
            long numberOfDocuments = countDocumentInIndex(client, destinationIndex);
            log.debug("Number of document created after sleep is equal to '{}'.", numberOfDocuments);
            assertThat(numberOfDocuments, greaterThan(25L));
        }
    }

    @Test
    public void shouldNotCreateGenericWatchWithIncorrectParameterName() throws Exception {
        String watchId = "my-watch-with-incorrect-parameter-name";
        String watchPath = String.format("/_signals/watch/%s/%s", DEFAULT_TENANT, watchId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()
        ) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "invalid-name").atMsInterval(100).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['name':instance.name, 'watch':instance.id]")//
                .name("testsink").throttledFor("2000ms").build();

            HttpResponse response = restClient.putJson(watchPath, watch);

            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_BAD_REQUEST));
            assertThat(response.getBodyAsDocNode(), containsValue("$.detail['params.0'][0].error", "Invalid value"));
            assertThat(response.getBodyAsDocNode(), containsValue("$.detail['params.0'][0].value", "invalid-name"));
        }
    }

    @Test
    public void shouldUpdateGenericWatchWhenParameterListIsTheSame() throws Exception {
        String watchId = "my-watch-should-update-watch-when-parameters-list-is-same";
        String watchPath = String.format("/_signals/watch/%s/%s", DEFAULT_TENANT, watchId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()
        ) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "name") //
                .atMsInterval(Long.MAX_VALUE).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['name':instance.name, 'watch':instance.id]")//
                .name("testsink").throttledFor("10h").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            watch = new WatchBuilder(watchId).instances(true, "name") //
                .atMsInterval(Long.MAX_VALUE).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['name':instance.name, 'watch':instance.id]")//
                .name("testsinkUpdated").throttledFor("10h").build();

            response = restClient.putJson(watchPath, watch);

            log.debug("Update watch response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            response = restClient.get(watchPath);
            log.debug("Get watch status code '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("_source.actions[0].name", "testsinkUpdated"));
        }
    }

    @Test
    public void shouldNotUpdateGenericWatchWhenParameterListWasChanged() throws Exception {
        String watchId = "my-watch-should-not-update-watch-when-parameters-list-was-changed";
        String watchPath = String.format("/_signals/watch/%s/%s", DEFAULT_TENANT, watchId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()
        ) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "name") //
                .atMsInterval(Long.MAX_VALUE).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['name':instance.name, 'watch':instance.id]")//
                .name("testsink").throttledFor("10h").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            watch = new WatchBuilder(watchId).instances(true, "name", "new_parameter") //
                .atMsInterval(Long.MAX_VALUE).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['name':instance.name, 'watch':instance.id]")//
                .name("testsinkUpdated").throttledFor("10h").build();

            response = restClient.putJson(watchPath, watch);

            log.debug("Update watch response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_BAD_REQUEST));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containSubstring("error", "instances.params"));
            assertThat(body, containSubstring("error", "name"));
            assertThat(body, containSubstring("error", "Watch is invalid"));
            assertThat(body, containSubstring("error", "has distinct instance parameters list"));
        }
    }

    @Test
    public void shouldAllowUsageOfInstanceSeparatorInNonGenericWatchId() throws Exception {
        String watchId = "my+watch+id+with+plus%2Finstances%2Fsign";
        String watchPath = String.format("/_signals/watch/%s/%s", DEFAULT_TENANT, watchId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()
        ) {
            final String destinationIndex = ("destination-index-for-" + watchId).toLowerCase();
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(false ) //
                .atMsInterval(Long.MAX_VALUE).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['name':instance.name, 'watch':instance.id]")//
                .name("testsink").throttledFor("10h").build();

            HttpResponse response = restClient.putJson(watchPath, watch);

            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
        }
    }

    @Test
    public void shouldNotAllowUsageOfInstanceSeparatorInGenericWatchId() throws Exception {
        String watchId = "my-generic-watch-id-with-instance-separator-%2Finstances%2F";
        String watchPath = String.format("/_signals/watch/%s/%s", DEFAULT_TENANT, watchId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()
        ) {
            final String destinationIndex = "destination-index-for-instance-separator-test";
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "name") //
                .atMsInterval(Long.MAX_VALUE).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['name':instance.name, 'watch':instance.id]")//
                .name("testsink").throttledFor("10h").build();

            HttpResponse response = restClient.putJson(watchPath, watch);

            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_BAD_REQUEST));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containSubstring("error", "Generic watch id cannot contain '/instances/' string."));
        }
    }

    @Test
    public void shouldStopAndResumeAllInstancesExecution() throws Exception {
        String watchId = "watch-id-should-stop-all-instances-execution-when-generic-watch-is-de-activated";
        String watchPath = watchPath(watchId);
        String instancesPath = allInstancesPath(watchId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "instance_parameter")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['created_by':instance.id]")//
                .throttledFor("1ms").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of("instance_id_1", DocNode.of("instance_parameter", 7),
                "instance_id_2", DocNode.of("instance_parameter", 8),
                "instance_id_3", DocNode.of("instance_parameter", 9));
            response = restClient.putJson(instancesPath, node);
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await("watch execution").atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex) > 0);

            // disable generic watch
            response = restClient.delete(watchPath + "/_active");

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Thread.sleep(1000);// wait till generic watch is disabled
            long previousNumberOfExecution = countDocumentInIndex(client, destinationIndex);
            Thread.sleep(3000);
            //make sure that watch is not executed because generic watch is deactivate
            long currentNumberOfExecution = countDocumentInIndex(client, destinationIndex);
            assertThat(currentNumberOfExecution, equalTo(previousNumberOfExecution));

            // let's resume watch execution
            response = restClient.put(watchPath + "/_active");
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Awaitility.await("resume watch execution").atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex) > currentNumberOfExecution);

            response = restClient.delete(watchPath);
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            // watch should be not executed after deletion
            Thread.sleep(1000);
            long countOfDocumentsJustAfterDeletion = countDocumentInIndex(client, destinationIndex);
            Thread.sleep(2000);
            long countOfDocumentsAfterDeletion = countDocumentInIndex(client, destinationIndex);
            assertThat(countOfDocumentsAfterDeletion, equalTo(countOfDocumentsJustAfterDeletion));
        }
    }

    @Test
    public void shouldDisableAndEnableWatchInstance() throws Exception {
        String watchId = "watch-id-should-resume-instance-execution-when-instance-is-activated";
        String watchPath = watchPath(watchId);
        String instanceId = "instance_to_be_disabled_and_enabled";
        String instancePath = instancePath(watchId, instanceId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "instance_parameter")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).throttledFor("1ms").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            response = restClient.putJson(instancePath, DocNode.of("instance_parameter", 7));
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex) > 0);
            response = restClient.delete(instancePath + "/_active");
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Thread.sleep(1000);
            long previousCountOfDocuments = countDocumentInIndex(client, destinationIndex);
            Thread.sleep(2000);
            long currentCountOfDocuments = countDocumentInIndex(client, destinationIndex);
            assertThat(currentCountOfDocuments, equalTo(previousCountOfDocuments));

            response = restClient.put(instancePath + "/_active");

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex) > previousCountOfDocuments);
        }
    }

    @Test
    public void shouldNotExecuteGenericWatchAsAnonymousWatch() throws Exception {
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()) {
            final String destinationIndex = "destination-index-for-anonymous-watch";
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder("anonymous-watch").instances(true, "instance_parameter")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).throttledFor("1ms").name("testsink").build();
            String watchJson = watch.toJson();
            DocNode executeWatchRequest = DocNode.of("watch", DocNode.parse(Format.JSON).from(watchJson));
            String executePath = "/_signals/watch/" + DEFAULT_TENANT + "/_execute";
            String body = executeWatchRequest.toJsonString();
            log.debug("Execute endpoint will receive the following watch definition '{}'.", body);

            HttpResponse response = restClient.postJson(executePath, body);

            log.debug("Execute watch response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CONFLICT));
            DocNode responseBody = response.getBodyAsDocNode();
            assertThat(responseBody, containsValue("$.error", "Generic watch is not executable."));
        }
    }

    @Test
    public void shouldExecuteGenericWatchInstanceViaRestEndpoint() throws Exception {
        String watchId = "watch-should-execute-generic-watch-instance-via-rest-endpoint";
        String watchPath = watchPath(watchId);
        String instanceId = "execute_me_via_rest_api";
        String instancePath = instancePath(watchId, instanceId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient();
            MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook")
        ){
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            DocNode webhookBody = DocNode.of("Color","{{instance.color}}", "Shp", "{{instance.shape}}", "Opacity","{{instance.transparency}}");
            Watch watch = new WatchBuilder(watchId).instances(true, "color", "shape", "transparency")//
                .cronTrigger(CRON_ALMOST_NEVER).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch").then()//
                .postWebhook(webhookProvider.getUri()).body(webhookBody).throttledFor("1h").name("webhook-action-name")
                .build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of("color", "black", "shape", "rectangular", "transparency", "minimal");
            response = restClient.putJson(instancePath, node);
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));

            response = restClient.post(instancePath + "/_execute");

            log.debug("Execute watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Awaitility.await().atMost(3, SECONDS).until(() -> webhookProvider.getRequestCount() > 0);
            DocNode lastRequestBody =  webhookProvider.getLastRequestBodyAsDocNode();
            assertThat(lastRequestBody.size(), equalTo(3));
            assertThat(lastRequestBody, containsValue("Color", "black"));
            assertThat(lastRequestBody, containsValue("Shp", "rectangular"));
            assertThat(lastRequestBody, containsValue("Opacity", "minimal"));
        }
    }

    @Test
    public void shouldExecuteSingleInstanceWatchViaRestEndpoint() throws Exception {
        String watchId = "watch-should-execute-single-instance-watch-via-rest-endpoint";
        String watchPath = watchPath(watchId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient();
            MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook")
        ){
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            DocNode webhookBody = DocNode.of("Color","black", "Shp", "rectangular", "Opacity","minimal");
            Watch watch = new WatchBuilder(watchId).instances(false )//
                .cronTrigger(CRON_ALMOST_NEVER).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch").then()//
                .postWebhook(webhookProvider.getUri()).body(webhookBody).throttledFor("1h").name("webhook-action-name") //
                .build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));

            response = restClient.post(watchPath + "/_execute");

            log.debug("Execute watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Awaitility.await().atMost(3, SECONDS).until(() -> webhookProvider.getRequestCount() > 0);
            DocNode lastRequestBody =  webhookProvider.getLastRequestBodyAsDocNode();
            assertThat(lastRequestBody.size(), equalTo(3));
            assertThat(lastRequestBody, containsValue("Color", "black"));
            assertThat(lastRequestBody, containsValue("Shp", "rectangular"));
            assertThat(lastRequestBody, containsValue("Opacity", "minimal"));
        }
    }

    @Test
    public void shouldNotExecuteWatchWhichDoesNotExist() throws Exception {
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources()) {

            HttpResponse response = restClient.post(watchPath("does-not-exist") + "/_execute");

            log.debug("Execute non existing watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_NOT_FOUND));
            assertThat(response.getBodyAsDocNode(), containsValue("error", "No watch with id does-not-exist"));
        }
    }

    @Test
    public void shouldNotExecuteGenericWatchInstanceWhichDoesNotExist() throws Exception {
        String watchId = "watch-should-not-execute-generic-watch-instance-via-rest-if-instance-does-not-exist";
        String watchPath = watchPath(watchId);
        String instanceId = "execute_me_via_rest_api";
        String instanceIdDoesNotExists = "this_instance_does_not_exist";
        String instancePath = instancePath(watchId, instanceId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient();
            MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook")
        ){
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            DocNode webhookBody = DocNode.of("Color","{{instance.color}}", "Shp", "{{instance.shape}}", "Opacity","{{instance.transparency}}");
            Watch watch = new WatchBuilder(watchId).instances(true, "color", "shape", "transparency")//
                .cronTrigger(CRON_ALMOST_NEVER).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch").then()//
                .postWebhook(webhookProvider.getUri()).body(webhookBody).throttledFor("1h").name("webhook-action-name")
                .build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of("color", "black", "shape", "rectangular", "transparency", "minimal");
            response = restClient.putJson(instancePath, node);
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));

            response = restClient.post(instancePath(watchId, instanceIdDoesNotExists) + "/_execute");

            log.debug("Execute watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_NOT_FOUND));
            assertThat(response.getBodyAsDocNode(), containSubstring("$.error", "Generic watch"));
            assertThat(response.getBodyAsDocNode(), containSubstring("$.error", "not found"));
            assertThat(response.getBodyAsDocNode(), containSubstring("$.error", instanceIdDoesNotExists));
            assertThat(response.getBodyAsDocNode(), containSubstring("$.error", watchId));
            Thread.sleep(1000);
            assertThat(webhookProvider.getRequestCount(), equalTo(0));
        }
    }

    @Test
    public void shouldReportErrorsWhenGenericWatchInstanceIsExecutedViaRestEndpoint() throws Exception {
        String watchId = "watch-should-report-errors-when-watch-instance-is-executed-via-rest-endpoint";
        String watchPath = watchPath(watchId);
        String instanceId = "i_am_buggy";
        String instancePath = instancePath(watchId, instanceId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()
        ) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            DocNode webhookBody = DocNode.of("Color", "{{instance.color}}", "Shp", "{{instance.shape}}", "Opacity", "{{instance.transparency}}");
            Watch watch = new WatchBuilder(watchId).instances(true, "color", "shape", "transparency")//
                .cronTrigger(CRON_ALMOST_NEVER).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch").then()//
                .postWebhook("https://localhost:1/not_existing_endpoint").body(webhookBody).throttledFor("1h").name("webhook-action-name")
                .build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of("color", "black", "shape", "rectangular", "transparency", "minimal");
            response = restClient.putJson(instancePath, node);
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));

            response = restClient.post(instancePath + "/_execute");

            log.debug("Execute watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThat(response.getBodyAsDocNode(), containsValue("status.code", "ACTION_FAILED"));
        }
    }

    @Test
    public void shouldReportErrorWhenExecuteWatchRequestDoesNotContainWatchDefinitionOrWatchId() throws Exception {
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources()){
            String executeWatchPath = "/_signals/watch/" + DEFAULT_TENANT + "/_execute";

            HttpResponse response = restClient.postJson(executeWatchPath, DocNode.EMPTY);

            log.debug("Execute watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_BAD_REQUEST));
            DocNode body = response.getBodyAsDocNode();
            String expectedErrorMessage = "The request body does not contain 'watch' attribute. Path param 'watch_id' is also missing. " +
                "Please provide one of these parameters.";
            assertThat(response.getBodyAsDocNode(), containsValue("$.error", expectedErrorMessage));
        }
    }

    @Test
    public void shouldNotCreateGenericWatchInstanceInCaseOfConflictWithNonGenericWatch() throws Exception {
        String watchId = "id-conflict";
        final String instanceId = "instance_id_one";
        String nonGenericWatchId = watchId + "%2Finstances%2F" + instanceId;
        String path = allInstancesPath(watchId);
        createWatch(DEFAULT_TENANT, nonGenericWatchId, false);
        createGenericWatch(DEFAULT_TENANT, watchId, "param_name");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {

            DocNode requestBody = DocNode.of(instanceId, DocNode.of("param_name", "param-value"));

            HttpResponse response = client.putJson(path, requestBody);

            log.debug("Create multiple watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_BAD_REQUEST));
            String errorJsonPath = "error.details['id-conflict/instances/instance_id_one'][0].error";
            String expectedErrorMessage = "Non generic watch with the same ID already exists.";
            assertThat(response.getBodyAsDocNode(), containsValue(errorJsonPath, expectedErrorMessage));
        }
    }

    @Test
    public void shouldNotCreateNonGenericWatchWhenNameConflictWithInstanceIsPossible() throws Exception {
        String genericWatchId = "future-id-conflict-possible";
        String nonGenericWatchId = genericWatchId + "%2Finstances%2F" + "no-conflict-at-the-moment";
        createGenericWatch(DEFAULT_TENANT, genericWatchId, "param_name");

        String watchPath = "/_signals/watch/" + DEFAULT_TENANT + "/" + nonGenericWatchId;
        try (GenericRestClient restClient = getAdminRestClient()) {
            Watch watch = new WatchBuilder(genericWatchId).cronTrigger(CRON_ALMOST_NEVER)//
                .search(INDEX_SOURCE).query("{\"match_all\" : {} }").as("testsearch")//
                .then().index("testsink").throttledFor("1h").name("testsink").build();
            String watchJson = watch.toJson();
            HttpResponse response = restClient.putJson(watchPath, watchJson);
            assertThat(response.getStatusCode(), equalTo(SC_BAD_REQUEST));
            String errorJsonPath = "detail.watchId[0].error";
            String expectedErrorMessage = "Possible ID conflict with one of generic watch instances";
            assertThat(response.getBodyAsDocNode(), containsValue(errorJsonPath, expectedErrorMessage));
        }
    }

    @Override
    protected GenericRestClient getAdminRestClient() {
        return cluster.getRestClient(USER_ADMIN);
    }
}