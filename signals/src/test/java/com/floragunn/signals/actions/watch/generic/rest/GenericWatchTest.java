package com.floragunn.signals.actions.watch.generic.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.MockWebserviceProvider;
import com.floragunn.signals.SignalsModule;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchParametersRepository;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.floragunn.searchguard.test.TestSgConfig.TenantPermission.ALL_TENANTS_AND_ACCESS;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containSubstring;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.http.HttpStatus.SC_BAD_REQUEST;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

public class GenericWatchTest {

    private static final Logger log = LogManager.getLogger(GenericWatchTest.class);

    private static final String DEFAULT_TENANT = "_main";
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
            client.index(new IndexRequest(INDEX_SOURCE).source(XContentType.JSON, "key1", "1", "key2", "2")).actionGet();
            client.index(new IndexRequest(INDEX_SOURCE).setRefreshPolicy(IMMEDIATE).source(XContentType.JSON, "key1", "3", "key2", "4")) //
                .actionGet();
            client.index(new IndexRequest(INDEX_SOURCE).setRefreshPolicy(IMMEDIATE).source(XContentType.JSON, "key1", "5", "key2", "6")) //
                .actionGet();
        }
    }

    @Test
    public void shouldCreateGenericWatchParameters() throws Exception {
        String watchId = "my-watch-create-generic-watch-parameters";
        String instanceId = "instance_id_should_create_generic_watch_parameters";
        String path = instancePath(watchId, instanceId);
        createGenericWatch(DEFAULT_TENANT, watchId, "vm_id");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("vm_id", 258);

            HttpResponse response = client.putJson(path, node.toJsonString());

            log.debug("Create generic watch instance response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
        }
    }

    @Test
    public void shouldNotCreateGenericWatchParametersWhenWatchDoesNotExist() throws Exception {
        String watchId = "my-watch-create-generic-watch-parameters-when-watch-does-not-exists";
        String instanceId = "instance_id_should_create_generic_watch_parameters";
        String path = instancePath(watchId, instanceId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("id", 258);

            HttpResponse response = client.putJson(path, node.toJsonString());

            log.debug("Create generic watch instance response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_NOT_FOUND));
        }
    }

    @Test
    public void shouldNotCreateGenericWatchParametersWhenWatchIsNotGeneric() throws Exception {
        String watchId = "my-watch-create-generic-watch-parameters-when-watch-is-not-generic";
        String instanceId = "instance_id_should_not_create_generic_watch_parameters";
        String path = instancePath(watchId, instanceId);
        createWatch(DEFAULT_TENANT, watchId, false);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("id", 258);

            HttpResponse response = client.putJson(path, node.toJsonString());

            log.debug("Create genetic watch response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_NOT_FOUND));
        }
    }

    @Test
    public void shouldNotCreateInstanceWhenGenericWatchDoesNotExist() throws Exception {
        String watchId = "my-watch-do-not-create-instance-parameters-when-generic-watch-does-not-exist";
        String instanceId = "instance_id_should_not_create_generic_watch_parameters";
        String notExistingTenantName = "tenant-does-not-exists";
        String path = String.format("/_signals/watch/%s/%s/instances/%s", notExistingTenantName, watchId, instanceId);
        createGenericWatch(DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("id", 258);

            HttpResponse response = client.putJson(path, node.toJsonString());

            log.debug("Create generic watch instance response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_NOT_FOUND));
        }
    }

    @Test
    public void shouldLoadGenericWatchParametersWithVariousDataTypes() throws Exception {
        String watchId = "my-watch-load-generic-watch-parameters-with-various-data-types";
        String instanceId = "instance_id_should_load_parameters";
        String path = instancePath(watchId, instanceId);
        createGenericWatch(DEFAULT_TENANT, watchId, "vm_id", "name", "time", "int", "long", "double", "bool");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("vm_id", 258, "name","kirk", "time", "2023-05-15T13:07:52.000Z")
                .with(DocNode.of("int", Integer.MAX_VALUE, "long", Long.MIN_VALUE, "double", Math.PI, "bool", false));
            HttpResponse response = client.putJson(path, node.toJsonString());
            log.debug("Create watch instance response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));

            response = client.get(path + "/parameters");

            log.debug("Get genetic watch instance parameters response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("data.vm_id", 258));
            assertThat(body, containsValue("data.name", "kirk"));
            assertThat(body, containsValue("data.time", "2023-05-15T13:07:52.000Z"));
            assertThat(body, containsValue("data.int", Integer.MAX_VALUE));
            assertThat(body, containsValue("data.long", Long.MIN_VALUE));
            assertThat(body, containsValue("data.double", Math.PI));
            assertThat(body, containsValue("data.bool", false));
        }
    }

    @Test
    public void shouldNotUseNestedValuesInList() throws Exception {
        String watchId = "my-watch-should-not-use-nested-value-list";
        String instanceId = "instance_id_should_use_nested_values_in_list";
        String path = instancePath(watchId, instanceId);
        createGenericWatch(DEFAULT_TENANT, watchId, "list");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            ImmutableMap<String, String> firstMap = ImmutableMap.of("one-key", "one-value", "two-key", "two-value");
            ImmutableMap<String, Integer> secondMap = ImmutableMap.of("three-key", 3, "four-key", 4);
            DocNode node = DocNode.of("list", Arrays.asList("one", firstMap, secondMap));

            HttpResponse response = client.putJson(path, node.toJsonString());

            log.debug("Create generic watch instance response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_BAD_REQUEST));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containSubstring("error.details['instance_id_should_use_nested_values_in_list.list[1]'][0]", "Forbidden parameter value type"));
            assertThat(body, containSubstring("error.details['instance_id_should_use_nested_values_in_list.list[1]'][0]", "Map"));
            assertThat(body, containSubstring("error.details['instance_id_should_use_nested_values_in_list.list[2]'][0]", "Forbidden parameter value type"));
            assertThat(body, containSubstring("error.details['instance_id_should_use_nested_values_in_list.list[2]'][0]", "Map"));
        }
    }

    @Test
    public void shouldNotUseNestedParameters() throws Exception {
        String watchId = "my-watch-should-not-use-nested-parameters";
        String instanceId = "instance_id_should_not_use_map";
        String path = instancePath(watchId, instanceId);
        createGenericWatch(DEFAULT_TENANT, watchId, "map_parameter");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("map_parameter", ImmutableMap.of("map", "is", "not", "allowed"));

            HttpResponse response = client.putJson(path, node.toJsonString());

            log.debug("Create generic watch with map response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_BAD_REQUEST));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containSubstring("error.message", "Forbidden parameter value type"));
            assertThat(body, containSubstring("error.message", "Map"));
        }
    }

    @Test
    public void shouldStoreParameterWithTheSameNameButWithVariousTypes() throws Exception {
        String instanceId = "instance_parameters";
        String watchId1 = "my-watch-parameters-with-same-name-but-various-types-one";
        String watchId2 = "my-watch-parameters-with-same-name-but-various-types-two";
        String watchId3 = "my-watch-parameters-with-same-name-but-various-types-three";
        String pathWatch1 = instancePath(watchId1, instanceId);
        String pathWatch2 = instancePath(watchId2, instanceId);
        String pathWatch3 = instancePath(watchId3, instanceId);
        createGenericWatch(DEFAULT_TENANT, watchId1, "param");
        createGenericWatch(DEFAULT_TENANT, watchId2, "param");
        createGenericWatch(DEFAULT_TENANT, watchId3, "param");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("param", "2023-05-15T13:07:52.000Z");
            HttpResponse response = client.putJson(pathWatch1, node.toJsonString());
            log.debug("Watch 1 instance parameters response '{}'.", response.getBody());
            node = DocNode.of("param", 10);
            response = client.putJson(pathWatch2, node.toJsonString());
            log.debug("Watch 2 instance parameters response '{}'.", response.getBody());
            node = DocNode.of("param", "ten");

            response = client.putJson(pathWatch3, node.toJsonString());

            log.debug("Watch 3 instance parameters response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
        }
    }

    @Test
    public void shouldHaveOwnParameterCopy() throws Exception {
        final String instanceId = "common_parameters";
        final String watchIdOne = "my-watch-own-parameter-copy-one";
        final String watchIdTwo = "my-watch-own-parameter-copy-two";
        final String pathWatch1 = instancePath(watchIdOne, instanceId);
        final String pathWatch2 = instancePath(watchIdTwo, instanceId);
        createGenericWatch(DEFAULT_TENANT, watchIdOne, "common_parameter_name");
        createGenericWatch(DEFAULT_TENANT, watchIdTwo, "common_parameter_name");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("common_parameter_name", "first-value");
            HttpResponse response = client.putJson(pathWatch1, node.toJsonString());
            log.debug("Watch 1 instance parameters response '{}'.", response.getBody());
            node = DocNode.of("common_parameter_name", "second-value");
            response = client.putJson(pathWatch2, node.toJsonString());
            log.debug("Watch 2 instance parameters response '{}'.", response.getBody());

            response = client.get(pathWatch1 + "/parameters");

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThat(response.getBodyAsDocNode(), containsValue("data.common_parameter_name", "first-value"));
            response = client.get(pathWatch2 + "/parameters");
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThat(response.getBodyAsDocNode(), containsValue("data.common_parameter_name", "second-value"));
        }
    }

    @Test
    public void shouldDeleteWatchInstance() throws Exception {
        String watchId = "my-watch-to-be-deleted";
        String instanceId = "instance_id_should_use_nested_values_in_map";
        String path = instancePath(watchId, instanceId);
        createGenericWatch(DEFAULT_TENANT, watchId, "message");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("message", "please do not delete me!" );
            HttpResponse response = client.putJson(path, node.toJsonString());
            log.debug("Create watch instance response '{}'.", response.getBody());
            response = client.get(path + "/parameters");
            log.debug("Get generic watch instance parameters response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));

            response = client.delete(path);

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            response = client.get(path + "/parameters");
            assertThat(response.getStatusCode(), equalTo(SC_NOT_FOUND));
        }
    }

    @Test
    public void shouldNotDeleteWatchInstanceWhichDoesNotExist() throws Exception {
        String watchId = "non-existing-watch-instance-to-be-deleted";
        String instanceId = "instance_id_should_use_nested_values_in_map";
        String path = instancePath(watchId, instanceId);
        createGenericWatch(DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {

            HttpResponse response = client.delete(path);

            assertThat(response.getStatusCode(), equalTo(SC_NOT_FOUND));
        }
    }

    @Test
    public void shouldReturnValidationErrorWhenWatchInstancesAreCreatedWithEmptyBody() throws Exception {
        String watchId = "watch-with-many-instances-empty-body";
        String path = allInstancesPath(watchId);
        createGenericWatch(DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode requestBody = DocNode.EMPTY;

            HttpResponse response = client.putJson(path, requestBody);

            log.debug("Create multiple watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(400));
        }
    }

    @Test
    public void shouldCreateSingleWatchInstanceWithUsageOfBulkRequest() throws Exception {
        String watchId = "watch-with-many-instances-one-parameter-set";
        String path = allInstancesPath(watchId);
        createGenericWatch(DEFAULT_TENANT, watchId, "param_name");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            final String instanceId = "instance_id_one";
            DocNode requestBody = DocNode.of(instanceId, DocNode.of("param_name", "param-value"));

            HttpResponse response = client.putJson(path, requestBody);

            log.debug("Create multiple watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            response = client.get(path + "/" + instanceId + "/parameters");
            log.debug("Stored watch instance parameters '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThat(response.getBodyAsDocNode(), containsValue("data.param_name", "param-value"));
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
            response = client.get(path + "/" + instanceIdOne + "/parameters");
            log.debug("Stored watch instance '{}' parameters '{}'.", instanceIdOne, response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThat(response.getBodyAsDocNode(), containsValue("data.param_name_1", "param-value-1"));
            assertThat(response.getBodyAsDocNode(), containsValue("data.param_name_2", 1));
            response = client.get(path + "/" + instanceIdTwo + "/parameters");
            log.debug("Stored watch instance '{}' parameters '{}'.", instanceIdTwo, response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThat(response.getBodyAsDocNode(), containsValue("data.param_name_1", "param-value-2"));
            assertThat(response.getBodyAsDocNode(), containsValue("data.param_name_2", 2));
            response = client.get(path + "/" + instanceIdThree + "/parameters");
            log.debug("Stored watch instance '{}' parameters '{}'.", instanceIdThree, response.getBody());
            assertThat(response.getBodyAsDocNode(), containsValue("data.param_name_1", "param-value-3"));
            assertThat(response.getBodyAsDocNode(), containsValue("data.param_name_2", 3));
        }
    }

    @Test
    public void shouldUpdateWatchInstance() throws Exception {
        String watchId = "watch-to-be-updated";
        final String instanceId = "instance_id_one";
        String path = instancePath(watchId, instanceId);
        createGenericWatch(DEFAULT_TENANT, watchId, "param_name_0");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode requestBody = DocNode.of("param_name_0", "param-value-0");
            HttpResponse response = client.putJson(path, requestBody);
            log.debug("Create watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            requestBody = DocNode.of("param_name_0", "param-value-updated");

            response = client.putJson(path, requestBody);

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            response = client.get(path + "/parameters");
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("data.param_name_0", "param-value-updated"));
        }
    }

    @Test
    public void shouldUpdateWatchInstancesViaBulkRequest() throws Exception {
        String watchId = "watch-to-be-updated-by-bulk-request";
        final String instanceOne = "instance_id_one";
        String instanceTwo = "new_instance_id_two";
        String path = allInstancesPath(watchId);
        createGenericWatch(DEFAULT_TENANT, watchId, "param_name_0");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode requestBody = DocNode.of("param_name_0", "param-value-0");
            HttpResponse response = client.putJson(path + "/" + instanceOne, requestBody);
            log.debug("Create watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            requestBody = DocNode.of(instanceOne, DocNode.of("param_name_0", "param-value-updated"),
                instanceTwo, DocNode.of("param_name_0", "param-value-2"));

            response = client.putJson(path, requestBody);

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            response = client.get(path + "/" + instanceOne + "/parameters");
            log.debug("Watch instance '{}' parameters after update '{}'.", instanceOne, response.getBody());
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("data.param_name_0", "param-value-updated"));
            response = client.get(path + "/" + instanceTwo + "/parameters");
            log.debug("Watch instance '{}' parameters after update '{}'.", instanceTwo, response.getBody());
            body = response.getBodyAsDocNode();
            assertThat(body, containsValue("data.param_name_0", "param-value-2"));
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
    public void shouldReturnNotFoundResponseWhenWatchHasNoInstancesDefined() throws Exception {
        String watchId = "watch-without-defined-instances";
        String path = allInstancesPath(watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {

            HttpResponse response = client.get(path);

            log.debug("Get all watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_NOT_FOUND));
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
            DocNode node = DocNode.of("watch_instance_id", DocNode.of("instance_parameter", 7));

            response = restClient.putJson(parametersPath, node.toJsonString());

            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS).until(() -> countDocumentInIndex(client, destinationIndex) > 0);
        }
    }

    @Test
    public void shouldUseGenericWatchInstanceIdParameters() throws Exception {
        String watchId = "watch-should-use-generic-watch-instance-id-parameters";
        String watchPath = watchPath(watchId);
        String parametersPath = allInstancesPath(watchId);
        String instanceId = "watch_instance_id";
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient() //
        ){
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "instance_parameter") //
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['created_by':instance.id]")//
                .throttledFor("1h").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of(instanceId, DocNode.of("instance_parameter", 7));

            response = restClient.putJson(parametersPath, node.toJsonString());

            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS).until(() -> countDocumentInIndex(client, destinationIndex) > 0);
            DocNode firstHit = DocNode.parse(Format.JSON).from(findAllDocumentSearchHits(client, destinationIndex)[0].getSourceAsString());
            assertThat(firstHit, containsValue("created_by", instanceId));
        }
    }

    @Test
    public void shouldReportErrorWhenWatchParameterNameIsId() throws Exception {
        String watchId = "watch-id-should-notify-user-that-id-parameter-name-is-reserved";
        String watchPath = watchPath(watchId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()) {

            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "id")//
                .atMsInterval(200).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).throttledFor("1m").name("testsink").build();

            HttpResponse response = restClient.putJson(watchPath, watch);

            log.debug("Create generic watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_BAD_REQUEST));
            assertThat(response.getBodyAsDocNode(), containSubstring("error", "Instance parameter name 'id' is invalid"));
        }
    }

    @Test
    public void shouldUseAnotherGenericWatchInstanceIdAsParameters() throws Exception {
        String watchId = "watch-should-use-another-generic-watch-instance-id-as-parameter";
        String watchPath = watchPath(watchId);
        String parametersPath = allInstancesPath(watchId);
        String instanceId = "another_watch_instance_id";
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient() //
        ){
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "instance_parameter")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['created_by':instance.id]").throttledFor("1h")//
                .name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of(instanceId, DocNode.of("instance_parameter", 7));

            response = restClient.putJson(parametersPath, node.toJsonString());

            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS).until(() -> countDocumentInIndex(client, destinationIndex) > 0);
            DocNode firstHit = DocNode.parse(Format.JSON).from(findAllDocumentSearchHits(client, destinationIndex)[0].getSourceAsString());
            assertThat(firstHit, containsValue("created_by", instanceId));
        }
    }

    @Test
    public void shouldUseWatchInstanceParameter() throws Exception {
        String watchId = "watch-should-use-generic-watch-instance-parameter";
        String watchPath = watchPath(watchId);
        String parametersPath = allInstancesPath(watchId);
        String instanceId = "watch_instance_id";
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient() //
        ){
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "instance_parameter")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['priority':instance.instance_parameter]")//
                .throttledFor("1h").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of(instanceId, DocNode.of("instance_parameter", 7));

            response = restClient.putJson(parametersPath, node.toJsonString());

            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS).until(() -> countDocumentInIndex(client, destinationIndex) > 0);
            DocNode firstHit = DocNode.parse(Format.JSON).from(findAllDocumentSearchHits(client, destinationIndex)[0].getSourceAsString());
            assertThat(firstHit, containsValue("priority", 7));
        }
    }

    @Test
    public void shouldUseAnotherWatchInstanceParameter() throws Exception {
        String watchId = "watch-should-use-another-genetic-watch-instance-parameter";
        String watchPath = watchPath(watchId);
        String parametersPath = allInstancesPath(watchId);
        String instanceId = "watch_instance_id";
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient() //
        ){
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "instance_parameter")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['priority':instance.instance_parameter]")//
                .throttledFor("1h").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of(instanceId, DocNode.of("instance_parameter", 3));

            response = restClient.putJson(parametersPath, node.toJsonString());

            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS).until(() -> countDocumentInIndex(client, destinationIndex) > 0);
            DocNode firstHit = DocNode.parse(Format.JSON).from(findAllDocumentSearchHits(client, destinationIndex)[0].getSourceAsString());
            assertThat(firstHit, containsValue("priority", 3));
        }
    }

    @Test
    public void shouldUseItsOwnInstanceParameterValue() throws Exception {
        String watchId = "watch-should-use-own-generic-watch-instance-parameter-value";
        String watchPath = watchPath(watchId);
        String parametersPath = allInstancesPath(watchId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient() //
        ){
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "value")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['name':instance.value]").throttledFor("1h")//
                .name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of("first_instance_id", DocNode.of("value", "one"),
                "second_instance_id", DocNode.of("value", "two"));

            response = restClient.putJson(parametersPath, node.toJsonString());

            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "one") > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "two") > 0);
        }
    }

    @Test
    public void shouldUseManyParameters() throws Exception {
        String watchId = "watch-should-use-many-parameters";
        String watchPath = watchPath(watchId);
        String instanceId = "multiple_parameters_instance";
        String parametersPath = instancePath(watchId, instanceId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient() //
        ){
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "color", "shape", "transparency")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch").then().index(destinationIndex) //
                .transform(null, "['Color':instance.color, 'Shp':instance.shape, 'Opacity':instance.transparency]") //
                .throttledFor("1h").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of("color", "blue", "shape", "round", "transparency", "medium");

            response = restClient.putJson(parametersPath, node.toJsonString());

            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS).until(() -> countDocumentInIndex(client, destinationIndex) > 0);
            String createdDocument = findAllDocumentSearchHits(client, destinationIndex)[0].getSourceAsString();
            log.debug("Document created by generic watch instance '{}'.", createdDocument);
            DocNode firstHit = DocNode.parse(Format.JSON).from(createdDocument);
            assertThat(firstHit.size(), equalTo(3));
            assertThat(firstHit, containsValue("Color", "blue"));
            assertThat(firstHit, containsValue("Shp", "round"));
            assertThat(firstHit, containsValue("Opacity", "medium"));
        }
    }

    @Test
    public void shouldUseManyParametersOtherParameterSet() throws Exception {
        String watchId = "watch-should-use-many-parameters-other-parameter-set";
        String watchPath = watchPath(watchId);
        String instanceId = "multiple_parameters_instance";
        String parametersPath = instancePath(watchId, instanceId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient() //
        ){
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "color", "shape", "transparency")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch").then().index(destinationIndex) //
                .transform(null, "['Color':instance.color, 'Shp':instance.shape, 'Opacity':instance.transparency]") //
                .throttledFor("1h").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of("color", "black", "shape", "rectangular", "transparency", "minimal");

            response = restClient.putJson(parametersPath, node.toJsonString());

            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS).until(() -> countDocumentInIndex(client, destinationIndex) > 0);
            String createdDocument = findAllDocumentSearchHits(client, destinationIndex)[0].getSourceAsString();
            log.debug("Document created by generic watch instance '{}'.", createdDocument);
            DocNode firstHit = DocNode.parse(Format.JSON).from(createdDocument);
            assertThat(firstHit.size(), equalTo(3));
            assertThat(firstHit, containsValue("Color", "black"));
            assertThat(firstHit, containsValue("Shp", "rectangular"));
            assertThat(firstHit, containsValue("Opacity", "minimal"));
        }
    }

    @Test
    public void shouldUseInstanceParameterInWebhookAction() throws Exception {
        String watchId = "watch-should-use-instance-parameter-in-webhook-action";
        String watchPath = watchPath(watchId);
        String instanceId = "multiple_parameters_instance";
        String parametersPath = instancePath(watchId, instanceId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient();
            MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook")
        ){
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            DocNode webhookBody = DocNode.of("Color","{{instance.color}}", "Shp", "{{instance.shape}}", "Opacity","{{instance.transparency}}");
            Watch watch = new WatchBuilder(watchId).instances(true, "color", "shape", "transparency")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch").then()//
                .postWebhook(webhookProvider.getUri()).body(webhookBody).throttledFor("1h").name("webhook-action-name")
                .build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of("color", "black", "shape", "rectangular", "transparency", "minimal");

            response = restClient.putJson(parametersPath, node.toJsonString());

            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS).until(() -> webhookProvider.getRequestCount() > 0);
            DocNode lastRequestBody =  webhookProvider.getLastRequestBodyAsDocNode();
            assertThat(lastRequestBody.size(), equalTo(3));
            assertThat(lastRequestBody, containsValue("Color", "black"));
            assertThat(lastRequestBody, containsValue("Shp", "rectangular"));
            assertThat(lastRequestBody, containsValue("Opacity", "minimal"));
        }
    }

    @Test
    public void shouldUseInstanceParameterInWebhookActionWithAnotherParameters() throws Exception {
        String watchId = "watch-should-use-instance-parameter-in-webhook-action-with-another-parameters";
        String watchPath = watchPath(watchId);
        String instanceId = "multiple_parameters_instance";
        String parametersPath = instancePath(watchId, instanceId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient();
            MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook")
        ){
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            DocNode webhookBody = DocNode.of("Color","{{instance.color}}", "Shp", "{{instance.shape}}", "Opacity","{{instance.transparency}}");
            Watch watch = new WatchBuilder(watchId).instances(true, "color", "shape", "transparency")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch").then()//
                .postWebhook(webhookProvider.getUri()).body(webhookBody).throttledFor("1h").name("webhook-action-name")//
                .build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of("color", "green", "shape", "square", "transparency", "almost full");

            response = restClient.putJson(parametersPath, node.toJsonString());

            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS).until(() -> webhookProvider.getRequestCount() > 0);
            DocNode lastRequestBody =  webhookProvider.getLastRequestBodyAsDocNode();
            assertThat(lastRequestBody.size(), equalTo(3));
            assertThat(lastRequestBody, containsValue("Color", "green"));
            assertThat(lastRequestBody, containsValue("Shp", "square"));
            assertThat(lastRequestBody, containsValue("Opacity", "almost full"));
        }
    }

    @Test
    public void shouldStoreWatchStateForGenericWatchInstances() throws Exception {
        String watchId = "watch-store-watch-state-for-generic-watch-instance";
        String watchPath = watchPath(watchId);
        String parametersPath = allInstancesPath(watchId);
        String instanceIdOne = "first_instance_id";
        String instanceIdTwo = "second_instance_id";
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient() //
        ) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "value")//
                .cronTrigger("* * * * * ?").search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['name':instance.value]").throttledFor("1h")//
                .name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of(instanceIdOne, DocNode.of("value", "one"), instanceIdTwo, DocNode.of("value", "two"));

            response = restClient.putJson(parametersPath, node.toJsonString());

            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "one") > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "two") > 0);
            response = restClient.get(String.format("/_signals/watch/%s/%s/instances/%s/_state", DEFAULT_TENANT, watchId, instanceIdOne));
            log.debug("Watch state response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("last_execution.watch.id", watchId + "+" + instanceIdOne));
            assertThat(body, containsValue("last_execution.data.testsearch.hits.total.value", 3));
            response = restClient.get(String.format("/_signals/watch/%s/%s/instances/%s/_state", DEFAULT_TENANT, watchId, instanceIdTwo));
            log.debug("Watch state response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            body = response.getBodyAsDocNode();
            assertThat(body, containsValue("last_execution.watch.id", watchId + "+" + instanceIdTwo));
            assertThat(body, containsValue("last_execution.data.testsearch.hits.total.value", 3));
        }
    }

    @Test
    public void shouldStopExecuteWatchInstanceAfterInstanceDeletion() throws Exception {
        String watchId = "watch-should-not-be-executed-after-deletion";
        String watchPath = watchPath(watchId);
        String instanceId = "watch_instance_id";
        String parametersPath = instancePath(watchId, instanceId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient() //
        ){
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "instance_parameter")//
                .atMsInterval(25).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).throttledFor("1ms").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of("instance_parameter", 3);
            response = restClient.putJson(parametersPath, node.toJsonString());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS).until(() -> countDocumentInIndex(client, destinationIndex) > 0);
            response = restClient.delete(parametersPath);
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Thread.sleep(2000);
            long numberOfDocumentAfterWatchInstanceDeletion = countDocumentInIndex(client, destinationIndex);
            Thread.sleep(2000);//wait for watch instance removal from scheduler
            assertThat(countDocumentInIndex(client, destinationIndex), equalTo(numberOfDocumentAfterWatchInstanceDeletion));
        }
    }

    @Test
    public void shouldStopExecuteAllWatchInstanceAfterDeletion() throws Exception {
        String watchId = "watch-should-stop-execution-all-instances-after-generic-watch-deletion";
        String watchPath = watchPath(watchId);
        String instanceIdOne = "watch_instance_id_one";
        String instanceIdTwo = "watch_instance_id_two";
        String parametersPathOne = instancePath(watchId, instanceIdOne);
        String parametersPathTwo = instancePath(watchId, instanceIdTwo);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient() //
        ){
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true, "instance_parameter") //
                .atMsInterval(25).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).throttledFor("1ms").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode node = DocNode.of("instance_parameter", 3);
            response = restClient.putJson(parametersPathOne, node.toJsonString());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            node = DocNode.of("instance_parameter", 4);
            response = restClient.putJson(parametersPathTwo, node.toJsonString());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS).until(() -> countDocumentInIndex(client, destinationIndex) > 0);
            response = restClient.delete(watchPath);
            log.debug("Generic watch deletion status code '{}' and response body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Thread.sleep(2000);//let's wait for watch removal from quartz scheduler
            long numberOfDocumentAfterGenericWatchDeletion = countDocumentInIndex(client, destinationIndex);
            Thread.sleep(2000);
            assertThat(countDocumentInIndex(client, destinationIndex), equalTo(numberOfDocumentAfterGenericWatchDeletion));
        }
    }

    @Test
    public void shouldDeleteGenericWatchWithItsInstance() throws Exception {
        String watchId = "my-watch-delete-generic-watch-with-its-instance";
        String instanceId = "instance_id";
        String watchPath = createGenericWatch(DEFAULT_TENANT, watchId, "instance");
        String instancePath = String.format("%s/instances/%s", watchPath, instanceId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("instance", "to be deleted together with generic watch");
            HttpResponse response = client.putJson(instancePath, node.toJsonString());
            log.debug("Create generic watch instance response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));

            response = client.delete(watchPath);

            assertThat(response.getStatusCode(), equalTo(SC_OK));

            Awaitility.await().atMost(3, SECONDS).until(() -> {
                HttpResponse instancesResponse = client.get(watchPath + "/instances");
                int statusCode = instancesResponse.getStatusCode();
                String body = instancesResponse.getBody();
                log.debug("Get deleted watch watch instances response status '{}' and body '{}'.", statusCode, body);
                return statusCode == SC_NOT_FOUND;
            });
        }
    }

    @Test
    public void shouldDeleteGenericWatchWithManyInstances() throws Exception {
        String watchId = "my-watch-delete-generic-watch-with-many-instances";
        String watchPath = createGenericWatch(DEFAULT_TENANT, watchId, "parameter");
        String parametersPath = allInstancesPath(watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.EMPTY;
            for(int i = 0; i < 100; ++i) {
                node = node.with(DocNode.of("instance_id_" + i, DocNode.of("parameter", "value")));
            }
            HttpResponse response = client.putJson(parametersPath, node.toJsonString());
            log.debug("Create generic watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));

            response = client.delete(watchPath);

            assertThat(response.getStatusCode(), equalTo(SC_OK));

            Awaitility.await().atMost(3, SECONDS).until(() -> {
                HttpResponse instancesResponse = client.get(parametersPath);
                return SC_NOT_FOUND == instancesResponse.getStatusCode();
            });
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
            for(int i = 0; i < (WatchParametersRepository.WATCH_PARAMETER_DATA_PAGE_SIZE + 5); ++i) {
                String name = "my_name_is_" + i;
                nameParameterValues.add(name);
                node = node.with(DocNode.of("instance_id_" + i, DocNode.of("name", name)));
            }
            //this line should run watch instances
            response = restClient.putJson(parametersPath, node.toJsonString());

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
                final int numberOfInstances = WatchParametersRepository.WATCH_PARAMETER_DATA_PAGE_SIZE * 2;
                Function<Integer, String> createInstanceId = i -> "instance_id_" + i;
                for (int i = 0; i < numberOfInstances; ++i) {
                    String instanceId = createInstanceId.apply(i);
                    node = node.with(DocNode.of(instanceId, DocNode.of("parameter", "value_" + i)));
                }
                HttpResponse response = client.putJson(parametersPath, node.toJsonString());
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
            DocNode node = DocNode.of("name", "initial_value");
            response = restClient.putJson(instancePath, node.toJsonString());
            log.debug("Create watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "initial_value") > 0);
            node = DocNode.of("name", "updated_value");

            response = restClient.putJson(instancePath, node.toJsonString());


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
            DocNode node = DocNode.of("name", "parameter_value");
            response = restClient.putJson(instancePath, node.toJsonString());
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
            response = restClient.putJson(instancesPath, node.toJsonString());
            log.debug("Create watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "Dave") > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "Dan") > 0);
            node = DocNode.of("first_instance", DocNode.of("name", "Angela"), "second_instance", DocNode.of("name", "Daisy"));

            response = restClient.putJson(instancesPath, node.toJsonString());

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
            response = restClient.putJson(instancesPath, node.toJsonString());
            log.debug("Create watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "Dave") > 0);
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentWithTerm(client, destinationIndex, "name.keyword", "Dan") > 0);
            node = DocNode.of("new_instance", DocNode.of("name", "Angela"), "second_instance", DocNode.of("name", "Daisy"));

            response = restClient.putJson(instancesPath, node.toJsonString());

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

            response = restClient.putJson(instancesPath, node.toJsonString());

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
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containSubstring("error", "Instance parameter name 'invalid-name' is invalid."));
        }
    }

    @Test
    public void shouldNotCreateGenericWatchInstanceWithMissingParameters() throws Exception {
        String watchId = "my-watch-should-not-create-generic-watch-instance-with-missing-parameters";
        String instanceId = "instance_id";
        String path = instancePath(watchId, instanceId);
        createGenericWatch(DEFAULT_TENANT, watchId, "vm_id", "name");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("vm_id", 258);

            HttpResponse response = client.putJson(path, node.toJsonString());

            log.debug("Create watch instance response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_BAD_REQUEST));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containSubstring("error.message", "does not contain required parameters: ['name']"));
        }
    }

    @Test
    public void shouldNotCreateGenericWatchInstanceWithTooManyParameters() throws Exception {
        String watchId = "my-watch-should-not-create-generic-watch-instance-with-too-many-parameters";
        String instanceId = "instance_id";
        String path = instancePath(watchId, instanceId);
        createGenericWatch(DEFAULT_TENANT, watchId, "vm_id", "name");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("vm_id", 258, "name", "Dave", "additional_param", 1);

            HttpResponse response = client.putJson(path, node.toJsonString());

            log.debug("Create watch instance response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_BAD_REQUEST));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containSubstring("error.message", "Incorrect parameter names: ['additional_param']."));
        }
    }

    @Test
    public void shouldDetectValidationErrorsDuringUpdate() throws Exception {
        String watchId = "my-watch-should-detect-validation-errors-during-update";
        String instanceId = "instance_id";
        String path = instancePath(watchId, instanceId);
        createGenericWatch(DEFAULT_TENANT, watchId, "vm_id", "name");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("vm_id", 258, "name", "Dave");
            HttpResponse response = client.putJson(path, node.toJsonString());
            log.debug("Create correct watch instance response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            node = DocNode.of("vm_id", 258, "name", "Dave", "additional_param","this parameter cause validation error");

            response = client.putJson(path, node.toJsonString());

            log.debug("Response which should contain validation errors '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_BAD_REQUEST));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containSubstring("error.message", "Incorrect parameter names: ['additional_param']."));
        }
    }

    @Test
    public void shouldDetectInvalidInstanceId() throws Exception {
        String watchId = "my-watch-should-not-create-instance-with-incorrect-instance-id";
        String instanceId = "1-invalid-id";
        String path = instancePath(watchId, instanceId);
        createGenericWatch(DEFAULT_TENANT, watchId, "vm_id", "name");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("vm_id", 258, "name", "Dave");

            HttpResponse response = client.putJson(path, node.toJsonString());

            log.debug("Create watch instance response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_BAD_REQUEST));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containSubstring("error.message", "Watch instance id is incorrect"));
        }
    }

    @Test
    public void shouldDetectValidationErrorsDuringBatchUpdate() throws Exception {
        String watchId = "my-watch-should-detect-validation-errors-during-bulk-update";
        String instanceId = "instance_id";
        String singleInstancePatch = instancePath(watchId, instanceId);
        String allInstancesPatch = allInstancesPath(watchId);
        createGenericWatch(DEFAULT_TENANT, watchId, "vm_id", "name");
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode paramersNode = DocNode.of("vm_id", 258, "name", "Dave");
            HttpResponse response = client.putJson(singleInstancePatch, paramersNode.toJsonString());
            log.debug("Create correct watch instance response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            paramersNode = DocNode.of("vm_id", 258, "name", "Dave", "additional_param","this parameter cause validation error");
            DocNode instanceNode = DocNode.of(instanceId, paramersNode);

            response = client.putJson(allInstancesPatch, instanceNode.toJsonString());

            log.debug("Bulk update response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_BAD_REQUEST));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containSubstring("error.message", "Incorrect parameter names: ['additional_param']."));
        }
    }

    @Test
    public void shouldValidateParametersWhenBulkRequestIsUsedToCreateWatches() throws Exception {
        String watchId = "my-watch-should-detect-validation-errors-when-bulk-request-is-used-to-create-watches";
        String watchPath = String.format("/_signals/watch/%s/%s", DEFAULT_TENANT, watchId);
        String instancesPath = watchPath + "/instances";
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
            DocNode node = DocNode.of("incorrect-watch-id?", DocNode.of("name", "Dave"),
                "missing_param_instance", DocNode.EMPTY,
                "third_instance", DocNode.of("name", "3", "additional_parameter", "what is not allowed"));

            response = restClient.putJson(instancesPath, node.toJsonString());

            log.debug("Create watch instances response status '{}' and body '{}'.",response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_BAD_REQUEST));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containSubstring("error.details['incorrect-watch-id?'][0].error", "Watch instance id is incorrect."));
            assertThat(body, containSubstring("error.details.missing_param_instance[0].error", "Watch instance does not contain required parameters: ['name']"));
            assertThat(body, containSubstring("error.details.third_instance[0].error", "Incorrect parameter names: ['additional_parameter']. Valid parameter names: ['name']"));
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
    public void shouldAllowUsageOfPlusSignInNonGenericWatchId() throws Exception {
        String watchId = "my+watch+id+with+plus+sign";
        String watchPath = String.format("/_signals/watch/%s/%s", DEFAULT_TENANT, watchId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()
        ) {
            final String destinationIndex = "destination-index-for-" + watchId;
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
    public void shouldNotAllowUsageOfPlusSignInGenericWatchId() throws Exception {
        String watchId = "my+generic+watch+id+with+plus+sign";
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
            assertThat(response.getStatusCode(), equalTo(SC_BAD_REQUEST));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containSubstring("error", "Generic watch id cannot contain '+' character."));
        }
    }

    @Test
    public void shouldReportValidationErrorWhenNonGenericWatchDefinesInstanceParameters() throws Exception {
        String watchId = "watch-id-non-generic-watch-with-instance-parameters";
        String watchPath = String.format("/_signals/watch/%s/%s", DEFAULT_TENANT, watchId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources();
            Client client = cluster.getInternalNodeClient()
        ) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(false, "params_are_not_allowed_for_non_generic_watch") //
                .atMsInterval(Long.MAX_VALUE).search(INDEX_SOURCE) //
                .query("{\"match_all\" : {} }").as("testsearch") //
                .then().index(destinationIndex).transform(null, "['name':instance.name, 'watch':instance.id]")//
                .name("testsink").throttledFor("10h").build();

            HttpResponse response = restClient.putJson(watchPath, watch);

            log.debug("Create watch response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_BAD_REQUEST));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containSubstring("error", "Watch is invalid: 'instances.enabled': Only generic watch is allowed to define instance parameters"));
        }
    }

    @Test
    public void shouldNotExecuteInstanceWhenGenericWatchIsInactive() throws Exception {
        String watchId = "watch-id-should-not-execute-instance-when-generic-watch-is-disabled";
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

            response = restClient.delete(watchPath + "/_active");

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode node = DocNode.of("watch_instance_id", DocNode.of("instance_parameter", 7));
            response = restClient.putJson(parametersPath, node.toJsonString());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Thread.sleep(3000);
            //make sure that watch is not executed because generic watch is deactivate
            long numberOfWatchActionExecution = countDocumentInIndex(client, destinationIndex);
            assertThat(numberOfWatchActionExecution, equalTo(0L));
        }
    }

    @Test
    public void shouldResumeInstanceExecutionAfterActivationOfGenericWatch() throws Exception {
        String watchId = "watch-id-should-resume-instance-execution-when-generic-watch-is-activated";
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
            response = restClient.delete(watchPath + "/_active");
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode node = DocNode.of("watch_instance_id", DocNode.of("instance_parameter", 7));
            response = restClient.putJson(parametersPath, node.toJsonString());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Thread.sleep(3000);
            //make sure that watch is not executed because generic watch is deactivate
            long numberOfWatchActionExecution = countDocumentInIndex(client, destinationIndex);
            assertThat(numberOfWatchActionExecution, equalTo(0L));

            response = restClient.put(watchPath + "/_active");
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex) > 0);
        }
    }

    @Test
    public void shouldStopInstanceExecutionAfterDeactivationOfGenericWatch() throws Exception {
        String watchId = "watch-id-should-stop-instance-execution-when-generic-watch-is-de-activated";
        String watchPath = watchPath(watchId);
        String parametersPath = allInstancesPath(watchId);
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
            DocNode node = DocNode.of("watch_instance_id", DocNode.of("instance_parameter", 7));
            response = restClient.putJson(parametersPath, node.toJsonString());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex) > 0);

            response = restClient.delete(watchPath + "/_active");

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Thread.sleep(1000);// wait for watch deactivation
            long previousNumberOfExecution = countDocumentInIndex(client, destinationIndex);
            Thread.sleep(3000);
            //make sure that watch is not executed because generic watch is deactivate
            long currentNumberOfExecution = countDocumentInIndex(client, destinationIndex);
            assertThat(currentNumberOfExecution, equalTo(previousNumberOfExecution));
        }
    }

    @Test
    public void shouldStopAllInstancesExecutionAfterDeactivationOfGenericWatch() throws Exception {
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
            response = restClient.putJson(instancesPath, node.toJsonString());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex) > 0);

            response = restClient.delete(watchPath + "/_active");

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Thread.sleep(1000);// wait till generic watch is disabled
            long previousNumberOfExecution = countDocumentInIndex(client, destinationIndex);
            Thread.sleep(3000);
            //make sure that watch is not executed because generic watch is deactivate
            long currentNumberOfExecution = countDocumentInIndex(client, destinationIndex);
            assertThat(currentNumberOfExecution, equalTo(previousNumberOfExecution));
        }
    }

    @Test
    public void shouldDisableWatchInstance() throws Exception {
        String watchId = "watch-id-should-disable-watch-instance";
        String watchPath = watchPath(watchId);
        String instanceId = "instance_id_to_be_disabled";
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
            DocNode node = DocNode.of("instance_parameter", 7);
            response = restClient.putJson(instancePath, node.toJsonString());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex) > 0);

            response = restClient.delete(instancePath + "/_active");

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Thread.sleep(1000); // wait for action deactivation
            long previousNumberOfExecution = countDocumentInIndex(client, destinationIndex);
            Thread.sleep(3000);
            //make sure that watch is not executed because generic watch is deactivate
            long currentNumberOfExecution = countDocumentInIndex(client, destinationIndex);
            assertThat(currentNumberOfExecution, equalTo(previousNumberOfExecution));
        }
    }

    @Test
    public void shouldStopExecutionOnlyOfDisabledWatchInstance() throws Exception {
        String watchId = "watch-id-should-stop-execution-only-of-disabled-instance";
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
            HttpResponse response = restClient.putJson(watchPath, watch);
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

            response = restClient.delete(instancePath(watchId, instanceId2) + "/_active");


            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Thread.sleep(1000);
            long previousNumberOfExecutionInstance1 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId1);
            long previousNumberOfExecutionInstance2 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId2);
            long previousNumberOfExecutionInstance3 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId3);
            Thread.sleep(3000);
            //make sure that only disabled watch is not executed
            long currentNumberOfExecutionInstance1 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId1);
            long currentNumberOfExecutionInstance2 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId2);
            long currentNumberOfExecutionInstance3 = countDocumentWithTerm(client, destinationIndex, "created_by.keyword", instanceId3);
            assertThat(currentNumberOfExecutionInstance1, greaterThan(previousNumberOfExecutionInstance1)); //is still executed
            assertThat(currentNumberOfExecutionInstance2, equalTo(previousNumberOfExecutionInstance2)); // is not executed
            assertThat(currentNumberOfExecutionInstance3, greaterThan(previousNumberOfExecutionInstance3));//is still executed
        }
    }

    @Test
    public void shouldNotDisableNonExistingWatchInstance() throws Exception {
        String watchId = "watch-id-should-not-disable-non-existing-watch-instance";
        String watchPath = watchPath(watchId);
        String instanceId = "instance_non_existing";
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


            response = restClient.delete(instancePath + "/_active");

            log.info("Disable non existing watch instance response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_NOT_FOUND));
        }
    }

    @Test
    public void shouldEnableWatchInstance() throws Exception {
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
            DocNode node = DocNode.of("instance_parameter", 7);
            response = restClient.putJson(instancePath, node.toJsonString());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex) > 0);
            response = restClient.delete(instancePath + "/_active");
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Thread.sleep(1000);
            long previousCountOfDocuments = countDocumentInIndex(client, destinationIndex);
            Thread.sleep(3000);
            long currentCountOfDocuments = countDocumentInIndex(client, destinationIndex);
            assertThat(currentCountOfDocuments, equalTo(previousCountOfDocuments));

            response = restClient.put(instancePath + "/_active");

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex) > previousCountOfDocuments);
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
            HttpResponse response = restClient.putJson(watchPath, watch);
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
        String instanceId1 = "instance_id_1";
        String instancePath = instancePath(watchId, instanceId1);
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
            HttpResponse response = restClient.putJson(watchPath, watch);
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

            response = restClient.put(instancePath(watchId, instanceId1) + "/_ack/" + actionName2);

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
            response = restClient.get(instancePath(watchId, instanceId1) + "/_state");
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
            HttpResponse response = restClient.putJson(watchPath, watch);
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
            HttpResponse response = restClient.putJson(watchPath, watch);
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
            response = restClient.put(instancePath(watchId, instanceId) + "/_ack");
            log.info("Ack watch instance action response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Thread.sleep(1000);
            long previousNumberOfExecutionAction1 = countDocumentInIndex(client, destinationIndex1);
            long previousNumberOfExecutionAction2 = countDocumentInIndex(client, destinationIndex2);
            long previousNumberOfExecutionAction3 = countDocumentInIndex(client, destinationIndex3);
            Thread.sleep(3000);
            //All watch actions are not executed
            long currentNumberOfExecutionAction1 = countDocumentInIndex(client, destinationIndex1);
            long currentNumberOfExecutionAction2 = countDocumentInIndex(client, destinationIndex2);
            long currentNumberOfExecutionAction3 = countDocumentInIndex(client, destinationIndex3);
            assertThat(currentNumberOfExecutionAction1, equalTo(previousNumberOfExecutionAction1)); //is not executed because is acked
            assertThat(currentNumberOfExecutionAction2, equalTo(previousNumberOfExecutionAction2)); //is not executed because is acked
            assertThat(currentNumberOfExecutionAction3, equalTo(previousNumberOfExecutionAction3));//is not executed because is acked
            response = restClient.delete(instancePath(watchId, instanceId) + "/_ack/" + actionName2);
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            Awaitility.await().atMost(3, SECONDS)
                .until(() -> countDocumentInIndex(client, destinationIndex2) > currentNumberOfExecutionAction2);
            response = restClient.get(instancePath(watchId, instanceId) + "/_state");
            log.debug("Get watch state response status code '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.actions." + actionName1 + ".last_status.code", "ACKED"));
            assertThat(body, not(containsValue("$.actions." + actionName2 + ".last_status.code", "ACKED")));
            assertThat(body, containsValue("$.actions." + actionName3 + ".last_status.code", "ACKED"));
        }
    }

    private String createGenericWatch(String tenant, String watchId, String...parameterNames) throws Exception {
        return createWatch(tenant, watchId, true, parameterNames);
    }

    private static String createWatch(String tenant, String watchId, boolean generic, String...parameterNames) throws Exception {
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            Watch watch = new WatchBuilder(watchId).instances(generic, parameterNames).cronTrigger("0 0 0 1 1 ?")//
                .search(INDEX_SOURCE).query("{\"match_all\" : {} }").as("testsearch")//
                .then().index("testsink").throttledFor("1h").name("testsink").build();
            String watchJson = watch.toJson();
            log.debug("Create watch '{}' with id '{}'.", watchJson, watchId);
            HttpResponse response = restClient.putJson(watchPath, watchJson);
            log.debug("Create watch '{}' response status '{}' and body '{}'", watchId, response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            return watchPath;
        }
    }

    private long countDocumentInIndex(Client client, String index) throws InterruptedException, ExecutionException {
        SearchResponse response = findAllDocuments(client, index);
        long count = response.getHits().getTotalHits().value;
        log.debug("Number of documents in index '{}' is '{}'", index, count);
        return count;
    }

    private static SearchResponse findAllDocuments(Client client, String index) throws InterruptedException, ExecutionException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        return findDocuments(client, index, searchSourceBuilder);
    }

    private static SearchResponse findDocuments(Client client, String index, SearchSourceBuilder searchSourceBuilder)
        throws InterruptedException, ExecutionException {
        SearchRequest request = new SearchRequest(index);
        request.source(searchSourceBuilder);
        return client.search(request).get();
    }

    private static long countDocumentWithTerm(Client client, String index, String fieldName, String fieldValue)
        throws ExecutionException, InterruptedException {
        if(!fieldName.endsWith(".keyword")) {
            String message = "Term query requires usage of not analyzed fields. Please append '.keyword' to your field name";
            throw new IllegalArgumentException(message);
        }
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery(fieldName, fieldValue));
        SearchResponse response = findDocuments(client, index, searchSourceBuilder);
        log.debug("Search document with term '{}' value '{}' is '{}'.", fieldName, fieldValue, response);
        long count = response.getHits().getTotalHits().value;
        log.debug("Number of documents with term '{}' and value '{}' is '{}'.", fieldName, fieldValue, count);
        return count;
    }

    private static String allInstancesPath(String watchId) {
        return String.format("/_signals/watch/%s/%s/instances", DEFAULT_TENANT, watchId);
    }

    private static String instancePath(String watchId, String instanceId) {
        return String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId, instanceId);
    }

    private static String watchPath(String watchId) {
        return "/_signals/watch/" + DEFAULT_TENANT + "/" + watchId;
    }

    private static SearchHit[] findAllDocumentSearchHits(Client client, String index) throws ExecutionException, InterruptedException {
        SearchResponse response = findAllDocuments(client, index);
        return response.getHits().getHits();
    }

}