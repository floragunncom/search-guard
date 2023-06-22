package com.floragunn.signals.actions.watch.template.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.SignalsModule;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static com.floragunn.searchguard.test.TestSgConfig.TenantPermission.ALL_TENANTS_AND_ACCESS;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsFieldPointedByJsonPath;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class WatchTemplateTest {

    private static final Logger log = LogManager.getLogger(WatchTemplateTest.class);

    private static final String DEFAULT_TENANT = "_main";
    private static final String INDEX_SOURCE = "test_source_index";

    private final static User USER_ADMIN = new User("admin").roles(new Role("signals_master")//
        .clusterPermissions("*")//
        .indexPermissions("*").on("*")//
        .tenantPermission(ALL_TENANTS_AND_ACCESS));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()//
        .user(USER_ADMIN).enableModule(SignalsModule.class)//
        .nodeSettings("signals.enabled", true)
        .build();

    @BeforeClass
    public static void setupTestData() {

        try (Client client = cluster.getInternalNodeClient()) {
            client.index(new IndexRequest(INDEX_SOURCE).source(XContentType.JSON, "key1", "1", "key2", "2")).actionGet();
            client.index(new IndexRequest(INDEX_SOURCE).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "key1", "3", "key2", "4"))
                .actionGet();
            client.index(new IndexRequest(INDEX_SOURCE).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "key1", "5", "key2", "6"))
                .actionGet();
        }
    }

    @Test
    public void shouldCreateTemplateParameters() throws Exception {
        String watchId = "my-watch-create-template-parameters";
        String instanceId = "instance_id_should_create_template_parameters";
        String path = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId, instanceId);
        createWatchTemplate(DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("id", 258);

            HttpResponse response = client.putJson(path, node.toJsonString());

            log.info("Create watch template response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(201));
        }
    }

    @Test
    public void shouldNotCreateTemplateParametersWhenWatchDoesNotExist() throws Exception {
        String watchId = "my-watch-create-template-parameters-when-watch-does-not-exists";
        String instanceId = "instance_id_should_create_template_parameters";
        String path = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId, instanceId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("id", 258);

            HttpResponse response = client.putJson(path, node.toJsonString());

            log.info("Create watch template response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(404));
        }
    }

    @Test
    public void shouldNotCreateTemplateParametersWhenWatchIsNotTemplate() throws Exception {
        String watchId = "my-watch-create-template-parameters-when-watch-is-not-template";
        String instanceId = "instance_id_should_create_template_parameters";
        String path = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId, instanceId);
        createWatch(DEFAULT_TENANT, watchId, false);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("id", 258);

            HttpResponse response = client.putJson(path, node.toJsonString());

            log.info("Create watch template response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(404));
        }
    }

    @Test
    public void shouldNotCreateTemplateWhenTenantDoesNotExist() throws Exception {
        String watchId = "my-watch-do-not-create-template-parameter-when-tenant-does-not-exist";
        String instanceId = "instance_id_should_create_template_parameters";
        String notExistingTenantName = "tenant-does-not-exists";
        String path = String.format("/_signals/watch/%s/%s/instances/%s", notExistingTenantName, watchId, instanceId);
        createWatchTemplate(DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("id", 258);

            HttpResponse response = client.putJson(path, node.toJsonString());

            log.info("Create watch template response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(404));
        }
    }

    @Test
    public void shouldLoadTemplateParameters() throws Exception {
        String watchId = "my-watch-load-template-parameters";
        String instanceId = "instance_id_should_load_parameters";
        String path = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId, instanceId);
        createWatchTemplate(DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("id", 258, "name","kirk", "time", "2023-05-15T13:07:52.000Z");
            HttpResponse response = client.putJson(path, node.toJsonString());
            log.info("Create watch template response '{}'.", response.getBody());

            response = client.get(path + "/parameters");

            log.info("Get watch template parameters response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("data.id", 258));
            assertThat(body, containsValue("data.name", "kirk"));
            assertThat(body, containsValue("data.time", "2023-05-15T13:07:52.000Z"));
        }
    }

    @Test
    public void shouldUseNestedValuesInList() throws Exception {
        String watchId = "my-watch-use-nested-value-list";
        String instanceId = "instance_id_should_use_nested_values_in_list";
        String path = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId, instanceId);
        createWatchTemplate(DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            ImmutableMap<String, String> firstMap = ImmutableMap.of("one-key", "one-value", "two-key", "two-value");
            ImmutableMap<String, Integer> secondMap = ImmutableMap.of("three-key", 3, "four-key", 4);
            DocNode node = DocNode.of("list", Arrays.asList(firstMap, secondMap));
            HttpResponse response = client.putJson(path, node.toJsonString());
            log.info("Create watch template response '{}'.", response.getBody());

            response = client.get( path + "/parameters");

            log.info("Get watch template parameters response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("data.list[0].one-key", "one-value"));
            assertThat(body, containsValue("data.list[0].two-key", "two-value"));
            assertThat(body, containsValue("data.list[1].three-key", 3));
            assertThat(body, containsValue("data.list[1].four-key", 4));
        }
    }

    @Test
    public void shouldUseNestedValuesInMap() throws Exception {
        String watchId = "my-watch-use-nested-values-in-map";
        String instanceId = "instance_id_should_use_nested_values_in_map";
        String path = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId, instanceId);
        createWatchTemplate(DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            ImmutableMap<String, String> firstMap = ImmutableMap.of("one-key", "one-value", "two-key", "two-value");
            ImmutableMap<String, Integer> secondMap = ImmutableMap.of("three-key", 3, "four-key", 4);
            DocNode node = DocNode.of("outer_map", ImmutableMap.of("first_map", firstMap, "second_map", secondMap));
            HttpResponse response = client.putJson(path, node.toJsonString());
            log.info("Create watch template response '{}'.", response.getBody());

            response = client.get(path + "/parameters");

            log.info("Get watch template parameters response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("data.outer_map.first_map.one-key", "one-value"));
            assertThat(body, containsValue("data.outer_map.first_map.two-key", "two-value"));
            assertThat(body, containsValue("data.outer_map.second_map.three-key", 3));
            assertThat(body, containsValue("data.outer_map.second_map.four-key", 4));
        }
    }

    @Test
    public void shouldStoreParameterWithTheSameNameButWithVariousTypes() throws Exception {
        String instanceId = "instance_parameters";
        String watchId1 = "my-watch-parameters-with-same-name-but-various-types-one";
        String watchId2 = "my-watch-parameters-with-same-name-but-various-types-two";
        String watchId3 = "my-watch-parameters-with-same-name-but-various-types-three";
        String pathWatch1 = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId1, instanceId);
        String pathWatch2 = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId2, instanceId);
        String pathWatch3 = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId3, instanceId);
        createWatchTemplate(DEFAULT_TENANT, watchId1);
        createWatchTemplate(DEFAULT_TENANT, watchId2);
        createWatchTemplate(DEFAULT_TENANT, watchId3);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("param", "2023-05-15T13:07:52.000Z");
            HttpResponse response = client.putJson(pathWatch1, node.toJsonString());
            log.info("Watch 1 instance parameters response '{}'.", response.getBody());
            node = DocNode.of("param", 10);
            response = client.putJson(pathWatch2, node.toJsonString());
            log.info("Watch 2 instance parameters response '{}'.", response.getBody());
            node = DocNode.of("param", "ten");

            response = client.putJson(pathWatch3, node.toJsonString());

            log.info("Watch 3 instance parameters response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(201));
        }
    }

    @Test
    public void shouldHaveOwnParameterCopy() throws Exception {
        final String instanceId = "common_parameters";
        final String watchIdOne = "my-watch-own-parameter-copy-one";
        final String watchIdTwo = "my-watch-own-parameter-copy-two";
        final String pathWatch1 = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchIdOne, instanceId);
        final String pathWatch2 = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchIdTwo, instanceId);
        createWatchTemplate(DEFAULT_TENANT, watchIdOne);
        createWatchTemplate(DEFAULT_TENANT, watchIdTwo);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("common-parameter-name", "first-value");
            HttpResponse response = client.putJson(pathWatch1, node.toJsonString());
            log.info("Watch 1 instance parameters response '{}'.", response.getBody());
            node = DocNode.of("common-parameter-name", "second-value");
            response = client.putJson(pathWatch2, node.toJsonString());
            log.info("Watch 2 instance parameters response '{}'.", response.getBody());

            response = client.get(pathWatch1 + "/parameters");

            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), containsValue("data.common-parameter-name", "first-value"));
            response = client.get(pathWatch2 + "/parameters");
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), containsValue("data.common-parameter-name", "second-value"));
        }
    }

    @Test
    public void shouldDeleteWatchInstance() throws Exception {
        String watchId = "my-watch-to-be-deleted";
        String instanceId = "instance_id_should_use_nested_values_in_map";
        String path = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId, instanceId);
        createWatchTemplate(DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("message", "please do not delete me!" );
            HttpResponse response = client.putJson(path, node.toJsonString());
            log.info("Create watch template response '{}'.", response.getBody());
            response = client.get(path + "/parameters");
            log.info("Get watch template parameters response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));

            response = client.delete(path);

            assertThat(response.getStatusCode(), equalTo(200));
            response = client.get(path + "/parameters");
            assertThat(response.getStatusCode(), equalTo(404));
        }
    }

    @Test
    public void shouldNotDeleteWatchInstanceWhichDoesNotExist() throws Exception {
        String watchId = "non-existing-watch-instance-to-be-deleted";
        String instanceId = "instance_id_should_use_nested_values_in_map";
        String path = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId, instanceId);
        createWatchTemplate(DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {

            HttpResponse response = client.delete(path);

            assertThat(response.getStatusCode(), equalTo(404));
        }
    }

    @Test
    public void shouldReturnValidationErrorWhenWatchInstancesAreCreatedWithEmptyBody() throws Exception {
        String watchId = "watch-with-many-instances-empty-body";
        String path = String.format("/_signals/watch/%s/%s/instances", DEFAULT_TENANT, watchId);
        createWatchTemplate(DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode requestBody = DocNode.EMPTY;

            HttpResponse response = client.putJson(path, requestBody);

            log.info("Create multiple watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(400));
        }
    }

    @Test
    public void shouldCreateSingleWatchInstanceWithUsageOfBulkRequest() throws Exception {
        String watchId = "watch-with-many-instances-one-parameter-set";
        String path = String.format("/_signals/watch/%s/%s/instances", DEFAULT_TENANT, watchId);
        createWatchTemplate(DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            final String instanceId = "instance-id-one";
            DocNode requestBody = DocNode.of(instanceId, DocNode.of("param-name", "param-value"));

            HttpResponse response = client.putJson(path, requestBody);

            log.info("Create multiple watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(201));
            response = client.get(path + "/" + instanceId + "/parameters");
            log.info("Stored watch instance parameters '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), containsValue("data.param-name", "param-value"));
        }
    }

    @Test
    public void shouldCreateThreeWatchInstanceWithUsageOfBulkRequest() throws Exception {
        String watchId = "watch-with-many-instances-create-three";
        String path = String.format("/_signals/watch/%s/%s/instances", DEFAULT_TENANT, watchId);
        createWatchTemplate(DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            final String instanceIdOne = "instance-id-one";
            final String instanceIdTwo = "instance-id-two";
            final String instanceIdThree = "instance-id-three";
            DocNode requestBody = DocNode.of(instanceIdOne, DocNode.of("param-name-1", "param-value-1"),
                instanceIdTwo, DocNode.of("param-name-2", "param-value-2"),
                instanceIdThree, DocNode.of("param-name-3", "param-value-3", "param-name-4", 4));

            HttpResponse response = client.putJson(path, requestBody);

            log.info("Create multiple watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(201));
            response = client.get(path + "/" + instanceIdOne + "/parameters");
            log.info("Stored watch instance '{}' parameters '{}'.", instanceIdOne, response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), containsValue("data.param-name-1", "param-value-1"));
            response = client.get(path + "/" + instanceIdTwo + "/parameters");
            log.info("Stored watch instance '{}' parameters '{}'.", instanceIdTwo, response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), containsValue("data.param-name-2", "param-value-2"));
            response = client.get(path + "/" + instanceIdThree + "/parameters");
            log.info("Stored watch instance '{}' parameters '{}'.", instanceIdThree, response.getBody());
            assertThat(response.getBodyAsDocNode(), containsValue("data.param-name-3", "param-value-3"));
            assertThat(response.getBodyAsDocNode(), containsValue("data.param-name-4", 4));
        }
    }

    @Test
    public void shouldUpdateWatchInstance() throws Exception {
        String watchId = "watch-to-be-updated";
        final String instanceId = "instance-id-one";
        String path = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId, instanceId);
        createWatchTemplate(DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode requestBody = DocNode.of("param-name-0", "param-value-0");
            HttpResponse response = client.putJson(path, requestBody);
            log.info("Create multiple watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(201));
            requestBody = DocNode.of("param-name-0", "param-value-updated", "new-parameter", "new-parameter-value");

            response = client.putJson(path, requestBody);

            assertThat(response.getStatusCode(), equalTo(200));
            response = client.get(path + "/parameters");
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("data.param-name-0", "param-value-updated"));
            assertThat(body, containsValue("data.new-parameter", "new-parameter-value"));
        }
    }

    @Test
    public void shouldRemoveWatchInstanceParameterDuringUpdate() throws Exception {
        String watchId = "watch-to-be-updated-and-remove-parameter";
        final String instanceId = "instance-id";
        String path = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId, instanceId);
        createWatchTemplate(DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            final String fieldToRemoval = "parameter-to-be-removed";
            DocNode requestBody = DocNode.of("param-name-0", "param-value-0", fieldToRemoval, "Oops!");
            HttpResponse response = client.putJson(path, requestBody);
            log.info("Create watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(201));
            requestBody = DocNode.of("param-name-0", "param-value-updated");

            response = client.putJson(path, requestBody);

            assertThat(response.getStatusCode(), equalTo(200));
            response = client.get(path + "/parameters");
            log.info("Watch parameters after update '{}'.", response.getBody());
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("data.param-name-0", "param-value-updated"));
            assertThat(body, not(containsFieldPointedByJsonPath("data", fieldToRemoval)));
        }
    }

    @Test
    public void shouldUpdateWatchInstancesViaBulkRequest() throws Exception {
        String watchId = "watch-to-be-updated-by-bulk-request";
        final String instanceOne = "instance-id-one";
        String instanceTwo = "new-instance-id-two";
        String path = String.format("/_signals/watch/%s/%s/instances", DEFAULT_TENANT, watchId);
        createWatchTemplate(DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            final String fieldToRemoval = "parameter-to-be-removed";
            DocNode requestBody = DocNode.of("param-name-0", "param-value-0", fieldToRemoval, "Oops!");
            HttpResponse response = client.putJson(path + "/" + instanceOne, requestBody);
            log.info("Create watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(201));
            requestBody = DocNode.of(instanceOne, DocNode.of("param-name-0", "param-value-updated"),
                instanceTwo, DocNode.of("param-name-2", "param-value-2"));

            response = client.putJson(path, requestBody);

            assertThat(response.getStatusCode(), equalTo(200));
            response = client.get(path + "/" + instanceOne + "/parameters");
            log.info("Watch instance '{}' parameters after update '{}'.", instanceOne, response.getBody());
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("data.param-name-0", "param-value-updated"));
            assertThat(body, not(containsFieldPointedByJsonPath("data", fieldToRemoval)));
            response = client.get(path + "/" + instanceTwo + "/parameters");
            log.info("Watch instance '{}' parameters after update '{}'.", instanceTwo, response.getBody());
            body = response.getBodyAsDocNode();
            assertThat(body, containsValue("data.param-name-2", "param-value-2"));
        }
    }

    @Test
    public void shouldLoadExistingWatchInstances() throws Exception {
        String watchId = "watch-for-load-all-instance-test";
        String instanceId = "zero-instance";
        String path = String.format("/_signals/watch/%s/%s/instances", DEFAULT_TENANT, watchId);
        createWatchTemplate(DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode requestBody = DocNode.of("param-name-0", "param-value-0", "param-name-1", "param-value-1");
            client.putJson(path + "/" + instanceId , requestBody);
            requestBody = DocNode.of("first-instance", DocNode.of("id", 1), "second-instance", DocNode.of("id", 2));
            client.putJson(path , requestBody);

            HttpResponse response = client.get(path);

            log.info("Get all watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("data.zero-instance.param-name-0", "param-value-0"));
            assertThat(body, containsValue("data.zero-instance.param-name-1", "param-value-1"));
            assertThat(body, containsValue("data.first-instance.id", 1));
            assertThat(body, containsValue("data.second-instance.id", 2));
        }
    }

    @Test
    public void shouldReturnNotFoundResponseWhenWatchHasNoInstancesDefined() throws Exception {
        String watchId = "watch-without-defined-instances";
        String path = String.format("/_signals/watch/%s/%s/instances", DEFAULT_TENANT, watchId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {

            HttpResponse response = client.get(path);

            log.info("Get all watch instances response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(404));
        }
    }

    @Test
    public void shouldNotExecuteWatchTemplate() throws Exception {
        String watchId = "watch-template-should-not-be-executed";
        String watchPath = "/_signals/watch/" + DEFAULT_TENANT + "/" + watchId;
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources(); Client client = cluster.getInternalNodeClient()) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true).cronTrigger("* * * * * ?").search(INDEX_SOURCE)
                .query("{\"match_all\" : {} }").as("testsearch")
                .then().index(destinationIndex).name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(201));

            Thread.sleep(1200);
            assertThat(countDocumentInIndex(client, destinationIndex), equalTo(0L));
        }
    }

    @Test
    public void shouldExecuteWatchTemplateInstance() throws Exception {
        String watchId = "watch-template-instance-should-be-executed";
        String watchPath = "/_signals/watch/" + DEFAULT_TENANT + "/" + watchId;
        String parametersPath = String.format("/_signals/watch/%s/%s/instances", DEFAULT_TENANT, watchId);
        try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN).trackResources(); Client client = cluster.getInternalNodeClient()) {
            final String destinationIndex = "destination-index-for-" + watchId;
            client.admin().indices().create(new CreateIndexRequest(destinationIndex)).actionGet();
            Watch watch = new WatchBuilder(watchId).instances(true).cronTrigger("* * * * * ?").search(INDEX_SOURCE)
                .query("{\"match_all\" : {} }").as("testsearch")
                .then().index(destinationIndex).name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(201));
            DocNode node = DocNode.of("watch_instance_id", DocNode.of("instance_parameter", 7));

            response = restClient.putJson(parametersPath, node.toJsonString());

            assertThat(response.getStatusCode(), equalTo(201));
            Awaitility.await().until(() -> countDocumentInIndex(client, destinationIndex) > 0);
        }
    }

    private void createWatchTemplate(String tenant, String watchId) throws Exception {
        createWatch(tenant, watchId, true);
    }

    private static void createWatch(String tenant, String watchId, boolean template) throws Exception {
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            Watch watch = new WatchBuilder(watchId).instances(template).cronTrigger("0 0 0 1 1 ?")//
                .search(INDEX_SOURCE).query("{\"match_all\" : {} }").as("testsearch")//
                .then().index("testsink").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());
            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_CREATED));
        }
    }

    private long countDocumentInIndex(Client client, String index) throws InterruptedException, ExecutionException {
        SearchRequest request = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request).get();
        long count = response.getHits().getTotalHits().value;
        log.info("Number of documents in index '{}' is '{}'", index, count);
        return count;
    }

}