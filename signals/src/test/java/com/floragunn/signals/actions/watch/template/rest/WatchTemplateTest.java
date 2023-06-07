package com.floragunn.signals.actions.watch.template.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.SignalsModule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;

import static com.floragunn.searchguard.test.TestSgConfig.TenantPermission.ALL_TENANTS_AND_ACCESS;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class WatchTemplateTest {

    private static final Logger log = LogManager.getLogger(WatchTemplateTest.class);

    private final static User USER_ADMIN = new User("admin").roles(new Role("signals_master")//
        .clusterPermissions("*")//
        .indexPermissions("*").on("*")//
        .tenantPermission(ALL_TENANTS_AND_ACCESS));
    public static final String DEFAULT_TENANT = "_main";

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()//
        .user(USER_ADMIN).enableModule(SignalsModule.class)//
        .nodeSettings("signals.enabled", true)
        .build();

    @Test
    public void shouldCreateTemplateParameters() throws Exception {
        String watchId = "my_watch";
        String instanceId = "instance_id_should_create_template_parameters";
        String path = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId, instanceId);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            DocNode node = DocNode.of("id", 258);

            HttpResponse response = client.putJson(path, node.toJsonString());

            log.info("Create watch template response '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(201));
        }
    }

    @Test
    public void shouldLoadTemplateParameters() throws Exception {
        String watchId = "my_watch";
        String instanceId = "instance_id_should_load_parameters";
        String path = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId, instanceId);
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
        String watchId = "my_watch";
        String instanceId = "instance_id_should_use_nested_values_in_list";
        String path = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId, instanceId);
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
        String watchId = "my_watch";
        String instanceId = "instance_id_should_use_nested_values_in_map";
        String path = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId, instanceId);
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
        String pathWatch1 = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, "my_watch_one", instanceId);
        String pathWatch2 = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, "my_watch_two", instanceId);
        String pathWatch3 = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, "my_watch_three", instanceId);
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
        String instanceId = "common_parameters";
        String pathWatch1 = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, "my_watch_one", instanceId);
        String pathWatch2 = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, "my_watch_two", instanceId);
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
        String watchId = "to-be-deleted";
        String instanceId = "instance_id_should_use_nested_values_in_map";
        String path = String.format("/_signals/watch/%s/%s/instances/%s", DEFAULT_TENANT, watchId, instanceId);
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
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {

            HttpResponse response = client.delete(path);

            assertThat(response.getStatusCode(), equalTo(404));
        }
    }

}