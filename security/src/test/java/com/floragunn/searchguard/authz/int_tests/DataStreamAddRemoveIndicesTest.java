package com.floragunn.searchguard.authz.int_tests;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestComponentTemplate;
import com.floragunn.searchguard.test.TestIndexTemplate;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class DataStreamAddRemoveIndicesTest {

    public static final String DS_TEST_ADD_INDEX_POSITIVE = "ds_test_add_index_positive";
    public static final String INDEX_DS_ADDITION_POSITIVE = "index_ds_addition_positive";
    public static final String DS_TEST_REMOVE_INDEX_POSITIVE = "ds_test_remove_index_positive";
    public static final String DS_TEST_MIXED_OPERATIONS = "ds_test_mixed_operations";

    static TestSgConfig.User USER_NO_PERMISSIONS = new TestSgConfig.User("no_permissions")
            .roles(new TestSgConfig.Role("no_permissions").clusterPermissions().indexPermissions().on().aliasPermissions().on().dataStreamPermissions().on());

    static TestSgConfig.User USER_WITH_PERMISSIONS_TO_ADD = new TestSgConfig.User("with_permissions_to_add")
            .roles(new TestSgConfig.Role("with_permissions_to_add").clusterPermissions()
                    .indexPermissions("indices:admin/data_stream/modify").on(INDEX_DS_ADDITION_POSITIVE + "*")
                    .aliasPermissions().on()
                    .dataStreamPermissions("indices:admin/data_stream/modify").on(DS_TEST_ADD_INDEX_POSITIVE + "*"));

    static TestSgConfig.User USER_WITH_MIXED_PERMISSIONS = new TestSgConfig.User("with_mixed_permissions")
            .roles(new TestSgConfig.Role("with_mixed_permissions").clusterPermissions()
                    .indexPermissions("indices:admin/data_stream/modify").on("index_mixed_*", ".ds-" + DS_TEST_MIXED_OPERATIONS + "*")
                    .aliasPermissions().on()
                    .dataStreamPermissions("indices:admin/data_stream/modify").on(DS_TEST_MIXED_OPERATIONS));

    static TestSgConfig.User USER_WITH_PERMISSIONS_TO_REMOVE = new TestSgConfig.User("with_permissions_to_remove")
            .roles(new TestSgConfig.Role("with_permissions_to_remove").clusterPermissions()
                    .indexPermissions("indices:admin/data_stream/modify").on(".ds-" + DS_TEST_REMOVE_INDEX_POSITIVE + "*")
                    .aliasPermissions().on()
                    .dataStreamPermissions("indices:admin/data_stream/modify").on(DS_TEST_REMOVE_INDEX_POSITIVE));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .users(USER_NO_PERMISSIONS, USER_WITH_PERMISSIONS_TO_ADD, USER_WITH_PERMISSIONS_TO_REMOVE, USER_WITH_MIXED_PERMISSIONS)//
            .indexTemplates(new TestIndexTemplate("ds_test", "ds_*").dataStream().composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL))//
            .authzDebug(true)//
            .enterpriseModulesEnabled()
            .useExternalProcessCluster().build();

    @Test
    public void addIndexToDataStream_noPermission() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_NO_PERMISSIONS)) {
            String dsName = "ds_test_add_index";
            String indexToAdd = "my_new_fake_index";
            createIndex(indexToAdd);
            createDataStream(dsName);
            List<String> dsBackingIndexNames = getDsBackingIndexNames(dsName);
            assertThat(dsBackingIndexNames, hasSize(1));
            assertThat(dsBackingIndexNames, everyItem(startsWith(".ds-" + dsName)));

            GenericRestClient.HttpResponse addIndexResponse = client.postJson("/_data_stream/_modify", """
                    {
                      "actions": [
                        {
                          "add_backing_index": {
                            "data_stream": "%s",
                            "index": "%s"
                          }
                        }
                      ]
                    }
                    """.formatted(dsName, indexToAdd));
            assertThat("User without permissions should get 403 response", addIndexResponse, isForbidden());
        }
    }

    @Test
    public void addIndexToDataStream_withAllPermission() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS_TO_ADD)) {
            String dsName = DS_TEST_ADD_INDEX_POSITIVE;
            String indexToAdd = INDEX_DS_ADDITION_POSITIVE;
            createIndex(indexToAdd);
            createDataStream(dsName);
            // Two additional data streams are created. User does not have permissions related to these data streams. This enables the test to
            // detect bugs related to index resolution when too many indices are resolved, e.g., all local indices are included in the result set
            // during index resolution for the request /_data_stream/_modify
            createDataStream("ds_one");
            createDataStream("ds_two");
            List<String> dsBackingIndexNames = getDsBackingIndexNames(dsName);
            assertThat(dsBackingIndexNames, hasSize(1));
            assertThat(dsBackingIndexNames, everyItem(startsWith(".ds-" + dsName)));

            String requestBody = """
                    {
                      "actions": [
                        {
                          "add_backing_index": {
                            "data_stream": "%s",
                            "index": "%s"
                          }
                        }
                      ]
                    }
                    """.formatted(dsName, indexToAdd);
            GenericRestClient.HttpResponse addIndexResponse = client.postJson("/_data_stream/_modify", requestBody);
            assertThat("User with permissions should get 200 response", addIndexResponse, isOk());
            dsBackingIndexNames = getDsBackingIndexNames(dsName);
            assertThat(INDEX_DS_ADDITION_POSITIVE + " was not included to data stream", dsBackingIndexNames.contains(INDEX_DS_ADDITION_POSITIVE), is(true));
        }
    }

    @Test
    public void addIndexToDataStream_withLackingDsPermissions() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS_TO_ADD)) {
            String dsName = "ds_user_is_lacking_permission_to_this_data_stream";
            String indexToAdd = INDEX_DS_ADDITION_POSITIVE + "_withlackingdspermissions";
            createIndex(indexToAdd);
            createDataStream(dsName);
            List<String> dsBackingIndexNames = getDsBackingIndexNames(dsName);
            assertThat(dsBackingIndexNames, hasSize(1));
            assertThat(dsBackingIndexNames, everyItem(startsWith(".ds-" + dsName)));

            String requestBody = """
                    {
                      "actions": [
                        {
                          "add_backing_index": {
                            "data_stream": "%s",
                            "index": "%s"
                          }
                        }
                      ]
                    }
                    """.formatted(dsName, indexToAdd);
            GenericRestClient.HttpResponse addIndexResponse = client.postJson("/_data_stream/_modify", requestBody);
            assertThat( addIndexResponse, isForbidden());
            dsBackingIndexNames = getDsBackingIndexNames(dsName);
            assertThat(indexToAdd + " was included to data stream", dsBackingIndexNames.contains(indexToAdd), is(false));
        }
    }

    @Test
    public void addIndexToDataStream_withLackingIndexPermission() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS_TO_ADD)) {
            String dsName = DS_TEST_ADD_INDEX_POSITIVE + "-withlackingindexpermission";
            String unauthorizedIndex = "index-user-lack-permission-to-include-index-into-data-stream";
            createIndex(unauthorizedIndex);
            createDataStream(dsName);
            List<String> dsBackingIndexNames = getDsBackingIndexNames(dsName);
            assertThat(dsBackingIndexNames, hasSize(1));
            assertThat(dsBackingIndexNames, everyItem(startsWith(".ds-" + dsName)));

            String requestBody = """
                    {
                      "actions": [
                        {
                          "add_backing_index": {
                            "data_stream": "%s",
                            "index": "%s"
                          }
                        }
                      ]
                    }
                    """.formatted(dsName, unauthorizedIndex);
            GenericRestClient.HttpResponse addIndexResponse = client.postJson("/_data_stream/_modify", requestBody);
            assertThat("User lacking index permission should get 403 response", addIndexResponse, isForbidden());
            dsBackingIndexNames = getDsBackingIndexNames(dsName);
            assertThat(unauthorizedIndex + " was included to data stream", dsBackingIndexNames.contains(unauthorizedIndex), is(false));
        }
    }

    @Test
    public void removeIndexFromDataStream() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_NO_PERMISSIONS)) {
            String dsName = "ds_test_remove_index";
            createDataStream(dsName);
            //we need to rollover ds since we cannot remove the only index that is the write index
            rolloverDataStream(dsName);
            List<String> dsBackingIndexNames = getDsBackingIndexNames(dsName);
            assertThat(dsBackingIndexNames, hasSize(2));
            assertThat(dsBackingIndexNames, everyItem(startsWith(".ds-" + dsName)));
            assertThat(dsBackingIndexNames.get(0), endsWith("01"));
            String dsBackingIndexToRemove = dsBackingIndexNames.get(0);

            GenericRestClient.HttpResponse response = client.postJson("/_data_stream/_modify", """
                    {
                      "actions": [
                        {
                          "remove_backing_index": {
                            "data_stream": "%s",
                            "index": "%s"
                          }
                        }
                      ]
                    }
                    """.formatted(dsName, dsBackingIndexToRemove));
            assertThat("User without permissions should get 403 response", response, isForbidden());
            dsBackingIndexNames = getDsBackingIndexNames(dsName);
            assertThat("Backing index should still be present after forbidden remove", dsBackingIndexNames.contains(dsBackingIndexToRemove), is(true));
        }
    }

    @Test
    public void removeIndexFromDataStream_withAllPermission() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_WITH_PERMISSIONS_TO_REMOVE)) {
            String dsName = DS_TEST_REMOVE_INDEX_POSITIVE;
            createDataStream(dsName);
            //we need to rollover ds since we cannot remove the only index that is the write index
            rolloverDataStream(dsName);
            List<String> dsBackingIndexNames = getDsBackingIndexNames(dsName);
            assertThat(dsBackingIndexNames, hasSize(2));
            assertThat(dsBackingIndexNames, everyItem(startsWith(".ds-" + dsName)));
            assertThat(dsBackingIndexNames.get(0), endsWith("01"));
            String dsBackingIndexToRemove = dsBackingIndexNames.get(0);

            GenericRestClient.HttpResponse response = client.postJson("/_data_stream/_modify", """
                    {
                      "actions": [
                        {
                          "remove_backing_index": {
                            "data_stream": "%s",
                            "index": "%s"
                          }
                        }
                      ]
                    }
                    """.formatted(dsName, dsBackingIndexToRemove));
            assertThat("User with permissions should get 200 response", response, isOk());
            dsBackingIndexNames = getDsBackingIndexNames(dsName);
            assertThat(dsBackingIndexNames + " not removed from data stream", dsBackingIndexNames.contains(dsBackingIndexToRemove), is(false ));
        }
    }

    private void rolloverDataStream(String dsName) throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminClient.post(dsName + "/_rollover");
            assertThat(response, isOk());
        }
    }

    private void createDataStream(String dsName) throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminClient.put("_data_stream/" + dsName);
            assertThat(response, isOk());
        }
    }

    private void createIndex(String indexName) throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminClient.putJson(indexName, """
                    {
                      "mappings": {
                        "properties": {
                          "@timestamp": {
                            "type": "date",
                            "format": "date_optional_time"
                          }
                        }
                      }
                    }
                    """);
            assertThat(response, isOk());
        }
    }

    private List<String> getDsBackingIndexNames(String dsName) throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminClient.get("_data_stream/" + dsName);
            assertThat(response, isOk());
            List<String> dsBackingIndexNames = (List<String>) response.getBodyAsDocNode().findByJsonPath("$.data_streams[0].indices[*].index_name");
            assertThat(dsBackingIndexNames, not(empty()));
            return dsBackingIndexNames;
        }
    }
}
