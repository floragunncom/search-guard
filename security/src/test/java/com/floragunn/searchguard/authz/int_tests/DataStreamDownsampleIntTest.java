/*
 * Copyright 2024 floragunn GmbH
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

package com.floragunn.searchguard.authz.int_tests;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.RestMatchers;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.hamcrest.Matcher;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;

import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.valueSatisfiesMatcher;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class DataStreamDownsampleIntTest {

    private static final String DATA_STREAM_NAME = "tsds-iot";

    static TestSgConfig.User USER_WITH_ACCESS_TO_DATA_STREAM_AND_TARGET_INDEX = new TestSgConfig.User("ds_target_access")//
            .description("access to ds and downsample target index")//
            .roles(//
                    new TestSgConfig.Role("r1")
                            .dataStreamPermissions(
                                    "indices:admin/data_stream/get",
                                    "indices:admin/rollover", "indices:monitor/stats",
                                    "indices:admin/block/add",
                                    "indices:admin/xpack/downsample"
                            ).on(DATA_STREAM_NAME)
                            .indexPermissions("indices:admin/create")
                            .on("downsample_target*")

            );

    static TestSgConfig.User USER_WITH_ACCESS_ONLY_TO_DATA_STREAM = new TestSgConfig.User("only_ds_access")//
            .description("access only to ds")//
            .roles(//
                    new TestSgConfig.Role("r1")
                            .dataStreamPermissions(
                                    "indices:admin/data_stream/get",
                                    "indices:admin/rollover", "indices:monitor/stats",
                                    "indices:admin/block/add",
                                    "indices:admin/xpack/downsample"
                            ).on(DATA_STREAM_NAME)

            );

    static TestSgConfig.User USER_ASYNC_1 = new TestSgConfig.User("async_one")//
            .description("async one")//
            .roles(//
                    new TestSgConfig.Role("async_one") //
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO") // needed for _async_search
                            .dataStreamPermissions("SGS_READ") //
                            .on(DATA_STREAM_NAME)

            );

    static TestSgConfig.User USER_ASYNC_2 = new TestSgConfig.User("async_two")//
            .description("async two")//
            .roles(//
                    new TestSgConfig.Role("async_two") //
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO") // needed for _async_search
                            .dataStreamPermissions("SGS_READ") //
                            .on(DATA_STREAM_NAME)

            );
    static TestSgConfig.User USER_ASYNC_BYPASS_OWNER_CHECK = new TestSgConfig.User("async_bypass_owner_check")//
            .description("async two")//
            .roles(//
                    new TestSgConfig.Role("async_bypass_owner_check") //
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "indices:searchguard:async_search/_all_owners") //
                            .dataStreamPermissions("SGS_READ") //
                            .on(DATA_STREAM_NAME)

            );

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
            .singleNode()
            .sslEnabled()
            .authzDebug(true)
            .users(USER_WITH_ACCESS_TO_DATA_STREAM_AND_TARGET_INDEX, USER_WITH_ACCESS_ONLY_TO_DATA_STREAM,
                    USER_ASYNC_1, USER_ASYNC_2, USER_ASYNC_BYPASS_OWNER_CHECK)
            .enterpriseModulesEnabled()
            .dlsFls(new TestSgConfig.DlsFls())
            .useExternalProcessCluster()
            .build();

    @BeforeClass
    public static void beforeClass() throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {

            GenericRestClient.HttpResponse response = adminCertClient.putJson("_index_template/tsds-iot-template", DocNode.of(
                    "index_patterns", Collections.singletonList(DATA_STREAM_NAME + "*"),
                    "data_stream", DocNode.EMPTY,
                    "template", DocNode.of(
                            "settings.index.mode", "time_series",
                            "settings.index.routing_path", "device_id",
                            "mappings.properties.device_id.type", "keyword",
                            "mappings.properties.device_id.time_series_dimension", true,
                            "mappings.properties.temperature.type", "half_float",
                            "mappings.properties.temperature.time_series_metric", "gauge",
                            "mappings.properties.@timestamp.type", "date"
                    )
            ));

            assertThat(response, isOk());

            response = adminCertClient.postJson(DATA_STREAM_NAME + "/_bulk?refresh=true", String.format(
                    """
                    {"create": {}}
                    { "@timestamp": "%s", "device_id": "1", "temperature": 30.5 }
                    {"create": {}}
                    { "@timestamp": "%s", "device_id": "2", "temperature": 14 }
                    {"create": {}}
                    { "@timestamp": "%s", "device_id": "1", "temperature": 18 }
                    {"create": {}}
                    { "@timestamp": "%s", "device_id": "2", "temperature": 10.5 }
                    """,
                    Instant.now().minus(10, ChronoUnit.MINUTES).toString(),
                    Instant.now().minus(15, ChronoUnit.MINUTES).toString(),
                    Instant.now().minus(7, ChronoUnit.MINUTES).toString(),
                    Instant.now().plus(3, ChronoUnit.MINUTES)
            ));

            assertThat(response, isOk());
            assertThat(response, json(nodeAt("errors", equalTo(false))));
            assertThat(response, json(nodeAt("$.items[*].create.result", hasSize(4))));
            assertThat(response, json(nodeAt("$.items[*].create.result", everyItem(equalTo("created")))));
        }
    }

    @Test
    public void testDownsampleTimeSeriesDataStreamIndex() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_WITH_ACCESS_TO_DATA_STREAM_AND_TARGET_INDEX)) {
            GenericRestClient.HttpResponse response = client.get("/_data_stream/" + DATA_STREAM_NAME);
            assertThat(response, isOk());

            String dataStreamBackingIndex = response.getBodyAsDocNode().findSingleValueByJsonPath("$.data_streams[0].indices[0].index_name", String.class);

            response = client.post(DATA_STREAM_NAME + "/_rollover");
            assertThat(response, isOk());

            response = client.put(dataStreamBackingIndex + "/_block/write");
            assertThat(response, isOk());

            response = client.postJson(dataStreamBackingIndex + "/_downsample/" + "downsample_target_1", DocNode.of("fixed_interval", "1h"));
            assertThat(response, isOk());
        }

        try (GenericRestClient client = cluster.getRestClient(USER_WITH_ACCESS_ONLY_TO_DATA_STREAM)) {
            GenericRestClient.HttpResponse response = client.get("/_data_stream/" + DATA_STREAM_NAME);
            assertThat(response, isOk());

            String dataStreamBackingIndex = response.getBodyAsDocNode().findSingleValueByJsonPath("$.data_streams[0].indices[0].index_name", String.class);

            response = client.post(DATA_STREAM_NAME + "/_rollover");
            assertThat(response, isOk());

            response = client.put(dataStreamBackingIndex + "/_block/write");
            assertThat(response, isOk());

            response = client.postJson(dataStreamBackingIndex + "/_downsample/" + "downsample_target_2", DocNode.of("fixed_interval", "1h"));
            assertThat(response, isForbidden());
        }
    }

    /*
     * This test feet better to the class ResourceOwnerServiceTests. However, to test async search status request we need external process cluster
     * which is expensive to start. Therefore, we keep it here.
     */
    @Test
    public void testAsyncSearch() throws Exception {
        try (GenericRestClient client1 = cluster.getRestClient(USER_ASYNC_1);
                GenericRestClient client2 = cluster.getRestClient(USER_ASYNC_2);
                GenericRestClient clientBypassOwnerCheck = cluster.getRestClient(USER_ASYNC_BYPASS_OWNER_CHECK)) {
            // user associated with client1 starts an async search operation so she or he becomes the resource owner
            GenericRestClient.HttpResponse response = client1.postJson("/" + DATA_STREAM_NAME + "/_async_search?&wait_for_completion_timeout=1ms",
                    DocNode.EMPTY);
            assertThat(response, RestMatchers.isOk());
            String asyncSearchId = response.getBodyAsDocNode().getAsString("id");
            assertThat(asyncSearchId, notNullValue());

            // status for the allowed user (resource owner)
            response = client1.get("/_async_search/status/" + asyncSearchId);
            assertThat(response, RestMatchers.isOk());

            // status for the not allowed user
            response = client2.get("/_async_search/status/" + asyncSearchId);
            assertThat(response, RestMatchers.isForbidden());
            Matcher<String> reasonMatcher = containsString("is not owned by user " + USER_ASYNC_2.getName());
            assertThat(response.getBodyAsDocNode(), valueSatisfiesMatcher("$.error.reason", String.class, reasonMatcher));

            // status for the user bypassing owner check
            response = clientBypassOwnerCheck.get("/_async_search/status/" + asyncSearchId);
            assertThat(response, RestMatchers.isOk());
        }
    }

}