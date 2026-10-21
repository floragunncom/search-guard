/*
 * Copyright 2026 floragunn GmbH
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

import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * ES 9.5 can create backing indices for past timestamps in time series data streams on the write path
 * (data_stream.past_tsdb_index_creation_enabled). The index creation is executed as the action
 * indices:admin/data_stream/auto_create_past_tsdb on behalf of the writing user and thus needs to be covered by the user's privileges.
 */
public class DataStreamPastTsdbIndexCreationIntTest {

    static final String DATA_STREAM_PREFIX = "tsds_past_index_creation";
    static final String DATA_STREAM_DENIED = DATA_STREAM_PREFIX + "_denied";
    static final String DATA_STREAM_ALLOWED = DATA_STREAM_PREFIX + "_allowed";

    static TestSgConfig.User USER_WRITE_ONLY = new TestSgConfig.User("write_only")
            .roles(new TestSgConfig.Role("write_only").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS")
                    .dataStreamPermissions("SGS_WRITE").on(DATA_STREAM_PREFIX + "*"));

    static TestSgConfig.User USER_WRITE_AND_CREATE_INDEX = new TestSgConfig.User("write_and_create_index")
            .roles(new TestSgConfig.Role("write_and_create_index").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS")
                    .dataStreamPermissions("SGS_WRITE", "SGS_CREATE_INDEX").on(DATA_STREAM_PREFIX + "*"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().authzDebug(true)
            .nodeSettings("data_stream.past_tsdb_index_creation_enabled", true)
            .users(USER_WRITE_ONLY, USER_WRITE_AND_CREATE_INDEX)
            .useExternalProcessCluster().build();

    @BeforeClass
    public static void createIndexTemplate() throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminCertClient.putJson("_index_template/" + DATA_STREAM_PREFIX + "-template", DocNode.of(
                    "index_patterns", Collections.singletonList(DATA_STREAM_PREFIX + "*"),
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
        }
    }

    @Test
    public void pastTimestamp_withoutPrivilegeForPastIndexCreation() throws Exception {
        createDataStream(DATA_STREAM_DENIED);

        try (GenericRestClient client = cluster.getRestClient(USER_WRITE_ONLY)) {
            GenericRestClient.HttpResponse response = client.postJson(DATA_STREAM_DENIED + "/_bulk?refresh=true", bulkCreate(Instant.now()));
            assertThat(response, isOk());
            assertThat(response, json(nodeAt("errors", equalTo(false))));
            assertThat(response, json(nodeAt("$.items[0].create.result", equalTo("created"))));

            response = client.postJson(DATA_STREAM_DENIED + "/_bulk?refresh=true", bulkCreate(Instant.now().minus(2, ChronoUnit.DAYS)));
            assertThat(response, isOk());
            assertThat(response, json(nodeAt("errors", equalTo(true))));
            assertThat(response, json(nodeAt("$.items[0].create.error.type", equalTo("security_exception"))));
            assertThat(getBackingIndexNames(DATA_STREAM_DENIED), hasSize(1));
        }
    }

    @Test
    public void pastTimestamp_withPrivilegeForPastIndexCreation() throws Exception {
        createDataStream(DATA_STREAM_ALLOWED);

        try (GenericRestClient client = cluster.getRestClient(USER_WRITE_AND_CREATE_INDEX)) {
            GenericRestClient.HttpResponse response = client.postJson(DATA_STREAM_ALLOWED + "/_bulk?refresh=true",
                    bulkCreate(Instant.now().minus(2, ChronoUnit.DAYS)));
            assertThat(response, isOk());
            assertThat(response, json(nodeAt("errors", equalTo(false))));
            assertThat(response, json(nodeAt("$.items[0].create.result", equalTo("created"))));
            assertThat(getBackingIndexNames(DATA_STREAM_ALLOWED), hasSize(2));
        }
    }

    private static String bulkCreate(Instant timestamp) {
        return """
                {"create": {}}
                { "@timestamp": "%s", "device_id": "1", "temperature": 30.5 }
                """.formatted(timestamp.toString());
    }

    private static void createDataStream(String dataStreamName) throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminCertClient.put("_data_stream/" + dataStreamName);
            assertThat(response, isOk());
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> getBackingIndexNames(String dataStreamName) throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminCertClient.get("_data_stream/" + dataStreamName);
            assertThat(response, isOk());
            return (List<String>) response.getBodyAsDocNode().findByJsonPath("$.data_streams[0].indices[*].index_name");
        }
    }
}
