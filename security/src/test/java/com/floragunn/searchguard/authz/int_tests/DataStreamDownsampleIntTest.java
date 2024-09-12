package com.floragunn.searchguard.authz.int_tests;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;

import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;

public class DataStreamDownsampleIntTest {

    private static final String DATA_STREAM_NAME = "tsds-iot";

    static TestSgConfig.User LIMITED_USER = new TestSgConfig.User("limited_user")//
            .description("unlimited complete")//
            .roles(//
                    new TestSgConfig.Role("r1")
                            .dataStreamPermissions(
                                    "indices:admin/data_stream/get",
                                    "indices:admin/rollover", "indices:monitor/stats",
                                    "indices:admin/block/add",
                                    "indices:admin/xpack/downsample"
                            ).on(DATA_STREAM_NAME)

            );

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
            .singleNode()
            .sslEnabled()
            .users(LIMITED_USER)
            .dlsFls(new TestSgConfig.DlsFls().useImpl("flx")) // it doesn't work with legacy impl
            .enterpriseModulesEnabled()
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
        try (GenericRestClient client = cluster.getRestClient(LIMITED_USER)) {
            GenericRestClient.HttpResponse response = client.get("/_data_stream/" + DATA_STREAM_NAME);
            assertThat(response, isOk());

            String dataStreamBackingIndex = response.getBodyAsDocNode().findSingleValueByJsonPath("$.data_streams[0].indices[0].index_name", String.class);

            response = client.post(DATA_STREAM_NAME + "/_rollover");
            assertThat(response, isOk());

            response = client.put(dataStreamBackingIndex + "/_block/write");
            assertThat(response, isOk());

            response = client.postJson(dataStreamBackingIndex + "/_downsample/" + "downsample_target", DocNode.of("fixed_interval", "1h"));
            assertThat(response, isOk());
        }
    }
}
