/*
 * Copyright 2016-2022 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */

package com.floragunn.searchguard.enterprise.dlsfls;

import java.util.Set;
import java.util.regex.Pattern;

import com.floragunn.codova.documents.DocNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.BasicJsonPathDefaultConfiguration;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestData;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.jayway.jsonpath.Configuration.Defaults;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

import static com.floragunn.searchguard.test.RestMatchers.distinctNodesAt;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.noValueAt;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;

public class FmWithVariousFormattingIntTest {

    private static final Logger log = LogManager.getLogger(FmWithVariousFormattingIntTest.class);
    private final static String REGULAR_INDEX_NAME = "my-regular-index-000001";

    static final TestSgConfig.User A_TEST_USER = new TestSgConfig.User("a_test_user") //
            .roles(new Role("a_test_role") //
                    .clusterPermissions("CLUSTER_COMPOSITE_OPS_RO") //
                    .indexPermissions("SGS_INDICES_ALL") //
                    .maskedFields("location") //
                    .on("my*"));

    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled() //
            .dlsFls(DLSFLS).users(A_TEST_USER)
            .nodeSettings("indices.id_field_data.enabled", true) // needed for sorting by document _id
            .embedded()
            .build();

    @BeforeClass
    public static void setupTestData() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            // Data related to geo point masking tests
            DocNode mappings = DocNode.of("mappings.properties.location.type", "geo_point");
            client.putJson("/" + REGULAR_INDEX_NAME, mappings);


            client.putJson("/" + REGULAR_INDEX_NAME + "/_doc/1?refresh=true", DocNode.of(
                    "text", "Geopoint as an object using GeoJSON format",
                    "location", DocNode.of("type", "Point", "coordinates", ImmutableList.of(-71.34, 41.12))));

            client.putJson("/" + REGULAR_INDEX_NAME + "/_doc/2?refresh=true", DocNode.of(
                    "text", "Geopoint as a WKT POINT primitive",
                    "location", "POINT (-71.34 41.12)"
            ));

            client.putJson("/" + REGULAR_INDEX_NAME + "/_doc/3?refresh=true", DocNode.of(
                    "text", "Geopoint as an object with 'lat' and 'lon' keys",
                    "location", DocNode.of("lat", 41.12, "lon", -71.34)
            ));

            client.putJson("/" + REGULAR_INDEX_NAME + "/_doc/4?refresh=true", DocNode.of(
                    "text", "Geopoint as an array",
                    "location", ImmutableList.of(-71.34, 41.12)
            ));

            client.putJson("/" + REGULAR_INDEX_NAME + "/_doc/5?refresh=true", DocNode.of(
                    "text", "Geopoint as a string",
                    "location", "41.12,-71.34"
            ));

            client.putJson("/" + REGULAR_INDEX_NAME + "/_doc/6?refresh=true", DocNode.of(
                    "text", "Geopoint as a geohash",
                    "location", "drm3btev3e86"
            ));
        }
    }

    @Test
    public void shouldMaskGeoPoint() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(A_TEST_USER)) {
            GenericRestClient.HttpResponse response = client.get("/" + REGULAR_INDEX_NAME + "/_search?pretty&sort=_id:asc");
            log.info("Response status code: {} and body '{}'",response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("hits.hits[*]._source.text", everyItem(containsString("Geopoint")))));
            assertThat(response, json(noValueAt("hits.hits[0]._source.location")));
            assertThat(response, json(nodeAt("hits.hits[1]._source.location", is("14d582bf2558ca29eb5abbdc65893f8ff7f3b74d760c0344fb78ae595751b443"))));
            assertThat(response, json(noValueAt("hits.hits[2]._source.location")));
            assertThat(response, json(noValueAt("hits.hits[3]._source.location")));
            assertThat(response, json(nodeAt("hits.hits[4]._source.location", is("9c39927fff39ffbcc038768751f540f149a84c05c7f8b87545bca79d410a1225"))));
            assertThat(response, json(nodeAt("hits.hits[5]._source.location", is("2246bc1683251cfed0145a3145b5d39b08991790b55ea934688520f778202764"))));
        }
    }
}
