/*
 * Copyright 2016-2026 by floragunn GmbH - All rights reserved
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * DLS and FLS applied to the completion suggester (POST /suggest/_search with a "suggest" section).
 *
 * Java port of the sgi8 integration test test_dls_fls_suggest.sh (test suite enterprise_1): same index mapping, same sample
 * documents, same role definition and the same assertions. Uses an external process cluster to run against a real ES.
 *
 * The test runs against two indices with identical content:
 * <ul>
 * <li>"suggest" with a single shard, which is the Elasticsearch default for an index created without settings.</li>
 * <li>"suggest_multi_shard" with five shards and one replica. This is the shape of the index in the sgi8 test suite: the sgi8
 * post-init installs a legacy index template "default" for all indices with number_of_shards 5 and number_of_replicas 1 (see
 * sgi8 server/tmpl/docker-compose.tmpl). Elasticsearch only uses batched query execution (one NodeQueryRequest per data node
 * instead of one ShardSearchRequest per shard) when a data node holds at least two shards of the search. DLS on suggest
 * requests must also work on that path, which the single shard index never exercises.</li>
 * </ul>
 */
@RunWith(Parameterized.class)
public class DlsFlsSuggestIntTest {

    static final String INDEX_SINGLE_SHARD = "suggest";
    static final String INDEX_MULTI_SHARD = "suggest_multi_shard";
    static final String INDEX_PATTERN = "suggest*";
    static final String SUGGESTION_NAME = "example-suggest";

    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin")
            .roles(new Role("all_access").indexPermissions("*").on("*").clusterPermissions("*"));

    /**
     * Mirrors the sgi8 role sg_suggest_dls: unlimited on the suggest index, DLS restricts to country US, FLS hides the name field.
     */
    static final TestSgConfig.User SUGGEST_DLS_USER = new TestSgConfig.User("suggest_dls").roles(//
            new Role("sg_suggest_dls").indexPermissions("SGS_UNLIMITED")//
                    .dls("{ \"bool\": { \"must\": { \"match\": { \"country\": \"US\" }}}}")//
                    .fls("~name")//
                    .on(INDEX_PATTERN));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls();

    static final DocNode SUGGEST_QUERY = DocNode.of(//
            "suggest." + SUGGESTION_NAME + ".prefix", "511 Chur", //
            "suggest." + SUGGESTION_NAME + ".completion.field", "suggest");

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .users(ADMIN, SUGGEST_DLS_USER).resources("dlsfls")//
            .useExternalProcessCluster().build();

    @BeforeClass
    public static void setupTestData() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            createIndex(client, INDEX_SINGLE_SHARD, 1);
            createIndex(client, INDEX_MULTI_SHARD, 5);
        }
    }

    private static void createIndex(GenericRestClient client, String index, int shards) throws Exception {
        // Same mapping as sgi8 shared/sampledata/mappings_suggest.json
        HttpResponse response = client.putJson("/" + index, DocNode.of(//
                "settings.index.number_of_shards", shards, //
                "mappings.properties.country.type", "text", //
                "mappings.properties.name.type", "text", //
                "mappings.properties.suggest.type", "completion"));
        Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

        // Same documents as sgi8 shared/sampledata/suggest.json
        List<DocNode> bulk = new ArrayList<>();
        addDocument(bulk, "1", "John Doe", "US", "511 Church St, Richmond, Vic 3121");
        addDocument(bulk, "2", "Jane Doe", "US", "511 Church Avenue, Washington, Vic 3121");
        addDocument(bulk, "3", "Hans Meier", "DE", "511 Church Strasse, Bamberg");
        addDocument(bulk, "4", "Helene Meier", "DE", "511 Church Platz, München");
        addDocument(bulk, "5", "Herman Miller", "US", "Church Avenue, Washington, Vic 3121");
        addDocument(bulk, "6", "Paul Smith", "US", "The Church Avenue, Washington, Vic 3121");
        addDocument(bulk, "7", "Peter Schmidt", "DE", "The Church Avenue, Berlin");
        addDocument(bulk, "8", "Mark Hamilton", "US", "Totally different address");

        response = client.putNdJson("/" + index + "/_bulk?refresh=true", bulk.toArray(new DocNode[0]));
        Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
        Assert.assertEquals(response.getBody(), Boolean.FALSE, response.getBodyAsDocNode().get("errors"));
    }

    private static void addDocument(List<DocNode> bulk, String id, String name, String country, String suggest) {
        bulk.add(DocNode.of("index", DocNode.of("_id", id)));
        bulk.add(DocNode.of("name", name, "country", country, "suggest", suggest));
    }

    private final String index;

    public DlsFlsSuggestIntTest(String index) {
        this.index = index;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Object[] parameters() {
        return new Object[] { INDEX_SINGLE_SHARD, INDEX_MULTI_SHARD };
    }

    @Test
    public void admin_seesAllCountriesAndNames() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            HttpResponse response = client.postJson("/" + index + "/_search?pretty=true", SUGGEST_QUERY);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

            List<DocNode> options = getOptions(response);
            assertThat(response.getBody(), options, hasSize(5));
            assertThat(response.getBody(), countries(options), containsInAnyOrder("US", "US", "DE", "DE", "US"));
            assertThat(response.getBody(), options.get(0).getAsNode("_source").getAsString("name"), equalTo("Jane Doe"));
        }
    }

    @Test
    public void suggestDlsUser_seesOnlyUsAndNoName() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(SUGGEST_DLS_USER)) {
            HttpResponse response = client.postJson("/" + index + "/_search?pretty=true", SUGGEST_QUERY);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

            List<DocNode> options = getOptions(response);
            assertThat(response.getBody(), options, hasSize(3));
            assertThat(response.getBody(), countries(options), containsInAnyOrder("US", "US", "US"));
            assertThat(response.getBody(), countries(options), everyItem(not(equalTo("DE"))));

            // The name field must be removed by FLS
            Assert.assertNull(response.getBody(), options.get(0).getAsNode("_source").get("name"));
            for (DocNode option : options) {
                Assert.assertFalse(response.getBody(), option.getAsNode("_source").hasNonNull("name"));
            }
        }
    }

    private static List<DocNode> getOptions(HttpResponse response) throws Exception {
        return response.getBodyAsDocNode().getAsNode("suggest").getAsListOfNodes(SUGGESTION_NAME).get(0).getAsListOfNodes("options");
    }

    private static List<String> countries(List<DocNode> options) {
        List<String> result = new ArrayList<>();

        for (DocNode option : options) {
            result.add(option.getAsNode("_source").getAsString("country"));
        }

        return result;
    }
}
