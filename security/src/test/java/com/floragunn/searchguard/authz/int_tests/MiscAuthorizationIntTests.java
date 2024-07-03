/*
 * Copyright 2020-2024 floragunn GmbH
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

import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsFieldPointedByJsonPath;
import static com.floragunn.searchsupport.junit.matcher.ExceptionsMatchers.messageContainsMatcher;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

import org.apache.http.HttpStatus;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.google.common.collect.ImmutableMap;

public class MiscAuthorizationIntTests {


    private static TestSgConfig.User NEG_LOOKAHEAD_USER = new TestSgConfig.User("neg_lookahead_user").roles(
            new Role("neg_lookahead_user_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS").indexPermissions("SGS_READ").on("/^(?!t.*).*/"));

    private static TestSgConfig.User REGEX_USER = new TestSgConfig.User("regex_user")
            .roles(new Role("regex_user_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS").indexPermissions("SGS_READ").on("/[^a-z].*/"));

    private static TestSgConfig.User HIDDEN_TEST_USER = new TestSgConfig.User("hidden_test_user").roles(
            new Role("hidden_test_user_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS").indexPermissions("*").on("hidden_test_not_hidden"));

    private static TestCertificates certificatesContext = TestCertificates.builder().ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard")
            .addNodes("CN=node-0.example.com,OU=SearchGuard,O=SearchGuard").addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addAdminClients("CN=admin-0.example.com;OU=SearchGuard;O=SearchGuard").build();

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster.Embedded anotherCluster = new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext)
            .user("resolve_test_user", "secret", new Role("resolve_test_user_role").indexPermissions("*").on("resolve_test_allow_*"))//
            .embedded().build();

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext).remote("my_remote", anotherCluster)
            .user("resolve_test_user", "secret",
                    new Role("resolve_test_user_role").indexPermissions("*").on("resolve_test_allow_*").indexPermissions("*")
                            .on("/alias_resolve_test_index_allow_.*/")) //
            .user("exclusion_test_user_basic", "secret",
                    new Role("exclusion_test_user_role").clusterPermissions("*").indexPermissions("*").on("exclude_test_*")
                            .excludeIndexPermissions("*").on("exclude_test_disallow_*"))//
            .user("exclusion_test_user_basic_no_pattern", "secret",
                    new Role("exclusion_test_user_basic_no_pattern_role").clusterPermissions("*").indexPermissions("*").on("exclude_test_*")
                            .excludeIndexPermissions("*").on("exclude_test_disallow_2"))//            
            .user("exclusion_test_user_write", "secret",
                    new Role("exclusion_test_user_action_exclusion_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS")//
                            .indexPermissions("*").on("write_exclude_test_*")//
                            .excludeIndexPermissions("SGS_WRITE").on("write_exclude_test_disallow_*"))//  
            .user("exclusion_test_user_write_no_pattern", "secret",
                    new Role("exclusion_test_user_write_no_pattern_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS")//
                            .indexPermissions("*").on("write_exclude_test_*")//
                            .excludeIndexPermissions("SGS_WRITE").on("write_exclude_test_disallow_2"))//  
            .user("exclusion_test_user_cluster_permission", "secret",
                    new Role("exclusion_test_user_cluster_permission_role").clusterPermissions("*")
                            .excludeClusterPermissions("indices:data/read/msearch").indexPermissions("*").on("exclude_test_*")
                            .excludeIndexPermissions("*").on("exclude_test_disallow_*"))//
            .user("admin", "admin", new Role("admin_role").clusterPermissions("*"))//
            .user("permssion_rest_api_user", "secret", new Role("permssion_rest_api_user_role").clusterPermissions("indices:data/read/mtv"))//
            .users().embedded().build();

    @ClassRule
    public static LocalCluster.Embedded clusterFof = new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext)
            .remote("my_remote", anotherCluster).ignoreUnauthorizedIndices(false)
            .user("resolve_test_user", "secret",
                    new Role("resolve_test_user_role").indexPermissions("*").on("resolve_test_allow_*").indexPermissions("*")
                            .on("/alias_resolve_test_index_allow_.*/")) //            
            .user("exclusion_test_user_basic", "secret",
                    new Role("exclusion_test_user_role").clusterPermissions("*").indexPermissions("*").on("exclude_test_*")
                            .excludeIndexPermissions("*").on("exclude_test_disallow_*"))//
            .user("exclusion_test_user_basic_no_pattern", "secret",
                    new Role("exclusion_test_user_basic_no_pattern_role").clusterPermissions("*").indexPermissions("*").on("exclude_test_*")
                            .excludeIndexPermissions("*").on("exclude_test_disallow_2"))//                   
            .user("exclusion_test_user_write", "secret",
                    new Role("exclusion_test_user_action_exclusion_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS")//
                            .indexPermissions("*").on("write_exclude_test_*")//
                            .excludeIndexPermissions("SGS_WRITE").on("write_exclude_test_disallow_*"))//  
            .user("exclusion_test_user_write_no_pattern", "secret",
                    new Role("exclusion_test_user_write_no_pattern_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS")//
                            .indexPermissions("*").on("write_exclude_test_*")//
                            .excludeIndexPermissions("SGS_WRITE").on("write_exclude_test_disallow_2"))//             
            .user("exclusion_test_user_cluster_permission", "secret",
                    new Role("exclusion_test_user_cluster_permission_role").clusterPermissions("*")
                            .excludeClusterPermissions("indices:data/read/msearch").indexPermissions("*").on("exclude_test_*")
                            .excludeIndexPermissions("*").on("exclude_test_disallow_*"))//
            .users(NEG_LOOKAHEAD_USER, REGEX_USER, HIDDEN_TEST_USER)//
            .embedded().build();

    @BeforeClass
    public static void setupTestData() {

        try (Client client = cluster.getInternalNodeClient()) {
            client.index(new IndexRequest("resolve_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();

            client.index(new IndexRequest("alias_resolve_test_index_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                    "index", "alias_resolve_test_index_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("alias_resolve_test_index_allow_aliased_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source(XContentType.JSON, "index", "alias_resolve_test_index_allow_aliased_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("alias_resolve_test_index_allow_aliased_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source(XContentType.JSON, "index", "alias_resolve_test_index_allow_aliased_2", "b", "y", "date", "1985/01/01")).actionGet();
            client.admin().indices().aliases(
                    new IndicesAliasesRequest().addAliasAction(AliasActions.add().alias("alias_resolve_test_alias_1").index("alias_resolve_test_*")))
                    .actionGet();

            client.index(new IndexRequest("exclude_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("exclude_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("exclude_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("exclude_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();
        }

        try (Client client = clusterFof.getInternalNodeClient()) {
            client.index(new IndexRequest("resolve_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();

            client.admin().indices()
                    .aliases(new IndicesAliasesRequest()
                            .addAliasAction(new AliasActions(AliasActions.Type.ADD).alias("resolve_test_allow_alias").indices("resolve_test_*")))
                    .actionGet();

            client.index(new IndexRequest("hidden_test_not_hidden").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "hidden_test_not_hidden", "b", "y", "date", "1985/01/01")).actionGet();

            client.admin().indices().create(new CreateIndexRequest(".hidden_test_actually_hidden").settings(ImmutableMap.of("index.hidden", true)))
                    .actionGet();
            client.index(new IndexRequest(".hidden_test_actually_hidden").id("test").source("a", "b").setRefreshPolicy(RefreshPolicy.IMMEDIATE))
                    .actionGet();

            client.index(new IndexRequest("exclude_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("exclude_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("exclude_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("exclude_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();

            client.index(new IndexRequest("tttexclude_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "tttexclude_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
        }

        try (Client client = anotherCluster.getInternalNodeClient()) {
            client.index(new IndexRequest("resolve_test_allow_remote_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "x",
                    "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_allow_remote_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a",
                    "xx", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_disallow_remote_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a",
                    "xx", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_disallow_remote_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a",
                    "xx", "b", "yy", "date", "1985/01/01")).actionGet();
        }
    }

    @Test
    public void detailsAboutMissingPermissions_shouldBeReturnedOnlyWhenAuthzDebugIsEnabled() throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient();
             GenericRestClient userClient = cluster.getRestClient("exclusion_test_user_basic", "secret")) {

            GenericRestClient.HttpResponse finalResponse = cluster.callAndRestoreConfig(CType.AUTHZ, () -> {
                GenericRestClient.HttpResponse httpResponse = adminCertClient.putJson("/_searchguard/config/authz", DocNode.of("debug", true));
                assertThat(httpResponse, isOk());

                httpResponse = userClient.get("alias_resolve_test_alias_1");
                assertThat(httpResponse, isForbidden());
                assertThat(httpResponse.getBody(), httpResponse.getBodyAsDocNode(), containsFieldPointedByJsonPath("error", "missing_permissions"));

                httpResponse = adminCertClient.putJson("/_searchguard/config/authz", DocNode.EMPTY);
                assertThat(httpResponse, isOk());

                 return userClient.get("alias_resolve_test_alias_1");
            });
            assertThat(finalResponse, isForbidden());
            assertThat(finalResponse.getBody(), finalResponse.getBodyAsDocNode(),
                not(containsFieldPointedByJsonPath("error", "missing_permissions")));
        }
    }

    @Test
    public void resolveTestRemote() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("resolve_test_user", "secret")) {

            HttpResponse httpResponse = restClient.get("/_resolve/index/my_remote:resolve_test_*");

            assertThat(httpResponse, isOk());
            assertThat(httpResponse,
                    json(nodeAt("indices[*].name", contains("my_remote:resolve_test_allow_remote_1", "my_remote:resolve_test_allow_remote_2"))));
        }
    }

    @Test
    public void resolveTestLocalRemoteMixed() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("resolve_test_user", "secret")) {

            HttpResponse httpResponse = restClient.get("/_resolve/index/resolve_test_*,my_remote:resolve_test_*_remote_*");

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(nodeAt("indices[*].name", contains("resolve_test_allow_1", "resolve_test_allow_2",
                    "my_remote:resolve_test_allow_remote_1", "my_remote:resolve_test_allow_remote_2"))));
        }
    }


    @Test
    public void excludeBasic() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient("exclusion_test_user_basic", "secret")) {

            HttpResponse httpResponse = restClient.get("/exclude_test_*/_search");

            assertThat(httpResponse, isOk());
            assertThat(httpResponse,
                    json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));
        }
    }

    @Test
    public void excludeBasicNoPattern() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient("exclusion_test_user_basic_no_pattern", "secret")) {

            HttpResponse httpResponse = restClient.get("/exclude_test_*/_search");

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(nodeAt("hits.hits[*]._source.index",
                    containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2", "exclude_test_disallow_1"))));
        }
    }

    @Test
    public void excludeWrite() throws Exception {
        try (Client client = cluster.getInternalNodeClient()) {
            client.index(new IndexRequest("write_exclude_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "write_exclude_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("write_exclude_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "write_exclude_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("write_exclude_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                    "index", "write_exclude_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("write_exclude_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                    "index", "write_exclude_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();
        }
        try (GenericRestClient restClient = cluster.getRestClient("exclusion_test_user_write", "secret");
                RestHighLevelClient client = cluster.getRestHighLevelClient("exclusion_test_user_write", "secret")) {

            HttpResponse httpResponse = restClient.get("/write_exclude_test_*/_search");

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("write_exclude_test_allow_1",
                    "write_exclude_test_allow_2", "write_exclude_test_disallow_1", "write_exclude_test_disallow_2"))));

            IndexResponse indexResponse = client.index(new IndexRequest("write_exclude_test_allow_1").source("a", "b"), RequestOptions.DEFAULT);

            assertThat(indexResponse.getResult(), equalTo(DocWriteResponse.Result.CREATED));

            ElasticsearchStatusException e = (ElasticsearchStatusException) assertThatThrown(
                    () -> client.index(new IndexRequest("write_exclude_test_disallow_1").source("a", "b"), RequestOptions.DEFAULT),
                    instanceOf(ElasticsearchStatusException.class), messageContainsMatcher("Insufficient permissions"));

            assertThat(e.status(), equalTo(RestStatus.FORBIDDEN));
        }
    }

    @Test
    public void excludeBasicFof() throws Exception {

        try (GenericRestClient restClient = clusterFof.getRestClient("exclusion_test_user_basic", "secret")) {

            HttpResponse httpResponse = restClient.get("/exclude_test_*/_search");
            assertThat(httpResponse, isForbidden());

            httpResponse = restClient.get("/exclude_test_allow_*/_search");
            assertThat(httpResponse, isOk());

            assertThat(httpResponse, json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

            httpResponse = restClient.get("/exclude_test_disallow_1/_search");
            assertThat(httpResponse, isForbidden());
        }
    }

    @Test
    public void excludeBasicFofNoPattern() throws Exception {

        try (GenericRestClient restClient = clusterFof.getRestClient("exclusion_test_user_basic_no_pattern", "secret")) {

            HttpResponse httpResponse = restClient.get("/exclude_test_*/_search");
            assertThat(httpResponse, isForbidden());

            httpResponse = restClient.get("/exclude_test_allow_*/_search");
            assertThat(httpResponse, isOk());

            assertThat(httpResponse, json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

            httpResponse = restClient.get("/exclude_test_disallow_1/_search");
            assertThat(httpResponse, isOk());

            httpResponse = restClient.get("/exclude_test_disallow_2/_search");
            assertThat(httpResponse, isForbidden());
        }
    }

    @Test
    public void excludeWriteFof() throws Exception {
        try (Client client = clusterFof.getInternalNodeClient()) {
            client.index(new IndexRequest("write_exclude_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "write_exclude_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("write_exclude_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "write_exclude_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("write_exclude_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                    "index", "write_exclude_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("write_exclude_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                    "index", "write_exclude_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();
        }

        try (GenericRestClient restClient = cluster.getRestClient("exclusion_test_user_write", "secret");
                RestHighLevelClient client = clusterFof.getRestHighLevelClient("exclusion_test_user_write", "secret")) {

            HttpResponse httpResponse = restClient.get("/write_exclude_test_*/_search");

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("write_exclude_test_allow_1",
                    "write_exclude_test_allow_2", "write_exclude_test_disallow_1", "write_exclude_test_disallow_2"))));

            IndexResponse indexResponse = client.index(new IndexRequest("write_exclude_test_allow_1").source("a", "b"), RequestOptions.DEFAULT);

            assertThat(indexResponse.getResult(), equalTo(DocWriteResponse.Result.CREATED));

            ElasticsearchStatusException e = (ElasticsearchStatusException) assertThatThrown(
                    () -> client.index(new IndexRequest("write_exclude_test_disallow_1").source("a", "b"), RequestOptions.DEFAULT),
                    instanceOf(ElasticsearchStatusException.class), messageContainsMatcher("Insufficient permissions"));
            assertThat(e.status(), equalTo(RestStatus.FORBIDDEN));
        }
    }

    @Test
    public void excludeClusterPermission() throws Exception {
        try (GenericRestClient basicCestClient = cluster.getRestClient("exclusion_test_user_basic", "secret");
                GenericRestClient clusterPermissionCestClient = cluster.getRestClient("exclusion_test_user_cluster_permission", "secret")) {

            HttpResponse httpResponse = basicCestClient.get("/exclude_test_*/_search");

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

            httpResponse = clusterPermissionCestClient.get("/exclude_test_*/_search");

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

            httpResponse = basicCestClient.postJson("/exclude_test_*/_msearch", "{}\n{\"query\": {\"match_all\": {}}}\n");
            assertThat(httpResponse, isOk());

            assertThat(httpResponse,
                    json(nodeAt("responses[0].hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

            httpResponse = clusterPermissionCestClient.postJson("/exclude_test_*/_msearch", "{}\n{\"query\": {\"match_all\": {}}}\n");
            assertThat(httpResponse, isForbidden());
        }
    }

    @Test
    public void permissionApi_evaluateClusterAndTenantPrivileges() throws Exception {
        try (GenericRestClient adminRestClient = cluster.getRestClient("admin", "admin");
                GenericRestClient permissionRestClient = cluster.getRestClient("permssion_rest_api_user", "secret")) {
            HttpResponse httpResponse = adminRestClient.get("/_searchguard/permission?permissions=indices:data/read/mtv,indices:data/read/viva");

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(nodeAt("permissions['indices:data/read/mtv']", equalTo(true))));
            assertThat(httpResponse, json(nodeAt("permissions['indices:data/read/viva']", equalTo(true))));

            httpResponse = permissionRestClient.get("/_searchguard/permission?permissions=indices:data/read/mtv,indices:data/read/viva");

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(nodeAt("permissions['indices:data/read/mtv']", equalTo(true))));
            assertThat(httpResponse, json(nodeAt("permissions['indices:data/read/viva']", equalTo(false))));
        }

    }
}
