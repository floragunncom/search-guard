package com.floragunn.searchguard;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchTemplateRequest;
import co.elastic.clients.elasticsearch.core.SearchTemplateResponse;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.elasticsearch.indices.ShrinkResponse;
import co.elastic.clients.json.JsonData;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.client.RestHighLevelClient;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.google.common.collect.ImmutableMap;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;

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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class PrivilegesEvaluatorTest {

    private static TestSgConfig.User RESIZE_USER_WITHOUT_CREATE_INDEX_PRIV = new TestSgConfig.User("resize_user_without_create_index_priv")
            .roles(new Role("resize_role").clusterPermissions("*").indexPermissions("indices:admin/resize", "indices:monitor/stats")
                    .on("resize_test_source"));

    private static TestSgConfig.User RESIZE_USER = new TestSgConfig.User("resize_user")
            .roles(new Role("resize_role").clusterPermissions("*").indexPermissions("indices:admin/resize", "indices:monitor/stats")
                    .on("resize_test_source").indexPermissions("SGS_CREATE_INDEX").on("resize_test_target"));

    private static TestSgConfig.User SEARCH_TEMPLATE_USER = new TestSgConfig.User("search_template_user").roles(new Role("search_template_role")
            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_SEARCH_TEMPLATES").indexPermissions("SGS_READ").on("resolve_test_*"));

    private static TestSgConfig.User SEARCH_NO_TEMPLATE_USER = new TestSgConfig.User("search_no_template_user").roles(
            new Role("search_no_template_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS").indexPermissions("SGS_READ").on("resolve_test_*"));

    private static TestSgConfig.User NEG_LOOKAHEAD_USER = new TestSgConfig.User("neg_lookahead_user").roles(
            new Role("neg_lookahead_user_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS").indexPermissions("SGS_READ").on("/^(?!t.*).*/"));

    private static TestSgConfig.User REGEX_USER = new TestSgConfig.User("regex_user")
            .roles(new Role("regex_user_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS").indexPermissions("SGS_READ").on("/[^a-z].*/"));

    private static TestSgConfig.User SEARCH_TEMPLATE_LEGACY_USER = new TestSgConfig.User("search_template_legacy_user")
            .roles(new Role("search_template_legacy_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS").indexPermissions("SGS_READ")
                    .on("resolve_test_*").indexPermissions("indices:data/read/search/template").on("*"));

    private static TestSgConfig.User HIDDEN_TEST_USER = new TestSgConfig.User("hidden_test_user").roles(
            new Role("hidden_test_user_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS").indexPermissions("*").on("hidden_test_not_hidden"));

    private static TestCertificates certificatesContext = TestCertificates.builder().ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard")
            .addNodes("CN=node-0.example.com,OU=SearchGuard,O=SearchGuard").addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addAdminClients("CN=admin-0.example.com,OU=SearchGuard,O=SearchGuard").build();

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster anotherCluster = new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext)
            .user("resolve_test_user", "secret", new Role("resolve_test_user_role").indexPermissions("*").on("resolve_test_allow_*"))//
            .build();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext).remote("my_remote", anotherCluster)
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
            .users(SEARCH_TEMPLATE_USER, SEARCH_NO_TEMPLATE_USER, SEARCH_TEMPLATE_LEGACY_USER).build();

    @ClassRule
    public static LocalCluster clusterFof = new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext)
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
            .users(RESIZE_USER, RESIZE_USER_WITHOUT_CREATE_INDEX_PRIV, NEG_LOOKAHEAD_USER, REGEX_USER, HIDDEN_TEST_USER)//
            .build();

    @BeforeClass
    public static void setupTestData() {
            Client client = cluster.getInternalNodeClient();
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

            Client clientFof = clusterFof.getInternalNodeClient();
            clientFof.index(new IndexRequest("resolve_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            clientFof.index(new IndexRequest("resolve_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
            clientFof.index(new IndexRequest("resolve_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
            clientFof.index(new IndexRequest("resolve_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "resolve_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();

            clientFof.admin().indices()
                    .aliases(new IndicesAliasesRequest()
                            .addAliasAction(new AliasActions(AliasActions.Type.ADD).alias("resolve_test_allow_alias").indices("resolve_test_*")))
                    .actionGet();

            clientFof.index(new IndexRequest("hidden_test_not_hidden").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "hidden_test_not_hidden", "b", "y", "date", "1985/01/01")).actionGet();

            clientFof.admin().indices().create(new CreateIndexRequest(".hidden_test_actually_hidden").settings(ImmutableMap.of("index.hidden", true)))
                    .actionGet();
            clientFof.index(new IndexRequest(".hidden_test_actually_hidden").id("test").source("a", "b").setRefreshPolicy(RefreshPolicy.IMMEDIATE))
                    .actionGet();

            clientFof.index(new IndexRequest("exclude_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            clientFof.index(new IndexRequest("exclude_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
            clientFof.index(new IndexRequest("exclude_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
            clientFof.index(new IndexRequest("exclude_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();

            clientFof.index(new IndexRequest("tttexclude_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "tttexclude_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();

            Client anotherClient = anotherCluster.getInternalNodeClient();
            anotherClient.index(new IndexRequest("resolve_test_allow_remote_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "x",
                    "b", "y", "date", "1985/01/01")).actionGet();
            anotherClient.index(new IndexRequest("resolve_test_allow_remote_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a",
                    "xx", "b", "yy", "date", "1985/01/01")).actionGet();
            anotherClient.index(new IndexRequest("resolve_test_disallow_remote_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a",
                    "xx", "b", "yy", "date", "1985/01/01")).actionGet();
            anotherClient.index(new IndexRequest("resolve_test_disallow_remote_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a",
                    "xx", "b", "yy", "date", "1985/01/01")).actionGet();
    }

    @Test
    public void detailsAboutMissingPermissions_shouldBeReturnedOnlyWhenAuthzDebugIsEnabled() throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient();
             GenericRestClient userClient = cluster.getRestClient("exclusion_test_user_basic", "secret")) {

            cluster.callAndRestoreConfig(CType.AUTHZ, () -> {

                HttpResponse httpResponse = adminCertClient.get("/_searchguard/config/authz");
                assertThat(httpResponse, isOk());

                DocNode authzConfig = httpResponse.getBodyAsDocNode();
                //authz debug enabled
                authzConfig = authzConfig.with("debug", true);

                httpResponse = adminCertClient.putJson("/_searchguard/config/authz", authzConfig);
                assertThat(httpResponse, isOk());

                httpResponse = userClient.get("alias_resolve_test_alias_1");
                assertThat(httpResponse, isForbidden());
                assertThat(httpResponse.getBody(), httpResponse.getBodyAsDocNode(), containsFieldPointedByJsonPath("error", "missing_permissions"));

                //authz debug disabled
                authzConfig = authzConfig.with("debug", false);

                httpResponse = adminCertClient.putJson("/_searchguard/config/authz", authzConfig);
                assertThat(httpResponse, isOk());

                httpResponse = userClient.get("alias_resolve_test_alias_1");
                assertThat(httpResponse, isForbidden());
                assertThat(httpResponse.getBody(), httpResponse.getBodyAsDocNode(), not(containsFieldPointedByJsonPath("error", "missing_permissions")));

                return null;
            });

        }
    }

    @Test
    public void resolveTestLocal() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient("resolve_test_user", "secret")) {
            HttpResponse httpResponse = restClient.get("/_resolve/index/resolve_test_*");

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(nodeAt("indices[*].name", contains("resolve_test_allow_1", "resolve_test_allow_2"))));
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
    public void resolveTestAliasAndIndexMixed() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("resolve_test_user", "secret")) {

            HttpResponse httpResponse = restClient.get("/_resolve/index/alias_resolve_test_*");

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(nodeAt("indices[*].name", containsInAnyOrder("alias_resolve_test_index_allow_aliased_1",
                    "alias_resolve_test_index_allow_aliased_2", "alias_resolve_test_index_allow_1"))));
        }
    }

    @Test
    public void readAliasAndIndexMixed() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("resolve_test_user", "secret")) {

            HttpResponse httpResponse = restClient.get("/alias_resolve_test_*/_search");

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("alias_resolve_test_index_allow_aliased_1",
                    "alias_resolve_test_index_allow_aliased_2", "alias_resolve_test_index_allow_1"))));
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
        Client internalClient = cluster.getInternalNodeClient();
            internalClient.index(new IndexRequest("write_exclude_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "write_exclude_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            internalClient.index(new IndexRequest("write_exclude_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "write_exclude_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
            internalClient.index(new IndexRequest("write_exclude_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                    "index", "write_exclude_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
            internalClient.index(new IndexRequest("write_exclude_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                    "index", "write_exclude_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();
        try (GenericRestClient restClient = cluster.getRestClient("exclusion_test_user_write", "secret");
                RestHighLevelClient client = cluster.getRestHighLevelClient("exclusion_test_user_write", "secret")) {

            HttpResponse httpResponse = restClient.get("/write_exclude_test_*/_search");

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("write_exclude_test_allow_1",
                    "write_exclude_test_allow_2", "write_exclude_test_disallow_1", "write_exclude_test_disallow_2"))));

            IndexResponse indexResponse = client.index("write_exclude_test_allow_1", Map.of("a", "b"));

            assertThat(indexResponse.result(), equalTo(Result.Created));

            ElasticsearchException e = (ElasticsearchException) assertThatThrown(() ->
                    client.index("write_exclude_test_disallow_1", Map.of("a", "b")),
                    instanceOf(ElasticsearchException.class), messageContainsMatcher("Insufficient permissions"));

            assertThat(e.status(), equalTo(RestStatus.FORBIDDEN.getStatus()));
            assertThat(e.getMessage(), containsString("Insufficient permissions"));
        }
    }

    @Test
    public void excludeBasicFof() throws Exception {

        try (GenericRestClient restClient = clusterFof.getRestClient("exclusion_test_user_basic", "secret")) {

            HttpResponse httpResponse = restClient.get("/exclude_test_*/_search");
            assertThat(httpResponse, isForbidden());

            httpResponse = restClient.get("/exclude_test_allow_*/_search");
            assertThat(httpResponse, isOk());

            assertThat(httpResponse,
                    json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

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

            assertThat(httpResponse,
                    json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

            httpResponse = restClient.get("/exclude_test_disallow_1/_search");
            assertThat(httpResponse, isOk());

            httpResponse = restClient.get("/exclude_test_disallow_2/_search");
            assertThat(httpResponse, isForbidden());
        }
    }

    @Test
    public void excludeWriteFof() throws Exception {
        Client clientFof = clusterFof.getInternalNodeClient();
        clientFof.index(new IndexRequest("write_exclude_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                "write_exclude_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
        clientFof.index(new IndexRequest("write_exclude_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                "write_exclude_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
        clientFof.index(new IndexRequest("write_exclude_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                "index", "write_exclude_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
        clientFof.index(new IndexRequest("write_exclude_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                "index", "write_exclude_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();

        try (GenericRestClient restClient = cluster.getRestClient("exclusion_test_user_write", "secret");
                RestHighLevelClient client = clusterFof.getRestHighLevelClient("exclusion_test_user_write", "secret")) {

            HttpResponse httpResponse = restClient.get("/write_exclude_test_*/_search");

            assertThat(httpResponse, isOk());
            assertThat(httpResponse, json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("write_exclude_test_allow_1",
                    "write_exclude_test_allow_2", "write_exclude_test_disallow_1", "write_exclude_test_disallow_2"))));

            IndexResponse indexResponse = client.index("write_exclude_test_allow_1", Map.of("a", "b"));

            assertThat(indexResponse.result(), equalTo(Result.Created));


            ElasticsearchException e = (ElasticsearchException) assertThatThrown(() ->
                            client.index("write_exclude_test_disallow_1", Map.of("a", "b")),
                    instanceOf(ElasticsearchException.class), messageContainsMatcher("Insufficient permissions"));
            assertThat(e.status(), equalTo(RestStatus.FORBIDDEN.getStatus()));
        }
    }

    @Test
    public void excludeClusterPermission() throws Exception {
        try (GenericRestClient basicCestClient = cluster.getRestClient("exclusion_test_user_basic", "secret");
                GenericRestClient clusterPermissionCestClient = cluster.getRestClient("exclusion_test_user_cluster_permission", "secret")) {

            HttpResponse httpResponse = basicCestClient.get("/exclude_test_*/_search");

            assertThat(httpResponse, isOk());
            assertThat(httpResponse,
                    json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

            httpResponse = clusterPermissionCestClient.get("/exclude_test_*/_search");

            assertThat(httpResponse, isOk());
            assertThat(httpResponse,
                    json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

            httpResponse = basicCestClient.postJson("/exclude_test_*/_msearch", "{}\n{\"query\": {\"match_all\": {}}}\n");
            assertThat(httpResponse, isOk());

            assertThat(httpResponse,
                    json(nodeAt("responses[0].hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

            httpResponse = clusterPermissionCestClient.postJson("/exclude_test_*/_msearch", "{}\n{\"query\": {\"match_all\": {}}}\n");
            assertThat(httpResponse, isForbidden());
        }
    }

    @Test
    public void evaluateClusterAndTenantPrivileges() throws Exception {
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

    @Test
    public void testResizeAction() throws Exception {
        String sourceIndex = "resize_test_source";
        String targetIndex = "resize_test_target";

        Client clientFof = clusterFof.getInternalNodeClient();
        clientFof.index(new IndexRequest(sourceIndex).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index", "a", "b", "y",
                "date", "1985/01/01")).actionGet();

        clientFof.admin().indices()
                .updateSettings(new UpdateSettingsRequest(sourceIndex).settings(Settings.builder().put("index.blocks.write", true).build()))
                .actionGet();

        Thread.sleep(300);

        try (RestHighLevelClient client = clusterFof.getRestHighLevelClient(RESIZE_USER_WITHOUT_CREATE_INDEX_PRIV)) {
            assertThatThrown(() ->
                    client.getJavaClient().indices().shrink(r->r.index("whatever").target(targetIndex)),
                    instanceOf(ElasticsearchException.class), messageContainsMatcher("Insufficient permissions"));
        }

        try (RestHighLevelClient client = clusterFof.getRestHighLevelClient(RESIZE_USER_WITHOUT_CREATE_INDEX_PRIV)) {
            assertThatThrown(() ->
                    client.getJavaClient().indices().shrink(r->r.index(sourceIndex).target(targetIndex)),
                    instanceOf(ElasticsearchException.class), messageContainsMatcher("Insufficient permissions"));
        }

        try (RestHighLevelClient client = clusterFof.getRestHighLevelClient(RESIZE_USER)) {
            assertThatThrown(() ->
                    client.getJavaClient().indices().shrink(r->r.index("whatever").target(targetIndex)),
                    instanceOf(ElasticsearchException.class), messageContainsMatcher("Insufficient permissions"));
        }

        try (RestHighLevelClient client = clusterFof.getRestHighLevelClient(RESIZE_USER)) {
            ShrinkResponse shrinkResponse = client.getJavaClient().indices().shrink(r->r.index(sourceIndex).target(targetIndex));
            assertThat(shrinkResponse.toString(), shrinkResponse.acknowledged(), is(true));
        }


        clientFof = clusterFof.getInternalNodeClient();
        boolean exists = clientFof.admin().indices().getIndex(new GetIndexRequest().indices(targetIndex)).actionGet().indices().length > 0;
        assertThat(exists, is(true));

    }

    @Test
    public void searchTemplate() throws Exception {

        SearchTemplateRequest searchTemplateRequest = new co.elastic.clients.elasticsearch.core.SearchTemplateRequest.Builder()
                .index("resolve_test_allow_*")
                .source("{\"query\": {\"term\": {\"b\": \"{{x}}\" } } }")
                .params(Map.of("x", JsonData.of("yy")))
                .build();

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(SEARCH_TEMPLATE_USER)) {
            SearchTemplateResponse<Map> searchTemplateResponse = client.getJavaClient()
                .searchTemplate(searchTemplateRequest, Map.class);
            HitsMetadata<Map> searchResponse = searchTemplateResponse.hits();

            assertThat(searchResponse.toString(), searchResponse.total().value(), equalTo(1L));
        }

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(SEARCH_NO_TEMPLATE_USER)) {
            ElasticsearchException e = (ElasticsearchException) assertThatThrown(() ->
                    client.getJavaClient().searchTemplate(searchTemplateRequest, Map.class), instanceOf(ElasticsearchException.class));
            assertThat(e.toString(), e.status(), equalTo(RestStatus.FORBIDDEN.getStatus()));
        }
    }

    @Test
    public void negativeLookaheadPattern() throws Exception {

        try (GenericRestClient restClient = clusterFof.getRestClient(NEG_LOOKAHEAD_USER)) {

            HttpResponse httpResponse = restClient.get("*/_search");

            assertThat(httpResponse.getBody(), httpResponse.getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));

            httpResponse = restClient.get("r*/_search");

            assertThat(httpResponse.getBody(), httpResponse.getStatusCode(), equalTo(HttpStatus.SC_OK));
        }
    }

    @Test
    public void regexPattern() throws Exception {

        try (GenericRestClient restClient = clusterFof.getRestClient(REGEX_USER)) {

            HttpResponse httpResponse = restClient.get("*/_search");

            assertThat(httpResponse.getBody(), httpResponse.getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));
        }
    }

    @Test
    public void resolveTestHidden() throws Exception {

        try (GenericRestClient restClient = clusterFof.getRestClient(HIDDEN_TEST_USER)) {
            HttpResponse httpResponse = restClient.get("/*hidden_test*/_search?expand_wildcards=all&pretty=true");
            assertThat(httpResponse.getBody(), httpResponse.getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));

            httpResponse = restClient.get("/*hidden_test*/_search?pretty=true");
            assertThat(httpResponse.getBody(), httpResponse.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(httpResponse.getBody(), httpResponse.getBody(), not(containsString("hidden_test_actually_hidden")));
        }

    }
}
