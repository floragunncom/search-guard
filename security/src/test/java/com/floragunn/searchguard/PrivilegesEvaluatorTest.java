package com.floragunn.searchguard;

import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class PrivilegesEvaluatorTest {

    @ClassRule
    public static LocalCluster anotherCluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .setInSgConfig("sg_config.dynamic.do_not_fail_on_forbidden", "true")
            .user("resolve_test_user", "secret", new Role("resolve_test_user_role").indexPermissions("*").on("resolve_test_allow_*"))//
            .build();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().remote("my_remote", anotherCluster)
            .setInSgConfig("sg_config.dynamic.do_not_fail_on_forbidden", "true")
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
            .build();

    @ClassRule
    public static LocalCluster clusterFof = new LocalCluster.Builder().singleNode().sslEnabled().remote("my_remote", anotherCluster)
            .setInSgConfig("sg_config.dynamic.do_not_fail_on_forbidden", "false")
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
            .build();

    @BeforeClass
    public static void setupTestData() {

        try (Client client = cluster.getInternalClient()) {
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

        try (Client client = clusterFof.getInternalClient()) {
            client.index(new IndexRequest("exclude_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("exclude_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("exclude_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("exclude_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "exclude_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();
        }

        try (Client client = anotherCluster.getInternalClient()) {
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
    public void resolveTestLocal() throws Exception {
        RestHelper rh = cluster.restHelper();

        Header auth = basicAuth("resolve_test_user", "secret");

        HttpResponse httpResponse = rh.executeGetRequest("/_resolve/index/resolve_test_*", auth);

        Assert.assertThat(httpResponse, isOk());
        Assert.assertThat(httpResponse, json(nodeAt("indices[*].name", contains("resolve_test_allow_1", "resolve_test_allow_2"))));
    }

    @Test
    public void resolveTestRemote() throws Exception {
        RestHelper rh = cluster.restHelper();
        Header auth = basicAuth("resolve_test_user", "secret");

        HttpResponse httpResponse = rh.executeGetRequest("/_resolve/index/my_remote:resolve_test_*", auth);

        Assert.assertThat(httpResponse, isOk());
        Assert.assertThat(httpResponse,
                json(nodeAt("indices[*].name", contains("my_remote:resolve_test_allow_remote_1", "my_remote:resolve_test_allow_remote_2"))));
    }

    @Test
    public void resolveTestLocalRemoteMixed() throws Exception {
        RestHelper rh = cluster.restHelper();
        Header auth = basicAuth("resolve_test_user", "secret");

        HttpResponse httpResponse = rh.executeGetRequest("/_resolve/index/resolve_test_*,my_remote:resolve_test_*_remote_*", auth);

        Assert.assertThat(httpResponse, isOk());
        Assert.assertThat(httpResponse, json(nodeAt("indices[*].name", contains("resolve_test_allow_1", "resolve_test_allow_2",
                "my_remote:resolve_test_allow_remote_1", "my_remote:resolve_test_allow_remote_2"))));
    }

    @Test
    public void resolveTestAliasAndIndexMixed() throws Exception {
        RestHelper rh = cluster.restHelper();
        Header auth = basicAuth("resolve_test_user", "secret");

        HttpResponse httpResponse = rh.executeGetRequest("/_resolve/index/alias_resolve_test_*", auth);

        Assert.assertThat(httpResponse, isOk());
        Assert.assertThat(httpResponse, json(nodeAt("indices[*].name", containsInAnyOrder("alias_resolve_test_index_allow_aliased_1",
                "alias_resolve_test_index_allow_aliased_2", "alias_resolve_test_index_allow_1"))));
    }

    @Test
    public void readAliasAndIndexMixed() throws Exception {
        RestHelper rh = cluster.restHelper();
        Header auth = basicAuth("resolve_test_user", "secret");

        HttpResponse httpResponse = rh.executeGetRequest("/alias_resolve_test_*/_search", auth);

        Assert.assertThat(httpResponse, isOk());
        Assert.assertThat(httpResponse, json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("alias_resolve_test_index_allow_aliased_1",
                "alias_resolve_test_index_allow_aliased_2", "alias_resolve_test_index_allow_1"))));
    }

    @Test
    public void excludeBasic() throws Exception {
        RestHelper rh = cluster.restHelper();
        Header auth = basicAuth("exclusion_test_user_basic", "secret");

        HttpResponse httpResponse = rh.executeGetRequest("/exclude_test_*/_search", auth);

        Assert.assertThat(httpResponse, isOk());
        Assert.assertThat(httpResponse,
                json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));
    }

    @Test
    public void excludeBasicNoPattern() throws Exception {
        RestHelper rh = cluster.restHelper();
        Header auth = basicAuth("exclusion_test_user_basic_no_pattern", "secret");

        HttpResponse httpResponse = rh.executeGetRequest("/exclude_test_*/_search", auth);

        Assert.assertThat(httpResponse, isOk());
        Assert.assertThat(httpResponse, json(
                nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2", "exclude_test_disallow_1"))));
    }

    @Test
    public void excludeWrite() throws Exception {
        try (Client client = cluster.getInternalClient()) {
            client.index(new IndexRequest("write_exclude_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "write_exclude_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("write_exclude_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "write_exclude_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("write_exclude_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                    "index", "write_exclude_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("write_exclude_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                    "index", "write_exclude_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();
        }

        Header auth = basicAuth("exclusion_test_user_write", "secret");
        RestHelper rh = cluster.restHelper();

        HttpResponse httpResponse = rh.executeGetRequest("/write_exclude_test_*/_search", auth);

        Assert.assertThat(httpResponse, isOk());
        Assert.assertThat(httpResponse, json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("write_exclude_test_allow_1",
                "write_exclude_test_allow_2", "write_exclude_test_disallow_1", "write_exclude_test_disallow_2"))));

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("exclusion_test_user_write", "secret")) {
            IndexResponse indexResponse = client.index(new IndexRequest("write_exclude_test_allow_1").source("a", "b"), RequestOptions.DEFAULT);

            Assert.assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

            try {
                client.index(new IndexRequest("write_exclude_test_disallow_1").source("a", "b"), RequestOptions.DEFAULT);

                Assert.fail();
            } catch (ElasticsearchStatusException e) {
                Assert.assertEquals(RestStatus.FORBIDDEN, e.status());
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("no permissions for [indices:data/write/index]"));
            }
        }
    }

    @Test
    public void excludeBasicFof() throws Exception {
        RestHelper rh = clusterFof.restHelper();
        Header auth = basicAuth("exclusion_test_user_basic", "secret");

        HttpResponse httpResponse = rh.executeGetRequest("/exclude_test_*/_search", auth);
        Assert.assertThat(httpResponse, isForbidden());

        httpResponse = rh.executeGetRequest("/exclude_test_allow_*/_search", auth);
        Assert.assertThat(httpResponse, isOk());

        Assert.assertThat(httpResponse,
                json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

        httpResponse = rh.executeGetRequest("/exclude_test_disallow_1/_search", auth);
        Assert.assertThat(httpResponse, isForbidden());
    }

    @Test
    public void excludeBasicFofNoPattern() throws Exception {
        RestHelper rh = clusterFof.restHelper();
        Header auth = basicAuth("exclusion_test_user_basic_no_pattern", "secret");

        HttpResponse httpResponse = rh.executeGetRequest("/exclude_test_*/_search", auth);
        Assert.assertThat(httpResponse, isForbidden());

        httpResponse = rh.executeGetRequest("/exclude_test_allow_*/_search", auth);
        Assert.assertThat(httpResponse, isOk());

        Assert.assertThat(httpResponse,
                json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

        httpResponse = rh.executeGetRequest("/exclude_test_disallow_1/_search", auth);
        Assert.assertThat(httpResponse, isOk());

        httpResponse = rh.executeGetRequest("/exclude_test_disallow_2/_search", auth);
        Assert.assertThat(httpResponse, isForbidden());
    }

    @Test
    public void excludeWriteFof() throws Exception {
        try (Client client = clusterFof.getInternalClient()) {
            client.index(new IndexRequest("write_exclude_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "write_exclude_test_allow_1", "b", "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("write_exclude_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "index",
                    "write_exclude_test_allow_2", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("write_exclude_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                    "index", "write_exclude_test_disallow_1", "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("write_exclude_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON,
                    "index", "write_exclude_test_disallow_2", "b", "yy", "date", "1985/01/01")).actionGet();
        }

        Header auth = basicAuth("exclusion_test_user_write", "secret");
        RestHelper rh = clusterFof.restHelper();

        HttpResponse httpResponse = rh.executeGetRequest("/write_exclude_test_*/_search", auth);

        Assert.assertThat(httpResponse, isOk());
        Assert.assertThat(httpResponse, json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("write_exclude_test_allow_1",
                "write_exclude_test_allow_2", "write_exclude_test_disallow_1", "write_exclude_test_disallow_2"))));

        try (RestHighLevelClient client = clusterFof.getRestHighLevelClient("exclusion_test_user_write", "secret")) {
            IndexResponse indexResponse = client.index(new IndexRequest("write_exclude_test_allow_1").source("a", "b"), RequestOptions.DEFAULT);

            Assert.assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

            try {
                client.index(new IndexRequest("write_exclude_test_disallow_1").source("a", "b"), RequestOptions.DEFAULT);

                Assert.fail();
            } catch (ElasticsearchStatusException e) {
                Assert.assertEquals(RestStatus.FORBIDDEN, e.status());
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("no permissions for [indices:data/write/index]"));
            }
        }
    }

    @Test
    public void excludeClusterPermission() throws Exception {
        RestHelper rh = cluster.restHelper();

        HttpResponse httpResponse = rh.executeGetRequest("/exclude_test_*/_search", basicAuth("exclusion_test_user_basic", "secret"));

        Assert.assertThat(httpResponse, isOk());
        Assert.assertThat(httpResponse,
                json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

        httpResponse = rh.executeGetRequest("/exclude_test_*/_search", basicAuth("exclusion_test_user_cluster_permission", "secret"));

        Assert.assertThat(httpResponse, isOk());
        Assert.assertThat(httpResponse,
                json(nodeAt("hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

        httpResponse = rh.executePostRequest("/exclude_test_*/_msearch", "{}\n{\"query\": {\"match_all\": {}}}\n",
                basicAuth("exclusion_test_user_basic", "secret"));
        Assert.assertThat(httpResponse, isOk());

        Assert.assertThat(httpResponse,
                json(nodeAt("responses[0].hits.hits[*]._source.index", containsInAnyOrder("exclude_test_allow_1", "exclude_test_allow_2"))));

        httpResponse = rh.executePostRequest("/exclude_test_*/_msearch", "{}\n{\"query\": {\"match_all\": {}}}\n",
                basicAuth("exclusion_test_user_cluster_permission", "secret"));
        Assert.assertThat(httpResponse, isForbidden());
    }

    @Test
    public void evaluateClusterAndTenantPrivileges() throws Exception {
        RestHelper rh = cluster.restHelper();

        HttpResponse httpResponse = rh.executeGetRequest("/_searchguard/permission?permissions=indices:data/read/mtv,indices:data/read/viva",
                basicAuth("admin", "admin"));

        Assert.assertThat(httpResponse, isOk());
        Assert.assertThat(httpResponse,
                json(nodeAt("permissions['indices:data/read/mtv']", equalTo(true))));
        Assert.assertThat(httpResponse,
                json(nodeAt("permissions['indices:data/read/viva']", equalTo(true))));

        httpResponse = rh.executeGetRequest("/_searchguard/permission?permissions=indices:data/read/mtv,indices:data/read/viva",
                basicAuth("permssion_rest_api_user", "secret"));

        Assert.assertThat(httpResponse, isOk());
        Assert.assertThat(httpResponse,
                json(nodeAt("permissions['indices:data/read/mtv']", equalTo(true))));
        Assert.assertThat(httpResponse,
                json(nodeAt("permissions['indices:data/read/viva']", equalTo(false))));
        
    }

    private static Header basicAuth(String username, String password) {
        return new BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((username + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));
    }
}
