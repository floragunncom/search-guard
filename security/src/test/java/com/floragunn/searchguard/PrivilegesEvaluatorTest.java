package com.floragunn.searchguard;

import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.Matchers.contains;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class PrivilegesEvaluatorTest {

    private static RestHelper rh = null;

    @ClassRule
    public static LocalCluster anotherCluster = new LocalCluster.Builder().singleNode().sslEnabled()
            .setInSgConfig("sg_config.dynamic.do_not_fail_on_forbidden", "true")
            .user("resolve_test_user", "secret", new Role("resolve_test_user_role").indexPermissions("*").on("resolve_test_allow_*")).build();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().remote("my_remote", anotherCluster)
            .setInSgConfig("sg_config.dynamic.do_not_fail_on_forbidden", "true")
            .user("resolve_test_user", "secret", new Role("resolve_test_user_role").indexPermissions("*").on("resolve_test_allow_*")).build();

    @BeforeClass
    public static void setupTestData() {

        try (Client client = cluster.getInternalClient()) {
            client.index(new IndexRequest("resolve_test_allow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "x", "b",
                    "y", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_allow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "xx", "b",
                    "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_disallow_1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "xx",
                    "b", "yy", "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("resolve_test_disallow_2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "xx",
                    "b", "yy", "date", "1985/01/01")).actionGet();
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

    @BeforeClass
    public static void setupDependencies() {
        rh = cluster.restHelper();
    }

    @Test
    public void resolveTestLocal() throws Exception {
        Header auth = basicAuth("resolve_test_user", "secret");

        HttpResponse httpResponse = rh.executeGetRequest("/_resolve/index/resolve_test_*", auth);

        System.out.println(httpResponse.getBody());

        Assert.assertThat(httpResponse, isOk());
        Assert.assertThat(httpResponse, json(nodeAt("indices[*].name", contains("resolve_test_allow_1", "resolve_test_allow_2"))));
    }

    @Test
    public void resolveTestRemote() throws Exception {
        Header auth = basicAuth("resolve_test_user", "secret");

        HttpResponse httpResponse = rh.executeGetRequest("/_resolve/index/my_remote:resolve_test_*", auth);

        System.out.println(httpResponse.getBody());

        Assert.assertThat(httpResponse, isOk());
        Assert.assertThat(httpResponse,
                json(nodeAt("indices[*].name", contains("my_remote:resolve_test_allow_remote_1", "my_remote:resolve_test_allow_remote_2"))));
    }

    @Test
    public void resolveTestLocalRemoteMixed() throws Exception {
        Header auth = basicAuth("resolve_test_user", "secret");

        HttpResponse httpResponse = rh.executeGetRequest("/_resolve/index/resolve_test_*,my_remote:resolve_test_*_remote_*", auth);

        System.out.println(httpResponse.getBody());

        Assert.assertThat(httpResponse, isOk());
        Assert.assertThat(httpResponse, json(nodeAt("indices[*].name", contains("resolve_test_allow_1", "resolve_test_allow_2",
                "my_remote:resolve_test_allow_remote_1", "my_remote:resolve_test_allow_remote_2"))));
    }

    private static Header basicAuth(String username, String password) {
        return new BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((username + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));
    }
}
