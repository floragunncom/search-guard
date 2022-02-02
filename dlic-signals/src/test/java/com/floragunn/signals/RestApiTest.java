package com.floragunn.signals;

import org.apache.http.HttpStatus;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.ScriptService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class RestApiTest {

    private static ScriptService scriptService;

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("sg_config/signals")
            .nodeSettings("signals.enabled", true, "signals.index_names.log", "signals_main_log").enterpriseModulesEnabled()
            .enableModule(SignalsModule.class).build();

    @BeforeClass
    public static void setupTestData() {

        try (Client client = cluster.getInternalNodeClient()) {
            client.index(new IndexRequest("testsource").source(XContentType.JSON, "key1", "val1", "key2", "val2")).actionGet();

            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "x", "b", "y"))
                    .actionGet();
            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "xx", "b", "yy"))
                    .actionGet();
        }
    }

    @BeforeClass
    public static void setupDependencies() {
        scriptService = cluster.getInjectable(ScriptService.class);

    }

    @Test
    public void testPutWatchWithEnterpriseFeatures() throws Exception {
        String tenant = "_main";
        String watchId = "put_test_with_enterprise_features";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            HttpResponse response = restClient.putJson("/_signals/account/jira/default",
                    "{\"url\": \"http://localhost:1234\", \"user_name\": \"horst\", \"auth_token\": \"xyz\"}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then()
                    .act("jira", "project", "Test", "issue.type", "Bug", "issue.summary", "Bla", "issue.description", "Blub").name("testsink")
                    .build();

            response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.get(watchPath);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", "put_test", response.getBody(), -1);

        }
    }

}
