package com.floragunn.signals;

import java.net.InetAddress;
import java.net.URI;
import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.quartz.TimeOfDay;

import com.browserup.bup.BrowserUpProxy;
import com.browserup.bup.BrowserUpProxyServer;
import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.network.SocketUtils;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient.HttpResponse;
import com.floragunn.searchsupport.junit.LoggingTestWatcher;
import com.floragunn.signals.support.JsonBuilder;
import com.floragunn.signals.util.WatchLogSearch;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;
import com.floragunn.signals.watch.action.handlers.email.EmailAccount;
import com.floragunn.signals.watch.action.handlers.email.EmailAction;
import com.floragunn.signals.watch.action.handlers.email.EmailAction.Attachment;
import com.floragunn.signals.watch.action.handlers.slack.SlackAccount;
import com.floragunn.signals.watch.action.handlers.slack.SlackActionConf;
import com.floragunn.signals.watch.common.HttpRequestConfig;
import com.floragunn.signals.watch.init.WatchInitializationService;
import com.floragunn.signals.watch.result.ActionLog;
import com.floragunn.signals.watch.result.Status;
import com.floragunn.signals.watch.result.WatchLog;
import com.floragunn.signals.watch.severity.SeverityLevel;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.GreenMailUtil;
import com.icegreen.greenmail.util.ServerSetup;

import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public class RestApiTest {
    private static final Logger log = LogManager.getLogger(RestApiTest.class);

    private static ScriptService scriptService;
    private static BrowserUpProxy httpProxy;

    @Rule
    public LoggingTestWatcher loggingTestWatcher = new LoggingTestWatcher();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("sg_config/signals")
            .nodeSettings("signals.enabled", true, "signals.index_names.log", "signals__main_log", "signals.enterprise.enabled", false,
                    "searchguard.diagnosis.action_stack.enabled", true, "signals.watch_log.refresh_policy", "immediate",
                    "signals.watch_log.sync_indexing", true)
            .build();

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
    public static void setupDependencies() throws Exception {
        scriptService = cluster.getInjectable(ScriptService.class);
        httpProxy = new BrowserUpProxyServer();
        httpProxy.start(0, InetAddress.getByName("127.0.0.8"), InetAddress.getByName("127.0.0.9"));
    }
    

   
    @AfterClass
    public static void tearDown() {
        if (httpProxy != null) {
            httpProxy.abort();
        }
    }

    @Test
    public void testGetWatchUnauthorized() throws Exception {
        String tenant = "_main";
        String watchId = "get_watch_unauth";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient("noshirt", "redshirt")) {

            HttpResponse response = restClient.get(watchPath);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

        }
    }

    @Test
    public void testPutWatch() throws Exception {
        String tenant = "_main";
        String watchId = "put_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch")).actionGet();

            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.get(watchPath);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", "put_test", response.getBody(), -1);

            awaitMinCountOfDocuments(client, "testsink_put_watch", 1);

        }
    }

    @Test
    public void testWatchStateAfterPutWatch() throws Exception {
        String tenant = "_main";
        String watchId = "put_state_after_put_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            Watch watch = new WatchBuilder(watchId).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = awaitRestGet(watchPath + "/_state", restClient);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            response = restClient.postJson("/_signals/watch/" + tenant + "/_search/_state",
                    "{ \"query\": {\"match\": {\"_id\": \"_main/put_state_after_put_test\"}}}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Assert.assertTrue(response.getBody(), response.getBody().contains("\"hits\":{\"total\":{\"value\":1,\"relation\":\"eq\"}"));
        }
    }

    @Ignore
    @Test
    public void testPutWatchWithSeverity() throws Exception {
        String tenant = "_main";
        String watchId = "put_test_severity";
        String testSink = "testsink_" + watchId;
        String testSinkResolve = "testsink_resolve_" + watchId;
        String testSource = "testsource_" + watchId;
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            client.index(new IndexRequest(testSource).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("1").source(XContentType.JSON, "a", "x", "b", "y"))
                    .actionGet();

            client.admin().indices().create(new CreateIndexRequest(testSink)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(testSinkResolve)).actionGet();

            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search(testSource).query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").consider("data.testsearch.hits.total.value").greaterOrEqual(1)
                    .as(SeverityLevel.ERROR).when(SeverityLevel.ERROR).index(testSink).name("a1").and().whenResolved(SeverityLevel.ERROR)
                    .index(testSinkResolve).name("r1").build();

            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.get(watchPath);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", "put_test", response.getBody(), -1);

            awaitMinCountOfDocuments(client, testSink, 1);

            Assert.assertEquals(getDocs(client, testSinkResolve), 0, getCountOfDocuments(client, testSinkResolve));

            client.delete(new DeleteRequest(testSource).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("1")).actionGet();

            awaitMinCountOfDocuments(client, testSinkResolve, 1);

            Thread.sleep(2000);

            Assert.assertEquals(getDocs(client, testSinkResolve), 1, getCountOfDocuments(client, testSinkResolve));

        }
    }

    @Ignore
    @Test
    public void testPutWatchWithSeverityValidation() throws Exception {
        String tenant = "_main";
        String watchId = "put_test_severity_validation";
        String testSink = "testsink_" + watchId;
        String testSinkResolve = "testsink_resolve_" + watchId;
        String testSource = "testsource_" + watchId;
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            client.index(new IndexRequest(testSource).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("1").source(XContentType.JSON, "a", "x", "b", "y"))
                    .actionGet();

            client.admin().indices().create(new CreateIndexRequest(testSink)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(testSinkResolve)).actionGet();

            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search(testSource).query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").consider("data.testsearch.hits.total.value").greaterOrEqual(1)
                    .as(SeverityLevel.ERROR).when(SeverityLevel.INFO).index(testSink).name("a1").and().whenResolved(SeverityLevel.ERROR)
                    .index(testSinkResolve).name("r1").build();

            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBody().contains("Uses a severity which is not defined by severity mapping: [info]"));
        }
    }

    @Ignore
    @Test
    public void testPutWatchWithSeverity2() throws Exception {
        String tenant = "_main";
        String watchId = "put_test_severity2";
        String testSink = "testsink_" + watchId;
        String testSinkResolve1 = "testsink_resolve1_" + watchId;
        String testSinkResolve2 = "testsink_resolve2_" + watchId;
        String testSource = "testsource_" + watchId;
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            client.index(new IndexRequest(testSource).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("1").source(XContentType.JSON, "a", "x", "b", "y"))
                    .actionGet();
            awaitMinCountOfDocuments(client, testSource, 1);

            client.admin().indices().create(new CreateIndexRequest(testSink)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(testSinkResolve1)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(testSinkResolve2)).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(400).search(testSource).query("{\"match_all\" : {} }").as("testsearch")
                    .consider("data.testsearch.hits.total.value").greaterOrEqual(1).as(SeverityLevel.ERROR).greaterOrEqual(2)
                    .as(SeverityLevel.CRITICAL).when(SeverityLevel.ERROR, SeverityLevel.CRITICAL).index(testSink).name("a1").throttledFor("24h").and()
                    .whenResolved(SeverityLevel.ERROR).index(testSinkResolve1).name("r1").and().whenResolved(SeverityLevel.CRITICAL)
                    .index(testSinkResolve2).name("r2").build();

            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.get(watchPath);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", "put_test", response.getBody(), -1);

            log.info("Created watch; as it should find one doc in " + testSource + ", it should go to severity ERROR and write exactly one doc to "
                    + testSink);

            awaitMinCountOfDocuments(client, testSink, 1);

            Thread.sleep(500);

            Assert.assertEquals(0, getCountOfDocuments(client, testSinkResolve1));
            Assert.assertEquals(0, getCountOfDocuments(client, testSinkResolve2));
            Assert.assertEquals(1, getCountOfDocuments(client, testSink));

            log.info("Adding one doc to " + testSource + "; this should raise severity from ERROR to CRITICAL and write exactly one doc to "
                    + testSink);

            client.index(new IndexRequest(testSource).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("2").source(XContentType.JSON, "a", "x", "b", "y"))
                    .actionGet();

            awaitMinCountOfDocuments(client, testSink, 2);
            Thread.sleep(500);

            Assert.assertEquals(0, getCountOfDocuments(client, testSinkResolve1));
            Assert.assertEquals(0, getCountOfDocuments(client, testSinkResolve2));
            Assert.assertEquals(getDocs(client, testSink), 2, getCountOfDocuments(client, testSink));

            client.delete(new DeleteRequest(testSource).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("1")).actionGet();

            awaitMinCountOfDocuments(client, testSinkResolve2, 1);

            Thread.sleep(200);

            Assert.assertEquals(getDocs(client, testSinkResolve1), 0, getCountOfDocuments(client, testSinkResolve1));
            Assert.assertEquals(getDocs(client, testSinkResolve2), 1, getCountOfDocuments(client, testSinkResolve2));
            Assert.assertEquals(getDocs(client, testSink), 2, getCountOfDocuments(client, testSink));

            client.delete(new DeleteRequest(testSource).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("2")).actionGet();

            awaitMinCountOfDocuments(client, testSinkResolve1, 1);

            Thread.sleep(200);

            Assert.assertEquals(getDocs(client, testSinkResolve1), 1, getCountOfDocuments(client, testSinkResolve1));
            Assert.assertEquals(getDocs(client, testSinkResolve2), 1, getCountOfDocuments(client, testSinkResolve2));
            Assert.assertEquals(getDocs(client, testSink), 2, getCountOfDocuments(client, testSink));

        }
    }

    @Test
    public void testPutWatchWithDash() throws Exception {
        String tenant = "dash-tenant";
        String watchId = "dash-watch";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch_with_dash")).actionGet();

            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch_with_dash").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.get(watchPath);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", "put_test", response.getBody(), -1);

            awaitMinCountOfDocuments(client, "testsink_put_watch_with_dash", 1);

            restClient.delete(watchPath);

            Thread.sleep(500);

            response = restClient.get(watchPath);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        }
    }

    @Test
    public void testPutWatchWithoutSchedule() throws Exception {
        String tenant = "_main";
        String watchId = "without_schedule";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            Watch watch = new WatchBuilder(watchId).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch_with_dash").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.get(watchPath);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", "put_test", response.getBody(), -1);

            Assert.assertTrue(response.getBody(), watch.getSchedule().getTriggers().isEmpty());
        }
    }

    @Test
    public void testAuthTokenFilter() throws Exception {
        String tenant = "_main";
        String watchId = "filter";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.get(watchPath);

            Assert.assertFalse(response.getBody(), response.getBody().contains("auth_token"));

            watch = Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", watchId, response.getBody(), -1);

            Assert.assertNull(response.getBody(), watch.getAuthToken());
        }
    }

    @Ignore
    @Test
    public void testPutInvalidWatch() throws Exception {
        String tenant = "_main";
        String watchId = "put_invalid_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {
            String watchJson = "{\"trigger\":{\"schedule\":{\"timezone\":\"Europe/Berlino\",\"cron\":[\"* * argh * * ?\"],\"x\": 2}}," //
                    + "\"checks\":["
                    + "{\"type\":\"searchx\",\"name\":\"testsearch\",\"target\":\"testsearch\",\"request\":{\"indices\":[\"testsource\"],\"body\":{\"query\":{\"match_all\":{}}}}},"
                    + "{\"type\":\"static\",\"name\":\"teststatic\",\"target\":\"teststatic\",\"value\":{\"bla\":{\"blub\":42}}},"
                    + "{\"type\":\"transform\",\"target\":\"testtransform\",\"source\":\"1 + x\"}," //
                    + "{\"type\":\"calc\",\"name\":\"testcalc\",\"source\":\"1 +\"}" //
                    + "]," //
                    + "\"actions\":[{\"type\":\"index\",\"index\":\"testsink_put_watch\"}],\"horst\": true}";

            HttpResponse response = restClient.putJson(watchPath, watchJson);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

            JsonNode parsedResponse = DefaultObjectMapper.readTree(response.getBody());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, parsedResponse.path("status").asInt());
            Assert.assertEquals(response.getBody(), "Invalid value",
                    parsedResponse.path("detail").path("checks[testsearch].type").path(0).path("error").asText());
            Assert.assertEquals(response.getBody(), "searchx",
                    parsedResponse.path("detail").path("checks[testsearch].type").path(0).path("value").asText());
            Assert.assertEquals(response.getBody(), "cannot resolve symbol [x]",
                    parsedResponse.path("detail").path("checks[].source").path(0).path("error").asText());
            Assert.assertTrue(response.getBody(),
                    parsedResponse.path("detail").path("trigger.schedule.cron").path(0).path("error").asText().contains("Invalid cron expression"));
            Assert.assertTrue(response.getBody(),
                    parsedResponse.path("detail").path("trigger.schedule.x").path(0).path("error").asText().contains("Unsupported attribute"));
            Assert.assertEquals(response.getBody(), "Required attribute is missing",
                    parsedResponse.path("detail").path("actions[].name").path(0).path("error").asText());
            Assert.assertEquals(response.getBody(), "unexpected end of script.",
                    parsedResponse.path("detail").path("checks[testcalc].source").path(0).path("error").asText());
            Assert.assertEquals(response.getBody(), "Unsupported attribute", parsedResponse.path("detail").path("horst").path(0).get("error").asText());

        }
    }

    @Test
    public void testPutInvalidWatchJsonSyntaxError() throws Exception {
        String tenant = "_main";
        String watchId = "put_invalid_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {
            String watchJson = "{\"trigger\":{";

            HttpResponse response = restClient.putJson(watchPath, watchJson);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

            JsonNode parsedResponse = DefaultObjectMapper.readTree(response.getBody());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, parsedResponse.get("status").asInt());
            Assert.assertTrue(response.getBody(),
                    parsedResponse.get("detail").get("_").get(0).get("error").asText().contains("Error while parsing JSON document"));
        }
    }

    @Test
    public void testPutWatchUnauthorized() throws Exception {

        String tenant = "_main";
        String watchId = "put_watch_unauth";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("redshirt3", "redshirt").trackResources()) {

            Watch watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());
        }
    }

    @Test
    public void testPutWatchWithUnauthorizedCheck() throws Exception {

        String tenant = "_main";
        String watchId = "put_watch_with_unauth_check";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("redshirt2", "redshirt").trackResources()) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch_with_unauth_check")).actionGet();

            Watch watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch_with_unauth_action").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            WatchLog watchLog = awaitWatchLog(client, tenant, watchId);

            Assert.assertEquals(watchLog.toString(), Status.Code.EXECUTION_FAILED, watchLog.getStatus().getCode());
            Assert.assertTrue(watchLog.toString(), watchLog.getStatus().getDetail().contains("Error while executing SearchInput testsearch"));
            Assert.assertTrue(watchLog.toString(), watchLog.getStatus().getDetail().contains("no permissions for [indices:data/read/search]"));
        }
    }

    @Test
    public void testHttpWhitelist() throws Exception {

        String tenant = "_main";
        String watchId = "http_whitelist";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook");
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {
            try {
                client.admin().indices().create(new CreateIndexRequest("testsink_put_watch_with_credentials")).actionGet();

                Watch watch = new WatchBuilder("put_test").atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                        .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().postWebhook(webhookProvider.getUri()).throttledFor("0")
                        .name("testhook").build();
                HttpResponse response = restClient.putJson(watchPath, watch.toJson());

                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                Thread.sleep(600);

                Assert.assertTrue(webhookProvider.getRequestCount() > 0);

                response = restClient.putJson("/_signals/settings/http.allowed_endpoints", "[\"https://unkown*\",\"https://whatever*\"]");

                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                response = restClient.get("/_signals/settings/http.allowed_endpoints");

                Thread.sleep(300);

                long requestCount = webhookProvider.getRequestCount();

                Thread.sleep(600);
                Assert.assertEquals(requestCount, webhookProvider.getRequestCount());

            } finally {
                restClient.putJson("/_signals/settings/http.allowed_endpoints", "[\"*\"]");
                restClient.delete(watchPath);
            }
        }
    }

    @Test
    public void testHttpDefaultProxy() throws Exception {

        String tenant = "_main";
        String watchId = "http_default_proxy";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook");
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {
            try {
                webhookProvider.acceptConnectionsOnlyFromInetAddress(InetAddress.getByName("127.0.0.9"));

                Watch watch = new WatchBuilder("put_test").atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                        .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().postWebhook(webhookProvider.getUri()).throttledFor("0")
                        .name("testhook").build();
                HttpResponse response = restClient.putJson(watchPath, watch.toJson());

                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                Thread.sleep(600);

                Assert.assertEquals(0, webhookProvider.getRequestCount());

                response = restClient.putJson("/_signals/settings/http.proxy", "\"http://127.0.0.8:" + httpProxy.getPort() + "\"");

                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                response = restClient.get("/_signals/settings/http.proxy");

                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
                Assert.assertEquals(response.getBody(), "\"http://127.0.0.8:" + httpProxy.getPort()+ "\"");

                
                Thread.sleep(600);

                Assert.assertTrue(webhookProvider.getRequestCount() > 0);

                response = restClient.delete("/_signals/settings/http.proxy");
                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
                
            } finally {
                restClient.delete(watchPath);
                restClient.delete("/_signals/settings/http.proxy");
            }
        }
    }
    
    @Test
    public void testHttpExplicitProxy() throws Exception {

        String tenant = "_main";
        String watchId = "http_explicit_proxy";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook");
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {
            try {
                webhookProvider.acceptConnectionsOnlyFromInetAddress(InetAddress.getByName("127.0.0.9"));

                Watch watch = new WatchBuilder("put_test").atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                        .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().postWebhook(webhookProvider.getUri()).throttledFor("0")
                        .name("testhook").build();
                HttpResponse response = restClient.putJson(watchPath, watch.toJson());

                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                Thread.sleep(600);

                Assert.assertEquals(0, webhookProvider.getRequestCount());

                watch = new WatchBuilder("put_test").atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                        .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().postWebhook(webhookProvider.getUri())
                        .proxy("http://127.0.0.8:" + httpProxy.getPort()).throttledFor("0").name("testhook").build();
                response = restClient.putJson(watchPath, watch.toJson());
                
                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                Thread.sleep(600);

                Assert.assertTrue(webhookProvider.getRequestCount() > 0);

            } finally {
                restClient.delete(watchPath);
            }
        }
    }

    @Test
    public void testHttpExplicitNoProxy() throws Exception {

        String tenant = "_main";
        String watchId = "http_explicit_no_proxy";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook");
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {
            try {
                HttpResponse response = restClient.putJson("/_signals/settings/http.proxy", "\"http://127.0.0.8:" + httpProxy.getPort() + "\"");
                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                Thread.sleep(200);

                Watch watch = new WatchBuilder("put_test").atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                        .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().postWebhook(webhookProvider.getUri()).proxy("none").throttledFor("0")
                        .name("testhook").build();
                response = restClient.putJson(watchPath, watch.toJson());

                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                Thread.sleep(600);

                Assert.assertTrue(webhookProvider.getRequestCount() > 0);
                Assert.assertEquals(webhookProvider.getLastRequestClientAddress(), InetAddress.getByName("127.0.0.1"));                
            } finally {
                restClient.delete(watchPath);
                restClient.delete("/_signals/settings/http.proxy");
            }
        }
    }
    
    
    @Ignore
    @Test
    public void testPutWatchWithCredentials() throws Exception {

        String tenant = "_main";
        String watchId = "put_watch_with_credentials";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook");
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {

            try {
                client.admin().indices().create(new CreateIndexRequest("testsink_put_watch_with_credentials")).actionGet();

                Watch watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                        .as("testsearch").put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().postWebhook(webhookProvider.getUri())
                        .basicAuth("admin", "secret").name("testhook").build();
                HttpResponse response = restClient.putJson(watchPath, watch.toJson());

                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                response = restClient.get(watchPath + "?pretty");
                //this seems failing because in "get watch action" there is no real deserialization of a watch object
                //and so the tox params are not effective
                Assert.assertFalse(response.getBody(), response.getBody().contains("secret"));
                Assert.assertTrue(response.getBody(), response.getBody().contains("password__protected"));

                Thread.sleep(3000);
                Assert.assertEquals(1, webhookProvider.getRequestCount());

            } finally {
                restClient.delete(watchPath);
            }
        }

    }

    @Test
    public void testPutWatchWithUnauthorizedAction() throws Exception {

        String tenant = "_main";
        String watchId = "put_watch_with_unauth_action";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("redshirt1", "redshirt").trackResources()) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch_with_unauth_action")).actionGet();

            Watch watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch_with_unauth_action").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            WatchLog watchLog = awaitWatchLog(client, tenant, watchId);

            Assert.assertEquals(watchLog.toString(), Status.Code.ACTION_FAILED, watchLog.getStatus().getCode());

            ActionLog actionLog = watchLog.getActions().get(0);

            Assert.assertEquals(actionLog.toString(), Status.Code.ACTION_FAILED, actionLog.getStatus().getCode());
            Assert.assertTrue(actionLog.toString(), actionLog.getStatus().getDetail().contains("no permissions for [indices:data/write/index]"));

        }
    }

    @Test
    public void testPutWatchWithTenant() throws Exception {

        String tenant = "test1";
        String watchId = "put_watch_with_tenant";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String watchPathWithWrongTenant = "/_signals/watch/_main/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch_with_tenant")).actionGet();

            Watch watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch_with_tenant").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.get(watchPath);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", "put_test", response.getBody(), -1);

            response = restClient.get(watchPathWithWrongTenant);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        }
    }

    @Test
    public void testPutWatchWithTenant2() throws Exception {

        String tenant = "redshirt_club";
        String watchId = "put_watch_with_tenant2";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String watchPathWithWrongTenant = "/_signals/watch/_main/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("redshirt3", "redshirt").trackResources()) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch_with_tenant2")).actionGet();

            Watch watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch_with_tenant2").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.get(watchPath);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", "put_test", response.getBody(), -1);

            response = restClient.get(watchPathWithWrongTenant);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        }
    }

    @Test
    public void testPutWatchWithUnauthorizedTenant() throws Exception {

        String tenant = "test1";
        String watchId = "put_watch_with_unauthorized_tenant";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("redshirt1", "redshirt").trackResources()) {

            Watch watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch_with_tenant").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

        }
    }

    @Test
    public void testDeleteWatch() throws Exception {

        String tenant = "_main";
        String watchId = "delete_watch";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {
            client.admin().indices().create(new CreateIndexRequest("testsink_delete_watch")).actionGet();

            Watch watch = new WatchBuilder("put_test").atMsInterval(10).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_delete_watch").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.get(watchPath);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            awaitMinCountOfDocuments(client, "testsink_delete_watch", 1);

            restClient.delete(watchPath);

            response = restClient.get(watchPath);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_NOT_FOUND, response.getStatusCode());

            Thread.sleep(1500);

            long docCount = getCountOfDocuments(client, "testsink_delete_watch");

            Thread.sleep(1000);

            long newDocCount = getCountOfDocuments(client, "testsink_delete_watch");

            Assert.assertEquals(docCount, newDocCount);

        }
    }

    @Test
    public void testExecuteAnonymousWatch() throws Exception {

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {

            Watch watch = new WatchBuilder("execution_test_anon").cronTrigger("*/2 * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                    .as("testsearch").put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink").name("testsink").build();

            HttpResponse response = restClient.postJson("/_signals/watch/_main/_execute", "{\"watch\": " + watch.toJson() + "}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

        }
    }

    @Test
    public void testExecuteWatchById() throws Exception {
        String tenant = "_main";
        String watchId = "execution_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {

            Watch watch = new WatchBuilder(watchId).cronTrigger("0 0 */1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.postJson(watchPath + "/_execute", "{}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
        }
    }

    @Test
    public void testExecuteAnonymousWatchWithGoto() throws Exception {

        String testSink = "testsink_anon_watch_with_goto";

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {

            Watch watch = new WatchBuilder("execution_test_anon").cronTrigger("*/2 * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                    .as("testsearch").put("{\"bla\": {\"blub\": 42}, \"x\": \"1\"}").as("teststatic").then().index(testSink).docId("1")
                    .refreshPolicy(RefreshPolicy.IMMEDIATE).name("testsink").build();

            HttpResponse response = restClient.postJson("/_signals/watch/_main/_execute",
                    "{\"watch\": " + watch.toJson() + ", \"goto\": \"teststatic\"}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            GetResponse getResponse = client.get(new GetRequest(testSink, "1")).actionGet();

            Assert.assertTrue(getResponse.toString(), getResponse.getSource().get("testsource") == null);
            Assert.assertTrue(getResponse.toString(), getResponse.getSource().get("teststatic") != null);

        }
    }

    @Test
    public void testExecuteAnonymousWatchWithInput() throws Exception {

        String testSink = "testsink_anon_watch_with_input";

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {

            Watch watch = new WatchBuilder("execution_test_anon").cronTrigger("*/2 * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                    .as("testsearch").put("{\"bla\": {\"blub\": 42}, \"x\": \"1\"}").as("teststatic").then().index(testSink).docId("1")
                    .refreshPolicy(RefreshPolicy.IMMEDIATE).name("testsink").build();

            HttpResponse response = restClient.postJson("/_signals/watch/_main/_execute",
                    "{\"watch\": " + watch.toJson() + ", \"goto\": \"_actions\", \"input\": { \"ext_input\": \"a\"}}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            GetResponse getResponse = client.get(new GetRequest(testSink, "1")).actionGet();

            Assert.assertTrue(getResponse.toString(), getResponse.getSource().get("testsource") == null);
            Assert.assertTrue(getResponse.toString(), getResponse.getSource().get("teststatic") == null);
            Assert.assertTrue(getResponse.toString(), getResponse.getSource().get("ext_input") != null);

        }
    }

    @Ignore
    @Test
    public void testExecuteAnonymousWatchWithShowAllRuntimeAttributes() throws Exception {

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {

            Watch watch = new WatchBuilder("execution_test_anon").cronTrigger("*/2 * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                    .as("testsearch").put("{\"bla\": {\"blub\": 42}}").as("teststatic").consider("data.testsearch.hits.total.value").greaterOrEqual(1)
                    .as(SeverityLevel.ERROR).when(SeverityLevel.ERROR).index("testsink").name("testsink").build();

            HttpResponse response = restClient.postJson("/_signals/watch/_main/_execute",
                    "{\"watch\": " + watch.toJson() + ", \"show_all_runtime_attributes\": true}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            JsonNode responseJson = DefaultObjectMapper.readTree(response.getBody());

            Assert.assertEquals(response.getBody(), "error", responseJson.at("/runtime_attributes/severity/level").asText());
            Assert.assertFalse(response.getBody(), responseJson.at("/runtime_attributes/trigger").isNull());
            Assert.assertTrue(response.getBody(), responseJson.at("/runtime_attributes/trigger/triggered_time").isNull());
            Assert.assertEquals(response.getBody(), "42", responseJson.at("/runtime_attributes/data/teststatic/bla/blub").asText());

        }
    }

    @Test
    public void testActivateWatchAuth() throws Exception {
        String tenant = "_main";
        String watchId = "activate_auth_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {

            Watch watch = new WatchBuilder("deactivate_test").inactive().atMsInterval(100).search("testsource").query("{\"match_all\" : {} }")
                    .as("testsearch").put("{\"bla\": {\"blub\": 42}}").as("teststatic").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.putJson(watchPath + "/_active", "");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = getWatchByRest(tenant, watchId, restClient);

            Assert.assertEquals(true, watch.isActive());

            response = restClient.delete(watchPath + "/_active");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = getWatchByRest(tenant, watchId, restClient);
            Assert.assertFalse(watch.isActive());

            response = restClient.delete(watchPath + "/_active");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = getWatchByRest(tenant, watchId, restClient);
            Assert.assertFalse(watch.isActive());

            response = restClient.putJson(watchPath + "/_active", "");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = getWatchByRest(tenant, watchId, restClient);
            Assert.assertTrue(watch.isActive());
        }
    }

    @Test
    public void testDeactivateWatch() throws Exception {
        String tenant = "_main";
        String watchId = "deactivate_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {

            client.admin().indices().create(new CreateIndexRequest("testsink_deactivate_watch")).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_deactivate_watch").throttledFor("0").name("testsink")
                    .build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            awaitMinCountOfDocuments(client, "testsink_deactivate_watch", 1);

            response = restClient.delete(watchPath + "/_active");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Watch updatedWatch = getWatchByRest(tenant, watchId, restClient);

            Assert.assertFalse(updatedWatch.isActive());

            Thread.sleep(1500);

            long executionCountWhenDeactivated = getCountOfDocuments(client, "testsink_deactivate_watch");

            Thread.sleep(1000);

            long lastExecutionCount = getCountOfDocuments(client, "testsink_deactivate_watch");

            Assert.assertEquals(executionCountWhenDeactivated, lastExecutionCount);

            response = restClient.putJson(watchPath + "/_active", "");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            awaitMinCountOfDocuments(client, "testsink_deactivate_watch", lastExecutionCount + 1);

        }
    }

    @Test
    public void testDeactivateTenant() throws Exception {
        String tenant = "_main";
        String watchId = "deactivate_tenant_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String testSink = "testsink_" + watchId;

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {

            client.admin().indices().create(new CreateIndexRequest(testSink)).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index(testSink).throttledFor("0").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            awaitMinCountOfDocuments(client, testSink, 1);

            response = restClient.delete("/_signals/tenant/" + tenant + "/_active");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Thread.sleep(210);

            long executionCountWhenDeactivated = getCountOfDocuments(client, testSink);

            Thread.sleep(310);

            long lastExecutionCount = getCountOfDocuments(client, testSink);

            Assert.assertEquals(executionCountWhenDeactivated, lastExecutionCount);

            response = restClient.put("/_signals/tenant/" + tenant + "/_active");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            awaitMinCountOfDocuments(client, testSink, lastExecutionCount + 1);

        } finally {
            try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {
                restClient.put("/_signals/tenant/" + tenant + "/_active");
                restClient.delete(watchPath);
            }
        }
    }

    @Test
    public void testDeactivateGlobally() throws Exception {
        String tenant = "_main";
        String watchId = "deactivate_globally_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String testSink = "testsink_" + watchId;

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {

            client.admin().indices().create(new CreateIndexRequest(testSink)).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index(testSink).throttledFor("0").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            awaitMinCountOfDocuments(client, testSink, 1);

            response = restClient.delete("/_signals/admin/_active");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Thread.sleep(210);

            long executionCountWhenDeactivated = getCountOfDocuments(client, testSink);

            Thread.sleep(310);

            long lastExecutionCount = getCountOfDocuments(client, testSink);

            Assert.assertEquals(executionCountWhenDeactivated, lastExecutionCount);

            response = restClient.put("/_signals/admin/_active");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            awaitMinCountOfDocuments(client, testSink, lastExecutionCount + 1);
        } finally {
            try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {
                restClient.put("/_signals/admin/_active");
                restClient.delete(watchPath);
            }
        }
    }

    @Ignore
    @Test
    //FLAKY
    public void testAckWatch() throws Exception {
        String tenant = "_main";
        String watchId = "ack_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            client.admin().indices().create(new CreateIndexRequest("testsource_ack_watch")).actionGet();
            client.admin().indices().create(new CreateIndexRequest("testsink_ack_watch")).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search("testsource_ack_watch").query("{\"match_all\" : {} }").as("testsearch")
                    .checkCondition("data.testsearch.hits.hits.length > 0").then().index("testsink_ack_watch").refreshPolicy(RefreshPolicy.IMMEDIATE)
                    .throttledFor("0").name("testaction").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Thread.sleep(220);

            response = restClient.put(watchPath + "/_ack/testaction");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_PRECONDITION_FAILED, response.getStatusCode());

            client.index(new IndexRequest("testsource_ack_watch").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "key1",
                    "val1", "key2", "val2")).actionGet();

            awaitMinCountOfDocuments(client, "testsink_ack_watch", 1);

            response = restClient.put(watchPath + "/_ack/testaction");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Thread.sleep(500);

            response = restClient.get(watchPath + "/_state");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            JsonNode statusDoc = DefaultObjectMapper.readTree(response.getBody());
            Assert.assertEquals(response.getBody(), "uhura", statusDoc.at("/actions/testaction/acked/by").textValue());

            Thread.sleep(200);

            long executionCountAfterAck = getCountOfDocuments(client, "testsink_ack_watch");

            Thread.sleep(310);

            long currentExecutionCount = getCountOfDocuments(client, "testsink_ack_watch");

            Assert.assertEquals(executionCountAfterAck, currentExecutionCount);

            // Make condition go away

            client.delete(new DeleteRequest("testsource_ack_watch", "1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)).actionGet();

            Thread.sleep(310);

            currentExecutionCount = getCountOfDocuments(client, "testsink_ack_watch");

            Assert.assertEquals(executionCountAfterAck, currentExecutionCount);

            response = restClient.get(watchPath + "/_state");

            statusDoc = DefaultObjectMapper.readTree(response.getBody());
            Assert.assertFalse(response.getBody(), statusDoc.get("actions").get("testaction").hasNonNull("acked"));

            // Create condition again

            client.index(new IndexRequest("testsource_ack_watch").id("2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "key1",
                    "val1", "key2", "val2")).actionGet();

            //Test is here FLAKY
            awaitMinCountOfDocuments(client, "testsink_ack_watch", executionCountAfterAck + 1);

            currentExecutionCount = getCountOfDocuments(client, "testsink_ack_watch");

            Assert.assertNotEquals(executionCountAfterAck, currentExecutionCount);

            response = restClient.delete(watchPath + "/_active");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
        }
    }

    @Ignore
    @Test
    public void testUnAckOfFreshWatch() throws Exception {
        String tenant = "_main";
        String watchId = "unack_of_fresh_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            client.admin().indices().create(new CreateIndexRequest("testsource_unack_watch")).actionGet();
            client.admin().indices().create(new CreateIndexRequest("testsink_unack_watch")).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search("testsource_unack_watch").query("{\"match_all\" : {} }").as("testsearch")
                    .checkCondition("data.testsearch.hits.hits.length > 0").then().index("testsink_unack_watch")
                    .refreshPolicy(RefreshPolicy.IMMEDIATE).throttledFor("0").name("testaction").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Thread.sleep(1000);

            response = restClient.delete(watchPath + "/_ack");

            Assert.assertEquals(response.getBody(), 412, response.getStatusCode());
            Assert.assertEquals(response.getBody(), "No actions are in an un-acknowlegable state", response.toJsonNode().path("error").asText());
        }
    }

    @Test
    public void testSearchWatch() throws Exception {
        String tenant = "_main";
        String watchId = "search_watch";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {

            Watch watch = new WatchBuilder("put_test").cronTrigger("0 0 1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .then().index("testsink_search_watch").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath + "1", watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            watch = new WatchBuilder("put_test").cronTrigger("0 0 1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("findme").then()
                    .index("testsink_search_watch").name("testsink").build();
            response = restClient.putJson(watchPath + "2", watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            watch = new WatchBuilder("put_test").cronTrigger("0 0 1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("findme").then()
                    .index("testsink_search_watch").name("testsink").build();
            response = restClient.putJson(watchPath + "3", watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.postJson("/_signals/watch/" + tenant + "/_search", "{ \"query\": {\"match\": {\"checks.name\": \"findme\"}}}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Assert.assertTrue(response.getBody(), response.getBody().contains("\"hits\":{\"total\":{\"value\":2,\"relation\":\"eq\"}"));

            response = restClient.postJson("/_signals/watch/" + tenant + "/_search", "{ \"query\": {\"match\": {\"_name\": \"search_watch3\"}}}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Assert.assertTrue(response.getBody(), response.getBody().contains("\"hits\":{\"total\":{\"value\":1,\"relation\":\"eq\"}"));

        }
    }

    @Test
    public void testSearchWatchWithoutBody() throws Exception {
        String tenant = "unit_test_search_watch_without_body";
        String watchId = "search_watch_without_body";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {

            Watch watch = new WatchBuilder("put_test").cronTrigger("0 0 1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .then().index("testsink_search_watch").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath + "1", watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            watch = new WatchBuilder("put_test").cronTrigger("0 0 1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("findme").then()
                    .index("testsink_search_watch").name("testsink").build();
            response = restClient.putJson(watchPath + "2", watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            watch = new WatchBuilder("put_test").cronTrigger("0 0 1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("findme").then()
                    .index("testsink_search_watch").name("testsink").build();
            response = restClient.putJson(watchPath + "3", watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.get("/_signals/watch/" + tenant + "/_search");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Assert.assertTrue(response.getBody(), response.getBody().contains("\"hits\":{\"total\":{\"value\":3,\"relation\":\"eq\"}"));

        }
    }

    @Test
    public void testSearchWatchScroll() throws Exception {
        String tenant = "_main";
        String watchId = "search_watch_scroll";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {

            Watch watch = new WatchBuilder("put_test").cronTrigger("0 0 1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .then().index("testsink_search_watch").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath + "1", watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            watch = new WatchBuilder("put_test").cronTrigger("0 0 1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("findme").then()
                    .index("testsink_search_watch").name("testsink").build();
            response = restClient.putJson(watchPath + "2", watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            watch = new WatchBuilder("put_test").cronTrigger("0 0 1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("findme").then()
                    .index("testsink_search_watch").name("testsink").build();
            response = restClient.putJson(watchPath + "3", watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.postJson("/_signals/watch/" + tenant + "/_search?scroll=60s&size=1",
                    "{ \"sort\": [{\"_meta.last_edit.date\": {\"order\": \"asc\"}}], \"query\": {\"match\": {\"checks.name\": \"findme\"}}}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Assert.assertTrue(response.getBody(), response.getBody().contains("\"_id\":\"_main/search_watch_scroll2\""));

            JsonNode responseJsonNode = DefaultObjectMapper.readTree(response.getBody());

            String scrollId = responseJsonNode.get("_scroll_id").asText(null);

            Assert.assertNotNull(scrollId);

            response = restClient.postJson("/_search/scroll", "{ \"scroll\": \"60s\", \"scroll_id\": \"" + scrollId + "\"}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Assert.assertTrue(response.getBody(), response.getBody().contains("\"_id\":\"_main/search_watch_scroll3\""));

        }
    }

    @Test
    public void testEmailDestination() throws Exception {
        String tenant = "_main";
        String watchId = "smtp_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        final int smtpPort = SocketUtils.findAvailableTcpPort();

        GreenMail greenMail = new GreenMail(new ServerSetup(smtpPort, "127.0.0.1", ServerSetup.PROTOCOL_SMTP));
        greenMail.start();

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {

            try {
                EmailAccount destination = new EmailAccount();
                destination.setHost("localhost");
                destination.setPort(smtpPort);

                Assert.assertTrue(destination.toJson().contains("\"type\":\"email\""));
                Assert.assertFalse(destination.toJson().contains("session_timeout"));

                Attachment attachment = new EmailAction.Attachment();
                attachment.setType(Attachment.AttachmentType.RUNTIME);

                //Add smtp destination
                HttpResponse response = restClient.putJson("/_signals/account/email/default", destination.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                //Update test
                response = restClient.putJson("/_signals/account/email/default", destination.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                //Delete non existing destination
                response = restClient.delete("/_signals/account/email/aaa");
                Assert.assertEquals(response.getBody(), HttpStatus.SC_NOT_FOUND, response.getStatusCode());

                //Get non existing destination
                response = restClient.get("/_signals/account/email/aaabbb");
                Assert.assertEquals(response.getBody(), HttpStatus.SC_NOT_FOUND, response.getStatusCode());

                //Get existing destination
                response = restClient.get("/_signals/account/email/default");
                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                //Define a watch with an smtp action
                Watch watch = new WatchBuilder("smtp_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                        .as("testsearch").then().email("Test Mail Subject").to("mustache@cc.xx").from("mustache@df.xx").account("default")
                        .body("We searched {{data.testsearch._shards.total}} shards").attach("attachment.txt", attachment).name("testsmtpsink")
                        .build();

                response = restClient.putJson(watchPath, watch.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                //we expect one email to be sent (rest is throttled)
                if (!greenMail.waitForIncomingEmail(20000, 1)) {
                    Assert.fail("Timeout waiting for mails");
                }

                String message = GreenMailUtil.getWholeMessage(greenMail.getReceivedMessages()[0]);

                //Check mail to contain resolved subject line
                Assert.assertTrue(message, message.contains("We searched 5 shards"));
                Assert.assertTrue(message, message.contains("Test Mail Subject"));

            } finally {
                restClient.delete(watchPath);
                restClient.delete("/_signals/account/email/default");
                greenMail.stop();
            }
        }
    }

    @Test
    public void testEmailDestinationWithRuntimeDataAndBasicText() throws Exception {
        String tenant = "_main";
        String watchId = "smtp_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        final int smtpPort = SocketUtils.findAvailableTcpPort();

        GreenMail greenMail = new GreenMail(new ServerSetup(smtpPort, "127.0.0.1", ServerSetup.PROTOCOL_SMTP));
        greenMail.start();

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {

            try {

                try (MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook")) {

                    HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webhookProvider.getUri()),
                            "/{{data.teststatic.path}}", null, "{{data.teststatic.body}}", null, null, null);

                    httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

                    EmailAccount destination = new EmailAccount();
                    destination.setHost("localhost");
                    destination.setPort(smtpPort);

                    Assert.assertTrue(destination.toJson().contains("\"type\":\"email\""));
                    Assert.assertFalse(destination.toJson().contains("session_timeout"));

                    Attachment attachment = new EmailAction.Attachment();
                    attachment.setType(Attachment.AttachmentType.RUNTIME);

                    Attachment attachment2 = new EmailAction.Attachment();
                    attachment2.setType(Attachment.AttachmentType.REQUEST);
                    attachment2.setRequestConfig(httpRequestConfig);

                    //Add smtp destination
                    HttpResponse response = restClient.putJson("/_signals/account/email/default", destination.toJson());
                    Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                    //Update test
                    response = restClient.putJson("/_signals/account/email/default", destination.toJson());
                    Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                    //Delete non existing destination
                    response = restClient.delete("/_signals/account/email/aaa");
                    Assert.assertEquals(response.getBody(), HttpStatus.SC_NOT_FOUND, response.getStatusCode());

                    //Get non existing destination
                    response = restClient.get("/_signals/account/email/aaabbb");
                    Assert.assertEquals(response.getBody(), HttpStatus.SC_NOT_FOUND, response.getStatusCode());

                    //Get existing destination
                    response = restClient.get("/_signals/account/email/default");
                    Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                    //Define a watch with an smtp action
                    Watch watch = new WatchBuilder("smtp_test")
                            .put("{\n" + "   \"path\":\"hook\",\n" + "   \"body\":\"stuff\",\n" + "   \"x\":\"y\"\n" + "}").as("teststatic")
                            .cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch").then()
                            .email("Test Mail Subject").to("mustache@cc.xx").from("mustache@df.xx").account("default")
                            .body("We searched {{data.testsearch._shards.total}} shards").attach("runtime.txt", attachment)
                            .attach("some_response.txt", attachment2).name("testsmtpsink").build();

                    response = restClient.putJson(watchPath, watch.toJson());
                    Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                    //we expect one email to be sent (rest is throttled)
                    if (!greenMail.waitForIncomingEmail(20000, 1)) {
                        Assert.fail("Timeout waiting for mails");
                    }

                    String message = GreenMailUtil.getWholeMessage(greenMail.getReceivedMessages()[0]);

                    //Check mail to contain resolved subject line
                    Assert.assertTrue(message, message.contains("We searched 5 shards"));
                    Assert.assertTrue(message, message.contains("Content-Type: application/json; filename=runtime.txt; name=runtime"));
                    Assert.assertTrue(message, message.contains("Content-Type: text/plain; filename=some_response.txt; name=some_response"));
                    Assert.assertTrue(message, message.contains("Mockery"));
                    Assert.assertTrue(message, message.contains("Test Mail Subject"));
                }

            } finally {
                restClient.delete(watchPath);
                restClient.delete("/_signals/account/email/default");
                greenMail.stop();
            }
        }
    }

    @Test
    public void testEmailDestinationWithHtmlBody() throws Exception {
        String tenant = "_main";
        String watchId = "smtp_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        final int smtpPort = SocketUtils.findAvailableTcpPort();

        GreenMail greenMail = new GreenMail(new ServerSetup(smtpPort, "127.0.0.1", ServerSetup.PROTOCOL_SMTP));
        greenMail.start();

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {

            try {
                EmailAccount destination = new EmailAccount();
                destination.setHost("localhost");
                destination.setPort(smtpPort);

                Assert.assertTrue(destination.toJson().contains("\"type\":\"email\""));
                Assert.assertFalse(destination.toJson().contains("session_timeout"));

                Attachment attachment = new EmailAction.Attachment();
                attachment.setType(Attachment.AttachmentType.RUNTIME);

                //Add smtp destination
                HttpResponse response = restClient.putJson("/_signals/account/email/default", destination.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                //Update test
                response = restClient.putJson("/_signals/account/email/default", destination.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                //Delete non existing destination
                response = restClient.delete("/_signals/account/email/aaa");
                Assert.assertEquals(response.getBody(), HttpStatus.SC_NOT_FOUND, response.getStatusCode());

                //Get non existing destination
                response = restClient.get("/_signals/account/email/aaabbb");
                Assert.assertEquals(response.getBody(), HttpStatus.SC_NOT_FOUND, response.getStatusCode());

                //Get existing destination
                response = restClient.get("/_signals/account/email/default");
                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                //Define a watch with an smtp action
                Watch watch = new WatchBuilder("smtp_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                        .as("testsearch").then().email("Test Mail Subject").to("mustache@cc.xx").from("mustache@df.xx").account("default")
                        .body("a body").htmlBody("<p>We searched {{data.x}} shards<p/>").attach("attachment.txt", attachment).name("testsmtpsink")
                        .build();

                response = restClient.putJson(watchPath, watch.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                //we expect one email to be sent (rest is throttled)
                if (!greenMail.waitForIncomingEmail(20000, 1)) {
                    Assert.fail("Timeout waiting for mails");
                }

                String message = GreenMailUtil.getWholeMessage(greenMail.getReceivedMessages()[0]);

                //Check mail to contain resolved subject line
                Assert.assertTrue(message, message.contains("<p>We searched  shards<p/>"));
                Assert.assertTrue(message, message.contains("a body"));
                Assert.assertTrue(message, message.contains("Test Mail Subject"));

            } finally {
                restClient.delete(watchPath);
                restClient.delete("/_signals/account/email/default");
                greenMail.stop();
            }
        }
    }

    @Test
    public void testNonExistingEmailAccount() throws Exception {
        String tenant = "_main";
        String watchId = "smtp_test_non_existing_account";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {

            restClient.delete("/_signals/account/email/default");

            try {

                Watch watch = new WatchBuilder("smtp_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                        .as("testsearch").then().email("Test Mail Subject").to("mustache@cc.xx").from("mustache@df.xx")
                        .body("We searched {{data.testsearch._shards.total}} shards").name("testsmtpsink").build();

                HttpResponse response = restClient.putJson(watchPath, watch.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
                Assert.assertTrue(response.getBody(), response.getBody().contains("Account does not exist: email/default"));

            } finally {
                restClient.delete(watchPath);
                restClient.delete("/_signals/account/email/default");
            }
        }
    }

    @Test
    public void testSlackDestination() throws Exception {
        String tenant = "_main";
        String watchId = "slack_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {

            try {
                SlackAccount destination = new SlackAccount();
                destination.setUrl(new URI("https://hooks.slack.com/services/SECRET"));

                Assert.assertTrue(destination.toJson().contains("\"type\":\"slack\""));

                SlackActionConf slackActionConf = new SlackActionConf();
                slackActionConf.setText("Test from slack action");
                slackActionConf.setChannel("some channel");
                slackActionConf.setFrom("xyz");
                slackActionConf.setIconEmoji(":got:");
                slackActionConf.setAccount("default");

                //Add destination
                HttpResponse response = restClient.putJson("/_signals/account/slack/default", destination.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                //Define a watch with an smtp action
                Watch watch = new WatchBuilder("slack_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                        .as("testsearch").then().slack(slackActionConf).name("testslacksink").build();

                response = restClient.putJson(watchPath, watch.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            } finally {
                restClient.delete(watchPath);
                restClient.delete("/_signals/account/slack/default");
            }
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSlackDestinationWithBlocksAndText() throws Exception {
        String tenant = "_main";
        String watchId = "slack_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {

            try {
                SlackAccount destination = new SlackAccount();
                destination.setUrl(new URI("https://hooks.slack.com/services/SECRET"));

                Assert.assertTrue(destination.toJson().contains("\"type\":\"slack\""));

                String blocksRawJson = "[\n" + "\t\t{\n" + "\t\t\t\"type\": \"section\",\n" + "\t\t\t\"text\": {\n"
                        + "\t\t\t\t\"type\": \"mrkdwn\",\n" + "\t\t\t\t\"text\": \"A message *with some bold text*}.\"\n" + "\t\t\t}\n" + "\t\t}\n"
                        + "\t]";

                List blocks = DefaultObjectMapper.readValue(blocksRawJson, List.class);

                SlackActionConf slackActionConf = new SlackActionConf();
                slackActionConf.setText("Test from slack action");
                slackActionConf.setBlocks(blocks);
                slackActionConf.setChannel("some channel");
                slackActionConf.setFrom("xyz");
                slackActionConf.setIconEmoji(":got:");
                slackActionConf.setAccount("default");

                //Add destination
                HttpResponse response = restClient.putJson("/_signals/account/slack/default", destination.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                //Define a watch with an smtp action
                Watch watch = new WatchBuilder("slack_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                        .as("testsearch").then().slack(slackActionConf).name("testslacksink").build();

                response = restClient.putJson(watchPath, watch.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            } finally {
                restClient.delete(watchPath);
                restClient.delete("/_signals/account/slack/default");
            }
        }

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSlackDestinationWithAttachmentAndText() throws Exception {
        String tenant = "_main";
        String watchId = "slack_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {

            try {
                SlackAccount destination = new SlackAccount();
                destination.setUrl(new URI("https://hooks.slack.com/services/SECRET"));

                Assert.assertTrue(destination.toJson().contains("\"type\":\"slack\""));

                String attachmentRawJson = "[\n" + "      {\n" + "          \"fallback\": \"Plain-text summary of the attachment.\",\n"
                        + "          \"color\": \"#2eb886\",\n"
                        + "          \"pretext\": \"Optional text that appears above the attachment block\",\n"
                        + "          \"author_name\": \"Bobby Tables\",\n" + "          \"author_link\": \"http://flickr.com/bobby/\",\n"
                        + "          \"author_icon\": \"http://flickr.com/icons/bobby.jpg\",\n"
                        + "          \"title\": \"Slack API Documentation\",\n" + "          \"title_link\": \"https://api.slack.com/\",\n"
                        + "          \"text\": \"Optional text that appears within the attachment\",\n" + "          \"fields\": [\n"
                        + "              {\n" + "                  \"title\": \"Priority\",\n" + "                  \"value\": \"High\",\n"
                        + "                  \"short\": false\n" + "              }\n" + "          ],\n"
                        + "          \"image_url\": \"http://my-website.com/path/to/image.jpg\",\n"
                        + "          \"thumb_url\": \"http://example.com/path/to/thumb.png\",\n" + "          \"footer\": \"Slack API\",\n"
                        + "          \"footer_icon\": \"https://platform.slack-edge.com/img/default_application_icon.png\",\n"
                        + "          \"ts\": 123456789\n" + "      }\n" + "  ]";

                List attachments = DefaultObjectMapper.readValue(attachmentRawJson, List.class);

                SlackActionConf slackActionConf = new SlackActionConf();
                slackActionConf.setText("Test from slack action");
                slackActionConf.setAttachments(attachments);
                slackActionConf.setChannel("some channel");
                slackActionConf.setFrom("xyz");
                slackActionConf.setIconEmoji(":got:");
                slackActionConf.setAccount("default");

                //Add destination
                HttpResponse response = restClient.putJson("/_signals/account/slack/default", destination.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                //Define a watch with an smtp action
                Watch watch = new WatchBuilder("slack_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                        .as("testsearch").then().slack(slackActionConf).name("testslacksink").build();

                response = restClient.putJson(watchPath, watch.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            } finally {
                restClient.delete(watchPath);
                restClient.delete("/_signals/account/slack/default");
            }
        }
    }

    @Test
    public void testSlackDestinationWithMissingTextAndBlocks() throws Exception {
        String tenant = "_main";
        String watchId = "slack_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {

            try {
                SlackAccount destination = new SlackAccount();
                destination.setUrl(new URI("https://hooks.slack.com/services/SECRET"));

                Assert.assertTrue(destination.toJson().contains("\"type\":\"slack\""));

                SlackActionConf slackActionConf = new SlackActionConf();
                slackActionConf.setChannel("some channel");
                slackActionConf.setFrom("xyz");
                slackActionConf.setIconEmoji(":got:");
                slackActionConf.setAccount("default");

                //Add destination
                HttpResponse response = restClient.putJson("/_signals/account/slack/default", destination.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                //Define a watch with an smtp action
                Watch watch = new WatchBuilder("slack_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                        .as("testsearch").then().slack(slackActionConf).name("testslacksink").build();

                response = restClient.putJson(watchPath, watch.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
                Assert.assertTrue(response.getBody().contains("Watch is invalid: 'actions[testslacksink].text': Required attribute is missing\","));

            } finally {
                restClient.delete(watchPath);
                restClient.delete("/_signals/account/slack/default");
            }
        }
    }

    @Test
    public void testDeleteAccountInUse() throws Exception {
        String tenant = "_main";
        String watchId = "slack_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {

            try {
                SlackAccount destination = new SlackAccount();
                destination.setUrl(new URI("https://hooks.slack.com/services/SECRET"));

                SlackActionConf slackActionConf = new SlackActionConf();
                slackActionConf.setText("Test from slack action");
                slackActionConf.setAccount("test");
                slackActionConf.setFrom("some user");
                slackActionConf.setChannel("channel 1");

                HttpResponse response = restClient.putJson("/_signals/account/slack/test", destination.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                        .as("testsearch").then().slack(slackActionConf).name("testslacksink").build();

                response = restClient.putJson(watchPath, watch.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                response = restClient.delete("/_signals/account/slack/test");
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CONFLICT, response.getStatusCode());

                response = restClient.delete(watchPath);
                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                response = restClient.delete("/_signals/account/slack/test");
                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            } finally {
                restClient.delete(watchPath);
                restClient.delete("/_signals/account/slack/test");
            }
        }

    }

    @Test
    public void testDeleteAccountInUseFromNonDefaultTenant() throws Exception {

        String tenant = "redshirt_club";
        String watchId = "slack_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura");
                GenericRestClient redshirtRestClient = cluster.getRestClient("redshirt3", "redshirt")) {

            try {
                SlackAccount destination = new SlackAccount();
                destination.setUrl(new URI("https://hooks.slack.com/services/SECRET"));

                SlackActionConf slackActionConf = new SlackActionConf();
                slackActionConf.setText("Test from slack action");
                slackActionConf.setAccount("test");
                slackActionConf.setFrom("some user");
                slackActionConf.setChannel("channel 1");

                HttpResponse response = restClient.putJson("/_signals/account/slack/test", destination.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                        .as("testsearch").then().slack(slackActionConf).name("testslacksink").build();

                response = redshirtRestClient.putJson(watchPath, watch.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                response = restClient.delete("/_signals/account/slack/test");
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CONFLICT, response.getStatusCode());

                response = restClient.delete(watchPath);
                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                response = restClient.delete("/_signals/account/slack/test");
                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            } finally {
                restClient.delete(watchPath);
                restClient.delete("/_signals/account/slack/test");
            }
        }
    }

    @Test
    public void testPutWeeklySchedule() throws Exception {
        String tenant = "_main";
        String watchId = "test_weekly_schedule";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {

            try (Client client = cluster.getInternalNodeClient()) {

                Watch watch = new WatchBuilder("test").weekly(DayOfWeek.MONDAY, DayOfWeek.WEDNESDAY, new TimeOfDay(12, 0), new TimeOfDay(18, 0))
                        .search("testsource").query("{\"match_all\" : {} }").as("testsearch").put("{\"bla\": {\"blub\": 42}}").as("teststatic").then()
                        .index("testsink").name("testsink").build();

                HttpResponse response = restClient.putJson(watchPath, watch.toJson());

                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                response = restClient.get(watchPath);
                // TODO

            }
        }
    }

    @Test
    public void testPutExponentialThrottling() throws Exception {
        String tenant = "_main";
        String watchId = "test_exponential_throttling";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {

            try (Client client = cluster.getInternalNodeClient()) {

                Watch watch = new WatchBuilder("test").atMsInterval(1000).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                        .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink").throttledFor("1s**1.5|20s").name("testsink")
                        .build();

                HttpResponse response = restClient.putJson(watchPath, watch.toJson());
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                response = restClient.get(watchPath);
                // TODO

            }
        }
    }

    @Test
    public void testSearchAccount() throws Exception {
        String accountId = "search_account";
        String accountPath = "/_signals/account/slack/" + accountId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            SlackAccount slackDestination = new SlackAccount();
            slackDestination.setUrl(new URI("https://xyz.test.com"));

            HttpResponse response = restClient.putJson(accountPath + "1", slackDestination.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            slackDestination.setUrl(new URI("https://abc.test.com"));
            response = restClient.putJson(accountPath + "2", slackDestination.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            slackDestination = new SlackAccount();
            slackDestination.setUrl(new URI("https://abcdef.test.com"));

            response = restClient.putJson(accountPath + "3", slackDestination.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.postJson("/_signals/account/_search",
                    "{ \"sort\": [{\"type.keyword\": {\"order\": \"asc\"}}], \"query\": {\"match\": {\"_name\": \"" + accountId + "1\"}}}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Assert.assertTrue(response.getBody(), response.getBody().contains("https://xyz.test.com"));
        }
    }

    @Test
    public void testStateIsDeletedWhenWatchIsDeleted() throws Exception {
        String tenant = "_main";
        String watchId = "watch_delete_is_state_delete";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String testSink = "testsink_" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch")).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index(testSink).throttledFor("1000h").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            long watchVersion = response.toJsonNode().path("_version").asLong();

            List<WatchLog> watchLogs = new WatchLogSearch(client).index("signals__main_log").watchId(watchId).watchVersion(watchVersion)
                    .fromTheStart().count(3).await();

            log.info("First pass watchLogs: " + watchLogs);

            Assert.assertEquals(watchLogs.toString(),
                    Arrays.asList(Status.Code.ACTION_EXECUTED, Status.Code.ACTION_THROTTLED, Status.Code.ACTION_THROTTLED),
                    watchLogs.stream().map((logEntry) -> logEntry.getStatus().getCode()).collect(Collectors.toList()));

            response = restClient.delete(watchPath);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Thread.sleep(1000);

            response = restClient.putJson(watchPath, watch);

            long newWatchVersion = response.toJsonNode().path("_version").asLong();

            Assert.assertNotEquals(response.getBody(), watchVersion, newWatchVersion);

            watchLogs = new WatchLogSearch(client).index("signals__main_log").watchId(watchId).watchVersion(newWatchVersion).fromTheStart().count(3)
                    .await();
            
            log.info("Second pass watchLogs: " + watchLogs);


            Assert.assertEquals(watchLogs.toString(),
                    Arrays.asList(Status.Code.ACTION_EXECUTED, Status.Code.ACTION_THROTTLED, Status.Code.ACTION_THROTTLED),
                    watchLogs.stream().map((logEntry) -> logEntry.getStatus().getCode()).collect(Collectors.toList()));


        }
    }

    @Test
    public void testSearchAccountScroll() throws Exception {
        String accountId = "search_destination_scroll";
        String accountPath = "/_signals/account/slack/" + accountId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {

            SlackAccount slackDestination = new SlackAccount();
            slackDestination.setUrl(new URI("https://xyz.test.com"));

            HttpResponse response = restClient.putJson(accountPath + "1", slackDestination.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            slackDestination.setUrl(new URI("https://abc.test.com"));
            response = restClient.putJson(accountPath + "2", slackDestination.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            slackDestination = new SlackAccount();
            slackDestination.setUrl(new URI("https://abcdef.test.com"));

            response = restClient.putJson(accountPath + "3", slackDestination.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.postJson("/_signals/destination/_search?scroll=60s&size=1",
                    "{ \"sort\": [{\"type.keyword\": {\"order\": \"asc\"}}], \"query\": {\"match\": {\"type\": \"SLACK\"}}}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Assert.assertTrue(response.getBody(), response.getBody().contains("slack"));

            JsonNode responseJsonNode = DefaultObjectMapper.readTree(response.getBody());

            String scrollId = responseJsonNode.get("_scroll_id").asText(null);

            Assert.assertNotNull(scrollId);

            response = restClient.postJson("/_search/scroll", "{ \"scroll\": \"60s\", \"scroll_id\": \"" + scrollId + "\"}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Assert.assertTrue(response.getBody(), response.getBody().contains("slack"));
        }
    }

    @Test
    public void testConvEs() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {
            String input = new JsonBuilder.Object()
                    .attr("trigger",
                            new JsonBuilder.Object().attr("schedule",
                                    new JsonBuilder.Object().attr("daily", new JsonBuilder.Object().attr("at", "noon"))))
                    .attr("input", new JsonBuilder.Object().attr("simple", new JsonBuilder.Object().attr("x", "y")))
                    .attr("actions", new JsonBuilder.Object()
                            .attr("email_action",
                                    new JsonBuilder.Object().attr("email",
                                            new JsonBuilder.Object().attr("to", "horst@horst").attr("subject", "Hello World")
                                                    .attr("body", "Hallo {{ctx.payload.x}}").attr("attachments", "foo")))

                            .attr("email_action_with_http",
                                    new JsonBuilder.Object().attr("email", new JsonBuilder.Object().attr("to", "horst@horst")
                                            .attr("subject", "Hello World").attr("body", "Hallo {{ctx.payload.x}}").attr("attachments",
                                                    new JsonBuilder.Object().attr("my_image.png", new JsonBuilder.Object().attr("http",
                                                            new JsonBuilder.Object().attr("request",
                                                                    new JsonBuilder.Object().attr("url", "http://example.org/foo/my-image.png")))))))

                            .attr("email_action_with_reporting",
                                    new JsonBuilder.Object().attr("email",
                                            new JsonBuilder.Object().attr("to", "horst@horst").attr("subject", "Hello World")
                                                    .attr("body", "Hallo {{ctx.payload.x}}")
                                                    .attr("attachments", new JsonBuilder.Object().attr("dashboard.pdf",
                                                            new JsonBuilder.Object().attr("reporting", new JsonBuilder.Object().attr("url",
                                                                    "http://example.org:5601/api/reporting/generate/dashboard/Error-Monitoring"))))))

                            .attr("another_action", new JsonBuilder.Object().attr("index",
                                    new JsonBuilder.Object().attr("index", "foo").attr("execution_time_field", "holla"))))
                    .toJsonString();

            HttpResponse response = restClient.postJson("/_signals/convert/es", input);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
        }
    }

    @Test
    public void testPutAllowedEndpointsSetting() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {
            String endpointJson = "[\"x\",\"y\"]";

            try {
                HttpResponse response = restClient.putJson("/_signals/settings/http.allowed_endpoints", endpointJson);

                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                Thread.sleep(1000l);

                response = restClient.get("/_signals/settings/http.allowed_endpoints");

                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                Assert.assertEquals(endpointJson, response.getBody());

            } finally {
                restClient.putJson("/_signals/settings/http.allowed_endpoints", "\"*\"");
            }
        }
    }

    private long getCountOfDocuments(Client client, String index) throws InterruptedException, ExecutionException {
        SearchRequest request = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        request.source(searchSourceBuilder);

        SearchResponse response = client.search(request).get();

        return response.getHits().getTotalHits().value;
    }

    public String getDocs(Client client, String index) throws InterruptedException, ExecutionException {
        SearchRequest request = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        request.source(searchSourceBuilder);

        SearchResponse response = client.search(request).get();

        return Strings.toString(response.getHits());
    }

    private long awaitMinCountOfDocuments(Client client, String index, long minCount) throws Exception {
        long start = System.currentTimeMillis();

        for (int i = 0; i < 2000; i++) {
            Thread.sleep(10);
            long count = getCountOfDocuments(client, index);

            if (count >= minCount) {
                log.info("Found " + count + " documents in " + index + " after " + (System.currentTimeMillis() - start) + " ms");

                return count;
            }
        }

        Assert.fail("Did not find " + minCount + " documents in " + index + " after " + (System.currentTimeMillis() - start) + " ms");

        return 0;
    }

    private List<WatchLog> getMostRecentWatchLogs(Client client, String tenantName, String watchName, Long watchVersion, int count) {
        try {

            QueryBuilder queryBuilder;

            if (watchVersion == null) {
                queryBuilder = new TermQueryBuilder("watch_id", watchName);
            } else {
                queryBuilder = QueryBuilders.boolQuery().must(new TermQueryBuilder("watch_id", watchName))
                        .must(new TermQueryBuilder("watch_version", watchVersion));
            }

            SearchResponse searchResponse = client.search(new SearchRequest("signals_" + tenantName + "_log")
                    .source(new SearchSourceBuilder().size(count).sort("execution_end", SortOrder.DESC).query(queryBuilder))).actionGet();

            if (searchResponse.getHits().getHits().length == 0) {
                return Collections.emptyList();
            }

            ArrayList<WatchLog> result = new ArrayList<>(count);

            for (SearchHit searchHit : searchResponse.getHits().getHits()) {
                result.add(WatchLog.parse(searchHit.getId(), searchHit.getSourceAsString()));
            }

            Collections.reverse(result);

            return result;
        } catch (org.elasticsearch.index.IndexNotFoundException | SearchPhaseExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error in getMostRecenWatchLog(" + tenantName + ", " + watchName + ")", e);
        }
    }

    private WatchLog awaitWatchLog(Client client, String tenantName, String watchName) throws Exception {
        return awaitWatchLogs(client, tenantName, watchName, null, 1).get(0);
    }

    private List<WatchLog> awaitWatchLogs(Client client, String tenantName, String watchName, Long watchVersion, int count) throws Exception {
        try {
            long start = System.currentTimeMillis();
            Exception indexNotFoundException = null;

            for (int i = 0; i < 1000; i++) {
                Thread.sleep(10);

                try {
                    List<WatchLog> watchLogs = getMostRecentWatchLogs(client, tenantName, watchName, watchVersion, count);

                    if (watchLogs.size() == count) {
                        log.info("Found " + watchLogs + " for " + watchName + " after " + (System.currentTimeMillis() - start) + " ms");

                        return watchLogs;
                    } else if (i != 0 && i % 200 == 0) {
                        log.debug("Still waiting for watch logs; found so far: " + watchLogs);
                    }

                    indexNotFoundException = null;

                } catch (org.elasticsearch.index.IndexNotFoundException | SearchPhaseExecutionException e) {
                    indexNotFoundException = e;
                    continue;
                }
            }

            if (indexNotFoundException != null) {
                Assert.fail("Did not find watch log index for " + watchName + " after " + (System.currentTimeMillis() - start) + " ms: "
                        + indexNotFoundException);
            } else {
                SearchResponse searchResponse = client
                        .search(new SearchRequest("signals_" + tenantName + "_log")
                                .source(new SearchSourceBuilder().sort("execution_end", SortOrder.DESC).query(new MatchAllQueryBuilder())))
                        .actionGet();

                log.info("Did not find watch log for " + watchName + " after " + (System.currentTimeMillis() - start) + " ms\n\n"
                        + searchResponse.getHits());

                Assert.fail("Did not find watch log for " + watchName + " after " + (System.currentTimeMillis() - start) + " ms");
            }
            return null;
        } catch (Exception e) {
            log.error("Exception in awaitWatchLog for " + watchName + ")", e);
            throw new RuntimeException("Exception in awaitWatchLog for " + watchName + ")", e);
        }
    }

    private Watch getWatchByRest(String tenant, String id, GenericRestClient restClient) throws Exception {
        HttpResponse response = restClient.get("/_signals/watch/" + tenant + "/" + id);

        Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

        return Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", id, response.getBody(), -1);
    }

    private HttpResponse awaitRestGet(String request, GenericRestClient restClient) throws Exception {
        HttpResponse response = null;
        long start = System.currentTimeMillis();

        for (int i = 0; i < 100; i++) {
            response = restClient.get(request);

            if (response.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
                log.info(request + " returned " + response.getStatusCode() + " after " + (System.currentTimeMillis() - start) + "ms (" + i
                        + " retries)");

                return response;
            }

            Thread.sleep(10);
        }

        return response;

    }

}
