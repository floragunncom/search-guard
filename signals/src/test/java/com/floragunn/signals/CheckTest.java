package com.floragunn.signals;

import com.browserup.bup.BrowserUpProxy;
import com.browserup.bup.BrowserUpProxyServer;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient.HttpResponse;
import com.floragunn.signals.execution.CheckExecutionException;
import com.floragunn.signals.execution.ExecutionEnvironment;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.execution.WatchExecutionContextData;
import com.floragunn.signals.execution.WatchExecutionContextData.TriggerInfo;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;
import com.floragunn.signals.watch.action.invokers.ActionInvocationType;
import com.floragunn.signals.watch.checks.Calc;
import com.floragunn.signals.watch.checks.Condition;
import com.floragunn.signals.watch.checks.HttpInput;
import com.floragunn.signals.watch.checks.SearchInput;
import com.floragunn.signals.watch.common.HttpClientConfig;
import com.floragunn.signals.watch.common.HttpProxyConfig;
import com.floragunn.signals.watch.common.HttpRequestConfig;
import com.floragunn.signals.watch.init.WatchInitializationService;
import com.floragunn.signals.watch.result.Status;
import com.floragunn.signals.watch.result.WatchLog;
import net.jcip.annotations.NotThreadSafe;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.*;

import java.net.InetAddress;
import java.net.URI;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.floragunn.searchguard.test.helper.certificate.NodeCertificateType.transport_and_rest;
import static com.floragunn.searchguard.test.helper.certificate.TestCertificateFactory.rsaBaseCertificateFactory;

@NotThreadSafe
public class CheckTest {
    private static final Logger log = LogManager.getLogger(CheckTest.class);

    private static NamedXContentRegistry xContentRegistry;
    private static ScriptService scriptService;
    private static BrowserUpProxy httpProxy;

    public static TestCertificates certificatesContext = TestCertificates.builder()
            .defaults(defaults -> defaults.setValidityDays(30)
                    .setNodeOid("1.2.3.4.5.5")
                    .setNodeIpList()
                    .setNodeDnsList()
                    .setNodeCertificateType(transport_and_rest))
            .ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard")
            .addNodes("CN=node-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addAdminClients("CN=admin-0.example.com,OU=SearchGuard,O=SearchGuard")
            .build();

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster anotherCluster = new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext).resources("sg_config/signals")
            .nodeSettings("signals.enabled", false, "searchguard.enterprise_modules_enabled", false).build();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext).resources("sg_config/signals")
            .nodeSettings("signals.enabled", true, "searchguard.enterprise_modules_enabled", false).remote("my_remote", anotherCluster).build();

    @BeforeClass
    public static void setupTestData() {

        try (Client client = cluster.getInternalNodeClient()) {
            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "x", "b", "y",
                    "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "xx", "b", "yy",
                    "date", getYesterday())).actionGet();
        }

        try (Client client = anotherCluster.getInternalNodeClient()) {
            client.index(new IndexRequest("ccs_testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "x", "b", "y",
                    "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("ccs_testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "xx", "b", "yy",
                    "date", getYesterday())).actionGet();
        }
    }

    @BeforeClass
    public static void setupDependencies() throws Exception {
        xContentRegistry = cluster.getInjectable(NamedXContentRegistry.class);
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
    public void searchTest() throws Exception {

        try (Client client = cluster.getInternalNodeClient()) {

            SearchInput searchInput = new SearchInput("test", "test", "testsource", "{\"query\": {\"term\" : {\"a\": \"x\"} }}");
            searchInput.compileScripts(new WatchInitializationService(null, scriptService));

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put(new NestedValueMap.Path("x", "hits", "total"), 7);
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            boolean result = searchInput.execute(ctx);

            Assert.assertTrue(result);

            @SuppressWarnings("unchecked")
            List<Map<?, Map<?, ?>>> searchResult = (List<Map<?, Map<?, ?>>>) runtimeData.get(new NestedValueMap.Path("test", "hits", "hits"));

            Assert.assertEquals(1, searchResult.size());
            Assert.assertEquals("x", searchResult.get(0).get("_source").get("a"));
        }
    }

    @Test
    public void searchWithTemplateTest() throws Exception {

        try (Client client = cluster.getInternalNodeClient()) {

            SearchInput searchInput = new SearchInput("test", "test", "testsource", "{\"query\": {\"term\" : {\"a\": \"{{data.match}}\"} }}");
            searchInput.compileScripts(new WatchInitializationService(null, scriptService));

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put(new NestedValueMap.Path("match"), "xx");
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            boolean result = searchInput.execute(ctx);

            Assert.assertTrue(result);

            @SuppressWarnings("unchecked")
            List<Map<?, Map<?, ?>>> searchResult = (List<Map<?, Map<?, ?>>>) runtimeData.get(new NestedValueMap.Path("test", "hits", "hits"));

            Assert.assertEquals(1, searchResult.size());
            Assert.assertEquals("yy", searchResult.get(0).get("_source").get("b"));
        }
    }

    @Test
    @Ignore
    public void searchWithScheduleDateTest() throws Exception {

        try (Client client = cluster.getInternalNodeClient()) {

            SearchInput searchInput = new SearchInput("test", "test", "testsource",
                    "{\"query\": {\"range\" : {\"date\": {\"gte\": \"{{trigger.scheduled_time}}||-1M\", \"lt\": \"{{trigger.scheduled_time}}\", \"format\": \"strict_date_time\"} } }}");
            searchInput.compileScripts(new WatchInitializationService(null, scriptService));

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put(new NestedValueMap.Path("match"), "xx");
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT,
                    new WatchExecutionContextData(runtimeData, null, new TriggerInfo(new Date(), new Date(), new Date(), new Date()), null));

            boolean result = searchInput.execute(ctx);

            Assert.assertTrue(result);

            @SuppressWarnings("unchecked")
            List<Map<?, Map<?, ?>>> searchResult = (List<Map<?, Map<?, ?>>>) runtimeData.get(new NestedValueMap.Path("test", "hits", "hits"));

            Assert.assertEquals(1, searchResult.size());
            Assert.assertEquals("yy", searchResult.get(0).get("_source").get("b"));
        }
    }

    @Test
    public void searchWithTemplateMappingTest() throws Exception {
        String tenant = "_main";
        String watchId1 = "search_with_template_mapping_1";
        String watchPath1 = "/_signals/watch/" + tenant + "/" + watchId1;
        String watchId2 = "search_with_template_mapping_2";
        String watchPath2 = "/_signals/watch/" + tenant + "/" + watchId2;

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {
            try {
                Watch watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                        .attr("size", 1).as("testsearch").then().index("testsink_" + watchId1).name("testsink").build();
                HttpResponse response = restClient.putJson(watchPath1, watch);

                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                response = restClient.get(watchPath1);

                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").put("{\"size\": 1}").as("constants").search("testsource")
                        .query("{\"match_all\" : {} }").attr("size", "{{data.constants.size}}").as("testsearch").then().index("testsink_" + watchId2)
                        .name("testsink").build();
                response = restClient.putJson(watchPath2, watch);

                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                response = restClient.get(watchPath2);

                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            } finally {
                restClient.delete(watchPath1);
                restClient.delete(watchPath2);
            }
        }
    }

    @Test
    public void searchScheduledTest() throws Exception {
        String tenant = "_main";
        String watchId = "search_scheduled_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {
            try {
                Watch watch = new WatchBuilder(watchId).atMsInterval(300).unthrottled().search("my_remote:ccs_testsource")
                        .query("{\"match_all\" : {} }").as("testsearch").build();
                HttpResponse response = restClient.putJson(watchPath, watch);

                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                response = restClient.get(watchPath);

                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                watch = Watch.parseFromElasticDocument(new WatchInitializationService(null, scriptService), "test", "put_test", response.getBody(),
                        -1);

                WatchLog watchLog = awaitWatchLog(client, tenant, watchId);

                Assert.assertEquals(watchLog.toString(), Status.Code.NO_ACTION, watchLog.getStatus().getCode());

            } finally {
                restClient.delete(watchPath);
            }
        }
    }

    @Test
    public void httpInputTest() throws Exception {
        try (Client client = cluster.getInternalNodeClient(); MockWebserviceProvider webserviceProvider = new MockWebserviceProvider("/service")) {

            webserviceProvider.setResponseBody("{\"foo\": \"bar\", \"x\": 55}");
            webserviceProvider.setResponseContentType("text/json");

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webserviceProvider.getUri()), null,
                    null, null, null, null, null);
            httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

            HttpInput httpInput = new HttpInput("test", "test", httpRequestConfig, new HttpClientConfig(null, null, null, null));

            NestedValueMap runtimeData = new NestedValueMap();
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            boolean result = httpInput.execute(ctx);

            Assert.assertTrue(result);

            Map<?, ?> inputResult = (Map<?, ?>) runtimeData.get("test");

            Assert.assertEquals("bar", inputResult.get("foo"));
            Assert.assertEquals(55, inputResult.get("x"));
        }
    }

    @Test
    public void httpInputTestContentTypeHasCharset() throws Exception {
        try (Client client = cluster.getInternalNodeClient(); MockWebserviceProvider webserviceProvider = new MockWebserviceProvider("/service")) {

            webserviceProvider.setResponseBody("{\"foo\": \"bar\", \"x\": 55}");
            webserviceProvider.setResponseContentType("text/json; charset=utf-8");

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webserviceProvider.getUri()), null,
                    null, null, null, null, null);
            httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

            HttpInput httpInput = new HttpInput("test", "test", httpRequestConfig, new HttpClientConfig(null, null, null, null));

            NestedValueMap runtimeData = new NestedValueMap();
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            boolean result = httpInput.execute(ctx);

            Assert.assertTrue(result);

            Map<?, ?> inputResult = (Map<?, ?>) runtimeData.get("test");

            Assert.assertEquals("bar", inputResult.get("foo"));
            Assert.assertEquals(55, inputResult.get("x"));
        }
    }

    @Test
    public void httpInputTextTest() throws Exception {
        try (Client client = cluster.getInternalNodeClient(); MockWebserviceProvider webserviceProvider = new MockWebserviceProvider("/service")) {

            String text = "{\"foo\": \"bar\", \"x\": 55}";

            webserviceProvider.setResponseBody(text);
            webserviceProvider.setResponseContentType("text/plain");

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webserviceProvider.getUri()), null,
                    null, null, null, null, null);
            httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

            HttpInput httpInput = new HttpInput("test", "test", httpRequestConfig, new HttpClientConfig(null, null, null, null));

            NestedValueMap runtimeData = new NestedValueMap();
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            boolean result = httpInput.execute(ctx);

            Assert.assertTrue(result);

            Assert.assertEquals(text, runtimeData.get("test"));
        }
    }


    @Test
    public void httpInputProxyTest() throws Exception {
        try (Client client = cluster.getInternalNodeClient(); MockWebserviceProvider webserviceProvider = new MockWebserviceProvider("/service")) {

            webserviceProvider.setResponseBody("{\"foo\": \"bar\", \"x\": 55}");
            webserviceProvider.setResponseContentType("text/json");
            webserviceProvider.acceptConnectionsOnlyFromInetAddress(InetAddress.getByName("127.0.0.9"));

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webserviceProvider.getUri()), null,
                    null, null, null, null, null);
            httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

            HttpInput httpInput = new HttpInput("test", "test", httpRequestConfig, new HttpClientConfig(null, null, null, null));

            NestedValueMap runtimeData = new NestedValueMap();
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            try {
                httpInput.execute(ctx);
                Assert.fail();
            } catch (CheckExecutionException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("We are not accepting connections from"));
            }

            httpInput = new HttpInput("test", "test", httpRequestConfig,
                    new HttpClientConfig(null, null, null, HttpProxyConfig.create("http://127.0.0.8:" + httpProxy.getPort())));

            boolean result = httpInput.execute(ctx);

            Assert.assertTrue(result);

            Map<?, ?> inputResult = (Map<?, ?>) runtimeData.get("test");

            Assert.assertEquals("bar", inputResult.get("foo"));
            Assert.assertEquals(55, inputResult.get("x"));
        }
    }

    @Test(expected = CheckExecutionException.class)
    public void httpWrongContentTypeTest() throws Exception {
        try (Client client = cluster.getInternalNodeClient(); MockWebserviceProvider webserviceProvider = new MockWebserviceProvider("/service")) {

            String text = "{\"foo\": \"bar\", \"x\": 55}";

            webserviceProvider.setResponseBody(text);
            webserviceProvider.setResponseContentType("application/vnd.floragunnmegapearls");

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webserviceProvider.getUri()), null,
                    null, null, null, null, "application/x-yaml");
            httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

            HttpInput httpInput = new HttpInput("test", "test", httpRequestConfig, new HttpClientConfig(null, null, null, null));

            NestedValueMap runtimeData = new NestedValueMap();
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            httpInput.execute(ctx);

            Assert.fail();
        }
    }

    @Test
    public void httpInputTimeoutTest() throws Exception {
        try (Client client = cluster.getInternalNodeClient(); MockWebserviceProvider webserviceProvider = new MockWebserviceProvider("/service")) {

            webserviceProvider.setResponseBody("{\"foo\": \"bar\", \"x\": 55}");
            webserviceProvider.setResponseContentType("text/json");
            webserviceProvider.setResponseDelayMs(3330);

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webserviceProvider.getUri()), null,
                    null, null, null, null, null);
            httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

            HttpInput httpInput = new HttpInput("test", "test", httpRequestConfig, new HttpClientConfig(1, 1, null, null));

            NestedValueMap runtimeData = new NestedValueMap();
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            httpInput.execute(ctx);

            Assert.fail();
        } catch (CheckExecutionException e) {
            Assert.assertTrue(e.toString(), e.getCause().toString().contains("Read timed out"));
        }
    }

    @Ignore
    @Test
    public void testConditionTrue() throws Exception {

        try (Client client = cluster.getInternalNodeClient()) {

            Condition scriptCondition = new Condition(null, "data.x.hits.total > 5", "painless", Collections.emptyMap());
            scriptCondition.compileScripts(new WatchInitializationService(null, scriptService));

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put(new NestedValueMap.Path("x", "hits", "total"), 7);
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            boolean result = scriptCondition.execute(ctx);

            Assert.assertTrue(result);
        }
    }

    @Ignore
    @Test
    public void testConditionFalse() throws Exception {

        try (Client client = cluster.getInternalNodeClient()) {

            Condition scriptCondition = new Condition(null, "data.x.hits.total > 510", "painless", Collections.emptyMap());
            scriptCondition.compileScripts(new WatchInitializationService(null, scriptService));

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put(new NestedValueMap.Path("x", "hits", "total"), 7);
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            boolean result = scriptCondition.execute(ctx);

            Assert.assertFalse(result);
        }
    }

    @Ignore
    @Test(expected = CheckExecutionException.class)
    public void testInvalidCondition() throws Exception {

        try (Client client = cluster.getInternalNodeClient()) {

            Condition scriptCondition = new Condition(null, "data.x.hits.hits.length > data.constants.threshold", "painless", Collections.emptyMap());
            scriptCondition.compileScripts(new WatchInitializationService(null, scriptService));

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put(new NestedValueMap.Path("x", "hits", "total"), 7);
            runtimeData.put(new NestedValueMap.Path("constants", "threshold"), 5);

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            scriptCondition.execute(ctx);

            Assert.fail();
        }
    }

    @Ignore
    @Test
    public void testCalc() throws Exception {

        try (Client client = cluster.getInternalNodeClient()) {

            Calc calc = new Calc(null, "data.x.y = 5", "painless", Collections.emptyMap());
            calc.compileScripts(new WatchInitializationService(null, scriptService));

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put(new NestedValueMap.Path("x", "hits", "total"), 7);
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            boolean result = calc.execute(ctx);

            Assert.assertTrue(result);

            Assert.assertEquals(5, runtimeData.get(new NestedValueMap.Path("x", "y")));
        }
    }


    private static String getYesterday() {
        LocalDate now = LocalDate.now().minus(1, ChronoUnit.DAYS);

        return now.format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
    }

    private WatchLog awaitWatchLog(Client client, String tenantName, String watchName) throws Exception {
        try {
            long start = System.currentTimeMillis();
            Exception indexNotFoundException = null;

            for (int i = 0; i < 1000; i++) {
                Thread.sleep(10);

                try {
                    WatchLog watchLog = getMostRecentWatchLog(client, tenantName, watchName);

                    if (watchLog != null) {
                        log.info("Found " + watchLog + " for " + watchName + " after " + (System.currentTimeMillis() - start) + " ms");

                        return watchLog;
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
                        .search(new SearchRequest(".signals_log_*")
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

    private WatchLog getMostRecentWatchLog(Client client, String tenantName, String watchName) {
        try {
            SearchResponse searchResponse = client.search(new SearchRequest(".signals_log_*").source(
                            new SearchSourceBuilder().size(1).sort("execution_end", SortOrder.DESC).query(new MatchQueryBuilder("watch_id", watchName))))
                    .actionGet();

            if (searchResponse.getHits().getHits().length == 0) {
                return null;
            }

            SearchHit searchHit = searchResponse.getHits().getHits()[0];

            return WatchLog.parse(searchHit.getId(), searchHit.getSourceAsString());
        } catch (org.elasticsearch.index.IndexNotFoundException | SearchPhaseExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error in getMostRecenWatchLog(" + tenantName + ", " + watchName + ")", e);
        }
    }
}
