package com.floragunn.signals;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.ScriptService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;
import com.floragunn.signals.execution.CheckExecutionException;
import com.floragunn.signals.execution.ExecutionEnvironment;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.execution.WatchExecutionContextData;
import com.floragunn.signals.execution.WatchExecutionContextData.TriggerInfo;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.WatchBuilder;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.action.invokers.ActionInvocationType;
import com.floragunn.signals.watch.checks.Calc;
import com.floragunn.signals.watch.checks.HttpInput;
import com.floragunn.signals.watch.checks.Condition;
import com.floragunn.signals.watch.checks.SearchInput;
import com.floragunn.signals.watch.common.HttpClientConfig;
import com.floragunn.signals.watch.common.HttpRequestConfig;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class CheckTest {

    private static NamedXContentRegistry xContentRegistry;
    private static ScriptService scriptService;
    private static RestHelper rh = null;

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("sg_config/signals")
            .nodeSettings("signals.enabled", true, "signals.index_names.log", "signals_main_log").build();

    @BeforeClass
    public static void setupTestData() {

        try (Client client = cluster.getInternalClient()) {
            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "x", "b", "y",
                    "date", "1985/01/01")).actionGet();
            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "xx", "b", "yy",
                    "date", getYesterday())).actionGet();
        }
    }

    @BeforeClass
    public static void setupDependencies() {
        xContentRegistry = cluster.getInjectable(NamedXContentRegistry.class);
        scriptService = cluster.getInjectable(ScriptService.class);
        rh = cluster.restHelper();
    }

    @Test
    public void searchTest() throws Exception {

        try (Client client = cluster.getInternalClient()) {

            SearchInput searchInput = new SearchInput("test", "test", "testsource", "{\"query\": {\"term\" : {\"a\": \"x\"} }}");
            searchInput.compileScripts(new WatchInitializationService(null, scriptService));

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put(new NestedValueMap.Path("x", "hits", "total"), 7);
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            boolean result = searchInput.execute(ctx);

            System.out.println(runtimeData);

            Assert.assertTrue(result);

            @SuppressWarnings("unchecked")
            List<Map<?, Map<?, ?>>> searchResult = (List<Map<?, Map<?, ?>>>) runtimeData.get(new NestedValueMap.Path("test", "hits", "hits"));

            Assert.assertEquals(1, searchResult.size());
            Assert.assertEquals("x", searchResult.get(0).get("_source").get("a"));
        }
    }

    @Test
    public void searchWithTemplateTest() throws Exception {

        try (Client client = cluster.getInternalClient()) {

            SearchInput searchInput = new SearchInput("test", "test", "testsource", "{\"query\": {\"term\" : {\"a\": \"{{data.match}}\"} }}");
            searchInput.compileScripts(new WatchInitializationService(null, scriptService));

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put(new NestedValueMap.Path("match"), "xx");
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            boolean result = searchInput.execute(ctx);

            System.out.println(runtimeData);

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

        try (Client client = cluster.getInternalClient()) {

            SearchInput searchInput = new SearchInput("test", "test", "testsource",
                    "{\"query\": {\"range\" : {\"date\": {\"gte\": \"{{trigger.scheduled_time}}||-1M\", \"lt\": \"{{trigger.scheduled_time}}\", \"format\": \"strict_date_time\"} } }}");
            searchInput.compileScripts(new WatchInitializationService(null, scriptService));

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put(new NestedValueMap.Path("match"), "xx");
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT,
                    new WatchExecutionContextData(runtimeData, new TriggerInfo(new Date(), new Date(), new Date(), new Date()), null));

            boolean result = searchInput.execute(ctx);

            System.out.println(runtimeData);

            Assert.assertTrue(result);

            @SuppressWarnings("unchecked")
            List<Map<?, Map<?, ?>>> searchResult = (List<Map<?, Map<?, ?>>>) runtimeData.get(new NestedValueMap.Path("test", "hits", "hits"));

            Assert.assertEquals(1, searchResult.size());
            Assert.assertEquals("yy", searchResult.get(0).get("_source").get("b"));
        }
    }

    @Test
    public void searchWithTemplateMappingTest() throws Exception {
        Header auth = basicAuth("uhura", "uhura");
        String tenant = "_main";
        String watchId1 = "search_with_template_mapping_1";
        String watchPath1 = "/_signals/watch/" + tenant + "/" + watchId1;
        String watchId2 = "search_with_template_mapping_2";
        String watchPath2 = "/_signals/watch/" + tenant + "/" + watchId2;

        try (Client client = cluster.getInternalClient()) {
            Watch watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").attr("size", 1)
                    .as("testsearch").then().index("testsink_" + watchId1).name("testsink").build();
            HttpResponse response = rh.executePutRequest(watchPath1, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executeGetRequest(watchPath1, auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").put("{\"size\": 1}").as("constants").search("testsource")
                    .query("{\"match_all\" : {} }").attr("size", "{{data.constants.size}}").as("testsearch").then().index("testsink_" + watchId2)
                    .name("testsink").build();
            response = rh.executePutRequest(watchPath2, watch.toJson(), auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = rh.executeGetRequest(watchPath2, auth);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

        } finally {
            rh.executeDeleteRequest(watchPath1, auth);
            rh.executeDeleteRequest(watchPath2, auth);
        }
    }

    @Test
    public void httpInputTest() throws Exception {
        try (Client client = cluster.getInternalClient(); MockWebserviceProvider webserviceProvider = new MockWebserviceProvider("/service")) {

            webserviceProvider.setResponseBody("{\"foo\": \"bar\", \"x\": 55}");
            webserviceProvider.setResponseContentType("text/json");

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webserviceProvider.getUri()), null,
                    null, null, null, null, null);
            httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

            HttpInput httpInput = new HttpInput("test", "test", httpRequestConfig, new HttpClientConfig(null, null, null));

            NestedValueMap runtimeData = new NestedValueMap();
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            boolean result = httpInput.execute(ctx);

            System.out.println(runtimeData);

            Assert.assertTrue(result);

            Map<?, ?> inputResult = (Map<?, ?>) runtimeData.get("test");

            Assert.assertEquals("bar", inputResult.get("foo"));
            Assert.assertEquals(55, inputResult.get("x"));
        }
    }

    @Test
    public void httpInputTestContentTypeHasCharset() throws Exception {
        try (Client client = cluster.getInternalClient(); MockWebserviceProvider webserviceProvider = new MockWebserviceProvider("/service")) {

            webserviceProvider.setResponseBody("{\"foo\": \"bar\", \"x\": 55}");
            webserviceProvider.setResponseContentType("text/json; charset=utf-8");

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webserviceProvider.getUri()), null,
                    null, null, null, null, null);
            httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

            HttpInput httpInput = new HttpInput("test", "test", httpRequestConfig, new HttpClientConfig(null, null, null));

            NestedValueMap runtimeData = new NestedValueMap();
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            boolean result = httpInput.execute(ctx);

            System.out.println(runtimeData);

            Assert.assertTrue(result);

            Map<?, ?> inputResult = (Map<?, ?>) runtimeData.get("test");

            Assert.assertEquals("bar", inputResult.get("foo"));
            Assert.assertEquals(55, inputResult.get("x"));
        }
    }

    @Test
    public void httpInputTextTest() throws Exception {
        try (Client client = cluster.getInternalClient(); MockWebserviceProvider webserviceProvider = new MockWebserviceProvider("/service")) {

            String text = "{\"foo\": \"bar\", \"x\": 55}";

            webserviceProvider.setResponseBody(text);
            webserviceProvider.setResponseContentType("text/plain");

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webserviceProvider.getUri()), null,
                    null, null, null, null, null);
            httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

            HttpInput httpInput = new HttpInput("test", "test", httpRequestConfig, new HttpClientConfig(null, null, null));

            NestedValueMap runtimeData = new NestedValueMap();
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            boolean result = httpInput.execute(ctx);

            System.out.println(runtimeData);

            Assert.assertTrue(result);

            Assert.assertEquals(text, runtimeData.get("test"));
        }
    }

    @Test(expected = CheckExecutionException.class)
    public void httpWrongContentTypeTest() throws Exception {
        try (Client client = cluster.getInternalClient(); MockWebserviceProvider webserviceProvider = new MockWebserviceProvider("/service")) {

            String text = "{\"foo\": \"bar\", \"x\": 55}";

            webserviceProvider.setResponseBody(text);
            webserviceProvider.setResponseContentType("application/vnd.floragunnmegapearls");

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webserviceProvider.getUri()), null,
                    null, null, null, null, "application/x-yaml");
            httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService));

            HttpInput httpInput = new HttpInput("test", "test", httpRequestConfig, new HttpClientConfig(null, null, null));

            NestedValueMap runtimeData = new NestedValueMap();
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            httpInput.execute(ctx);

            Assert.fail();
        }
    }

    @Test
    public void testConditionTrue() throws Exception {

        try (Client client = cluster.getInternalClient()) {

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

    @Test
    public void testConditionFalse() throws Exception {

        try (Client client = cluster.getInternalClient()) {

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

    @Test(expected = CheckExecutionException.class)
    public void testInvalidCondition() throws Exception {

        try (Client client = cluster.getInternalClient()) {

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

    @Test
    public void testCalc() throws Exception {

        try (Client client = cluster.getInternalClient()) {

            Calc calc = new Calc(null, "data.x.y = 5", "painless", Collections.emptyMap());
            calc.compileScripts(new WatchInitializationService(null, scriptService));

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put(new NestedValueMap.Path("x", "hits", "total"), 7);
            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, null, ExecutionEnvironment.SCHEDULED,
                    ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));

            boolean result = calc.execute(ctx);

            System.out.println(runtimeData);

            Assert.assertTrue(result);

            Assert.assertEquals(5, runtimeData.get(new NestedValueMap.Path("x", "y")));
        }
    }

    private static Header basicAuth(String username, String password) {
        return new BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((username + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));
    }

    private static String getYesterday() {
        LocalDate now = LocalDate.now().minus(1, ChronoUnit.DAYS);

        return now.format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
    }
}
