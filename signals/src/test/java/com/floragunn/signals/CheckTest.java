package com.floragunn.signals;

import java.net.Socket;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchsupport.proxy.wiremock.WireMockRequestHeaderAddingFilter;
import com.floragunn.signals.accounts.AccountRegistry;
import com.floragunn.signals.proxy.service.HttpProxyHostRegistry;
import com.floragunn.signals.watch.common.throttle.ThrottlePeriodParser;
import com.floragunn.signals.watch.common.throttle.ValidatingThrottlePeriodParser;
import com.floragunn.signals.truststore.service.TrustManagerRegistry;
import com.floragunn.signals.watch.common.TlsConfig;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.execution.CheckExecutionException;
import com.floragunn.signals.execution.ExecutionEnvironment;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.execution.WatchExecutionContextData;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.net.ssl.X509ExtendedTrustManager;

import static com.floragunn.signals.watch.common.ValidationLevel.STRICT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@NotThreadSafe
@RunWith(MockitoJUnitRunner.class)
public class CheckTest {
    private static final Logger log = LogManager.getLogger(CheckTest.class);
    public static final String TRUSTSTORE_ID = "my-truststore-id-001";

    private static NamedXContentRegistry xContentRegistry;
    private static ScriptService scriptService;
    private static ClusterService clusterService;
    private static FeatureService featureService;
    private static ThrottlePeriodParser throttlePeriodParser;

    private static TestCertificates testCertificates = TestCertificates.builder().ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard")
            .addNodes("CN=node-0.example.com,OU=SearchGuard,O=SearchGuard").addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addAdminClients("CN=admin-0.example.com,OU=SearchGuard,O=SearchGuard").build();

    private static final WireMockRequestHeaderAddingFilter REQUEST_HEADER_ADDING_FILTER = new WireMockRequestHeaderAddingFilter("Proxy", "wire-mock");

    @ClassRule
    public static WireMockClassRule wireMockProxy = new WireMockClassRule(WireMockConfiguration.options()
            .bindAddress("127.0.0.8")
            .enableBrowserProxying(true)
            .proxyPassThrough(true)
            .dynamicPort()
            .extensions(REQUEST_HEADER_ADDING_FILTER));

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster.Embedded anotherCluster = new LocalCluster.Builder().singleNode().sslEnabled(testCertificates).resources("sg_config/signals")
            .nodeSettings("signals.enabled", false, "searchguard.enterprise_modules_enabled", false).enableModule(SignalsModule.class).embedded().build();

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled(testCertificates).resources("sg_config/signals")
            .nodeSettings("signals.enabled", true, "searchguard.enterprise_modules_enabled", false).remote("my_remote", anotherCluster)
            .enableModule(SignalsModule.class).waitForComponents("signals").embedded().build();

    @Mock
    private AccountRegistry accountRegistry;
    @Mock
    private TrustManagerRegistry trustManagerRegistry;
    @Mock
    private HttpProxyHostRegistry httpProxyHostRegistry;
    @Mock
    private X509ExtendedTrustManager trustManager;
    private final WatchInitializationService watchInitializationService = new WatchInitializationService(accountRegistry, scriptService, trustManagerRegistry, httpProxyHostRegistry, throttlePeriodParser, STRICT);

    @BeforeClass
    public static void setupTestData() {

        Client client = cluster.getInternalNodeClient();
        client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "x", "b", "y",
                "date", "1985/01/01")).actionGet();
        client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "xx", "b", "yy",
                "date", getYesterday())).actionGet();

        Client anotherClient = anotherCluster.getInternalNodeClient();
        anotherClient.index(new IndexRequest("ccs_testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "x", "b", "y",
                "date", "1985/01/01")).actionGet();
        anotherClient.index(new IndexRequest("ccs_testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "xx", "b", "yy",
                "date", getYesterday())).actionGet();
    }

    @BeforeClass
    public static void setupDependencies() throws Exception {
        xContentRegistry = cluster.getInjectable(NamedXContentRegistry.class);
        scriptService = cluster.getInjectable(ScriptService.class);
        throttlePeriodParser = new ValidatingThrottlePeriodParser(cluster.getInjectable(Signals.class).getSignalsSettings());
        clusterService = cluster.getInjectable(ClusterService.class);
        featureService = cluster.getInjectable(FeatureService.class);
    }

    @Before
    public void resetMock() {
        Mockito.reset(accountRegistry, trustManagerRegistry, trustManager, httpProxyHostRegistry);
    }

    @Test
    public void searchTest() throws Exception {

        SearchInput searchInput = new SearchInput("test", "test", "testsource", "{\"query\": {\"term\" : {\"a\": \"x\"} }}");
        searchInput.compileScripts(watchInitializationService);

        NestedValueMap runtimeData = new NestedValueMap();
        runtimeData.put(new NestedValueMap.Path("x", "hits", "total"), 7);
        WatchExecutionContext ctx = buildWatchExecutionContext(runtimeData);

        boolean result = searchInput.execute(ctx);

        Assert.assertTrue(result);

        @SuppressWarnings("unchecked")
        List<Map<?, Map<?, ?>>> searchResult = (List<Map<?, Map<?, ?>>>) runtimeData.get(new NestedValueMap.Path("test", "hits", "hits"));

        Assert.assertEquals(1, searchResult.size());
        Assert.assertEquals("x", searchResult.get(0).get("_source").get("a"));
    }

    @Test
    public void searchWithTemplateTest() throws Exception {

        SearchInput searchInput = new SearchInput("test", "test", "testsource", "{\"query\": {\"term\" : {\"a\": \"{{data.match}}\"} }}");
        searchInput.compileScripts(watchInitializationService);

        NestedValueMap runtimeData = new NestedValueMap();
        runtimeData.put(new NestedValueMap.Path("match"), "xx");
        WatchExecutionContext ctx = buildWatchExecutionContext(runtimeData);

        boolean result = searchInput.execute(ctx);

        Assert.assertTrue(result);

        @SuppressWarnings("unchecked")
        List<Map<?, Map<?, ?>>> searchResult = (List<Map<?, Map<?, ?>>>) runtimeData.get(new NestedValueMap.Path("test", "hits", "hits"));

        Assert.assertEquals(1, searchResult.size());
        Assert.assertEquals("yy", searchResult.get(0).get("_source").get("b"));
    }

    @Test
    @Ignore("TODO why is this ignored?")
    public void searchWithScheduleDateTest() throws Exception {

        SearchInput searchInput = new SearchInput("test", "test", "testsource",
                    "{\"query\": {\"range\" : {\"date\": {\"gte\": \"{{trigger.scheduled_time}}||-1M\", \"lt\": \"{{trigger.scheduled_time}}\", \"format\": \"strict_date_time\"} } }}");
        searchInput.compileScripts(watchInitializationService);

        NestedValueMap runtimeData = new NestedValueMap();
        runtimeData.put(new NestedValueMap.Path("match"), "xx");
        WatchExecutionContext ctx = buildWatchExecutionContext(runtimeData);

        boolean result = searchInput.execute(ctx);

        Assert.assertTrue(result);

        @SuppressWarnings("unchecked")
        List<Map<?, Map<?, ?>>> searchResult = (List<Map<?, Map<?, ?>>>) runtimeData.get(new NestedValueMap.Path("test", "hits", "hits"));

        Assert.assertEquals(1, searchResult.size());
        Assert.assertEquals("yy", searchResult.get(0).get("_source").get("b"));
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
                        .size(1).as("testsearch").then().index("testsink_" + watchId1).name("testsink").build();
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

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {
            try {
                Client client = cluster.getInternalNodeClient();
                Watch watch = new WatchBuilder(watchId).atMsInterval(300).unthrottled().search("my_remote:ccs_testsource")
                        .query("{\"match_all\" : {} }").as("testsearch").build();
                HttpResponse response = restClient.putJson(watchPath, watch);

                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                response = restClient.get(watchPath);

                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                watch = Watch.parseFromElasticDocument(watchInitializationService, "test", "put_test", response.getBody(),
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
        try (MockWebserviceProvider webserviceProvider = new MockWebserviceProvider("/service")) {
            webserviceProvider.setResponseBody("{\"foo\": \"bar\", \"x\": 55}");
            webserviceProvider.setResponseContentType("text/json");

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webserviceProvider.getUri()), null,
                    null, null, null, null, null, null);

            httpRequestConfig.compileScripts(watchInitializationService);

            HttpInput httpInput = new HttpInput("test", "test", httpRequestConfig, new HttpClientConfig(null, null, null, null));

            NestedValueMap runtimeData = new NestedValueMap();
            WatchExecutionContext ctx = buildWatchExecutionContext(runtimeData);

            boolean result = httpInput.execute(ctx);

            Assert.assertTrue(result);

            Map<?, ?> inputResult = (Map<?, ?>) runtimeData.get("test");

            Assert.assertEquals("bar", inputResult.get("foo"));
            Assert.assertEquals(55, inputResult.get("x"));
        }
    }

    @Test
    public void httpInputShouldSupportTrustStoreTest() throws Exception {
        try (MockWebserviceProvider webserviceProvider = new MockWebserviceProvider("/tls_service", true, false)) {
            //trustManager never throws exception that is each certificate is considered trusted
            Mockito.when(trustManagerRegistry.findTrustManager(TRUSTSTORE_ID)).thenReturn(Optional.of(trustManager));


            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webserviceProvider.getUri()), null,
                null, null, null, null, null, null);
            httpRequestConfig.compileScripts(watchInitializationService);

            TlsConfig tlsConfig = new TlsConfig(trustManagerRegistry, STRICT);
            tlsConfig.setTruststoreId(TRUSTSTORE_ID);
            tlsConfig.init();
            HttpInput httpInput = new HttpInput("test", "test", httpRequestConfig, new HttpClientConfig(null, null, tlsConfig, null));

            NestedValueMap runtimeData = new NestedValueMap();
            WatchExecutionContext ctx = buildWatchExecutionContext(runtimeData);

            boolean result = httpInput.execute(ctx);

            Assert.assertTrue(result);

            //check if trust manager was invoked to validate certificates
            verify(trustManager).checkServerTrusted(any(X509Certificate[].class), anyString(), any(Socket.class));
        }
    }

    @Test
    public void httpInputTestContentTypeHasCharset() throws Exception {
        try (MockWebserviceProvider webserviceProvider = new MockWebserviceProvider("/service")) {
            webserviceProvider.setResponseBody("{\"foo\": \"bar\", \"x\": 55}");
            webserviceProvider.setResponseContentType("text/json; charset=utf-8");

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webserviceProvider.getUri()), null,
                    null, null, null, null, null, null);
            httpRequestConfig.compileScripts(watchInitializationService);

            HttpInput httpInput = new HttpInput("test", "test", httpRequestConfig, new HttpClientConfig(null, null, null, null));

            NestedValueMap runtimeData = new NestedValueMap();
            WatchExecutionContext ctx = buildWatchExecutionContext(runtimeData);

            boolean result = httpInput.execute(ctx);

            Assert.assertTrue(result);

            Map<?, ?> inputResult = (Map<?, ?>) runtimeData.get("test");

            Assert.assertEquals("bar", inputResult.get("foo"));
            Assert.assertEquals(55, inputResult.get("x"));
        }
    }

    @Test
    public void httpInputTextTest() throws Exception {
        try (MockWebserviceProvider webserviceProvider = new MockWebserviceProvider("/service")) {
            String text = "{\"foo\": \"bar\", \"x\": 55}";

            webserviceProvider.setResponseBody(text);
            webserviceProvider.setResponseContentType("text/plain");

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webserviceProvider.getUri()), null,
                    null, null, null, null, null, null);
            httpRequestConfig.compileScripts(watchInitializationService);

            HttpInput httpInput = new HttpInput("test", "test", httpRequestConfig, new HttpClientConfig(null, null, null, null));

            NestedValueMap runtimeData = new NestedValueMap();
            WatchExecutionContext ctx = buildWatchExecutionContext(runtimeData);

            boolean result = httpInput.execute(ctx);

            Assert.assertTrue(result);

            Assert.assertEquals(text, runtimeData.get("test"));
        }
    }

    @Test
    public void httpInputProxyTest() throws Exception {
        try (MockWebserviceProvider webserviceProvider = new MockWebserviceProvider("/service")) {
            webserviceProvider.setResponseBody("{\"foo\": \"bar\", \"x\": 55}");
            webserviceProvider.setResponseContentType("text/json");

            webserviceProvider.acceptOnlyRequestsWithHeader(REQUEST_HEADER_ADDING_FILTER.getHeader());

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webserviceProvider.getUri()), null,
                    null, null, null, null, null, null);
            httpRequestConfig.compileScripts(watchInitializationService);

            HttpInput httpInput = new HttpInput("test", "test", httpRequestConfig, new HttpClientConfig(null, null, null, null));

            NestedValueMap runtimeData = new NestedValueMap();
            WatchExecutionContext ctx = buildWatchExecutionContext(runtimeData);

            try {
                httpInput.execute(ctx);
                Assert.fail();
            } catch (CheckExecutionException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage()
                        .contains(String.format("We are only accepting requests with the '%s' header set to '%s'",
                                REQUEST_HEADER_ADDING_FILTER.getHeader().getName(),
                                REQUEST_HEADER_ADDING_FILTER.getHeader().getValue()))
                );
            }

            HttpProxyConfig httpProxyConfig = HttpProxyConfig.create(
                    new ValidatingDocNode(DocNode.of("proxy", "http://127.0.0.8:" + wireMockProxy.port()), new ValidationErrors()),
                    httpProxyHostRegistry,
                    STRICT
            );
            httpInput = new HttpInput("test", "test", httpRequestConfig,
                    new HttpClientConfig(null, null, null, httpProxyConfig));

            boolean result = httpInput.execute(ctx);

            Assert.assertTrue(result);

            Map<?, ?> inputResult = (Map<?, ?>) runtimeData.get("test");

            Assert.assertEquals("bar", inputResult.get("foo"));
            Assert.assertEquals(55, inputResult.get("x"));
        }
    }

    @Test
    public void httpInput_proxyConfigLoadedFromConfigurationTest() throws Exception {
        try (MockWebserviceProvider webserviceProvider = new MockWebserviceProvider("/service")) {
            String uploadedProxyId = "my-proxy-1";

            webserviceProvider.setResponseBody("{\"foo\": \"bar\", \"x\": 55}");
            webserviceProvider.setResponseContentType("text/json");

            webserviceProvider.acceptOnlyRequestsWithHeader(REQUEST_HEADER_ADDING_FILTER.getHeader());

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webserviceProvider.getUri()), null,
                    null, null, null, null, null, null);
            httpRequestConfig.compileScripts(watchInitializationService);

            HttpInput httpInput = new HttpInput("test", "test", httpRequestConfig, new HttpClientConfig(null, null, null, null));

            NestedValueMap runtimeData = new NestedValueMap();
            WatchExecutionContext ctx = buildWatchExecutionContext(runtimeData);

            try {
                httpInput.execute(ctx);
                Assert.fail();
            } catch (CheckExecutionException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage()
                        .contains(String.format("We are only accepting requests with the '%s' header set to '%s'",
                                REQUEST_HEADER_ADDING_FILTER.getHeader().getName(),
                                REQUEST_HEADER_ADDING_FILTER.getHeader().getValue()))
                );
            }

            when(httpProxyHostRegistry.findHttpProxyHost(uploadedProxyId)).thenReturn(Optional.of(HttpHost.create("http://127.0.0.8:" + wireMockProxy.port())));

            HttpProxyConfig httpProxyConfig = HttpProxyConfig.create(
                    new ValidatingDocNode(DocNode.of("proxy", uploadedProxyId), new ValidationErrors()),
                    httpProxyHostRegistry,
                    STRICT
            );
            httpInput = new HttpInput("test", "test", httpRequestConfig,
                    new HttpClientConfig(null, null, null, httpProxyConfig));

            boolean result = httpInput.execute(ctx);

            Assert.assertTrue(result);

            Map<?, ?> inputResult = (Map<?, ?>) runtimeData.get("test");

            Assert.assertEquals("bar", inputResult.get("foo"));
            Assert.assertEquals(55, inputResult.get("x"));
        }
    }

    @Test(expected = CheckExecutionException.class)
    public void httpWrongContentTypeTest() throws Exception {
        try (MockWebserviceProvider webserviceProvider = new MockWebserviceProvider("/service")) {
            String text = "{\"foo\": \"bar\", \"x\": 55}";

            webserviceProvider.setResponseBody(text);
            webserviceProvider.setResponseContentType("application/vnd.floragunnmegapearls");

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webserviceProvider.getUri()), null,
                    null, null, null, null, null, "application/x-yaml");
            httpRequestConfig.compileScripts(watchInitializationService);

            HttpInput httpInput = new HttpInput("test", "test", httpRequestConfig, new HttpClientConfig(null, null, null, null));

            NestedValueMap runtimeData = new NestedValueMap();
            WatchExecutionContext ctx = buildWatchExecutionContext(runtimeData);

            httpInput.execute(ctx);

            Assert.fail();
        }
    }

    @Test
    public void httpInputTimeoutTest() throws Exception {
        try (MockWebserviceProvider webserviceProvider = new MockWebserviceProvider("/service")) {

            webserviceProvider.setResponseBody("{\"foo\": \"bar\", \"x\": 55}");
            webserviceProvider.setResponseContentType("text/json");
            webserviceProvider.setResponseDelayMs(3330);

            HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webserviceProvider.getUri()), null,
                    null, null, null, null, null, null);
            httpRequestConfig.compileScripts(watchInitializationService);

            HttpInput httpInput = new HttpInput("test", "test", httpRequestConfig, new HttpClientConfig(1, 1, null, null));

            NestedValueMap runtimeData = new NestedValueMap();
            WatchExecutionContext ctx = buildWatchExecutionContext(runtimeData);

            httpInput.execute(ctx);

            Assert.fail();
        } catch (CheckExecutionException e) {
            Assert.assertTrue(e.toString(), e.getCause().toString().contains("Read timed out"));
        }
    }

    @Test
    public void testConditionTrue() throws Exception {

        Condition scriptCondition = new Condition(null, "data.x.hits.total > 5", "painless", Collections.emptyMap());
        scriptCondition.compileScripts(watchInitializationService);

        NestedValueMap runtimeData = new NestedValueMap();
        runtimeData.put(new NestedValueMap.Path("x", "hits", "total"), 7);
        WatchExecutionContext ctx = buildWatchExecutionContext(runtimeData);

        boolean result = scriptCondition.execute(ctx);

        Assert.assertTrue(result);
    }

    @Test
    public void testConditionFalse() throws Exception {

        Condition scriptCondition = new Condition(null, "data.x.hits.total > 510", "painless", Collections.emptyMap());
        scriptCondition.compileScripts(watchInitializationService);

        NestedValueMap runtimeData = new NestedValueMap();
        runtimeData.put(new NestedValueMap.Path("x", "hits", "total"), 7);
        WatchExecutionContext ctx = buildWatchExecutionContext(runtimeData);

        boolean result = scriptCondition.execute(ctx);

        Assert.assertFalse(result);
    }

    @Test(expected = CheckExecutionException.class)
    public void testInvalidCondition() throws Exception {

        Condition scriptCondition = new Condition(null, "data.x.hits.hits.length > data.constants.threshold", "painless", Collections.emptyMap());
        scriptCondition.compileScripts(watchInitializationService);

        NestedValueMap runtimeData = new NestedValueMap();
        runtimeData.put(new NestedValueMap.Path("x", "hits", "total"), 7);
        runtimeData.put(new NestedValueMap.Path("constants", "threshold"), 5);

        WatchExecutionContext ctx = buildWatchExecutionContext(runtimeData);

        scriptCondition.execute(ctx);

        Assert.fail();
    }

    @Test
    public void testCalc() throws Exception {

        Calc calc = new Calc(null, "data.x.y = 5", "painless", Collections.emptyMap());
        calc.compileScripts(watchInitializationService);

        NestedValueMap runtimeData = new NestedValueMap();
        runtimeData.put(new NestedValueMap.Path("x", "hits", "total"), 7);
        WatchExecutionContext ctx = buildWatchExecutionContext(runtimeData);

        boolean result = calc.execute(ctx);

        Assert.assertTrue(result);

        Assert.assertEquals(5, runtimeData.get(new NestedValueMap.Path("x", "y")));
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

                    try {
                        log.info("Did not find watch log for " + watchName + " after " + (System.currentTimeMillis() - start) + " ms\n\n"
                                + searchResponse.getHits());
                    } finally {
                        searchResponse.decRef();
                    }

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
            try {
                if (searchResponse.getHits().getHits().length == 0) {
                    return null;
                }

                SearchHit searchHit = searchResponse.getHits().getHits()[0];

                return WatchLog.parse(searchHit.getId(), searchHit.getSourceAsString());
            } finally {
                searchResponse.decRef();
            }
        } catch (org.elasticsearch.index.IndexNotFoundException | SearchPhaseExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error in getMostRecenWatchLog(" + tenantName + ", " + watchName + ")", e);
        }
    }

    private WatchExecutionContext buildWatchExecutionContext(NestedValueMap runtimeData) {
        return new WatchExecutionContext(cluster.getInternalNodeClient(), scriptService, xContentRegistry, accountRegistry,
                ExecutionEnvironment.SCHEDULED, ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData),
                trustManagerRegistry, clusterService, featureService);
    }
}
