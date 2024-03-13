/*
 * Copyright 2020-2022 floragunn GmbH
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

package com.floragunn.signals;

import static com.floragunn.searchguard.test.TestSgConfig.Role.ALL_ACCESS;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.signals.watch.common.ValidationLevel.STRICT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

import java.net.InetAddress;
import java.net.URI;
import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.floragunn.searchsupport.proxy.wiremock.WireMockRequestHeaderAddingFilter;
import com.floragunn.signals.proxy.service.HttpProxyHostRegistry;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
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
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.quartz.TimeOfDay;

import com.floragunn.codova.config.temporal.DurationExpression;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.network.SocketUtils;
import com.floragunn.searchsupport.junit.LoggingTestWatcher;
import com.floragunn.signals.truststore.service.TrustManagerRegistry;
import com.floragunn.signals.util.WatchLogSearch;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;
import com.floragunn.signals.watch.action.handlers.email.EmailAccount;
import com.floragunn.signals.watch.action.handlers.email.EmailAction;
import com.floragunn.signals.watch.action.handlers.email.EmailAction.Attachment;
import com.floragunn.signals.watch.action.handlers.slack.SlackAccount;
import com.floragunn.signals.watch.action.handlers.slack.SlackActionConf;
import com.floragunn.signals.watch.common.HttpRequestConfig;
import com.floragunn.signals.watch.common.throttle.ThrottlePeriodParser;
import com.floragunn.signals.watch.common.throttle.ValidatingThrottlePeriodParser;
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
    public static final String USERNAME_UHURA = "uhura";
    public static final String UPLOADED_TRUSTSTORE_ID = "uploaded-truststore-id";

    private static ScriptService scriptService;
    private static ThrottlePeriodParser throttlePeriodParser;

    private static final WireMockRequestHeaderAddingFilter REQUEST_HEADER_ADDING_FILTER = new WireMockRequestHeaderAddingFilter("Proxy", "wire-mock");

    @Rule
    public WireMockRule wireMockProxy = new WireMockRule(WireMockConfiguration.options()
            .bindAddress("127.0.0.8")
            .enableBrowserProxying(true)
            .proxyPassThrough(true)
            .dynamicPort()
            .extensions(REQUEST_HEADER_ADDING_FILTER));

    @Rule
    public LoggingTestWatcher loggingTestWatcher = new LoggingTestWatcher();

    private static User USER_CERTIFICATE = new User("certificate-user").roles(ALL_ACCESS);

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("sg_config/signals")
            .nodeSettings("signals.enabled", true, "signals.index_names.log", "signals__main_log", "signals.enterprise.enabled", false,
                    "searchguard.diagnosis.action_stack.enabled", true, "signals.watch_log.refresh_policy", "immediate",
                    "signals.watch_log.sync_indexing", true).user(USER_CERTIFICATE)
            .enableModule(SignalsModule.class).waitForComponents("signals").enterpriseModulesEnabled().embedded().build();

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
        throttlePeriodParser = new ValidatingThrottlePeriodParser(cluster.getInjectable(Signals.class).getSignalsSettings());
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
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch")).actionGet();

            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.get(watchPath);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            WatchInitializationService initService = new WatchInitializationService(null, scriptService,
                Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), throttlePeriodParser, STRICT);
            watch = Watch.parseFromElasticDocument(initService, "test", "put_test", response.getBody(), -1);

            awaitMinCountOfDocuments(client, "testsink_put_watch", 1);

        }
    }

    @Test
    public void testPutWatchWithInvalidTrigger() throws Exception {
        String tenant = "_main";
        String watchId = "put_test_invalid_trigger";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String watchJson = "{ \"trigger\": {\"schedule\": {\"daily\": {\"at\": \"\"} } }, \"checks\":[], \"actions\":[], \"active\":false, \"log_runtime_data\":false }";

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            HttpResponse response = restClient.putJson(watchPath, watchJson);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

            Assert.assertTrue(response.getBody(), response.getBody().contains("Invalid value"));
            Assert.assertTrue(response.getBody(), response.getBody().contains("Time of day"));
        }
    }

    @Test
    public void testWatchStateAfterPutWatch() throws Exception {
        String tenant = "_main";
        String watchId = "put_state_after_put_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
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

    @Test
    public void testPutWatchWithSeverity() throws Exception {
        String tenant = "_main";
        String watchId = "put_test_severity";
        String testSink = "testsink_" + watchId;
        String testSinkResolve = "testsink_resolve_" + watchId;
        String testSource = "testsource_" + watchId;
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
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

            WatchInitializationService initService = new WatchInitializationService(null, scriptService,
                Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), throttlePeriodParser, STRICT);
            watch = Watch.parseFromElasticDocument(initService, "test", "put_test", response.getBody(), -1);

            awaitMinCountOfDocuments(client, testSink, 1);

            Assert.assertEquals(getDocs(client, testSinkResolve), 0, getCountOfDocuments(client, testSinkResolve));

            client.delete(new DeleteRequest(testSource).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("1")).actionGet();

            awaitMinCountOfDocuments(client, testSinkResolve, 1);

            Thread.sleep(2000);

            Assert.assertEquals(getDocs(client, testSinkResolve), 1, getCountOfDocuments(client, testSinkResolve));

        }
    }

    @Test
    public void testPutWatchWithSeverityValidation() throws Exception {
        String tenant = "_main";
        String watchId = "put_test_severity_validation";
        String testSink = "testsink_" + watchId;
        String testSinkResolve = "testsink_resolve_" + watchId;
        String testSource = "testsource_" + watchId;
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
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
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
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

            WatchInitializationService initService = new WatchInitializationService(null, scriptService,
                Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), throttlePeriodParser, STRICT);
            watch = Watch.parseFromElasticDocument(initService, "test", "put_test", response.getBody(), -1);

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
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch_with_dash")).actionGet();

            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch_with_dash").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.get(watchPath);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            WatchInitializationService initService = new WatchInitializationService(null, scriptService,
                Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), throttlePeriodParser, STRICT);
            watch = Watch.parseFromElasticDocument(initService, "test", "put_test", response.getBody(), -1);

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
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            Watch watch = new WatchBuilder(watchId).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch_with_dash").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.get(watchPath);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            WatchInitializationService initService = new WatchInitializationService(null, scriptService,
                Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), throttlePeriodParser, STRICT);
            watch = Watch.parseFromElasticDocument(initService, "test", "put_test", response.getBody(), -1);

            Assert.assertTrue(response.getBody(), watch.getSchedule().getTriggers().isEmpty());
        }
    }

    @Test
    public void testAuthTokenFilter() throws Exception {
        String tenant = "_main";
        String watchId = "filter";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.get(watchPath);

            Assert.assertFalse(response.getBody(), response.getBody().contains("auth_token"));

            WatchInitializationService initService = new WatchInitializationService(null, scriptService,
                Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), throttlePeriodParser, STRICT);
            watch = Watch.parseFromElasticDocument(initService, "test", watchId, response.getBody(), -1);

            Assert.assertNull(response.getBody(), watch.getAuthToken());
        }
    }

    @Test
    public void testPutInvalidWatch() throws Exception {
        String tenant = "_main";
        String watchId = "put_invalid_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA,
            USERNAME_UHURA)) {
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

            DocNode parsedResponse = response.getBodyAsDocNode();
            
            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, parsedResponse.get("status"));
            Assert.assertEquals(response.getBody(), "Invalid value",
                    parsedResponse.getAsNode("detail").getAsListOfNodes("checks[testsearch].type").get(0).get("error"));
            Assert.assertEquals(response.getBody(), "searchx",
                    parsedResponse.getAsNode("detail").getAsListOfNodes("checks[testsearch].type").get(0).get("value"));
            Assert.assertEquals(response.getBody(), "cannot resolve symbol [x]",
                    parsedResponse.getAsNode("detail").getAsListOfNodes("checks[].source").get(0).get("error"));
            Assert.assertTrue(response.getBody(), parsedResponse.getAsNode("detail").getAsListOfNodes("trigger.schedule.cron.0").get(0).get("error")
                    .toString().contains("Invalid cron expression"));
            Assert.assertTrue(response.getBody(), parsedResponse.getAsNode("detail").getAsListOfNodes("trigger.schedule.x").get(0).get("error")
                    .toString().contains("Unsupported attribute"));
            Assert.assertEquals(response.getBody(), "Required attribute is missing",
                    parsedResponse.getAsNode("detail").getAsListOfNodes("actions[].name").get(0).get("error"));
            Assert.assertEquals(response.getBody(), "unexpected end of script.",
                    parsedResponse.getAsNode("detail").getAsListOfNodes("checks[testcalc].source").get(0).get("error"));
            Assert.assertEquals(response.getBody(), "Unsupported attribute",
                    parsedResponse.getAsNode("detail").getAsListOfNodes("horst").get(0).get("error"));


        }
    }

    @Test
    public void testPutInvalidWatchJsonSyntaxError() throws Exception {
        String tenant = "_main";
        String watchId = "put_invalid_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA)) {
            String watchJson = "{\"trigger\":{";

            HttpResponse response = restClient.putJson(watchPath, watchJson);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

            DocNode parsedResponse = response.getBodyAsDocNode();

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, parsedResponse.get("status"));
            Assert.assertTrue(response.getBody(),
                    parsedResponse.getAsNode("detail").getAsListOfNodes("_").get(0).get("error").toString().contains("Invalid JSON document"));
        }
    }

    @Test
    public void testPutInvalidWatch_invalidHttpRequestBodyConfig_bothBodyAndJsonBodyFromAreSet() throws Exception {
        String tenant = "_main";
        String watchId = "put_invalid_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA,
            USERNAME_UHURA)) {
            DocNode watch = DocNode.of("actions", Collections.singletonList(
                    DocNode.of("type", "webhook", "name", "webhook_with_two_request_bodies",
                            "request", DocNode.of("method", "POST", "url", "https://my.test.web.hook/endpoint", "body", "first_body", "json_body_from", "second.body")
                    )
            ));
            System.out.println(watch.toJsonString());
            HttpResponse response = restClient.putJson(watchPath, watch.toJsonString());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

            DocNode parsedResponse = DocNode.parse(Format.getByContentType(response.getContentType())).from(response.getBody());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertEquals(response.getBody(), 2, parsedResponse.getAsNode("detail").size());
            Assert.assertEquals(response.getBody(),
            "Both body and json_body_from are set. These are mutually exclusive.",
                    parsedResponse.findSingleNodeByJsonPath("detail['actions[webhook_with_two_request_bodies].request.body'][0]").getAsString("error")
            );
            Assert.assertEquals(response.getBody(),
            "Both body and json_body_from are set. These are mutually exclusive.",
                    parsedResponse.findSingleNodeByJsonPath("detail['actions[webhook_with_two_request_bodies].request.json_body_from'][0]").getAsString("error")
            );
        }
    }

    @Test
    public void testPutInvalidWatch_httpRequestContentTypeAppXml_whenJsonBodyFromIsSet() throws Exception {
        String tenant = "_main";
        String watchId = "put_invalid_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA,
            USERNAME_UHURA)) {
            DocNode watch = DocNode.of("actions", Collections.singletonList(
                    DocNode.of("type", "webhook", "name", "json_body_from_and_wrong_content_type",
                            "request", DocNode.of("method", "POST", "url", "https://my.test.web.hook/endpoint", "json_body_from", "data.test", "headers", DocNode.of("Content-Type", "application/xml"))
                    )
            ));

            HttpResponse response = restClient.putJson(watchPath, watch.toJsonString());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

            DocNode parsedResponse = DocNode.parse(Format.getByContentType(response.getContentType())).from(response.getBody());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertEquals(response.getBody(), 1, parsedResponse.getAsNode("detail").size());
            Assert.assertEquals(response.getBody(),
            "Content type header should be set to application/json when json_body_from is used.",
                    parsedResponse.findSingleNodeByJsonPath("detail['actions[json_body_from_and_wrong_content_type].request.headers.Content-Type'][0]").getAsString("error")
            );
        }
    }

    @Test
    public void testPutWatch_bodyFromRuntimeDataPath_contentTypeShouldDefaultToAppJson() throws Exception {

        String tenant = "_main";
        String watchId = "put_watch_with_body_from_runtime_data_default_content_type_header";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
             MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook");
             GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {

            try {
                Watch watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?")
                        .put("{\"test\": \"test\"}").as("teststatic")
                        .then()
                        .postWebhook(webhookProvider.getUri())
                        .jsonBodyFrom("data.teststatic.test")
                        .name("webhook_with_default_content_type").build();
                HttpResponse response = restClient.putJson(watchPath, watch.toJson());

                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                Thread.sleep(3000);
                Assert.assertTrue(webhookProvider.getRequestCount() > 0);
                Header header = webhookProvider.getLastRequestHeader(HttpHeaders.CONTENT_TYPE);
                Assert.assertNotNull("content type header should be present", header);
                Assert.assertEquals("content type header should contain " + ContentType.APPLICATION_JSON.getMimeType(), ContentType.APPLICATION_JSON.getMimeType(), header.getValue());
                Assert.assertEquals("webhook request body should match", "\"test\"", webhookProvider.getLastRequestBody());
            } finally {
                restClient.delete(watchPath);
            }
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
            Assert.assertTrue(watchLog.toString(), watchLog.getStatus().getDetail().contains("Insufficient permissions"));
        }
    }

    @Test
    public void testHttpWhitelist() throws Exception {

        String tenant = "_main";
        String watchId = "http_whitelist";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook");
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA)) {
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
    public void testWebhookTruststore() throws Exception {

        String tenant = "_main";
        String watchId = "webhook-with-truststore";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook", true, false);
            GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            webhookProvider.uploadMockServerCertificateAsTruststore(cluster, USER_CERTIFICATE, UPLOADED_TRUSTSTORE_ID);
            Watch watch = new WatchBuilder("tls-webhook-test").atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then()
                .postWebhook(webhookProvider.getUri()).truststoreId(UPLOADED_TRUSTSTORE_ID).throttledFor("0")
                .name("testhook").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Thread.sleep(600);

            Assert.assertTrue(webhookProvider.getRequestCount() > 0);
        }
    }

    @Test
    public void testWebhookTruststoreFailureWithoutCorrectTruststore() throws Exception {

        String tenant = "_main";
        String watchId = "webhook-missing-truststore-configuration";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
            MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook", true, false);
            GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            client.admin().indices().create(new CreateIndexRequest("testsink-" + watchId)).actionGet();

            Watch watch = new WatchBuilder("tls-webhook-test").atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then()
                .postWebhook(webhookProvider.getUri()).throttledFor("0")
                .name("testhook").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Thread.sleep(600);

            // Request are not sent because webhookProvider provided uses untrusted certificate so that SearchGuard is not able to
            // establish connection with webhookProvider
            Assert.assertTrue(webhookProvider.getRequestCount() == 0);
        }
    }

    @Test
    public void shouldNotCreateWatchWhenWatchContainsIncorrectTruststoreId() throws Exception {

        String tenant = "_main";
        String watchId = "webhook-incorrect-truststore-id";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
            MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook", true, false);
            GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            client.admin().indices().create(new CreateIndexRequest("testsink-" + watchId)).actionGet();

            Watch watch = new WatchBuilder("tls-webhook-test").atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then()
                .postWebhook(webhookProvider.getUri()).truststoreId("not-existing-truststore-id").throttledFor("0")
                .name("testhook").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

        }
    }

    @Test
    public void testHttpDefaultProxy() throws Exception {

        String tenant = "_main";
        String watchId = "http_default_proxy";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook");
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA)) {
            try {

                webhookProvider.acceptOnlyRequestsWithHeader(REQUEST_HEADER_ADDING_FILTER.getHeader());

                Watch watch = new WatchBuilder("put_test").atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                        .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().postWebhook(webhookProvider.getUri()).throttledFor("0")
                        .name("testhook").build();
                HttpResponse response = restClient.putJson(watchPath, watch.toJson());

                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                Thread.sleep(600);

                Assert.assertEquals(0, webhookProvider.getRequestCount());

                response = restClient.putJson("/_signals/settings/http.proxy", "\"http://127.0.0.8:" + wireMockProxy.port() + "\"");

                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                response = restClient.get("/_signals/settings/http.proxy");

                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
                Assert.assertEquals(response.getBody(), "\"http://127.0.0.8:" + wireMockProxy.port() + "\"");

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
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA)) {
            try {
                webhookProvider.acceptOnlyRequestsWithHeader(REQUEST_HEADER_ADDING_FILTER.getHeader());

                Watch watch = new WatchBuilder("put_test").atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                        .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().postWebhook(webhookProvider.getUri()).throttledFor("0")
                        .name("testhook").build();
                HttpResponse response = restClient.putJson(watchPath, watch.toJson());

                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                Thread.sleep(600);

                Assert.assertEquals(0, webhookProvider.getRequestCount());

                watch = new WatchBuilder("put_test").atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                        .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().postWebhook(webhookProvider.getUri())
                        .proxy("http://127.0.0.8:" + wireMockProxy.port()).throttledFor("0").name("testhook").build();
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
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA)) {
            try {
                HttpResponse response = restClient.putJson("/_signals/settings/http.proxy", "\"http://127.0.0.8:" + wireMockProxy.port() + "\"");
                Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

                Thread.sleep(200);

                Watch watch = new WatchBuilder("put_test").atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                        .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().postWebhook(webhookProvider.getUri()).proxy("none")
                        .throttledFor("0").name("testhook").build();
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

    @Test
    public void testWatchWithProxyLoadedFromConfiguration() throws Exception {

        String tenant = "_main";
        String watchId = "http_proxy_loaded_from_config";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String proxyId = "proxy-1";
        String proxyPath = "/_signals/proxies/" + proxyId;

        try (MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook");
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            webhookProvider.acceptOnlyRequestsWithHeader(REQUEST_HEADER_ADDING_FILTER.getHeader());

            HttpResponse response = restClient.putJson(proxyPath, DocNode.of("name", "proxy", "uri", "http://127.0.0.8:" + wireMockProxy.port()));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Watch watch = new WatchBuilder("put_test").atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().postWebhook(webhookProvider.getUri()).throttledFor("0")
                    .name("testhook").build();
            response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Thread.sleep(600);

            Assert.assertEquals(0, webhookProvider.getRequestCount());

            watch = new WatchBuilder("put_test").atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().postWebhook(webhookProvider.getUri())
                    .proxy(proxyId).throttledFor("0").name("testhook").build();
            response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Thread.sleep(600);

            Assert.assertTrue(webhookProvider.getRequestCount() > 0);
        }
    }

    @Test
    public void testWatchWithProxyLoadedFromConfiguration_givenProxyDoesNotExist() throws Exception {

        String tenant = "_main";
        String watchId = "http_proxy_loaded_from_config";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook");
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {

            Watch watch = new WatchBuilder("put_test").atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().postWebhook(webhookProvider.getUri())
                    .proxy("missing-proxy-id").throttledFor("0").name("testhook").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertEquals("Http proxy 'missing-proxy-id' not found.",
                    response.getBodyAsDocNode().getAsNode("detail")
                            .getAsListOfNodes("actions[testhook].proxy").get(0).get("error")
            );

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
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {

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
            Assert.assertTrue(actionLog.toString(), actionLog.getStatus().getDetail().contains("Insufficient permissions"));

        }
    }

    @Test
    public void testPutWatchWithTenant() throws Exception {

        String tenant = "test1";
        String watchId = "put_watch_with_tenant";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String watchPathWithWrongTenant = "/_signals/watch/_main/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch_with_tenant")).actionGet();

            Watch watch = new WatchBuilder("put_test").cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch_with_tenant").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.get(watchPath);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            WatchInitializationService initService = new WatchInitializationService(null, scriptService,
                Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), throttlePeriodParser, STRICT);
            watch = Watch.parseFromElasticDocument(initService, "test", "put_test", response.getBody(), -1);

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

            WatchInitializationService initService = new WatchInitializationService(null, scriptService,
                Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), throttlePeriodParser, STRICT);
            watch = Watch.parseFromElasticDocument(initService, "test", "put_test", response.getBody(), -1);

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

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA,
            USERNAME_UHURA)) {
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

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA,
            USERNAME_UHURA)) {

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
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {

            Watch watch = new WatchBuilder(watchId).cronTrigger("0 0 */1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.postJson(watchPath + "/_execute", "{}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
        }
    }

    @Test
    public void testExecuteWatchByIdWhichUsesUploadedTruststore() throws Exception {
        String tenant = "_main";
        String watchId = "tls_execution_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources();
            MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/tls_endpoint", true, false)) {
            webhookProvider.uploadMockServerCertificateAsTruststore(cluster, USER_CERTIFICATE, UPLOADED_TRUSTSTORE_ID);

            Watch watch = new WatchBuilder(watchId).cronTrigger("0 0 */1 * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then()
                .postWebhook(webhookProvider.getUri()).truststoreId(UPLOADED_TRUSTSTORE_ID).throttledFor("0").name("send-http-request")
                .build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.postJson(watchPath + "/_execute", "{}");

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("status.code", "ACTION_EXECUTED"));
            assertThat(webhookProvider.getRequestCount(), greaterThan(0));
        }
    }

    @Test
    public void testExecuteAnonymousWatchWithGoto() throws Exception {

        String testSink = "testsink_anon_watch_with_goto";

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA,
            USERNAME_UHURA)) {

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
    public void testExecuteWatchByIdWhichUsesStoredProxyConfig() throws Exception {
        String tenant = "_main";
        String watchId = "stored_proxy_exec_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String proxyId = "stored-proxy";
        String proxyPath = "/_signals/proxies/" + proxyId;

        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources();
             MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/tls_endpoint")) {

            webhookProvider.acceptOnlyRequestsWithHeader(REQUEST_HEADER_ADDING_FILTER.getHeader());

            HttpResponse response = restClient.putJson(proxyPath, DocNode.of("name", "stored-proxy", "uri", "http://127.0.0.8:" + wireMockProxy.port()));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Watch watch = new WatchBuilder("test_with_stored_proxy").cronTrigger("0 0 */1 * * ?")
                    .then().postWebhook(webhookProvider.getUri()).proxy(proxyId).name("webhook")
                    .build();

            response = restClient.putJson(watchPath, watch.toJson());
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.postJson(watchPath + "/_execute", "{}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("status.code", "ACTION_EXECUTED"));
            assertThat(webhookProvider.getRequestCount(), greaterThan(0));
        }
    }

    @Test
    public void testExecuteAnonymousWatchWithInput() throws Exception {

        String testSink = "testsink_anon_watch_with_input";

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA,
            USERNAME_UHURA)) {

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

    @Test
    public void testExecuteAnonymousWatchWithShowAllRuntimeAttributes() throws Exception {

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA,
            USERNAME_UHURA)) {

            Watch watch = new WatchBuilder("execution_test_anon").cronTrigger("*/2 * * * * ?").search("testsource").query("{\"match_all\" : {} }")
                    .as("testsearch").put("{\"bla\": {\"blub\": 42}}").as("teststatic").consider("data.testsearch.hits.total.value").greaterOrEqual(1)
                    .as(SeverityLevel.ERROR).when(SeverityLevel.ERROR).index("testsink").name("testsink").build();

            HttpResponse response = restClient.postJson("/_signals/watch/_main/_execute",
                    "{\"watch\": " + watch.toJson() + ", \"show_all_runtime_attributes\": true}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            DocNode responseJson = response.getBodyAsDocNode();
            
            Assert.assertEquals(response.getBody(), "error", responseJson.get("runtime_attributes", "severity", "level"));
            Assert.assertFalse(response.getBody(), responseJson.get("runtime_attributes", "trigger") == null);
            Assert.assertTrue(response.getBody(), responseJson.get("runtime_attributes", "trigger", "triggered_time") == null);
            Assert.assertEquals(response.getBody(), "42", responseJson.get("runtime_attributes", "data", "teststatic", "bla", "blub").toString());


        }
    }

    @Test
    public void testActivateWatchAuth() throws Exception {
        String tenant = "_main";
        String watchId = "activate_auth_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {

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
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {

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

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA,
            USERNAME_UHURA)) {

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
            try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA)) {
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

        try (Client client = cluster.getInternalNodeClient(); GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA,
            USERNAME_UHURA)) {

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
            try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA)) {
                restClient.put("/_signals/admin/_active");
                restClient.delete(watchPath);
            }
        }
    }

    @Test
    public void testAckWatch() throws Exception {
        String tenant = "_main";
        String watchId = "ack_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
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

            DocNode statusDoc = response.getBodyAsDocNode();
            Assert.assertEquals(response.getBody(), "uhura", statusDoc.get("actions", "testaction", "acked", "by").toString());

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

            statusDoc = response.getBodyAsDocNode();
            Assert.assertFalse(response.getBody(), statusDoc.getAsNode("actions").getAsNode("testaction").hasNonNull("acked"));

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

    @Test
    public void testAckAndGetWatchWithSingleAction() throws Exception {
        String tenant = "_main";
        String watchId = "ack_and_get_test_watch_with_single_action";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String watchedIndex = "source_index_for_watch_" + watchId;
        String sinkIndex = "sink_index_" + watchId;

        try (Client client = cluster.getInternalNodeClient();
            GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            client.admin().indices().create(new CreateIndexRequest(watchedIndex)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(sinkIndex)).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search(watchedIndex).query("{\"match_all\" : {} }").as("testsearch")
                .checkCondition("data.testsearch.hits.hits.length > 0").then()
                .index(sinkIndex).refreshPolicy(RefreshPolicy.IMMEDIATE).throttledFor("0").name("testaction")
                .build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Thread.sleep(220);

            client.index(new IndexRequest(watchedIndex).id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "key1",
                "val1", "key2", "val2")).actionGet();

            awaitMinCountOfDocuments(client, sinkIndex, 1);

            response = restClient.put(watchPath + "/_ack_and_get/testaction");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            DocNode ackDoc = response.getBodyAsDocNode();
            Assert.assertEquals("testaction", ackDoc.getAsListOfNodes("acked").get(0).get("action_id"));
            Assert.assertEquals(USERNAME_UHURA, ackDoc.getAsListOfNodes("acked").get(0).get("by_user"));
        }
    }

    @Test
    public void testAckAndGetShouldReturnErrorResponseWhenActionDoesNotExists() throws Exception {
        String tenant = "_main";
        String watchId = "ack_and_get_test_watch_with_non_existing_action";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String watchedIndex = "source_index_for_watch_" + watchId;
        String sinkIndex = "sink_index_" + watchId;

        try (Client client = cluster.getInternalNodeClient();
            GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            client.admin().indices().create(new CreateIndexRequest(watchedIndex)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(sinkIndex)).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search(watchedIndex).query("{\"match_all\" : {} }").as("testsearch")
                .checkCondition("data.testsearch.hits.hits.length > 0").then()
                .index(sinkIndex).refreshPolicy(RefreshPolicy.IMMEDIATE).throttledFor("0").name("testaction")
                .build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Thread.sleep(220);

            client.index(new IndexRequest(watchedIndex).id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "key1",
                "val1", "key2", "val2")).actionGet();

            awaitMinCountOfDocuments(client, sinkIndex, 1);

            response = restClient.put(watchPath + "/_ack_and_get/non_existing_action_id");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_NOT_FOUND, response.getStatusCode());
        }
    }

    @Test
    public void testAckAndGetForOneActionForWatchWithDoubleAction() throws Exception {
        String tenant = "_main";
        String watchId = "ack_and_get_for_one_action_watch_with_double_action_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String watchedIndex = "source_index_for_watch_" + watchId;
        String sinkIndex = "sink_index_" + watchId;
        String additionalSinkIndex = "additional_sink_index_" + watchId;

        try (Client client = cluster.getInternalNodeClient();
            GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            client.admin().indices().create(new CreateIndexRequest(watchedIndex)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(sinkIndex)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(additionalSinkIndex)).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search(watchedIndex).query("{\"match_all\" : {} }").as("testsearch")
                .checkCondition("data.testsearch.hits.hits.length > 0").then()
                .index(sinkIndex).refreshPolicy(RefreshPolicy.IMMEDIATE).throttledFor("0").name("testactionone")
                .and().index(additionalSinkIndex).refreshPolicy(RefreshPolicy.IMMEDIATE).throttledFor("0").name("testactiontwo")
                .build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Thread.sleep(220);

            response = restClient.put(watchPath + "/_ack/testactionone");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_PRECONDITION_FAILED, response.getStatusCode());

            client.index(new IndexRequest(watchedIndex).id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "key1",
                "val1", "key2", "val2")).actionGet();

            awaitMinCountOfDocuments(client, sinkIndex, 1);
            awaitMinCountOfDocuments(client, additionalSinkIndex, 1);

            response = restClient.put(watchPath + "/_ack_and_get/testactionone");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            DocNode ackDoc = response.getBodyAsDocNode();
            Assert.assertEquals("testactionone", ackDoc.getAsListOfNodes("acked").get(0).get("action_id"));
            Assert.assertEquals(USERNAME_UHURA, ackDoc.getAsListOfNodes("acked").get(0).get("by_user"));
            
            assertThatActionIsNotAcked(restClient, watchPath, "testactiontwo");
        }
    }

    @Test
    public void testAckAndGetForInactiveWatchShouldReturnErrorResponse() throws Exception {
        String tenant = "_main";
        String watchId = "ack_and_get_for_inactive_watch_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String watchedIndex = "source_index_for_watch_" + watchId;
        String sinkIndex = "sink_index_" + watchId;
        String additionalSinkIndex = "additional_sink_index_" + watchId;

        try (
            Client client = cluster.getInternalNodeClient();
            GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            client.admin().indices().create(new CreateIndexRequest(watchedIndex)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(sinkIndex)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(additionalSinkIndex)).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search(watchedIndex).query("{\"match_all\" : {} }")
                    .as("testsearch").checkCondition("data.testsearch.hits.hits.length > 0").then()
                    .index(sinkIndex).refreshPolicy(RefreshPolicy.IMMEDIATE).throttledFor("0")
                    .name("testactionone").and().index(additionalSinkIndex).refreshPolicy(RefreshPolicy.IMMEDIATE).throttledFor("0")
                    .name("testactiontwo").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Thread.sleep(220);

            response = restClient.put(watchPath + "/_ack_and_get/testactionone");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_PRECONDITION_FAILED, response.getStatusCode());
        }
    }

    private static void assertThatActionIsNotAcked(GenericRestClient restClient, String watchPath, String actionName) throws Exception {
        HttpResponse response;
        Thread.sleep(500);
        response = restClient.get(watchPath + "/_state");
        Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
        DocNode statusDoc = response.getBodyAsDocNode();
        Assert.assertEquals("ACTION_EXECUTED", statusDoc.get("actions", actionName, "last_status", "code"));
        Assert.assertTrue(statusDoc.getAsNode("actions", actionName, "acked").isNull());
    }

    @Test
    public void testAckAndGetBothActionForWatchWithTwoAction() throws Exception {
        String tenant = "_main";
        String watchId = "ack_and_get_both_action_for_watch_with_two_actions_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String watchedIndex = "source_index_for_watch_" + watchId;
        String sinkIndex = "sink_index_" + watchId;
        String additionalSinkIndex = "additional_sink_index_" + watchId;

        try (Client client = cluster.getInternalNodeClient();
            GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            client.admin().indices().create(new CreateIndexRequest(watchedIndex)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(sinkIndex)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(additionalSinkIndex)).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search(watchedIndex).query("{\"match_all\" : {} }").as("testsearch")
                .checkCondition("data.testsearch.hits.hits.length > 0").then()
                .index(sinkIndex).refreshPolicy(RefreshPolicy.IMMEDIATE).throttledFor("0").name("testactionone")
                .and().index(additionalSinkIndex).refreshPolicy(RefreshPolicy.IMMEDIATE).throttledFor("0").name("testactiontwo")
                .build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Thread.sleep(220);

            client.index(new IndexRequest(watchedIndex).id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "key1",
                "val1", "key2", "val2")).actionGet();

            awaitMinCountOfDocuments(client, sinkIndex, 1);
            awaitMinCountOfDocuments(client, additionalSinkIndex, 1);

            response = restClient.put(watchPath + "/_ack_and_get");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            DocNode ackDoc = response.getBodyAsDocNode();           
            Assert.assertEquals(2, ackDoc.getAsListOfNodes("acked").size());
            Assert.assertEquals(ImmutableSet.of("testactionone", "testactiontwo"), ImmutableSet.of(ackDoc.findByJsonPath("acked[*].action_id")));
        }
    }

    @Test
    public void testDeAckAndGetFirstActionForWatchWithTwoAction() throws Exception {
        String tenant = "_main";
        String watchId = "deack_and_get_first_action_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String watchedIndex = "source_index_for_watch_" + watchId;
        String sinkIndex = "sink_index_" + watchId;
        String additionalSinkIndex = "additional_sink_index_" + watchId;

        try (Client client = cluster.getInternalNodeClient();
            GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            client.admin().indices().create(new CreateIndexRequest(watchedIndex)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(sinkIndex)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(additionalSinkIndex)).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search(watchedIndex).query("{\"match_all\" : {} }").as("testsearch")
                .checkCondition("data.testsearch.hits.hits.length > 0").then()
                .index(sinkIndex).refreshPolicy(RefreshPolicy.IMMEDIATE).throttledFor("0").name("testactionone")
                .and().index(additionalSinkIndex).refreshPolicy(RefreshPolicy.IMMEDIATE).throttledFor("0").name("testactiontwo")
                .build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Thread.sleep(220);

            client.index(new IndexRequest(watchedIndex).id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "key1",
                "val1", "key2", "val2")).actionGet();

            awaitMinCountOfDocuments(client, sinkIndex, 1);
            awaitMinCountOfDocuments(client, additionalSinkIndex, 1);

            response = restClient.put(watchPath + "/_ack_and_get");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Thread.sleep(220);

            response = restClient.delete(watchPath + "/_ack_and_get/testactionone");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            DocNode unackDoc = response.getBodyAsDocNode();           

            Assert.assertEquals(Arrays.asList("testactionone"), unackDoc.findByJsonPath("unacked_action_ids[0]"));
            assertThatActionIsNotAcked(restClient, watchPath, "testactionone");
        }
    }

    @Test
    public void testDeAckAndGetForNotExistingActionShouldReturnErrorResponse() throws Exception {
        String tenant = "_main";
        String watchId = "deack_and_get_non_existing_action_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String watchedIndex = "source_index_for_watch_" + watchId;
        String sinkIndex = "sink_index_" + watchId;
        String additionalSinkIndex = "additional_sink_index_" + watchId;

        try (Client client = cluster.getInternalNodeClient();
            GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            client.admin().indices().create(new CreateIndexRequest(watchedIndex)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(sinkIndex)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(additionalSinkIndex)).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search(watchedIndex).query("{\"match_all\" : {} }").as("testsearch")
                .checkCondition("data.testsearch.hits.hits.length > 0").then()
                .index(sinkIndex).refreshPolicy(RefreshPolicy.IMMEDIATE).throttledFor("0").name("testactionone")
                .and().index(additionalSinkIndex).refreshPolicy(RefreshPolicy.IMMEDIATE).throttledFor("0").name("testactiontwo")
                .build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Thread.sleep(220);

            client.index(new IndexRequest(watchedIndex).id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "key1",
                "val1", "key2", "val2")).actionGet();

            awaitMinCountOfDocuments(client, sinkIndex, 1);
            awaitMinCountOfDocuments(client, additionalSinkIndex, 1);

            response = restClient.put(watchPath + "/_ack_and_get");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Thread.sleep(220);

            response = restClient.delete(watchPath + "/_ack_and_get/this_action_does_not_exist");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_NOT_FOUND, response.getStatusCode());
        }
    }

    @Test
    public void testUnAckOfFreshWatch() throws Exception {
        String tenant = "_main";
        String watchId = "unack_of_fresh_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
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
            Assert.assertEquals(response.getBody(), "No actions are in an un-acknowlegable state", response.getBodyAsDocNode().get("error"));
        }
    }

    @Test
    public void testAckWithUnacknowledgableActions() throws Exception {
        String tenant = "_main";
        String watchId = "ack_with_unack_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String testSource = "testsource_" + watchId;
        String testSinkAck = "testsink_ack_" + watchId;
        String testSinkUnack = "testsink_unack_" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            client.admin().indices().create(new CreateIndexRequest(testSource)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(testSinkAck)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(testSinkUnack)).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search(testSource).query("{\"match_all\" : {} }").as("testsearch")
                    .checkCondition("data.testsearch.hits.hits.length > 0")//
                    .then().index(testSinkAck).refreshPolicy(RefreshPolicy.IMMEDIATE).throttledFor("0").name("testaction_ack")//
                    .and().index(testSinkUnack).refreshPolicy(RefreshPolicy.IMMEDIATE).ackEnabled(false).throttledFor("0").name("testaction_unack").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Thread.sleep(220);

            response = restClient.put(watchPath + "/_ack");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_PRECONDITION_FAILED, response.getStatusCode());

            client.index(new IndexRequest(testSource).id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "key1", "val1",
                    "key2", "val2")).actionGet();

            awaitMinCountOfDocuments(client, testSinkAck, 1);
            awaitMinCountOfDocuments(client, testSinkUnack, 1);

            response = restClient.put(watchPath + "/_ack");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Thread.sleep(500);

            response = restClient.get(watchPath + "/_state");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            DocNode statusDoc = response.getBodyAsDocNode();
            Assert.assertEquals(response.getBody(), "uhura", statusDoc.get("actions", "testaction_ack", "acked", "by").toString());
            Assert.assertTrue(response.getBody(), statusDoc.get("actions", "testaction_unack", "acked") == null);

            Thread.sleep(200);

            long testSinkAckExecutionCountAfterAck = getCountOfDocuments(client, testSinkAck);
            long testSinkUnackExecutionCountAfterAck = getCountOfDocuments(client, testSinkUnack);

            Thread.sleep(310);


            Assert.assertEquals(testSinkAckExecutionCountAfterAck, getCountOfDocuments(client, testSinkAck));
            Assert.assertNotEquals(testSinkUnackExecutionCountAfterAck, getCountOfDocuments(client, testSinkUnack));
        }
    }

    @Test
    public void testActionSpecificAckWithUnacknowledgableActions() throws Exception {
        String tenant = "_main";
        String watchId = "action_specific_ack_with_unack_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String testSource = "testsource_" + watchId;
        String testSinkAck = "testsink_ack_" + watchId;
        String testSinkUnack = "testsink_unack_" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            client.admin().indices().create(new CreateIndexRequest(testSource)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(testSinkAck)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(testSinkUnack)).actionGet();
            client.index(new IndexRequest(testSource).id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "key1", "val1",
                    "key2", "val2")).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search(testSource).query("{\"match_all\" : {} }").as("testsearch")
                    .checkCondition("data.testsearch.hits.hits.length > 0")//
                    .then().index(testSinkAck).refreshPolicy(RefreshPolicy.IMMEDIATE).throttledFor("0").name("testaction_ack")//
                    .and().index(testSinkUnack).refreshPolicy(RefreshPolicy.IMMEDIATE).ackEnabled(false).throttledFor("0").name("testaction_unack").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            awaitMinCountOfDocuments(client, testSinkAck, 1);
            awaitMinCountOfDocuments(client, testSinkUnack, 1);

            response = restClient.put(watchPath + "/_ack/testaction_unack");

            System.out.println(response.getBody());
            
            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertEquals(response.getBody(), "The action 'testaction_unack' is not acknowledgeable", response.getBodyAsDocNode().get("error"));
        }
    }


    @Test
    public void testAckWatchLink() throws Exception {
        String tenant = "_main";
        String watchId = "test_ack_watch_link";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String frontendBaseUrl = "http://my.frontend";

        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            EmailAccount account = new EmailAccount();
            account.setHost("localhost");
            account.setPort(9999);
            account.setDefaultFrom("test@test");

            HttpResponse response = restClient.putJson("/_signals/account/email/test_ack_watch_link", account.toJson());            
            response = restClient.putJson("/_signals/settings/frontend_base_url", DocNode.wrap(frontendBaseUrl).toJsonString());
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Watch watch = new WatchBuilder(watchId).atMsInterval(100000).put("{\"a\": 42}").as("testdata").checkCondition("data.testdata.a > 0")
                    .then().email("test").account("test_ack_watch_link").body("Watch Link: {{ack_watch_link}}\nAction Link: {{ack_action_link}}")
                    .to("test@test").name("testaction").build();
            response = restClient.putJson(watchPath, watch.toJson());
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Thread.sleep(100);

            response = restClient.postJson(watchPath + "/_execute", DocNode.of("simulate", true).toJsonString());
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            String mail = response.getBodyAsDocNode().findSingleNodeByJsonPath("actions[0].request").toString();
            Matcher mailMatcher = Pattern.compile("Watch Link: (\\S+)\nAction Link: (\\S+)", Pattern.MULTILINE).matcher(mail);
            
            if (!mailMatcher.find()) {
                Assert.fail(response.getBody());
            }
            
            Assert.assertEquals(response.getBody(), "http://my.frontend/app/searchguard-signals?sg_tenant=SGS_GLOBAL_TENANT#/watch/test_ack_watch_link/ack/", mailMatcher.group(1));
            Assert.assertEquals(response.getBody(), "http://my.frontend/app/searchguard-signals?sg_tenant=SGS_GLOBAL_TENANT#/watch/test_ack_watch_link/ack/testaction/", mailMatcher.group(2));
        }
    }

    
    @Test
    public void testSearchWatch() throws Exception {
        String tenant = "_main";
        String watchId = "search_watch";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {

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
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {

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
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {

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

            DocNode docNode = response.getBodyAsDocNode();
            String scrollId = docNode.getAsString("_scroll_id");
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

        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA)) {

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

        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA)) {

            try {

                try (MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/hook")) {

                    HttpRequestConfig httpRequestConfig = new HttpRequestConfig(HttpRequestConfig.Method.POST, new URI(webhookProvider.getUri()),
                            "/{{data.teststatic.path}}", null, "{{data.teststatic.body}}", null, null, null, null);

                    httpRequestConfig.compileScripts(new WatchInitializationService(null, scriptService,
                        Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), throttlePeriodParser, STRICT));

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

        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA)) {

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

        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA)) {

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

        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA)) {

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
        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA)) {

            try {
                SlackAccount destination = new SlackAccount();
                destination.setUrl(new URI("https://hooks.slack.com/services/SECRET"));

                Assert.assertTrue(destination.toJson().contains("\"type\":\"slack\""));

                String blocksRawJson = "[\n" + "\t\t{\n" + "\t\t\t\"type\": \"section\",\n" + "\t\t\t\"text\": {\n"
                        + "\t\t\t\t\"type\": \"mrkdwn\",\n" + "\t\t\t\t\"text\": \"A message *with some bold text*}.\"\n" + "\t\t\t}\n" + "\t\t}\n"
                        + "\t]";

                List blocks = DocNode.parse(Format.JSON).from(blocksRawJson).toList();
                
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

        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA)) {

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

                List attachments = DocNode.parse(Format.JSON).from(attachmentRawJson).toList();
                
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
        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA)) {

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
        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA)) {

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
        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA);
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
        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {

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

        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {

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
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
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
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            client.admin().indices().create(new CreateIndexRequest(testSink)).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index(testSink).throttledFor("1000h").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            long watchVersion = Long.parseLong(response.getBodyAsDocNode().getAsString("_version"));
            
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

            long newWatchVersion = Long.parseLong(response.getBodyAsDocNode().getAsString("_version"));
            
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
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {

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

            response = restClient.postJson("/_signals/account/_search?scroll=60s&size=1",
                    "{ \"sort\": [{\"type.keyword\": {\"order\": \"asc\"}}], \"query\": {\"match\": {\"type\": \"SLACK\"}}}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Assert.assertTrue(response.getBody(), response.getBody().contains("slack"));

            DocNode docNode = response.getBodyAsDocNode();
            String scrollId = docNode.getAsString("_scroll_id");
            Assert.assertNotNull(scrollId);

            response = restClient.postJson("/_search/scroll", "{ \"scroll\": \"60s\", \"scroll_id\": \"" + scrollId + "\"}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Assert.assertTrue(response.getBody(), response.getBody().contains("slack"));
        }
    }

    @Test
    public void testConvEs() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA)) {
            String input = DocNode.of("trigger.schedule.daily.at", "noon", "input.simple.x", "y", "actions",
                    DocNode.of("email_action.email", DocNode.of("to", "horst@horst", "body", "Hallo {{ctx.payload.x}}", "attachments", "foo")))
                    .toJsonString();

            HttpResponse response = restClient.postJson("/_signals/convert/es", input);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
        }
    }

    @Test
    public void testPutAllowedEndpointsSetting() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA)) {
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

    @Test
    public void staticInputMapping() throws Exception {
        String tenant = "_main";
        String watchId1 = "static_input_mapping1";
        String watchPath1 = "/_signals/watch/" + tenant + "/" + watchId1;
        String watchId2 = "static_input_mapping2";
        String watchPath2 = "/_signals/watch/" + tenant + "/" + watchId2;
        String testSink = "testsink_" + watchId1;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {
            client.admin().indices().create(new CreateIndexRequest(testSink)).actionGet();

            Watch watch = new WatchBuilder(watchId1).atMsInterval(100).put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index(testSink)
                    .throttledFor("1000h").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath1, watch);

            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            watch = new WatchBuilder(watchId1).atMsInterval(100).put("{\"bla\": \"now_a_different_type\"}").as("teststatic").then().index(testSink)
                    .throttledFor("1000h").name("testsink").build();
            response = restClient.putJson(watchPath1, watch);

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

            watch = new WatchBuilder(watchId2).atMsInterval(100).put("{\"bla\": 1234}").as("teststatic").then().index(testSink).throttledFor("1000h")
                    .name("testsink").build();
            response = restClient.putJson(watchPath2, watch);

            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            response = restClient.get(watchPath2);

            Assert.assertEquals(response.getBody(), 1234,
                    response.getBodyAsDocNode().getAsNode("_source").getAsListOfNodes("checks").get(0).getAsNode("value").get("bla"));

        }
    }

    @Test
    public void testPutWatch_throttlePeriodCannotBeShorterThanLowerBound() throws Exception {
        String tenant = "_main";
        String watchId = "put_test_throttle_period_shorter_than_lower_bound";
        String actionName = "indexAction";
        String testSink = "throttle_period_shorter_than_lower_bound";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        DurationExpression watchThrottle = DurationExpression.parse("2m");
        DurationExpression lowerBound = DurationExpression.parse("6m");

        try (Client client = cluster.getInternalNodeClient();
             GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {

            client.admin().indices().create(new CreateIndexRequest(testSink)).actionGet();

            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?")
                    .throttledFor(watchThrottle).then()
                    .index(testSink).name(actionName).throttledFor(watchThrottle)
                    .build();

            putDynamicSetting("execution.throttle_period_lower_bound", lowerBound.toString(), restClient);
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            String errorMsg = String.format("Throttle period: %s longer than configured lower bound: %s", watchThrottle, lowerBound);
            Assert.assertEquals(response.getBody(),
                    errorMsg,
                    response.getBodyAsDocNode().getAsNode("detail").getAsListOfNodes("throttle_period").get(0).getAsString("expected")
            );
            Assert.assertEquals(response.getBody(),
                    errorMsg,
                    response.getBodyAsDocNode().getAsNode("detail").getAsListOfNodes("actions[indexAction].throttle_period").get(0).getAsString("expected")

            );

            watchThrottle = DurationExpression.parse("1m**3");
            lowerBound = DurationExpression.parse("2m**2");
            watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?")
                    .throttledFor(watchThrottle).then()
                    .index(testSink).name(actionName).throttledFor(watchThrottle)
                    .build();
            putDynamicSetting("execution.throttle_period_lower_bound", lowerBound.toString(), restClient);
            response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            errorMsg = String.format("Throttle period: %s longer than configured lower bound: %s", watchThrottle, lowerBound);
            Assert.assertEquals(response.getBody(),
                    errorMsg,
                    response.getBodyAsDocNode().getAsNode("detail").getAsListOfNodes("throttle_period").get(0).getAsString("expected")
            );
            Assert.assertEquals(response.getBody(),
                    errorMsg,
                    response.getBodyAsDocNode().getAsNode("detail").getAsListOfNodes("actions[indexAction].throttle_period").get(0).getAsString("expected")

            );
        }
    }

    @Test
    public void testPutWatch_throttlePeriodLongerThanLowerBound() throws Exception {
        String tenant = "_main";
        String watchId = "put_test_throttle_period_longer_than_lower_bound";
        String actionName = "indexAction";
        String testSink = "throttle_period_longer_than_lower_bound";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        DurationExpression watchThrottle = DurationExpression.parse("6m");
        DurationExpression lowerBound = DurationExpression.parse("2m");

        try (Client client = cluster.getInternalNodeClient();
             GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {

            client.admin().indices().create(new CreateIndexRequest(testSink)).actionGet();

            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?")
                    .throttledFor(watchThrottle).then()
                    .index(testSink).name(actionName).throttledFor(watchThrottle)
                    .build();

            putDynamicSetting("execution.throttle_period_lower_bound", lowerBound.toString(), restClient);
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());
            watch = getWatchByRest(tenant, watchId, restClient);
            Assert.assertEquals(watch.toJson(), watchThrottle.toString(), watch.getThrottlePeriod().toString());
            Assert.assertEquals(watch.toJson(), watchThrottle.toString(), watch.getActionByName(actionName).getThrottlePeriod().toString());

            watchThrottle = DurationExpression.parse("3m**2");
            lowerBound = DurationExpression.parse("1m**2");
            watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?")
                    .throttledFor(watchThrottle).then()
                    .index(testSink).name(actionName).throttledFor(watchThrottle)
                    .build();
            putDynamicSetting("execution.throttle_period_lower_bound", lowerBound.toString(), restClient);
            response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            watch = getWatchByRest(tenant, watchId, restClient);
            Assert.assertEquals(watch.toJson(), watchThrottle.toString(), watch.getThrottlePeriod().toString());
            Assert.assertEquals(watch.toJson(), watchThrottle.toString(), watch.getActionByName(actionName).getThrottlePeriod().toString());
        }
    }

    @Test
    public void testPutWatch_unsetThrottlePeriodDefaultsToLowerBound() throws Exception {
        String tenant = "_main";
        String watchId = "put_test_throttle_period_defaults_to_lower_bound";
        String actionName = "indexAction";
        String testSink = "throttle_period_defaults_to_than_lower_bound";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        DurationExpression lowerBound = DurationExpression.parse("10m");

        try (Client client = cluster.getInternalNodeClient();
             GenericRestClient restClient = cluster.getRestClient(USERNAME_UHURA, USERNAME_UHURA).trackResources()) {

            client.admin().indices().create(new CreateIndexRequest(testSink)).actionGet();

            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").then()
                    .index(testSink).name(actionName)
                    .build();

            putDynamicSetting("execution.throttle_period_lower_bound", lowerBound.toString(), restClient);
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Watch watchState = getWatchByRest(tenant, watchId, restClient);
            Assert.assertEquals(watch.toJson(), lowerBound.toString(), watchState.getThrottlePeriod().toString());
            Assert.assertEquals(watch.toJson(), lowerBound.toString(), watchState.getActionByName(actionName).getThrottlePeriod().toString());

            deleteDynamicSetting("execution.throttle_period_lower_bound", restClient);

            response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            watchState = getWatchByRest(tenant, watchId, restClient);
            Assert.assertNull(watch.toJson(), watchState.getThrottlePeriod());
            Assert.assertNull(watch.toJson(), watchState.getActionByName(actionName).getThrottlePeriod());

        }
    }

    @Test
    public void endpointsSupportingTenantParameterShouldNotAcceptPrivateTenant() throws Exception {
        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {

            HttpResponse response = restClient.get("/_signals/watch/__user__/_search");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("error.message", "Signals does not support private tenants"));

            response = restClient.get("/_signals/watch/_main/_search");
            assertThat(response.getBody(), response.getBodyAsDocNode(), not(containsValue("error.message", "Signals does not support private tenants")));

            response = restClient.get("/_signals/watch/__user__/1");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("error.message", "Signals does not support private tenants"));

            response = restClient.get("/_signals/watch/_main/1");
            assertThat(response.getBody(), response.getBodyAsDocNode(), not(containsValue("error.message", "Signals does not support private tenants")));

            response = restClient.post("/_signals/watch/__user__/_search");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("error.message", "Signals does not support private tenants"));

            response = restClient.post("/_signals/watch/_main/_search");
            assertThat(response.getBody(), response.getBodyAsDocNode(), not(containsValue("error.message", "Signals does not support private tenants")));

            response = restClient.post("/_signals/watch/__user__/1/_execute");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("error.message", "Signals does not support private tenants"));

            response = restClient.post("/_signals/watch/_main/1/_execute");
            assertThat(response.getBody(), response.getBodyAsDocNode(), not(containsValue("error.message", "Signals does not support private tenants")));

            response = restClient.put("/_signals/tenant/__user__/_active");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("error.message", "Signals does not support private tenants"));

            response = restClient.put("/_signals/tenant/_main/_active");
            assertThat(response.getBody(), response.getBodyAsDocNode(), not(containsValue("error.message", "Signals does not support private tenants")));

            response = restClient.put("/_signals/watch/__user__/1/_ack_and_get");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("error.message", "Signals does not support private tenants"));

            response = restClient.put("/_signals/watch/_main/1/_ack_and_get");
            assertThat(response.getBody(), response.getBodyAsDocNode(), not(containsValue("error.message", "Signals does not support private tenants")));

            response = restClient.delete("/_signals/watch/__user__/1");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("error.message", "Signals does not support private tenants"));

            response = restClient.delete("/_signals/watch/_main/1");
            assertThat(response.getBody(), response.getBodyAsDocNode(), not(containsValue("error.message", "Signals does not support private tenants")));

            response = restClient.delete("/_signals/watch/__user__/1/_ack/1");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
            assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("error.message", "Signals does not support private tenants"));

            response = restClient.delete("/_signals/watch/_main/1/_ack/1");
            assertThat(response.getBody(), response.getBodyAsDocNode(), not(containsValue("error.message", "Signals does not support private tenants")));
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

        WatchInitializationService initService = new WatchInitializationService(null, scriptService,
            Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), throttlePeriodParser, STRICT);
        return Watch.parseFromElasticDocument(initService, "test", id, response.getBody(), -1);
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

    private void putDynamicSetting(String settingName, String settingValue, GenericRestClient restClient) throws Exception {
        HttpResponse response = restClient.putJson("/_signals/settings/" + settingName, DocNode.wrap(settingValue));

        Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
    }

    private void deleteDynamicSetting(String settingName, GenericRestClient restClient) throws Exception {
        HttpResponse response = restClient.delete("/_signals/settings/" + settingName);

        Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
    }

}
