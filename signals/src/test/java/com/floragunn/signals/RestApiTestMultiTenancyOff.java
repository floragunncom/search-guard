package com.floragunn.signals;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

import com.floragunn.signals.proxy.service.HttpProxyHostRegistry;
import com.floragunn.signals.watch.common.throttle.ThrottlePeriodParser;
import com.floragunn.signals.watch.common.throttle.ValidatingThrottlePeriodParser;
import com.floragunn.signals.truststore.service.TrustManagerRegistry;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;
import com.floragunn.signals.watch.init.WatchInitializationService;

import net.jcip.annotations.NotThreadSafe;
import org.mockito.Mockito;

import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.signals.watch.common.ValidationLevel.STRICT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

@NotThreadSafe
public class RestApiTestMultiTenancyOff {
    private static ScriptService scriptService;
    private static ThrottlePeriodParser throttlePeriodParser;

    @ClassRule 
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();
    
    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("sg_config/signals-no-mt")
            .nodeSettings("signals.enabled", true, "searchguard.enterprise_modules_enabled", false).enableModule(SignalsModule.class).waitForComponents("signals").embedded().build();

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
        throttlePeriodParser = new ValidatingThrottlePeriodParser(cluster.getInjectable(Signals.class).getSignalsSettings());
    }

    @Test
    public void testGetWatchInNotExistingTenantUnauthorized() throws Exception {

        String tenant = "schnickschnack";
        String watchId = "get_watch_unauth";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {

            HttpResponse response = restClient.get(watchPath);

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));
        }
    }

    @Test
    public void testGetWatchInNonDefaultTenantUnauthorized() throws Exception {

        String tenant = "redshirt_club";
        String watchId = "get_watch_unauth";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {

            HttpResponse response = restClient.get(watchPath);

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));
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
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_CREATED));

            response = restClient.get(watchPath);

            System.out.print(response.getBody());
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            WatchInitializationService initService = new WatchInitializationService(null, scriptService,
                Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), throttlePeriodParser, STRICT);
            watch = Watch.parseFromElasticDocument(initService, "test", "put_test", response.getBody(), -1);

            awaitMinCountOfDocuments(client, "testsink_put_watch", 1);

        }
    }

    @Test
    public void testPutWatchInNonExistingTenant() throws Exception {
        String tenant = "schnickschnack";
        String watchId = "put_test_non_existing_tenant";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));
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

    private long awaitMinCountOfDocuments(Client client, String index, long minCount) throws Exception {
        return Awaitility.await(String.format("Number of documents in index %s >= %d", index, minCount))
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(10))
                .until(() -> getCountOfDocuments(client, index), Matchers.greaterThanOrEqualTo(minCount));
    }

}
