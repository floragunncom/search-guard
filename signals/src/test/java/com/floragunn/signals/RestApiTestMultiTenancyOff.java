package com.floragunn.signals;

import java.util.concurrent.ExecutionException;

import com.floragunn.signals.watch.common.throttle.ThrottlePeriodParser;
import com.floragunn.signals.watch.common.throttle.ValidatingThrottlePeriodParser;
import com.floragunn.signals.truststore.service.TrustManagerRegistry;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
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

import static com.floragunn.signals.watch.common.ValidationLevel.STRICT;

@NotThreadSafe
public class RestApiTestMultiTenancyOff {
    private static final Logger log = LogManager.getLogger(RestApiTestMultiTenancyOff.class);

    private static ScriptService scriptService;
    private static ThrottlePeriodParser throttlePeriodParser;

    @ClassRule 
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();
    
    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("sg_config/signals-no-mt")
            .nodeSettings("signals.enabled", true, "searchguard.enterprise_modules_enabled", false).enableModule(SignalsModule.class).build();

    @BeforeClass
    public static void setupTestData() {

        Client client = cluster.getInternalNodeClient();
        client.index(new IndexRequest("testsource").source(XContentType.JSON, "key1", "val1", "key2", "val2")).actionGet();

        client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "x", "b", "y"))
                .actionGet();
        client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "xx", "b", "yy"))
                .actionGet();
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

            Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

        }
    }

    @Test
    public void testGetWatchInNonDefaultTenantUnauthorized() throws Exception {

        String tenant = "redshirt_club";
        String watchId = "get_watch_unauth";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {

            HttpResponse response = restClient.get(watchPath);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

        }
    }

    @Test
    public void testPutWatch() throws Exception {
        String tenant = "_main";
        String watchId = "put_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            Client client = cluster.getInternalNodeClient();
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch")).actionGet();

            Watch watch = new WatchBuilder(watchId).cronTrigger("* * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.get(watchPath);

            //System.out.print(response.getBody());
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            WatchInitializationService initService = new WatchInitializationService(null, scriptService,
                Mockito.mock(TrustManagerRegistry.class), throttlePeriodParser, STRICT);
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

            Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

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
        long start = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {
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

}
