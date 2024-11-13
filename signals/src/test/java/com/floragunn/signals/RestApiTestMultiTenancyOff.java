package com.floragunn.signals;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.signals.proxy.service.HttpProxyHostRegistry;
import com.floragunn.signals.util.WatchLogSearch;
import com.floragunn.signals.watch.common.throttle.ThrottlePeriodParser;
import com.floragunn.signals.watch.common.throttle.ValidatingThrottlePeriodParser;
import com.floragunn.signals.truststore.service.TrustManagerRegistry;
import com.floragunn.signals.watch.result.WatchLog;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;
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

import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.signals.watch.common.ValidationLevel.STRICT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

@NotThreadSafe
public class RestApiTestMultiTenancyOff {
    public static final String SIGNALS_LOGS_INDEX_NAME = "signals__main_log";
    public static final String HUGE_DOCUMENT_INDEX = "huge_document_index";
    public static final int HUGE_DOCUMENT_FIELD_COUNT = 970;

    private static ScriptService scriptService;
    private static ThrottlePeriodParser throttlePeriodParser;

    @ClassRule 
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();
    
    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("sg_config/signals-no-mt")
            .nodeSettings("signals.enabled", true, "searchguard.enterprise_modules_enabled", false, "signals.index_names.log", SIGNALS_LOGS_INDEX_NAME,
                    "signals.watch_log.mapping_total_fields_limit", -1).enableModule(SignalsModule.class).waitForComponents("signals")
            .embedded().build();

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

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            Client client = cluster.getInternalNodeClient();
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

    /*
    Test checks if the index template for the watch log index is created. In the test static configuration parameter `signals.watch_log.mapping_total_fields_limit`
    is equal to -1. Therefore, the log watch index can accommodate massive runtime data. Furthermore, the dynamic mappings are disabled
    for the property that stores runtime data; therefore, the runtime data are not searchable. A similar test case is placed in the method.
    The other test com.floragunn.signals.RestApiTest.testWatchLogContainsDocumentWithHugeFieldCountWithCustomMappingsTotalFieldLimit
    is placed in another test class because to run the test modification in the static configuration is needed.
     */
    @Test
    public void testWatchLogContainsDocumentWithHugeFieldCountAndFieldsAreNotSearchable() throws Exception {
        String tenant = "_main";
        String watchId = "watch_which_creates_huge_logs";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;
        String testSink = "testsink_" + watchId;

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            Client client = cluster.getInternalNodeClient();
            //prepare test data
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(HUGE_DOCUMENT_INDEX).settings(
                    Settings.builder().put("mapping.total_fields.limit", 3000).build());
            client.admin().indices().create(createIndexRequest).actionGet();
            Object[] hugeDocument = new String[HUGE_DOCUMENT_FIELD_COUNT * 2];
            for (int i = 0; i < hugeDocument.length; i += 2) {
                hugeDocument[i] = "key_" + i;
                hugeDocument[i + 1] = "value_" + i;
            }
            client.index(new IndexRequest(HUGE_DOCUMENT_INDEX).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, hugeDocument))
                    .actionGet();


            // The log index is deleted to create an index with fresh (empty) mapping. This way, other tests do not affect this test.
            try {
                client.admin().indices().delete(new DeleteIndexRequest(SIGNALS_LOGS_INDEX_NAME)).actionGet();
            } catch (IndexNotFoundException e) {
                // if index does not exist, this is totally ok
            }

            client.admin().indices().create(new CreateIndexRequest(testSink).settings(Settings.builder().put("mapping.total_fields.limit", 3000).build())).actionGet();

            Watch watch = new WatchBuilder(watchId).logRuntimeData(true).atMsInterval(100).search(HUGE_DOCUMENT_INDEX).query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index(testSink).throttledFor("1000h").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            long watchVersion = Long.parseLong(response.getBodyAsDocNode().getAsString("_version"));

            List<WatchLog> watchLogs = new WatchLogSearch(client).index(SIGNALS_LOGS_INDEX_NAME).watchId(watchId).watchVersion(watchVersion)
                    .fromTheStart().count(1).await();

            int watchSearchResultsCount = DocNode.wrap(watchLogs.get(0).getData()).getAsNode("testsearch") //
                    .getAsNode("hits") //
                    .getAsListOfNodes("hits") //
                    .get(0) //
                    .getAsNode("_source") //
                    .size();
            assertThat(watchSearchResultsCount, equalTo(HUGE_DOCUMENT_FIELD_COUNT));
            // ensure that runtime data in watch log index is not searchable
            TermQueryBuilder queryBuilder = QueryBuilders.termQuery("data.testsearch.hits.hits._source.key_418.keyword", "value_418");
            SearchResponse searchResponse = client.search(
                    new SearchRequest(SIGNALS_LOGS_INDEX_NAME).source(new SearchSourceBuilder().query(queryBuilder).size(1))).actionGet();
            assertThat(searchResponse.getHits().getHits(), arrayWithSize(0));
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
