package com.floragunn.signals.actions.summary;

import static com.floragunn.searchguard.test.TestSgConfig.Role.ALL_ACCESS;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containSubstring;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsAnyValues;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsFieldPointedByJsonPath;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.docNodeSizeEqualTo;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.fieldIsNull;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.valueSatisfiesMatcher;
import static com.floragunn.signals.actions.summary.PredefinedWatches.ACTION_CREATE_ALARM_ONE;
import static com.floragunn.signals.actions.summary.PredefinedWatches.ACTION_CREATE_ALARM_TWO;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.await;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.startsWith;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.SignalsModule;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class LoadOperatorSummaryActionTest {

    private static final Logger log = LogManager.getLogger(LoadOperatorSummaryActionTest.class);

    public static final Role ROLE_SIGNAL_ADMIN = new Role("signals_admin")//
            .tenantPermission("cluster:admin:searchguard:tenant:signals:*")//
            .on("SGS_GLOBAL_TENANT");
    public static final User USER_ADMIN = new User("admin").roles(ALL_ACCESS, ROLE_SIGNAL_ADMIN);
    public static final String INDEX_NAME_WATCHED_1 = "watched-source-index-01";
    public static final String INDEX_NAME_WATCHED_2 = "watched-source-index-02";
    public static final String INDEX_NAME_WATCHED_3 = "watched-source-index-03";
    public static final String INDEX_NAME_WATCHED_4 = "watched-source-index-04";
    public static final String INDEX_NAME_WATCHED_5 = "watched-source-index-05";
    public static final String INDEX_NAME_WATCHED_6 = "watched-source-index-06";
    public static final String INDEX_NAME_WATCHED_7 = "watched-source-index-07";
    public static final String INDEX_NAME_WATCHED_8 = "watched-source-index-08";
    public static final String INDEX_ALARMS = "alarms";
    public static final String INDEX_SIGNALS_WATCHES_STATE = ".signals_watches_state";
    public static final String EMPTY_JSON_BODY = DocNode.EMPTY.toString();

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode()
        .sslEnabled()
        .enableModule(SignalsModule.class)
        .user(USER_ADMIN)
        .dependsOn(javaSecurity)
        .build();

    @BeforeClass
    public static void createTestData() {
        Client client = cluster.getInternalNodeClient();
        client.index(new IndexRequest(INDEX_NAME_WATCHED_1).setRefreshPolicy(IMMEDIATE)
                .source(XContentType.JSON, "source_id", 1, "temperature", .0, " humidity", .0)).actionGet();
        client.index(new IndexRequest(INDEX_NAME_WATCHED_1).setRefreshPolicy(IMMEDIATE)
                .source(XContentType.JSON, "source_id", 1, "temperature", .1, " humidity", .1)).actionGet();
        client.index(new IndexRequest(INDEX_NAME_WATCHED_1).setRefreshPolicy(IMMEDIATE)
                .source(XContentType.JSON, "source_id", 1, "temperature", 4.3, " humidity", .0)).actionGet();

        client.index(new IndexRequest(INDEX_NAME_WATCHED_2).setRefreshPolicy(IMMEDIATE)
                .source(XContentType.JSON, "source_id", 2, "temperature", .0, " humidity", .0)).actionGet();
        client.index(new IndexRequest(INDEX_NAME_WATCHED_2).setRefreshPolicy(IMMEDIATE)
                .source(XContentType.JSON, "source_id", 2, "temperature", .1, " humidity", .1)).actionGet();
        client.index(new IndexRequest(INDEX_NAME_WATCHED_2).setRefreshPolicy(IMMEDIATE)
                .source(XContentType.JSON, "source_id", 2, "temperature", .1, " humidity", .0)).actionGet();

        client.index(new IndexRequest(INDEX_NAME_WATCHED_3).setRefreshPolicy(IMMEDIATE)
                .source(XContentType.JSON, "source_id", 3, "temperature", .0, " humidity", .0)).actionGet();
        client.index(new IndexRequest(INDEX_NAME_WATCHED_3).setRefreshPolicy(IMMEDIATE)
                .source(XContentType.JSON, "source_id", 3, "temperature", .1, " humidity", .1)).actionGet();
        client.index(new IndexRequest(INDEX_NAME_WATCHED_3).setRefreshPolicy(IMMEDIATE)
                .source(XContentType.JSON, "source_id", 3, "temperature", 13.3, " humidity", .0)).actionGet();

        client.index(new IndexRequest(INDEX_NAME_WATCHED_4).setRefreshPolicy(IMMEDIATE)
                .source(XContentType.JSON, "source_id", 4, "temperature", 36.5, " humidity", .0)).actionGet();

        client.index(new IndexRequest(INDEX_NAME_WATCHED_5).setRefreshPolicy(IMMEDIATE)
                .source(XContentType.JSON, "source_id", 5, "temperature", 7.25, " humidity", .0)).actionGet();

        client.index(new IndexRequest(INDEX_NAME_WATCHED_6).setRefreshPolicy(IMMEDIATE)
                .source(XContentType.JSON, "source_id", 6, "temperature", 7.35, " humidity", .0)).actionGet();

        client.index(new IndexRequest(INDEX_NAME_WATCHED_7).setRefreshPolicy(IMMEDIATE)
                .source(XContentType.JSON, "source_id", 7, "temperature", 7.45, " humidity", .0)).actionGet();

        client.index(new IndexRequest(INDEX_NAME_WATCHED_8).setRefreshPolicy(IMMEDIATE)
                .source(XContentType.JSON, "source_id", 8, "temperature", 7.55, " humidity", .0)).actionGet();

        client.admin().indices().create(new CreateIndexRequest(INDEX_ALARMS)).actionGet();
    }

    @After
    public void deleteWatchStateAndAlarms() {
        Client client = cluster.getPrivilegedInternalNodeClient();
        deleteDocumentsFromIndex(client, INDEX_SIGNALS_WATCHES_STATE);
        deleteDocumentsFromIndex(client, INDEX_ALARMS);
    }

    private static void deleteDocumentsFromIndex(Client client, String indexName) {
        SearchResponse searchResponse = client.search(new SearchRequest(indexName)).actionGet();
        for(SearchHit hit : searchResponse.getHits().getHits()) {
            String documentId = hit.getId();
            client.delete(new DeleteRequest(indexName).setRefreshPolicy(IMMEDIATE).id(documentId)).actionGet();
            log.debug("Deleted document '{}' from index '{}'. Document: '{}'.", documentId, indexName, hit.getSourceAsString());
        }
    }

    @Test
    public void shouldGetActionSummaryWithInfoLevel() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temperature-alerts-2", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.countWatchStatusWithAvailableStatusCode(INDEX_SIGNALS_WATCHES_STATE) > 0);

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary", EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 1));
            assertThat(body, containsValue("data.watches[0].watch_id", "temperature-alerts-2"));
            assertThat(body, containsAnyValues("data.watches[0].status_code", "ACTION_EXECUTED", "ACTION_THROTTLED"));
            assertThat(body, containsValue("data.watches[0].severity", "info"));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[0]", "description"));
            assertThat(body, containsValue("data.watches[0].severity_details.level", "info"));
            assertThat(body, containsValue("data.watches[0].severity_details.level_numeric", 1));
            assertThat(body, valueSatisfiesMatcher("data.watches[0].severity_details.current_value",
                Double.class, closeTo(4.3, 0.00001)));
            assertThat(body, valueSatisfiesMatcher("data.watches[0].severity_details.threshold",
                Double.class, closeTo(3.0, 0.00001)));
            assertThat(body, docNodeSizeEqualTo("data.watches[0].actions", 1));
            assertThat(body, containsValue("data.watches[0].actions.createAlarm.check_result", true));
            assertThat(body, containsAnyValues("data.watches[0].actions.createAlarm.status_code",
                "ACTION_EXECUTED","ACTION_THROTTLED"));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[0].actions.createAlarm", "triggered"));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[0].actions.createAlarm", "checked"));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[0].actions.createAlarm", "execution"));
            assertThat(body, fieldIsNull("data.watches[0].actions.createAlarm.error"));
            assertThat(body, fieldIsNull("data.watches[0].actions.createAlarm.status_details"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldGetActionSummaryWithErrorLevel() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temperature-alerts-1", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .15, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 0);

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary", EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 1));
            assertThat(body, containsValue("data.watches[0].watch_id", "temperature-alerts-1"));
            assertThat(body, containsAnyValues("data.watches[0].status_code", "ACTION_EXECUTED", "ACTION_THROTTLED"));
            assertThat(body, containsValue("data.watches[0].severity", "error"));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[0]", "description"));
            assertThat(body, containsValue("data.watches[0].severity_details.level", "error"));
            assertThat(body, containsValue("data.watches[0].severity_details.level_numeric", 3));
            assertThat(body, valueSatisfiesMatcher("data.watches[0].severity_details.current_value",
                Double.class, closeTo(13.3, 0.00001)));
            assertThat(body, valueSatisfiesMatcher("data.watches[0].severity_details.threshold",
                Double.class, closeTo(10.0, 0.00001)));
            assertThat(body, docNodeSizeEqualTo("data.watches[0].actions", 1));
            assertThat(body, containsValue("data.watches[0].actions.createAlarm.check_result", true));
            assertThat(body, containsAnyValues("data.watches[0].actions.createAlarm.status_code",
                "ACTION_EXECUTED", "ACTION_THROTTLED"));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[0].actions.createAlarm", "triggered"));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[0].actions.createAlarm", "checked"));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[0].actions.createAlarm", "execution"));
            assertThat(body, fieldIsNull("data.watches[0].actions.createAlarm.error"));
            assertThat(body, fieldIsNull("data.watches[0].actions.createAlarm.status_details"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldGetActionSummaryWithoutSeverity() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineSimpleTemperatureWatch("high-temp-alerts", INDEX_NAME_WATCHED_2, INDEX_ALARMS, .05);
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 0);

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary", EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 1));
            assertThat(body, containsValue("data.watches[0].watch_id", "high-temp-alerts"));
            assertThat(body, containsAnyValues("data.watches[0].status_code", "ACTION_EXECUTED", "ACTION_THROTTLED"));
            assertThat(body, fieldIsNull("data.watches[0].severity"));
            assertThat(body, fieldIsNull("data.watches[0].severity_details"));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[0]", "description"));
            assertThat(body, docNodeSizeEqualTo("data.watches[0].actions", 1));
            assertThat(body, containsValue("data.watches[0].actions.createAlarm.check_result", true));
            assertThat(body, containsAnyValues("data.watches[0].actions.createAlarm.status_code",
                "ACTION_EXECUTED", "ACTION_THROTTLED"));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[0].actions.createAlarm", "triggered"));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[0].actions.createAlarm", "checked"));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[0].actions.createAlarm", "execution"));
            assertThat(body, fieldIsNull("data.watches[0].actions.createAlarm.error"));
            assertThat(body, fieldIsNull("data.watches[0].actions.createAlarm.status_details"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldLoadSummaryOfMultipleWatch() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineSimpleTemperatureWatch("high-temp-alerts", INDEX_NAME_WATCHED_2, INDEX_ALARMS, .05);
        predefinedWatches.defineTemperatureSeverityWatch("temperature-alerts", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .15, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 0);
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, EMPTY_JSON_BODY);

            log.info("Watch summary of multiple watches response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 2));
            assertThat(body, containsValue("data.watches[0].watch_id", "temperature-alerts"));
            assertThat(body, containsAnyValues("data.watches[0].status_code", "ACTION_EXECUTED", "ACTION_THROTTLED"));
            assertThat(body, containsValue("data.watches[0].severity", "error"));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[0]", "severity"));
            assertThat(body, containsValue("data.watches[0].severity_details.level", "error"));
            assertThat(body, containsValue("data.watches[0].severity_details.level_numeric", 3));
            assertThat(body, valueSatisfiesMatcher("data.watches[0].severity_details.current_value", Double.class,
                closeTo(13.3, 0.001)));
            assertThat(body, valueSatisfiesMatcher("data.watches[0].severity_details.threshold", Double.class,
                closeTo(10.0, 0.001)));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[0].actions.createAlarm", "triggered"));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[0].actions.createAlarm", "checked"));
            assertThat(body, containsValue("data.watches[0].actions.createAlarm.check_result", true));
            assertThat(body, fieldIsNull("data.watches[0].actions.createAlarm.error"));
            assertThat(body, containsAnyValues("data.watches[0].actions.createAlarm.status_code",
                "ACTION_EXECUTED", "ACTION_THROTTLED"));
            assertThat(body, fieldIsNull("data.watches[0].actions.createAlarm.status_details"));


            assertThat(body, containsValue("data.watches[1].watch_id", "high-temp-alerts"));
            assertThat(body, containsAnyValues("data.watches[1].status_code", "ACTION_EXECUTED", "ACTION_THROTTLED"));
            assertThat(body, fieldIsNull("data.watches[1].severity"));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[1]", "severity"));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[1].actions.createAlarm", "triggered"));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[1].actions.createAlarm", "checked"));
            assertThat(body, containsValue("data.watches[1].actions.createAlarm.check_result", true));
            assertThat(body, fieldIsNull("data.watches[1].actions.createAlarm.error"));
            assertThat(body, containsAnyValues("data.watches[1].actions.createAlarm.status_code",
                "ACTION_EXECUTED", "ACTION_THROTTLED"));
            assertThat(body, fieldIsNull("data.watches[1].actions.createAlarm.status_details"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldLoadSummaryOfWatchWithMultipleActions() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineSimpleTemperatureWatchWitDoubleActions("double-alerts", INDEX_NAME_WATCHED_2, INDEX_ALARMS, .05);
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 0);

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary", "{}");

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 1));
            assertThat(body, docNodeSizeEqualTo("data.watches[0].actions", 2));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[0].actions", ACTION_CREATE_ALARM_ONE));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[0].actions", ACTION_CREATE_ALARM_TWO));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldSortByNumericSeverityDesc() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 2);
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 3));
            assertThat(body, containsValue("data.watches[0].watch_id", "temp-3"));
            assertThat(body, containsValue("data.watches[0].severity_details.level_numeric", 4));
            assertThat(body, containsValue("data.watches[1].watch_id", "temp-2"));
            assertThat(body, containsValue("data.watches[1].severity_details.level_numeric", 3));
            assertThat(body, containsValue("data.watches[2].watch_id", "temp-1"));
            assertThat(body, containsValue("data.watches[2].severity_details.level_numeric", 1));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldSortByNumericSeverityAsc() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 2);
            String sorting = "+severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 3));
            assertThat(body, containsValue("data.watches[0].watch_id", "temp-1"));
            assertThat(body, containsValue("data.watches[0].severity_details.level_numeric", 1));
            assertThat(body, containsValue("data.watches[1].watch_id", "temp-2"));
            assertThat(body, containsValue("data.watches[1].severity_details.level_numeric", 3));
            assertThat(body, containsValue("data.watches[2].watch_id", "temp-3"));
            assertThat(body, containsValue("data.watches[2].severity_details.level_numeric", 4));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldSortByStringSeverityAsc() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1.1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-4", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 5);

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=+severity", EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 5));
            assertThat(body, containsValue("data.watches[0].watch_id", "temp-3"));
            assertThat(body, containsValue("data.watches[0].severity", "critical"));
            assertThat(body, containsValue("data.watches[1].watch_id", "temp-2"));
            assertThat(body, containsValue("data.watches[1].severity", "error"));
            assertThat(body, containsAnyValues("data.watches[2].watch_id","temp-1.1", "temp-1"));
            assertThat(body, containsValue("data.watches[2].severity", "info"));
            assertThat(body, containsAnyValues("data.watches[3].watch_id","temp-1.1", "temp-1"));
            assertThat(body, containsValue("data.watches[3].severity", "info"));
            assertThat(body, containsValue("data.watches[4].watch_id", "temp-4"));
            assertThat(body, containsValue("data.watches[4].severity", "warning"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldSortByStringSeverityDesc() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1.1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-4", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 5);

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=-severity", EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 5));
            assertThat(body, containsValue("data.watches[0].watch_id", "temp-4"));
            assertThat(body, containsValue("data.watches[0].severity", "warning"));
            assertThat(body, containsAnyValues("data.watches[1].watch_id", "temp-1.1", "temp-1"));
            assertThat(body, containsValue("data.watches[1].severity", "info"));
            assertThat(body, containsAnyValues("data.watches[2].watch_id", "temp-1.1", "temp-1"));
            assertThat(body, containsValue("data.watches[2].severity", "info"));
            assertThat(body, containsValue("data.watches[3].watch_id", "temp-2"));
            assertThat(body, containsValue("data.watches[3].severity", "error"));
            assertThat(body, containsValue("data.watches[4].watch_id", "temp-3"));
            assertThat(body, containsValue("data.watches[4].severity", "critical"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldReportErrorWhenSortingOnIncorrectField() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 0);
            final String fieldName = "incorrect-field";
            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=+" + fieldName, EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(400));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containSubstring("error.message", "Cannot sort by unknown field"));
            assertThat(body, containSubstring("error.message", fieldName));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldNotTriggerActionDueToTooLowSeverity() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureWatchWithActionOnCriticalSeverity("critical-severity-action", INDEX_NAME_WATCHED_3,
            INDEX_ALARMS, .25);
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForFirstActionNonEmptyStatus(restClient);

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary", EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 1));
            assertThat(body, containsValue("data.watches[0].watch_id", "critical-severity-action"));
            assertThat(body, containsValue("data.watches[0].status_code", "NO_ACTION"));
            assertThat(body, containsValue("data.watches[0].severity", "error"));
            assertThat(body, containsFieldPointedByJsonPath("data.watches[0]", "description"));
            assertThat(body, containsValue("data.watches[0].severity_details.level", "error"));
            assertThat(body, containsValue("data.watches[0].severity_details.level_numeric", 3));
            assertThat(body, valueSatisfiesMatcher("data.watches[0].severity_details.current_value",
                Double.class, closeTo(13.3, 0.00001)));
            assertThat(body, valueSatisfiesMatcher("data.watches[0].severity_details.threshold",
                Double.class, closeTo(10.0, 0.00001)));
            assertThat(body, docNodeSizeEqualTo("data.watches[0].actions", 1));
            assertThat(body, containsValue("data.watches[0].actions.createAlarm.check_result", false));
            assertThat(body, containsValue("data.watches[0].actions.createAlarm.status_code", "NO_ACTION"));
            assertThat(body, containsValue("data.watches[0].actions.createAlarm.status_details",//
                "No action because current severity is lower than severity configured for action: error"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldSortByStatusCodeAsc() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureWatchWithActionOnCriticalSeverity("critical-severity-action-1", INDEX_NAME_WATCHED_3,
            INDEX_ALARMS, .25);
        predefinedWatches.defineTemperatureWatchWithActionOnCriticalSeverity("critical-severity-action-2", INDEX_NAME_WATCHED_3,
            INDEX_ALARMS, .25);
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 4);

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=+status_code", EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 4));
            assertThat(body, containsAnyValues("data.watches[0].watch_id", "temp-1", "temp-2"));
            assertThat(body, containsAnyValues("data.watches[0].status_code", "ACTION_EXECUTED", "ACTION_THROTTLED"));
            assertThat(body, containsAnyValues("data.watches[1].watch_id", "temp-1", "temp-2"));
            assertThat(body, containsAnyValues("data.watches[1].status_code","ACTION_EXECUTED", "ACTION_THROTTLED"));
            assertThat(body, containsAnyValues("data.watches[2].watch_id",
                "critical-severity-action-1", "critical-severity-action-2"));
            assertThat(body, containsValue("data.watches[2].status_code", "NO_ACTION"));
            assertThat(body, containsAnyValues("data.watches[3].watch_id",
                "critical-severity-action-1", "critical-severity-action-2"));
            assertThat(body, containsValue("data.watches[3].status_code", "NO_ACTION"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldSortByStatusCodeDesc() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureWatchWithActionOnCriticalSeverity("critical-severity-action-1", INDEX_NAME_WATCHED_3,
            INDEX_ALARMS, .25);
        predefinedWatches.defineTemperatureWatchWithActionOnCriticalSeverity("critical-severity-action-2", INDEX_NAME_WATCHED_3,
            INDEX_ALARMS, .25);
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 4);

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=-status_code", EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 4));
            assertThat(body, containsAnyValues("data.watches[0].watch_id", "critical-severity-action-1", "critical-severity-action-2"));
            assertThat(body, containsValue("data.watches[0].status_code", "NO_ACTION"));
            assertThat(body, containsAnyValues("data.watches[1].watch_id", "critical-severity-action-1", "critical-severity-action-2"));
            assertThat(body, containsValue("data.watches[1].status_code", "NO_ACTION"));
            assertThat(body, containsAnyValues("data.watches[2].watch_id", "temp-1", "temp-2"));
            assertThat(body, containsAnyValues("data.watches[2].status_code", String.class,"ACTION_EXECUTED", "ACTION_THROTTLED"));
            assertThat(body, containsAnyValues("data.watches[3].watch_id", "temp-1", "temp-2"));
            assertThat(body, containsAnyValues("data.watches[3].status_code", "ACTION_EXECUTED", "ACTION_THROTTLED"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldSortByMultipleFieldsDesc() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_6, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-4", INDEX_NAME_WATCHED_7, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-5", INDEX_NAME_WATCHED_8, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 5);
            String sortingExpression = "-severity_details.level_numeric,-severity_details.current_value";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sortingExpression, EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 5));
            assertThat(body, containsValue("data.watches[0].watch_id", "temp-1"));
            assertThat(body, containsValue("data.watches[0].severity_details.level_numeric", 4));
            assertThat(body, valueSatisfiesMatcher("data.watches[0].severity_details.current_value", Double.class,
                closeTo(36.5, 0.001)));
            assertThat(body, containsValue("data.watches[1].watch_id", "temp-5"));
            assertThat(body, containsValue("data.watches[1].severity_details.level_numeric", 2));
            assertThat(body, valueSatisfiesMatcher("data.watches[1].severity_details.current_value", Double.class,
                closeTo(7.55, 0.001)));
            assertThat(body, containsValue("data.watches[2].watch_id", "temp-4"));
            assertThat(body, containsValue("data.watches[2].severity_details.level_numeric", 2));
            assertThat(body, valueSatisfiesMatcher("data.watches[2].severity_details.current_value", Double.class,
                closeTo(7.45, 0.001)));
            assertThat(body, containsValue("data.watches[3].watch_id", "temp-3"));
            assertThat(body, containsValue("data.watches[3].severity_details.level_numeric", 2));
            assertThat(body, valueSatisfiesMatcher("data.watches[3].severity_details.current_value", Double.class,
                closeTo(7.35, 0.001)));
            assertThat(body, containsValue("data.watches[4].watch_id", "temp-2"));
            assertThat(body, containsValue("data.watches[4].severity_details.level_numeric", 2));
            assertThat(body, valueSatisfiesMatcher("data.watches[4].severity_details.current_value", Double.class,
                closeTo(7.25, 0.001)));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldSortByFirstFieldDescAndSecondAsc() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_6, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-4", INDEX_NAME_WATCHED_7, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-5", INDEX_NAME_WATCHED_8, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 5);
            String sortingExpression = "-severity_details.level_numeric,+severity_details.current_value";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sortingExpression, EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 5));
            assertThat(body, containsValue("data.watches[0].watch_id", "temp-1"));
            assertThat(body, containsValue("data.watches[0].severity_details.level_numeric", 4));
            assertThat(body, valueSatisfiesMatcher("data.watches[0].severity_details.current_value", Double.class,
                closeTo(36.5, 0.001)));
            assertThat(body, containsValue("data.watches[1].watch_id", "temp-2"));
            assertThat(body, containsValue("data.watches[1].severity_details.level_numeric", 2));
            assertThat(body, valueSatisfiesMatcher("data.watches[1].severity_details.current_value", Double.class,
                closeTo(7.25, 0.001)));
            assertThat(body, containsValue("data.watches[2].watch_id", "temp-3"));
            assertThat(body, containsValue("data.watches[2].severity_details.level_numeric", 2));
            assertThat(body, valueSatisfiesMatcher("data.watches[2].severity_details.current_value", Double.class,
                closeTo(7.35, 0.001)));
            assertThat(body, containsValue("data.watches[3].watch_id", "temp-4"));
            assertThat(body, containsValue("data.watches[3].severity_details.level_numeric", 2));
            assertThat(body, valueSatisfiesMatcher("data.watches[3].severity_details.current_value", Double.class,
                closeTo(7.45, 0.001)));
            assertThat(body, containsValue("data.watches[4].watch_id", "temp-5"));
            assertThat(body, containsValue("data.watches[4].severity_details.level_numeric", 2));
            assertThat(body, valueSatisfiesMatcher("data.watches[4].severity_details.current_value", Double.class,
                closeTo(7.55, 0.001)));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldSortByInternalActionStatusDesc() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineSimpleTemperatureWatchWitDoubleActionsAndVariousSeverity("watch-id-1", INDEX_NAME_WATCHED_1,
            INDEX_ALARMS, .25);
        predefinedWatches.defineSimpleTemperatureWatchWitDoubleActionsAndVariousSeverity("watch-id-2", INDEX_NAME_WATCHED_3,
            INDEX_ALARMS, .25);
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 2);
            String sortingExpression = "-actions.create_alarmOne.status_code";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sortingExpression, EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 2));
            assertThat(body, containsValue("data.watches[0].watch_id", "watch-id-1"));
            assertThat(body, containsValue("data.watches[0].actions.create_alarmOne.status_code", "NO_ACTION"));
            assertThat(body, containsValue("data.watches[0].actions.create_alarmTwo.status_code", "NO_ACTION"));
            assertThat(body, containsValue("data.watches[1].watch_id", "watch-id-2"));
            assertThat(body, containsAnyValues("data.watches[1].actions.create_alarmOne.status_code",
                "ACTION_EXECUTED", "ACTION_THROTTLED"));
            assertThat(body, containsValue("data.watches[1].actions.create_alarmTwo.status_code", "NO_ACTION"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldSortByInternalActionStatusAsc() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineSimpleTemperatureWatchWitDoubleActionsAndVariousSeverity("watch-id-1", INDEX_NAME_WATCHED_1,
            INDEX_ALARMS, .25);
        predefinedWatches.defineSimpleTemperatureWatchWitDoubleActionsAndVariousSeverity("watch-id-2", INDEX_NAME_WATCHED_3,
            INDEX_ALARMS, .25);
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 2);
            String sortingExpression = "+actions.create_alarmOne.status_code";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sortingExpression, EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 2));
            assertThat(body, containsValue("data.watches[0].watch_id", "watch-id-2"));
            assertThat(body, containsAnyValues("data.watches[0].actions.create_alarmOne.status_code",
                "ACTION_EXECUTED", "ACTION_THROTTLED"));
            assertThat(body, containsValue("data.watches[0].actions.create_alarmTwo.status_code", "NO_ACTION"));
            assertThat(body, containsValue("data.watches[1].watch_id", "watch-id-1"));
            assertThat(body, containsValue("data.watches[1].actions.create_alarmOne.status_code", "NO_ACTION"));
            assertThat(body, containsValue("data.watches[1].actions.create_alarmTwo.status_code", "NO_ACTION"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldSortByActionCheckedTimeAsc() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineSimpleTemperatureWatch("watch-id-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25);
        predefinedWatches.defineSimpleTemperatureWatch("watch-id-2", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25);
        predefinedWatches.defineSimpleTemperatureWatch("watch-id-3", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25);
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 3);
            String sortingExpression = "+actions.createAlarm.checked";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sortingExpression, EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 3));
            // just verify that errors related to sorting by date/time does not occured
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldSortByActionTriggeredTimeDesc() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineSimpleTemperatureWatch("watch-id-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25);
        predefinedWatches.defineSimpleTemperatureWatch("watch-id-2", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25);
        predefinedWatches.defineSimpleTemperatureWatch("watch-id-3", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25);
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 3);
            String sortingExpression = "-actions.createAlarm.triggered";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sortingExpression, EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 3));
            // just verify that errors related to sorting by date/time does not occured
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldSortByActionExecutionTimeAsc() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineSimpleTemperatureWatch("watch-id-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25);
        predefinedWatches.defineSimpleTemperatureWatch("watch-id-2", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25);
        predefinedWatches.defineSimpleTemperatureWatch("watch-id-3", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25);
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 3);
            String sortingExpression = "+actions.createAlarm.execution";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sortingExpression, EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 3));
            // just verify that errors related to sorting by date/time does not occured
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByStatusCode() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureWatchWithActionOnCriticalSeverity("critical-severity-action-1", INDEX_NAME_WATCHED_3,
            INDEX_ALARMS, .25);
        predefinedWatches.defineTemperatureWatchWithActionOnCriticalSeverity("critical-severity-action-2", INDEX_NAME_WATCHED_3,
            INDEX_ALARMS, .25);
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 4);
            String requestBody = DocNode.of("status_codes", Collections.singletonList("NO_ACTION")).toJsonString();

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=+status_code", requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 2));
            assertThat(body, containsAnyValues("data.watches[0].watch_id","critical-severity-action-1", "critical-severity-action-2"));
            assertThat(body, containsValue("data.watches[0].status_code", "NO_ACTION"));
            assertThat(body, containsAnyValues("data.watches[1].watch_id", "critical-severity-action-1","critical-severity-action-2"));
            assertThat(body, containsValue("data.watches[1].status_code", "NO_ACTION"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByStatusCodeWhichDoesNotOccursInStatuses() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureWatchWithActionOnCriticalSeverity("critical-severity-action-1",
            INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25);
        predefinedWatches.defineTemperatureWatchWithActionOnCriticalSeverity("critical-severity-action-2",
            INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25);
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 4);
            String requestBody = DocNode.of("status_codes", Collections.singletonList("notUsedStatus")).toJsonString();

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=+status_code", requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 0));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByMultipleStatusCodesStatusCodeWhichDoesNotOccursInStatuses() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureWatchWithActionOnCriticalSeverity("critical-severity-action-1",
            INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25);
        predefinedWatches.defineTemperatureWatchWithActionOnCriticalSeverity("critical-severity-action-2",
            INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25);
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 4);
            String requestBody = DocNode.of("status_codes", Arrays.asList("NO_ACTION", "ACTION_THROTTLED", "ACTION_EXECUTED"))//
                .toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 4));
            assertThat(body, valueSatisfiesMatcher("data.watches[0].watch_id", String.class, startsWith("critical-severity-action")));
            assertThat(body, containsValue("data.watches[0].status_code", "NO_ACTION"));
            assertThat(body, valueSatisfiesMatcher("data.watches[1].watch_id", String.class, startsWith("critical-severity-action")));
            assertThat(body, containsValue("data.watches[1].status_code", "NO_ACTION"));
            assertThat(body, valueSatisfiesMatcher("data.watches[2].watch_id", String.class, startsWith("temp-")));
            assertThat(body, containsAnyValues("data.watches[2].status_code", "ACTION_EXECUTED", "ACTION_THROTTLED"));
            assertThat(body, valueSatisfiesMatcher("data.watches[3].watch_id", String.class, startsWith("temp-")));
            assertThat(body, containsAnyValues("data.watches[3].status_code", "ACTION_EXECUTED", "ACTION_THROTTLED"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByWatchIdOne() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("one", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("two", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("three", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("one-and-two", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 4);
            String requestBody = DocNode.of("watch_id", "one").toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 2));
            assertThat(body, containsValue("data.watches[0].watch_id","one-and-two"));
            assertThat(body, containsValue("data.watches[1].watch_id","one"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByWatchIdThree() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("one", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("two", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("three", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("one-and-two", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 4);
            String requestBody = DocNode.of("watch_id", "three").toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 1));
            assertThat(body, containsValue("data.watches[0].watch_id","three"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldNotFindAnyWatchDuringFilteringById() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("one", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("two", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("three", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("one-and-two", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 4);
            String requestBody = DocNode.of("watch_id", "no-such-watch-id").toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 0));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByCriticalSeverity() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-4", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 3);
            String requestBody = DocNode.of("severities", Collections.singletonList("critical")).toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 1));
            assertThat(body, containsValue("data.watches[0].watch_id", "temp-4"));
            assertThat(body, containsValue("data.watches[0].severity", "critical"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByInfoOrErrorSeverity() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-4", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 3);
            String requestBody = DocNode.of("severities", Arrays.asList("error", "info")).toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 3));
            assertThat(body, containsValue("data.watches[0].watch_id", "temp-3"));
            assertThat(body, containsValue("data.watches[0].severity", "error"));
            assertThat(body, containsAnyValues("data.watches[1].watch_id", "temp-1", "temp-2"));
            assertThat(body, containsValue("data.watches[1].severity", "info"));
            assertThat(body, containsAnyValues("data.watches[2].watch_id", "temp-1", "temp-2"));
            assertThat(body, containsValue("data.watches[2].severity", "info"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByWarningSeverity() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-4", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 3);
            String requestBody = DocNode.of("severities", Collections.singletonList("warning")).toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 0));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByEqualNumeric4SeverityLeve() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-4", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 3);
            String requestBody = DocNode.of("level_numeric_equal_to", 4).toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 1));
            assertThat(body, containsValue("data.watches[0].watch_id", "temp-4"));
            assertThat(body, containsValue("data.watches[0].severity_details.level_numeric", 4));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByEqualNumeric3SeverityLeve() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-4", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 3);
            String requestBody = DocNode.of("level_numeric_equal_to", 3).toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 1));
            assertThat(body, containsValue("data.watches[0].watch_id", "temp-3"));
            assertThat(body, containsValue("data.watches[0].severity_details.level_numeric", 3));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByGreaterNumericSeverity1Leve() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-4", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 3);
            String requestBody = DocNode.of("level_numeric_greater_than", 1).toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 2));
            assertThat(body, containsValue("data.watches[0].watch_id", "temp-4"));
            assertThat(body, containsValue("data.watches[0].severity_details.level_numeric", 4));
            assertThat(body, containsValue("data.watches[1].watch_id", "temp-3"));
            assertThat(body, containsValue("data.watches[1].severity_details.level_numeric", 3));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByGreaterNumericSeverity2Leve() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-4", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 3);
            String requestBody = DocNode.of("level_numeric_greater_than", 2).toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 2));
            assertThat(body, containsValue("data.watches[0].watch_id", "temp-4"));
            assertThat(body, containsValue("data.watches[0].severity_details.level_numeric", 4));
            assertThat(body, containsValue("data.watches[1].watch_id", "temp-3"));
            assertThat(body, containsValue("data.watches[1].severity_details.level_numeric", 3));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByGreaterNumericSeverity3Leve() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-4", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 3);
            String requestBody = DocNode.of("level_numeric_greater_than", 3).toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 1));
            assertThat(body, containsValue("data.watches[0].watch_id", "temp-4"));
            assertThat(body, containsValue("data.watches[0].severity_details.level_numeric", 4));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByLessNumericSeverity3Leve() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-4", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 3);
            String requestBody = DocNode.of("level_numeric_less_than", 3).toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 2));
            assertThat(body, containsAnyValues("data.watches[0].watch_id", "temp-1", "temp-2"));
            assertThat(body, containsValue("data.watches[0].severity_details.level_numeric", 1));
            assertThat(body, containsAnyValues("data.watches[1].watch_id", "temp-1", "temp-2"));
            assertThat(body, containsValue("data.watches[1].severity_details.level_numeric", 1));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByLessNumericSeverity4Leve() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-4", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 3);
            String requestBody = DocNode.of("level_numeric_less_than", 4).toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 3));
            assertThat(body, containsAnyValues("data.watches[0].watch_id", "temp-3"));
            assertThat(body, containsValue("data.watches[0].severity_details.level_numeric", 3));
            assertThat(body, containsValue("data.watches[1].severity_details.level_numeric", 1));
            assertThat(body, containsValue("data.watches[2].severity_details.level_numeric", 1));
            assertThat(body, anyOf(
                containsAnyValues("data.watches[1].watch_id", "temp-1"),
                containsAnyValues("data.watches[1].watch_id", "temp-2")
            ));
            assertThat(body, anyOf(
                containsAnyValues("data.watches[2].watch_id", "temp-1"),
                containsAnyValues("data.watches[2].watch_id", "temp-2")
            ));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByNumericSeverityLeveBetweenGivenValues() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-4", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 3);
            String requestBody = DocNode.of("level_numeric_greater_than", 2,"level_numeric_less_than", 4).toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 1));
            assertThat(body, containsAnyValues("data.watches[0].watch_id", "temp-3"));
            assertThat(body, containsValue("data.watches[0].severity_details.level_numeric", 3));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldDetectIncorrectRangeCriteria() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("temp-1", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-2", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-3", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "createAlarm");
        predefinedWatches.defineTemperatureSeverityWatch("temp-4", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "createAlarm");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 3);
            String requestBody = DocNode.of("level_numeric_greater_than", 2,"level_numeric_less_than", 4)//
                .with("level_numeric_equal_to", 1)//
                .toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(400));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containSubstring("error.message", "Incorrect search criteria"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void sortingByNonExistingFieldShouldNotCauseException() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineSimpleTemperatureWatch("high-temp-alerts", INDEX_NAME_WATCHED_2, INDEX_ALARMS, .05);

        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            await().until(() -> predefinedWatches.getCountOfDocuments(INDEX_ALARMS) > 0);
            String sorting = "actions.not-existing-action.triggered";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 1));
            assertThat(body, containsValue("data.watches[0].watch_id", "high-temp-alerts"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void sortingByNonExistingFieldShouldNotCauseExceptionNoWatches() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            deleteWatchStateAndAlarms();
            String sorting = "actions.not-existing-action.triggered";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, EMPTY_JSON_BODY);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 0));
        }
    }

    @Test
    public void shouldFilterByActionsNames() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("one", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "actionOne");
        predefinedWatches.defineTemperatureSeverityWatch("two", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "actionTwo");
        predefinedWatches.defineTemperatureSeverityWatch("three", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "actionThree");
        predefinedWatches.defineTemperatureSeverityWatch("four", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "actionFour");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 4);
            String requestBody = DocNode.of("actions", Arrays.asList("actionOne", "actionTwo")).toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 2));
            assertThat(body, containsValue("data.watches[0].watch_id","two"));
            assertThat(body, containsValue("data.watches[1].watch_id","one"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterOutAllWatchesByActionsNames() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("one", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "actionOne");
        predefinedWatches.defineTemperatureSeverityWatch("two", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "actionTwo");
        predefinedWatches.defineTemperatureSeverityWatch("three", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "actionThree");
        predefinedWatches.defineTemperatureSeverityWatch("four", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "actionFour");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 4);
            String requestBody = DocNode.of("actions", Arrays.asList("actionFive")).toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 0));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByActionTimeRanges() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("one", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "actionOne");
        predefinedWatches.defineTemperatureSeverityWatch("two", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "actionTwo");
        predefinedWatches.defineTemperatureSeverityWatch("three", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "actionThree");
        predefinedWatches.defineTemperatureSeverityWatch("four", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "actionFour");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 4);
            String requestBody = DocNode.wrap(ImmutableMap.of(
                    "actions.actionFour.triggeredAfter", Instant.now().minus(1, ChronoUnit.DAYS),
                    "actions.actionFour.checkedAfter", Instant.now().minus(1, ChronoUnit.DAYS),
                    "actions.actionFour.executionAfter", Instant.now().minus(1, ChronoUnit.DAYS),
                    "actions.actionFour.triggeredBefore", Instant.now().plus(1, ChronoUnit.DAYS),
                    "actions.actionFour.checkedBefore", Instant.now().plus(1, ChronoUnit.DAYS))).toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 1));
            assertThat(body, containsValue("data.watches[0].watch_id","four"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterOutAllWatchesByActionTimeRanges() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("one", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "actionOne");
        predefinedWatches.defineTemperatureSeverityWatch("two", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "actionTwo");
        predefinedWatches.defineTemperatureSeverityWatch("three", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "actionThree");
        predefinedWatches.defineTemperatureSeverityWatch("four", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "actionFour");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 4);
            String requestBody = DocNode.wrap(ImmutableMap.of(
                "actions.actionFour.triggeredAfter", Instant.now().plus(1, ChronoUnit.DAYS),
                "actions.actionFour.checkedAfter", Instant.now().plus(1, ChronoUnit.DAYS),
                "actions.actionFour.executionAfter", Instant.now().plus(1, ChronoUnit.DAYS),
                "actions.actionFour.triggeredBefore", Instant.now().minus(1, ChronoUnit.DAYS),
                "actions.actionFour.checkedBefore", Instant.now().minus(1, ChronoUnit.DAYS))).toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 0));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByProperties() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("one", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "actionOne");
        predefinedWatches.defineTemperatureSeverityWatch("two", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "actionTwo");
        predefinedWatches.defineTemperatureSeverityWatch("three", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "actionThree");
        predefinedWatches.defineTemperatureSeverityWatch("four", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "actionFour");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 4);
            String requestBody = DocNode.wrap(ImmutableMap.of(
                "actions.actionOne.statusCode", "ACTION_THROTTLED",
                "actions.actionOne.checkResult", true)
            ).toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 1));
            assertThat(body, containsValue("data.watches[0].watch_id","one"));
            assertThat(body, containsValue("data.watches[0].actions.actionOne.status_code","ACTION_THROTTLED"));
            assertThat(body, containsValue("data.watches[0].actions.actionOne.check_result",true));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterByVariousCommonFieldsAndValues() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("one", INDEX_NAME_WATCHED_1, INDEX_ALARMS, 50.0, "action");
        predefinedWatches.defineTemperatureSeverityWatch("two", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .1, "action");
        predefinedWatches.defineTemperatureSeverityWatch("three", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "anotherAction");
        predefinedWatches.defineTemperatureSeverityWatch("four", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "anotherAction");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 4);
            String requestBody = DocNode.wrap(ImmutableMap.of(
                "actions", Arrays.asList("action"),
                "actions.action.checkResult", false)
            ).toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 1));
            assertThat(body, containsValue("data.watches[0].watch_id","one"));
            assertThat(body, containsValue("data.watches[0].status_code","NO_ACTION"));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    @Test
    public void shouldFilterOutAllWatchesByProperties() throws Exception {
        PredefinedWatches predefinedWatches = new PredefinedWatches(cluster, USER_ADMIN, "_main");
        predefinedWatches.defineTemperatureSeverityWatch("one", INDEX_NAME_WATCHED_1, INDEX_ALARMS, .25, "actionOne");
        predefinedWatches.defineTemperatureSeverityWatch("two", INDEX_NAME_WATCHED_3, INDEX_ALARMS, .25, "actionTwo");
        predefinedWatches.defineTemperatureSeverityWatch("three", INDEX_NAME_WATCHED_4, INDEX_ALARMS, .25, "actionThree");
        predefinedWatches.defineTemperatureSeverityWatch("four", INDEX_NAME_WATCHED_5, INDEX_ALARMS, .25, "actionFour");
        try (GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
            waitForWatchStatuses(predefinedWatches, 4);
            String requestBody = DocNode.wrap(ImmutableMap.of(
                "actions.actionOne.statusCode", "ACTION_THROTTLED",
                "actions.actionOne.checkResult", false)
            ).toJsonString();
            String sorting = "-severity_details.level_numeric";

            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary?sorting=" + sorting, requestBody);

            log.info("Watch summary response body '{}'.", response.getBody());
            assertThat(response.getStatusCode(), equalTo(200));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("data.watches", 0));
        } finally {
            predefinedWatches.deleteWatches();
        }
    }

    private static void waitForFirstActionNonEmptyStatus(GenericRestClient restClient) {
        await().ignoreException(AssertionError.class).until(() -> {
            HttpResponse response = restClient.postJson("/_signals/watch/_main/summary", "{}");
            log.info("Waiting for status code of the first action state, body '{}'.", response.getBody());
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, valueSatisfiesMatcher("data.watches[0].status_code", String.class, Matchers.not(Matchers.isEmptyOrNullString())));
            return true;
        });
    }

    private static void waitForWatchStatuses(PredefinedWatches predefinedWatches, int expectedNumberOfWatchStatuses) {
        await().pollDelay(500, MILLISECONDS)//
            .until(() -> predefinedWatches.countWatchStatusWithAvailableStatusCode(INDEX_SIGNALS_WATCHES_STATE) >= //
                expectedNumberOfWatchStatuses);
    }
}