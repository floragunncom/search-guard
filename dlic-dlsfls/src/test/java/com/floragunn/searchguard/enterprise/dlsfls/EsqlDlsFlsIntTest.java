/*
 * Copyright 2026 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */

package com.floragunn.searchguard.enterprise.dlsfls;

import static com.floragunn.searchguard.test.RestMatchers.isBadRequest;
import static com.floragunn.searchguard.test.RestMatchers.isCreated;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.common.settings.Settings;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestData;
import com.floragunn.searchguard.test.TestDataStream;
import com.floragunn.searchguard.test.TestIndexTemplate;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * DLS, FLS and field masking applied to ES|QL queries, with and without views.
 *
 * ES|QL reads the shards via indices:data/read/esql/data requests on the data nodes. DLS, FLS and field masking are
 * enforced there by the DLS/FLS DirectoryReader wrapper, independently of the query (direct FROM or view). FLS
 * additionally hides the fields from the field capabilities ES|QL uses to resolve columns, thus a query referencing a
 * hidden column fails with "Unknown column".
 *
 * Covered: index logs, data stream ds_logs, views over both, LOOKUP JOIN with a DLS restricted lookup index.
 *
 * Field masking of ip typed fields: ES|QL reads the masked (hashed) doc values into an ip typed column, which cannot be
 * decoded. Search Guard rejects such queries with HTTP 400 (see fm_hashedIp).
 */
public class EsqlDlsFlsIntTest {

    static final int DOC_COUNT = 200;
    static final TestData TEST_DATA = TestData.documentCount(DOC_COUNT).seed(1).get();

    static final String INDEX_NAME = "logs";
    static final TestData TEST_DATA_DS = TestData.documentCount(100).seed(2).timestampColumnName("@timestamp").deletedDocumentFraction(0).get();
    static final TestDataStream DS_LOGS = new TestDataStream("ds_logs", TEST_DATA_DS);
    static final String LOOKUP_INDEX = "dept_info";
    static final String VIEW_COUNT_BY_DEPT = "view_logs_count_by_dept";
    static final String VIEW_IPS = "view_logs_ips";
    static final String VIEW_LOCS = "view_logs_locs";

    static final String HEX_HASH = "[0-9a-f]+";
    static final String IP_ADDRESS = "[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+";

    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin")
            .roles(new Role("all_access").indexPermissions("*").on("*").clusterPermissions("*"));

    static final TestSgConfig.User DEPT_A_USER = new TestSgConfig.User("dept_a").roles(
            new Role("dept_a").indexPermissions("SGS_READ").dls(DocNode.of("prefix.dept.value", "dept_a")).on("logs*", "view_*")
                    .indexPermissions("SGS_READ").on(LOOKUP_INDEX).dataStreamPermissions("SGS_READ").dls(DocNode.of("prefix.dept.value", "dept_a"))
                    .on("ds_*").clusterPermissions("*"));

    /**
     * Unrestricted on logs, but DLS on the lookup index: only the row for dept_a_1 is visible
     */
    static final TestSgConfig.User LOOKUP_DLS_USER = new TestSgConfig.User("lookup_dls").roles(new Role("lookup_dls")
            .indexPermissions("SGS_READ").on("logs*").indexPermissions("SGS_READ").dls(DocNode.of("term.dept_key.value", "dept_a_1")).on(LOOKUP_INDEX)
            .clusterPermissions("*"));

    static final TestSgConfig.User DEPT_D_USER = new TestSgConfig.User("dept_d").roles(
            new Role("dept_d").indexPermissions("SGS_READ").dls(DocNode.of("term.dept.value", "dept_d")).on("logs*", "view_*").clusterPermissions("*"));

    static final TestSgConfig.User EXCLUDE_IP_USER = new TestSgConfig.User("exclude_ip").roles(
            new Role("exclude_ip").indexPermissions("SGS_READ").fls("~*_ip").on("logs*", "view_*").dataStreamPermissions("SGS_READ").fls("~*_ip")
                    .on("ds_*").clusterPermissions("*"));

    static final TestSgConfig.User INCLUDE_LOC_USER = new TestSgConfig.User("include_loc").roles(
            new Role("include_loc").indexPermissions("SGS_READ").fls("*_loc", "dept").on("logs*", "view_*").clusterPermissions("*"));

    static final TestSgConfig.User HASHED_IP_USER = new TestSgConfig.User("hashed_ip").roles(
            new Role("hashed_ip").indexPermissions("SGS_READ").maskedFields("*_ip").on("logs*", "view_*").clusterPermissions("*"));

    static final TestSgConfig.User HASHED_LOC_USER = new TestSgConfig.User("hashed_loc").roles(
            new Role("hashed_loc").indexPermissions("SGS_READ").maskedFields("*_loc").on("logs*", "view_*").dataStreamPermissions("SGS_READ")
                    .maskedFields("*_loc").on("ds_*").clusterPermissions("*"));

    /**
     * DLS, FLS and field masking combined in one role
     */
    static final TestSgConfig.User COMBINED_USER = new TestSgConfig.User("combined").roles(new Role("combined").indexPermissions("SGS_READ")
            .dls(DocNode.of("prefix.dept.value", "dept_a")).fls("~*_ip").maskedFields("*_loc").on("logs*", "view_*").clusterPermissions("*"));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .users(ADMIN, DEPT_A_USER, DEPT_D_USER, EXCLUDE_IP_USER, INCLUDE_LOC_USER, HASHED_IP_USER, HASHED_LOC_USER, COMBINED_USER,
                    LOOKUP_DLS_USER)
            .indexTemplates(TestIndexTemplate.DATA_STREAM_MINIMAL).dataStreams(DS_LOGS).resources("dlsfls")
            // ES|QL is only available with a real ES distribution
            .useExternalProcessCluster().build();

    @BeforeClass
    public static void setupTestData() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            TEST_DATA.createIndex(client, INDEX_NAME, Settings.builder().put("index.number_of_shards", 3).build());

            assertThat(client.putJson("/_query/view/" + VIEW_COUNT_BY_DEPT,
                    DocNode.of("query", "FROM " + INDEX_NAME + " | STATS c = COUNT(*) BY dept.keyword | SORT dept.keyword")), isOk());
            assertThat(client.putJson("/_query/view/" + VIEW_IPS, DocNode.of("query", "FROM " + INDEX_NAME + " | KEEP source_ip, dest_ip, dept.keyword")),
                    isOk());
            assertThat(client.putJson("/_query/view/" + VIEW_LOCS,
                    DocNode.of("query", "FROM " + INDEX_NAME + " | KEEP source_loc.keyword, dest_loc.keyword, dept.keyword")), isOk());
            assertThat(client.putJson("/_query/view/view_ds_count_by_dept",
                    DocNode.of("query", "FROM " + DS_LOGS.getName() + " | STATS c = COUNT(*) BY dept.keyword | SORT dept.keyword")), isOk());

            // Lookup index with one row per department
            assertThat(client.putJson("/" + LOOKUP_INDEX, DocNode.of("settings.index.mode", "lookup", "mappings.properties.dept_key.type", "keyword",
                    "mappings.properties.dept_name.type", "keyword")), isOk());
            for (String dept : new String[] { "dept_a_1", "dept_a_2", "dept_a_3", "dept_b_1", "dept_b_2", "dept_c", "dept_d" }) {
                assertThat(client.putJson("/" + LOOKUP_INDEX + "/_doc/" + dept + "?refresh=true", DocNode.of("dept_key", dept, "dept_name", "Department " + dept)),
                        isCreated());
            }
        }
    }

    private static GenericRestClient.HttpResponse query(TestSgConfig.User user, String query) throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query", DocNode.of("query", query));
            return response;
        }
    }

    private static long docCountForDeptPrefix(String prefix) {
        return TEST_DATA.getRetainedDocuments().values().stream().filter((doc) -> ((String) doc.get("dept")).startsWith(prefix)).count();
    }

    private static Map<String, Long> docCountByDept() {
        return TEST_DATA.getRetainedDocuments().values().stream()
                .collect(Collectors.groupingBy((doc) -> (String) doc.get("dept"), Collectors.counting()));
    }

    private static List<List<Object>> expectedCountByDept(String deptPrefix) {
        return expectedCountByDept(TEST_DATA, deptPrefix);
    }

    private static List<List<Object>> expectedCountByDept(TestData testData, String deptPrefix) {
        return testData.getRetainedDocuments().values().stream().collect(Collectors.groupingBy((doc) -> (String) doc.get("dept"), Collectors.counting()))
                .entrySet().stream().filter((e) -> e.getKey().startsWith(deptPrefix)).sorted(Map.Entry.comparingByKey())
                .map((e) -> List.<Object>of(e.getValue().intValue(), e.getKey())).collect(Collectors.toList());
    }

    // --- DLS

    @Test
    public void dls_count() throws Exception {
        assertThat(query(ADMIN, "FROM logs | STATS c = COUNT(*)"),
                json(nodeAt("values", equalTo(List.of(List.of(TEST_DATA.getRetainedDocuments().size()))))));
        assertThat(query(DEPT_A_USER, "FROM logs | STATS c = COUNT(*)"),
                json(nodeAt("values", equalTo(List.of(List.of((int) docCountForDeptPrefix("dept_a")))))));
        assertThat(query(DEPT_D_USER, "FROM logs | STATS c = COUNT(*)"),
                json(nodeAt("values", equalTo(List.of(List.of((int) docCountForDeptPrefix("dept_d")))))));
    }

    @Test
    public void dls_countByDept() throws Exception {
        String esql = "FROM logs | STATS c = COUNT(*) BY dept.keyword | SORT dept.keyword";
        assertThat(query(ADMIN, esql), json(nodeAt("values", equalTo(expectedCountByDept("")))));
        assertThat(query(DEPT_A_USER, esql), json(nodeAt("values", equalTo(expectedCountByDept("dept_a")))));
        assertThat(query(DEPT_D_USER, esql), json(nodeAt("values", equalTo(expectedCountByDept("dept_d")))));
    }

    @Test
    public void dls_rows() throws Exception {
        GenericRestClient.HttpResponse response = query(DEPT_A_USER, "FROM logs | KEEP dept.keyword | LIMIT 1000");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values[*][0]", everyItem(startsWith("dept_a")))));
        assertThat(response, json(nodeAt("values.length()", equalTo((int) docCountForDeptPrefix("dept_a")))));
    }

    @Test
    public void dls_filterOnHiddenDocuments() throws Exception {
        assertThat(query(DEPT_A_USER, "FROM logs | WHERE dept.keyword == \"dept_d\" | STATS c = COUNT(*)"),
                json(nodeAt("values", equalTo(List.of(List.of(0))))));
        assertThat(query(DEPT_D_USER, "FROM logs | WHERE dept.keyword == \"dept_d\" | STATS c = COUNT(*)"),
                json(nodeAt("values", equalTo(List.of(List.of((int) docCountForDeptPrefix("dept_d")))))));
    }

    @Test
    public void dls_view() throws Exception {
        assertThat(query(ADMIN, "FROM " + VIEW_COUNT_BY_DEPT), json(nodeAt("values", equalTo(expectedCountByDept("")))));
        assertThat(query(DEPT_A_USER, "FROM " + VIEW_COUNT_BY_DEPT), json(nodeAt("values", equalTo(expectedCountByDept("dept_a")))));
        assertThat(query(DEPT_D_USER, "FROM " + VIEW_COUNT_BY_DEPT), json(nodeAt("values", equalTo(expectedCountByDept("dept_d")))));
    }

    // --- FLS

    @Test
    public void fls_excludedColumnsAreNotVisible() throws Exception {
        GenericRestClient.HttpResponse response = query(ADMIN, "FROM logs | LIMIT 1");
        assertThat(response, json(nodeAt("columns[*].name", hasItems("source_ip", "dest_ip", "source_loc", "dept", "timestamp"))));

        response = query(EXCLUDE_IP_USER, "FROM logs | LIMIT 1");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("columns[*].name", not(hasItem("source_ip")))));
        assertThat(response, json(nodeAt("columns[*].name", not(hasItem("dest_ip")))));
        assertThat(response, json(nodeAt("columns[*].name", hasItems("source_loc", "dept", "timestamp"))));
    }

    @Test
    public void fls_excludedColumnCannotBeReferenced() throws Exception {
        GenericRestClient.HttpResponse response = query(EXCLUDE_IP_USER, "FROM logs | KEEP source_ip");
        assertThat(response, isBadRequest());
        assertThat(response.getBody(), containsString("source_ip"));

        response = query(EXCLUDE_IP_USER, "FROM logs | WHERE source_ip == \"1.2.3.4\" | STATS c = COUNT(*)");
        assertThat(response, isBadRequest());
    }

    @Test
    public void fls_includeMode() throws Exception {
        GenericRestClient.HttpResponse response = query(INCLUDE_LOC_USER, "FROM logs | LIMIT 1");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("columns[*].name", hasItems("source_loc", "source_loc.keyword", "dest_loc", "dept", "dept.keyword"))));
        assertThat(response, json(nodeAt("columns[*].name", not(hasItem("source_ip")))));
        assertThat(response, json(nodeAt("columns[*].name", not(hasItem("timestamp")))));
        assertThat(response, json(nodeAt("columns[*].name", not(hasItem("object.integer_field")))));

        assertThat(query(INCLUDE_LOC_USER, "FROM logs | KEEP timestamp"), isBadRequest());
        assertThat(query(INCLUDE_LOC_USER, "FROM logs | STATS c = COUNT(*) BY dept.keyword | SORT dept.keyword"),
                json(nodeAt("values", equalTo(expectedCountByDept("")))));
    }

    @Test
    public void fls_view() throws Exception {
        assertThat(query(ADMIN, "FROM " + VIEW_IPS + " | LIMIT 1"), isOk());
        // The view references the hidden columns source_ip and dest_ip
        assertThat(query(EXCLUDE_IP_USER, "FROM " + VIEW_IPS + " | LIMIT 1"), isBadRequest());
    }

    // --- Field masking

    /**
     * Field masking replaces the doc values of a field by a hash. For fields of type ip, ES|QL hands the doc values to its ip
     * column type; the hash cannot be decoded as an IP address. Search Guard rejects such queries with HTTP 400 (otherwise,
     * Elasticsearch would abort the response while writing it and close the connection). Masking of keyword fields works,
     * see fm_hashedLoc. Queries which do not touch the masked ip field are not affected.
     */
    @Test
    public void fm_hashedIp() throws Exception {
        GenericRestClient.HttpResponse response = query(ADMIN, "FROM logs | KEEP source_ip | LIMIT 20");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values[*][0]", everyItem(matchesPattern(IP_ADDRESS)))));

        response = query(HASHED_IP_USER, "FROM logs | KEEP source_ip | LIMIT 20");
        assertThat(response, isBadRequest("error.reason", "*Field masking for fields of type ip is currently not supported for ES|QL*"));

        response = query(HASHED_IP_USER, "FROM logs | STATS c = COUNT(*) BY source_ip");
        assertThat(response, isBadRequest("error.reason", "*Field masking for fields of type ip is currently not supported for ES|QL*"));

        response = query(HASHED_IP_USER, "FROM " + VIEW_IPS + " | LIMIT 20");
        assertThat(response, isBadRequest("error.reason", "*Field masking for fields of type ip is currently not supported for ES|QL*"));

        // Queries which do not use the masked field work
        assertThat(query(HASHED_IP_USER, "FROM logs | STATS c = COUNT(*) BY dept.keyword | SORT dept.keyword"),
                json(nodeAt("values", equalTo(expectedCountByDept("")))));
        response = query(HASHED_IP_USER, "FROM logs | KEEP source_loc.keyword | LIMIT 20");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values[*][0]", everyItem(not(matchesPattern(HEX_HASH))))));
    }

    @Test
    public void fm_hashedLoc() throws Exception {
        GenericRestClient.HttpResponse response = query(ADMIN, "FROM logs | KEEP source_loc.keyword | LIMIT 20");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values[*][0]", everyItem(not(matchesPattern(HEX_HASH))))));

        response = query(HASHED_LOC_USER, "FROM logs | KEEP source_loc.keyword | LIMIT 20");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values[*][0]", everyItem(matchesPattern(HEX_HASH)))));

        // Aggregating on a masked field must not reveal the clear text values
        response = query(HASHED_LOC_USER, "FROM logs | STATS c = COUNT(*) BY source_loc.keyword");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values[*][1]", everyItem(matchesPattern(HEX_HASH)))));
    }

    @Test
    public void fm_view() throws Exception {
        GenericRestClient.HttpResponse response = query(ADMIN, "FROM " + VIEW_LOCS + " | LIMIT 20");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values[*][0]", everyItem(not(matchesPattern(HEX_HASH))))));

        response = query(HASHED_LOC_USER, "FROM " + VIEW_LOCS + " | LIMIT 20");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values[*][0]", everyItem(matchesPattern(HEX_HASH)))));
        assertThat(response, json(nodeAt("values[*][1]", everyItem(matchesPattern(HEX_HASH)))));
        assertThat(response, json(nodeAt("values[*][2]", everyItem(startsWith("dept_")))));
    }

    // --- Async queries

    /**
     * DLS, FLS and field masking are applied while the query is running, i.e. at submit time. The GET request only reads
     * the stored result. See EsqlAsyncIntTest for the authorization of the async requests themselves.
     */
    @Test
    public void async_dlsAndFieldMasking() throws Exception {
        String asyncId = submitAsync(DEPT_A_USER, "FROM logs | STATS c = COUNT(*)");
        GenericRestClient.HttpResponse response = getAsyncResult(DEPT_A_USER, asyncId);
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values", equalTo(List.of(List.of((int) docCountForDeptPrefix("dept_a")))))));

        asyncId = submitAsync(HASHED_LOC_USER, "FROM logs | KEEP source_loc.keyword | LIMIT 20");
        response = getAsyncResult(HASHED_LOC_USER, asyncId);
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values[*][0]", everyItem(matchesPattern(HEX_HASH)))));
    }

    private static String submitAsync(TestSgConfig.User user, String query) throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            GenericRestClient.HttpResponse response = client.postJson("/_query/async",
                    DocNode.of("query", query, "wait_for_completion_timeout", "0s", "keep_on_completion", true));
            assertThat(response, isOk());
            return response.getBodyAsDocNode().getAsString("id");
        }
    }

    private static GenericRestClient.HttpResponse getAsyncResult(TestSgConfig.User user, String asyncId) throws Exception {
        try (GenericRestClient client = cluster.getRestClient(user)) {
            return client.get("/_query/async/" + asyncId + "?wait_for_completion_timeout=30s");
        }
    }

    // --- Data streams

    @Test
    public void dataStream_dls() throws Exception {
        String esql = "FROM " + DS_LOGS.getName() + " | STATS c = COUNT(*) BY dept.keyword | SORT dept.keyword";
        assertThat(query(ADMIN, esql), json(nodeAt("values", equalTo(expectedCountByDept(TEST_DATA_DS, "")))));
        assertThat(query(DEPT_A_USER, esql), json(nodeAt("values", equalTo(expectedCountByDept(TEST_DATA_DS, "dept_a")))));
        assertThat(query(DEPT_A_USER, "FROM view_ds_count_by_dept"), json(nodeAt("values", equalTo(expectedCountByDept(TEST_DATA_DS, "dept_a")))));
        assertThat(query(DEPT_A_USER, "FROM " + DS_LOGS.getName() + " | STATS c = COUNT(*)"),
                json(nodeAt("values", equalTo(List.of(List.of(expectedCountByDept(TEST_DATA_DS, "dept_a").stream().mapToInt((r) -> (Integer) r.get(0)).sum()))))));
    }

    @Test
    public void dataStream_fls() throws Exception {
        GenericRestClient.HttpResponse response = query(EXCLUDE_IP_USER, "FROM " + DS_LOGS.getName() + " | LIMIT 1");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("columns[*].name", not(hasItem("source_ip")))));
        assertThat(response, json(nodeAt("columns[*].name", hasItems("source_loc", "dept", "@timestamp"))));
        assertThat(query(EXCLUDE_IP_USER, "FROM " + DS_LOGS.getName() + " | KEEP source_ip"), isBadRequest());
    }

    @Test
    public void dataStream_fm() throws Exception {
        GenericRestClient.HttpResponse response = query(HASHED_LOC_USER, "FROM " + DS_LOGS.getName() + " | KEEP source_loc.keyword | LIMIT 20");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values[*][0]", everyItem(matchesPattern(HEX_HASH)))));
    }

    // --- LOOKUP JOIN

    @Test
    public void lookupJoin_dlsOnLookupIndex() throws Exception {
        String esql = "FROM logs | RENAME dept.keyword AS dept_key | LOOKUP JOIN " + LOOKUP_INDEX
                + " ON dept_key | STATS c = COUNT(*) BY dept_name | SORT dept_name";

        GenericRestClient.HttpResponse response = query(ADMIN, esql);
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values.length()", equalTo(docCountByDept().size()))));
        assertThat(response, json(nodeAt("values[*][1]", everyItem(startsWith("Department dept_")))));

        // Only the lookup row for dept_a_1 is visible; all other documents get a null dept_name
        response = query(LOOKUP_DLS_USER, esql);
        assertThat(response, isOk());
        int deptA1 = docCountByDept().get("dept_a_1").intValue();
        int others = TEST_DATA.getRetainedDocuments().size() - deptA1;
        assertThat(response, json(nodeAt("values", equalTo(List.of(List.of(deptA1, "Department dept_a_1"), Arrays.asList(others, null))))));
    }

    @Test
    public void lookupJoin_dlsOnSourceIndex() throws Exception {
        String esql = "FROM logs | RENAME dept.keyword AS dept_key | LOOKUP JOIN " + LOOKUP_INDEX
                + " ON dept_key | STATS c = COUNT(*) BY dept_name | SORT dept_name";
        List<List<Object>> expected = expectedCountByDept("dept_a").stream()
                .map((r) -> List.<Object>of(r.get(0), "Department " + r.get(1))).collect(Collectors.toList());

        assertThat(query(DEPT_A_USER, esql), json(nodeAt("values", equalTo(expected))));
    }

    // --- Combined

    @Test
    public void combined() throws Exception {
        GenericRestClient.HttpResponse response = query(COMBINED_USER, "FROM logs | LIMIT 1000");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("columns[*].name", not(hasItem("source_ip")))));
        assertThat(response, json(nodeAt("columns[*].name", not(hasItem("dest_ip")))));
        assertThat(response, json(nodeAt("values.length()", equalTo((int) docCountForDeptPrefix("dept_a")))));

        response = query(COMBINED_USER, "FROM logs | KEEP dept.keyword, source_loc.keyword | LIMIT 1000");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values[*][0]", everyItem(startsWith("dept_a")))));
        assertThat(response, json(nodeAt("values[*][1]", everyItem(matchesPattern(HEX_HASH)))));

        assertThat(query(COMBINED_USER, "FROM " + VIEW_COUNT_BY_DEPT), json(nodeAt("values", equalTo(expectedCountByDept("dept_a")))));
        assertThat(query(COMBINED_USER, "FROM " + VIEW_IPS), isBadRequest());
    }
}
