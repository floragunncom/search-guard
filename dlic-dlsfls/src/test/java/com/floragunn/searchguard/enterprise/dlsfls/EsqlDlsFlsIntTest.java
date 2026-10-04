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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.common.settings.Settings;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestData;
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
 */
public class EsqlDlsFlsIntTest {

    static final int DOC_COUNT = 200;
    static final TestData TEST_DATA = TestData.documentCount(DOC_COUNT).seed(1).get();

    static final String INDEX_NAME = "logs";
    static final String VIEW_COUNT_BY_DEPT = "view_logs_count_by_dept";
    static final String VIEW_IPS = "view_logs_ips";
    static final String VIEW_LOCS = "view_logs_locs";

    static final String HEX_HASH = "[0-9a-f]+";
    static final String IP_ADDRESS = "[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+";

    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin")
            .roles(new Role("all_access").indexPermissions("*").on("*").clusterPermissions("*"));

    static final TestSgConfig.User DEPT_A_USER = new TestSgConfig.User("dept_a").roles(
            new Role("dept_a").indexPermissions("SGS_READ").dls(DocNode.of("prefix.dept.value", "dept_a")).on("logs*", "view_*").clusterPermissions("*"));

    static final TestSgConfig.User DEPT_D_USER = new TestSgConfig.User("dept_d").roles(
            new Role("dept_d").indexPermissions("SGS_READ").dls(DocNode.of("term.dept.value", "dept_d")).on("logs*", "view_*").clusterPermissions("*"));

    static final TestSgConfig.User EXCLUDE_IP_USER = new TestSgConfig.User("exclude_ip").roles(
            new Role("exclude_ip").indexPermissions("SGS_READ").fls("~*_ip").on("logs*", "view_*").clusterPermissions("*"));

    static final TestSgConfig.User INCLUDE_LOC_USER = new TestSgConfig.User("include_loc").roles(
            new Role("include_loc").indexPermissions("SGS_READ").fls("*_loc", "dept").on("logs*", "view_*").clusterPermissions("*"));

    static final TestSgConfig.User HASHED_IP_USER = new TestSgConfig.User("hashed_ip").roles(
            new Role("hashed_ip").indexPermissions("SGS_READ").maskedFields("*_ip").on("logs*", "view_*").clusterPermissions("*"));

    static final TestSgConfig.User HASHED_LOC_USER = new TestSgConfig.User("hashed_loc").roles(
            new Role("hashed_loc").indexPermissions("SGS_READ").maskedFields("*_loc").on("logs*", "view_*").clusterPermissions("*"));

    /**
     * DLS, FLS and field masking combined in one role
     */
    static final TestSgConfig.User COMBINED_USER = new TestSgConfig.User("combined").roles(new Role("combined").indexPermissions("SGS_READ")
            .dls(DocNode.of("prefix.dept.value", "dept_a")).fls("~*_ip").maskedFields("*_loc").on("logs*", "view_*").clusterPermissions("*"));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .users(ADMIN, DEPT_A_USER, DEPT_D_USER, EXCLUDE_IP_USER, INCLUDE_LOC_USER, HASHED_IP_USER, HASHED_LOC_USER, COMBINED_USER)
            .resources("dlsfls")
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
        return docCountByDept().entrySet().stream().filter((e) -> e.getKey().startsWith(deptPrefix)).sorted(Map.Entry.comparingByKey())
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
     * Known limitation: Field masking replaces the doc values of a field by a hash. For fields of type ip, ES|QL hands the
     * doc values to its IP column type; the hash cannot be decoded as an IP address and Elasticsearch aborts the response
     * ("Failed trying to format bytes as IP address", the HTTP connection is closed). Field masking must produce a valid
     * (16 byte) IP address for such fields to be usable with ES|QL (and with doc value based features of _search, like
     * terms aggregations). Masking of keyword fields works, see fm_hashedLoc.
     */
    @Ignore("Field masking of ip typed fields is not compatible with ES|QL; see comment")
    @Test
    public void fm_hashedIp() throws Exception {
        GenericRestClient.HttpResponse response = query(ADMIN, "FROM logs | KEEP source_ip | LIMIT 20");
        assertThat(response, isOk());
        assertThat(response, json(nodeAt("values[*][0]", everyItem(matchesPattern(IP_ADDRESS)))));

        response = query(HASHED_IP_USER, "FROM logs | KEEP source_ip | LIMIT 20");
        assertThat(response, isOk());
        assertThat(response.getBody(), response.getBodyAsDocNode().findNodesByJsonPath("values[*][0]").size(), equalTo(20));
        assertThat(response, json(nodeAt("values[*][0]", everyItem(matchesPattern(HEX_HASH)))));
        assertThat(response, json(nodeAt("values[*][0]", everyItem(not(matchesPattern(IP_ADDRESS))))));
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
