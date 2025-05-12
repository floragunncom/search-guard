/*
 * Copyright 2025 floragunn GmbH
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

package com.floragunn.searchguard.authz.int_tests;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.awaitility.Awaitility;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsFieldPointedByJsonPath;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsList;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.docNodeSizeEqualTo;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.listDoesNotContain;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IndexAuthorizationEsqlIntTests {

    private static final String INDEX_EMPLOYEES_PREFIX = "employees";
    private static final String INDEX_EMPLOYEES_INTERNAL = INDEX_EMPLOYEES_PREFIX + "-internal";
    private static final String INDEX_EMPLOYEES_CONTRACTORS = INDEX_EMPLOYEES_PREFIX + "-contractors";
    private static final String INDEX_EMPLOYEES_POSITIONS = INDEX_EMPLOYEES_PREFIX + "_positions";
    private static final String ALIAS_PREFIX = "alias";
    private static final String ALIAS_EMPLOYEES_INTERNAL = ALIAS_PREFIX + "-" + INDEX_EMPLOYEES_INTERNAL;
    private static final String ALIAS_EMPLOYEES_CONTRACTORS = ALIAS_PREFIX + "-" + INDEX_EMPLOYEES_CONTRACTORS;
    private static final String ALIAS_EMPLOYEES_POSITIONS = ALIAS_PREFIX + "-" + INDEX_EMPLOYEES_POSITIONS;
    private static final String DS_PREFIX = "ds";
    private static final String DS_EMPLOYEES_INTERNAL_LOGIN = DS_PREFIX + "-" + INDEX_EMPLOYEES_INTERNAL + "_login";
    private static final String DS_EMPLOYEES_CONTRACTORS_LOGIN = DS_PREFIX + "-" + INDEX_EMPLOYEES_CONTRACTORS + "_login";
    private static final String RUN_QUERY_PATH = "/_query?format=json";
    private static final String RUN_ASYNC_QUERY_PATH = "/_query/async?format=json";

    static TestSgConfig.User UNLIMITED_USER = new TestSgConfig.User("unlimited_user")//
            .roles(//
                    new TestSgConfig.Role("r1")//
                            .clusterPermissions("*")
                            .indexPermissions("*").on("*")
                            .aliasPermissions("*").on("*")
                            .dataStreamPermissions("*").on("*")

            );

    static TestSgConfig.User READ_ONLY_USER = new TestSgConfig.User("read_only_user")//
            .roles(//
                    new TestSgConfig.Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO")
                            .indexPermissions("SGS_READ").on(INDEX_EMPLOYEES_PREFIX + "*")
                            .aliasPermissions("SGS_READ").on(ALIAS_PREFIX + "*")
                            .dataStreamPermissions("SGS_READ").on(DS_PREFIX + "*")

            );

    static TestSgConfig.User READ_ONLY_ALIASES_DS_USER = new TestSgConfig.User("read_only_aliases_ds_user")//
            .roles(//
                    new TestSgConfig.Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO")
                            .aliasPermissions("SGS_READ").on(ALIAS_PREFIX + "*")
                            .dataStreamPermissions("SGS_READ").on(DS_PREFIX + "*")

            );

    static TestSgConfig.User READ_ONLY_USER_WITH_DLS_FLS = new TestSgConfig.User("read_only_user_with_dls_fls")//
            .roles(//
                    new TestSgConfig.Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO")
                            .indexPermissions("SGS_READ")
                            .dls(DocNode.of("bool.must_not", DocNode.array(DocNode.of("match.first_name", "Sarah"), DocNode.of("match.first_name", "Liam"))))
                            .fls("~hire_date")
                            .on(INDEX_EMPLOYEES_PREFIX + "*")
                            .aliasPermissions("SGS_READ")
                            .dls(DocNode.of("bool.must_not", DocNode.array(DocNode.of("match.first_name", "Sarah"), DocNode.of("match.first_name", "Liam"))))
                            .fls("~hire_date")
                            .on(ALIAS_PREFIX + "*")
                            .dataStreamPermissions("SGS_READ")
                            .dls(DocNode.of("bool.must_not", DocNode.array(DocNode.of("match.employee_id", 1), DocNode.of("match.employee_id", 1001))))
                            .fls("~ip")
                            .on(DS_PREFIX + "*")

            );

    static TestSgConfig.User READ_ONLY_USER_WITHOUT_ESQL_CLUSTER_PERMS = new TestSgConfig
            .User("read_only_user_without_esql_cluster_perm")//
            .roles(//
                    new TestSgConfig.Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO")
                            .excludeClusterPermissions("indices:data/read/esql*")
                            .indexPermissions("SGS_READ").on(INDEX_EMPLOYEES_PREFIX + "*")
                            .aliasPermissions("SGS_READ").on(ALIAS_PREFIX + "*")
                            .dataStreamPermissions("SGS_READ").on(DS_PREFIX + "*")

            );

    static TestSgConfig.User READ_ONLY_EMPLOYEES_USER_WITHOUT_ESQL_INDEX_PERMS = new TestSgConfig
            .User("read_only_employees_user_without_esql_index_perms")//
            .roles(//
                    new TestSgConfig.Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO")
                            .indexPermissions("indices:data/read/esql/search_shards").on(INDEX_EMPLOYEES_INTERNAL)
                            .indexPermissions("indices:data/read/esql/resolve_fields").on(INDEX_EMPLOYEES_CONTRACTORS)
                            .aliasPermissions("indices:data/read/esql/search_shards").on(ALIAS_EMPLOYEES_INTERNAL)
                            .aliasPermissions("indices:data/read/esql/resolve_fields").on(ALIAS_EMPLOYEES_CONTRACTORS)
                            .dataStreamPermissions("indices:data/read/esql/search_shards").on(DS_EMPLOYEES_INTERNAL_LOGIN)
                            .dataStreamPermissions("indices:data/read/esql/resolve_fields").on(DS_EMPLOYEES_CONTRACTORS_LOGIN)
            );

    static TestSgConfig.User READ_ONLY_EMPLOYEES_INTERNAL_USER = new TestSgConfig.User("read_only_employees_internal_user")//
            .roles(//
                    new TestSgConfig.Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO")
                            .indexPermissions("SGS_READ").on(INDEX_EMPLOYEES_INTERNAL)
                            .aliasPermissions("SGS_READ").on(ALIAS_EMPLOYEES_INTERNAL)
                            .dataStreamPermissions("SGS_READ").on(DS_EMPLOYEES_INTERNAL_LOGIN)

            );

    static List<TestSgConfig.User> USERS = ImmutableList.of(
            UNLIMITED_USER, READ_ONLY_USER, READ_ONLY_ALIASES_DS_USER, READ_ONLY_USER_WITH_DLS_FLS,
            READ_ONLY_EMPLOYEES_INTERNAL_USER, READ_ONLY_USER_WITHOUT_ESQL_CLUSTER_PERMS,
            READ_ONLY_EMPLOYEES_USER_WITHOUT_ESQL_INDEX_PERMS
    );

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode()
            .sslEnabled()
            .users(USERS)
            .authzDebug(true)
            .enterpriseModulesEnabled()
            .useExternalProcessCluster()
            .build();

    @BeforeClass
    public static void beforeClass() throws Exception {
        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            DocNode employeesMappings = DocNode.of("mappings.properties", DocNode.of(
                    "employee_id.type", "long",
                    "first_name.type", "text",
                    "last_name.type", "text",
                    "position_id.type", "long",
                    "hire_date.type", "date",
                    "hire_date.format", "yyyy-MM-dd"
            ));
            DocNode positionsIndexConfig = DocNode.of("mappings.properties", DocNode.of(
                    "position_id.type", "long",
                    "position_name.type", "text"
            ), "settings.index.mode", "lookup");

            HttpResponse response = restClient.putJson(INDEX_EMPLOYEES_INTERNAL, employeesMappings);
            assertThat(response.getStatusCode(), is(200));
            response = restClient.putJson(INDEX_EMPLOYEES_CONTRACTORS, employeesMappings);
            assertThat(response.getStatusCode(), is(200));
            response = restClient.putJson(INDEX_EMPLOYEES_POSITIONS, positionsIndexConfig);
            assertThat(response.getStatusCode(), is(200));

            List<DocNode> internalEmployees = Arrays.asList(
                    DocNode.of("employee_id", 1, "first_name", "John", "last_name", "Smith", "position_id", 1, "hire_date", "2020-03-15"),
                    DocNode.of("employee_id", 2, "first_name", "Emily", "last_name", "Johnson", "position_id", 2, "hire_date", "2018-07-22"),
                    DocNode.of("employee_id", 3, "first_name", "Michael", "last_name", "Brown", "position_id", 3, "hire_date", "2019-11-05"),
                    DocNode.of("employee_id", 4, "first_name", "Sarah", "last_name", "Davis", "position_id", 4, "hire_date", "2021-01-10"),
                    DocNode.of("employee_id", 5, "first_name", "David", "last_name", "Wilson", "position_id", 5, "hire_date", "2017-09-30"),
                    DocNode.of("employee_id", 6, "first_name", "Laura", "last_name", "Martinez", "position_id", 6, "hire_date", "2022-02-14"),
                    DocNode.of("employee_id", 7, "first_name", "James", "last_name", "Taylor", "position_id", 7, "hire_date", "2016-06-01"),
                    DocNode.of("employee_id", 8, "first_name", "Olivia", "last_name", "Anderson", "position_id", 8, "hire_date", "2020-10-20"),
                    DocNode.of("employee_id", 9, "first_name", "Daniel", "last_name", "Thomas", "position_id", 9, "hire_date", "2019-04-18"),
                    DocNode.of("employee_id", 10, "first_name", "Mia", "last_name", "Moore", "position_id", 10, "hire_date", "2018-12-12")
            );

            List<DocNode> contractorEmployees = Arrays.asList(
                    DocNode.of("employee_id", 1001, "first_name", "Olivia", "last_name", "Martinez", "position_id", 1, "hire_date", "2021-08-15"),
                    DocNode.of("employee_id", 1002, "first_name", "Liam", "last_name", "Nguyen", "position_id", 2, "hire_date", "2022-03-30"),
                    DocNode.of("employee_id", 1003, "first_name", "Ava", "last_name", "Johnson", "position_id", 3, "hire_date", "2020-11-05"),
                    DocNode.of("employee_id", 1004, "first_name", "Noah", "last_name", "Patel", "position_id", 4, "hire_date", "2019-06-12"),
                    DocNode.of("employee_id", 1005, "first_name", "Emma", "last_name", "Robinson", "position_id", 5, "hire_date", "2023-01-25")
            );

            List<DocNode> positions = Arrays.asList(
                    DocNode.of("position_id", 1, "position_name", "Software Engineer"),
                    DocNode.of("position_id", 2, "position_name", "Marketing Manager"),
                    DocNode.of("position_id", 3, "position_name", "Sales Executive"),
                    DocNode.of("position_id", 4, "position_name", "HR Specialist"),
                    DocNode.of("position_id", 5, "position_name", "Product Designer"),
                    DocNode.of("position_id", 6, "position_name", "Customer Support"),
                    DocNode.of("position_id", 7, "position_name", "Financial Analyst"),
                    DocNode.of("position_id", 8, "position_name", "Content Writer"),
                    DocNode.of("position_id", 9, "position_name", "IT Technician"),
                    DocNode.of("position_id", 10, "position_name", "Business Development Manager")
            );

            String internalEmployeesBulkOperation = String.format("{ \"index\" : { \"_index\" : \"%s\" } }", INDEX_EMPLOYEES_INTERNAL);
            String employeesBulkBody = internalEmployees.stream()
                    .map(employee -> internalEmployeesBulkOperation + "\n" + employee.toJsonString())
                            .collect(Collectors.joining("\n")) + "\n";

            String contractorEmployeesBulkOperation = String.format("{ \"index\" : { \"_index\" : \"%s\" } }", INDEX_EMPLOYEES_CONTRACTORS);
            String otherEmployeesBulkBody = contractorEmployees.stream()
                    .map(employee -> contractorEmployeesBulkOperation + "\n" + employee.toJsonString())
                            .collect(Collectors.joining("\n")) + "\n";

            String positionsBulkOperation = String.format("{ \"index\" : { \"_index\" : \"%s\" } }", INDEX_EMPLOYEES_POSITIONS);
            String positionsBulkBody = positions.stream()
                    .map(position -> positionsBulkOperation + "\n" + position.toJsonString())
                            .collect(Collectors.joining("\n")) + "\n";

            response = restClient.postJson("/_bulk?refresh=true", employeesBulkBody);
            assertThat(response.getStatusCode(), is(200));

            response = restClient.postJson("/_bulk?refresh=true", otherEmployeesBulkBody);
            assertThat(response.getStatusCode(), is(200));

            response = restClient.postJson("/_bulk?refresh=true", positionsBulkBody);
            assertThat(response.getStatusCode(), is(200));

            DocNode createAliasesBody = DocNode.of("actions", DocNode.array(
                    DocNode.of("add.index", INDEX_EMPLOYEES_INTERNAL, "add.alias", ALIAS_EMPLOYEES_INTERNAL),
                    DocNode.of("add.index", INDEX_EMPLOYEES_CONTRACTORS, "add.alias", ALIAS_EMPLOYEES_CONTRACTORS),
                    DocNode.of("add.index", INDEX_EMPLOYEES_POSITIONS, "add.alias", ALIAS_EMPLOYEES_POSITIONS)
            ));

            response = restClient.postJson("/_aliases", createAliasesBody);
            assertThat(response.getStatusCode(), is(200));

            DocNode dsMappingsBody = DocNode.of(
                    "template.mappings.properties.@timestamp.type", "date",
                    "template.mappings.properties.@timestamp.format", "date_optional_time||epoch_millis",
                    "template.mappings.properties.employee_id.type", "long",
                    "template.mappings.properties.ip.type", "ip"
            );

            response = restClient.putJson("_component_template/ds-mappings", dsMappingsBody);
            assertThat(response.getStatusCode(), is(200));

            DocNode dsTemplateBody = DocNode.of(
                    "index_patterns", List.of(DS_PREFIX + "*"),
                    "data_stream", DocNode.EMPTY,
                    "composed_of", List.of("ds-mappings")
            );

            response = restClient.putJson("_index_template/ds-template", dsTemplateBody);
            assertThat(response.getStatusCode(), is(200));

            List<DocNode> employeesInternalLogins = Arrays.asList(
                    DocNode.of("@timestamp", "2025-05-09T14:20:00Z", "employee_id", 1, "ip", "192.168.34.102"),
                    DocNode.of("@timestamp", "2025-04-09T14:20:00Z", "employee_id", 2, "ip", "10.25.77.89"),
                    DocNode.of("@timestamp", "2025-03-09T14:20:00Z", "employee_id", 3, "ip", "172.16.201.45")
            );

            List<DocNode> employeesContractorsLogins = Arrays.asList(
                    DocNode.of("@timestamp", "2025-02-09T14:20:00Z", "employee_id", 1001, "ip", "203.0.113.58"),
                    DocNode.of("@timestamp", "2025-01-09T14:20:00Z", "employee_id", 1002, "ip", "8.45.133.220")
            );
            String employeesInternalLoginsBulkBody = employeesInternalLogins.stream()
                    .map(login -> "{ \"create\":{ } }" + "\n" + login.toJsonString())
                    .collect(Collectors.joining("\n")) + "\n";

            String employeesContractorsLoginsBulkBody = employeesContractorsLogins.stream()
                    .map(login -> "{ \"create\":{ } }" + "\n" + login.toJsonString())
                    .collect(Collectors.joining("\n")) + "\n";

            response = restClient.putJson(DS_EMPLOYEES_INTERNAL_LOGIN + "/_bulk?refresh=true", employeesInternalLoginsBulkBody);
            assertThat(response.getStatusCode(), is(200));

            response = restClient.putJson(DS_EMPLOYEES_CONTRACTORS_LOGIN + "/_bulk?refresh=true", employeesContractorsLoginsBulkBody);
            assertThat(response.getStatusCode(), is(200));
        }

    }

    @Test
    public void testQueryOneIndex_usersWithNoRestrictions() throws Exception {
        final String query = String.format("{\"query\": \"FROM %s | KEEP first_name, last_name, hire_date | SORT first_name ASC | LIMIT 1\"}", INDEX_EMPLOYEES_INTERNAL);

        for (TestSgConfig.User user : List.of(UNLIMITED_USER, READ_ONLY_USER, READ_ONLY_ALIASES_DS_USER)) {
            try (GenericRestClient restClient = cluster.getRestClient(user)) {

                HttpResponse response = restClient.postJson(RUN_QUERY_PATH, query);

                assertThat(user.getName() + " 200 response expected", response.getStatusCode(), is(200));
                assertThat(
                        "Invalid response body, user: " + user.getName() + ", body: " + response.getBody(),
                        response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 1)
                );
                assertThat(
                        "Invalid response body, user: " + user.getName() + ", body: " + response.getBody(),
                        response.getBodyAsDocNode(), containsList("$.values[0][*]",
                                equalTo("Daniel"), equalTo("Thomas"), containsString("2019-04-18")
                        )
                );
            }
        }
    }

    @Test
    public void testQueryOneIndex_checkDlsApplies() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(READ_ONLY_USER_WITH_DLS_FLS)) {

            String query = String.format("{\"query\": \"FROM %s | KEEP first_name | SORT first_name DESC | LIMIT 1\"}", INDEX_EMPLOYEES_INTERNAL);

            HttpResponse response = restClient.postJson(RUN_QUERY_PATH, query);

            assertThat(response.getStatusCode(), is(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 1));
            //without DLS it should return Sarah, with DLS it returns Olivia
            assertThat(response.getBodyAsDocNode(), containsList("$.values[0][*]", equalTo("Olivia")));

            //searching for Sarah with DLS returns no values
            query = String.format("{\"query\": \"FROM %s | KEEP first_name | WHERE MATCH(first_name, \\\"Sarah\\\")\"}", INDEX_EMPLOYEES_INTERNAL);

            response = restClient.postJson(RUN_QUERY_PATH, query);

            assertThat(response.getStatusCode(), is(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 0));
        }
    }

    @Test
    public void testQueryOneIndex_checkFlsApplies() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(READ_ONLY_USER_WITH_DLS_FLS)) {
            final String query = String.format("{\"query\": \"FROM %s | SORT first_name DESC | LIMIT 1\"}", INDEX_EMPLOYEES_INTERNAL);

            HttpResponse response = restClient.postJson(RUN_QUERY_PATH, query);

            assertThat(response.getStatusCode(), is(200));
            //without FLS it should return 5 columns, with FLS the hire_date is excluded
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 4));
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.columns[*].name", equalTo("hire_date")));

            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 1));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values[0]", 4));
            //with FLS none of the values contains Olivia's hire_date
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.values[0][*]", containsString("2020-10-20")));

            //try to rename excluded field
            final String queryWithRename = String.format("{\"query\": \"FROM %s | RENAME hire_date AS hire_d | SORT first_name DESC | LIMIT 1\"}", INDEX_EMPLOYEES_INTERNAL);
            response = restClient.postJson(RUN_QUERY_PATH, queryWithRename);

            assertThat(response.getStatusCode(), is(400));
            assertThat(response.getBody(), containsString("Unknown column"));
            assertThat(response.getBody(), containsString("hire_date"));
        }
    }

    @Test
    public void testQueryOneIndex_missingPrivileges() throws Exception {
        final String queryEmployeesInternal = String.format("{\"query\": \"FROM %s\"}", INDEX_EMPLOYEES_INTERNAL);
        final String queryEmployeesContractors = String.format("{\"query\": \"FROM %s\"}", INDEX_EMPLOYEES_CONTRACTORS);

        try (GenericRestClient restClient = cluster.getRestClient(READ_ONLY_USER_WITHOUT_ESQL_CLUSTER_PERMS)) {
            for (String query : List.of(queryEmployeesInternal, queryEmployeesContractors)) {
                HttpResponse response = restClient.postJson(RUN_QUERY_PATH, query);

                assertThat(response.getStatusCode(), is(403));
            }
        }

        try (GenericRestClient restClient = cluster.getRestClient(READ_ONLY_EMPLOYEES_USER_WITHOUT_ESQL_INDEX_PERMS)) {

            HttpResponse response = restClient.postJson(RUN_QUERY_PATH, queryEmployeesInternal);
            //this one ends with 400, since our plugin reduces indices and forces an
            // empty result on the org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest
            assertThat(response.getStatusCode(), is(400));
            assertThat(response.getBody(), containsString("Unknown index [" + INDEX_EMPLOYEES_INTERNAL));

            response = restClient.postJson(RUN_QUERY_PATH, queryEmployeesContractors);
            assertThat(response.getStatusCode(), is(403));
        }

        try (GenericRestClient restClient = cluster.getRestClient(READ_ONLY_EMPLOYEES_INTERNAL_USER)) {

            HttpResponse response = restClient.postJson(RUN_QUERY_PATH, queryEmployeesContractors);
            //this one ends with 400, since our plugin reduces indices and forces an
            // empty result on the org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest
            assertThat(response.getStatusCode(), is(400));
            assertThat(response.getBody(), containsString("Unknown index [" + INDEX_EMPLOYEES_CONTRACTORS));
        }
    }

    @Test
    public void testQueryTwoIndices_usersWithNoRestrictions() throws Exception {
        final String query = String.format(
                "{\"query\": \"FROM %s, %s | KEEP first_name | SORT first_name DESC | LIMIT 1000\"}",
                INDEX_EMPLOYEES_INTERNAL, INDEX_EMPLOYEES_CONTRACTORS
        );
        for (TestSgConfig.User user : List.of(UNLIMITED_USER, READ_ONLY_USER, READ_ONLY_ALIASES_DS_USER)) {
            try (GenericRestClient restClient = cluster.getRestClient(user)) {

                HttpResponse response = restClient.postJson(RUN_QUERY_PATH, query);

                assertThat(response.getStatusCode(), is(200));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 15));
            }
        }
    }

    @Test
    public void testQueryTwoIndices_checkDlsApplies() throws Exception {
        final String query = String.format(
                "{\"query\": \"FROM %s, %s | KEEP first_name | SORT first_name DESC | LIMIT 1000\"}",
                INDEX_EMPLOYEES_INTERNAL, INDEX_EMPLOYEES_CONTRACTORS
        );
        try (GenericRestClient restClient = cluster.getRestClient(READ_ONLY_USER_WITH_DLS_FLS)) {

            HttpResponse response = restClient.postJson(RUN_QUERY_PATH, query);

            assertThat(response.getStatusCode(), is(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 13));
            //Sarah and Liam excluded by DLS
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.values[*][*]", equalTo("Sarah"), equalTo("Liam")));
        }
    }

    @Test
    public void testQueryTwoIndices_checkFlsApplies() throws Exception {
        final String query = String.format(
                "{\"query\": \"FROM %s, %s | SORT first_name ASC | LIMIT 1000\"}",
                INDEX_EMPLOYEES_INTERNAL, INDEX_EMPLOYEES_CONTRACTORS
        );

        try (GenericRestClient restClient = cluster.getRestClient(READ_ONLY_USER_WITH_DLS_FLS)) {

            HttpResponse response = restClient.postJson(RUN_QUERY_PATH, query);

            assertThat(response.getStatusCode(), is(200));
            //without FLS it should return 5 columns, with FLS the hire_date is excluded
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 4));
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.columns[*].name", equalTo("hire_date")));
        }
    }

    @Test
    public void testQueryTwoIndices_missingPrivileges() throws Exception {
        final String query = String.format(
                "{\"query\": \"FROM %s, %s | SORT first_name ASC | LIMIT 1\"}",
                INDEX_EMPLOYEES_INTERNAL, INDEX_EMPLOYEES_CONTRACTORS
        );

        try (GenericRestClient restClient = cluster.getRestClient(READ_ONLY_EMPLOYEES_INTERNAL_USER)) {

            HttpResponse response = restClient.postJson(RUN_QUERY_PATH, query);
            //ES sends field caps request containing both indices, plugin reduces them to TEST_INDEX_EMPLOYEES_INTERNAL
            //since user lacks search shards on TEST_INDEX_EMPLOYEES_CONTRACTORS, he gets 403
            assertThat(response.getStatusCode(), is(403));
        }

        try (GenericRestClient restClient = cluster.getRestClient(READ_ONLY_EMPLOYEES_USER_WITHOUT_ESQL_INDEX_PERMS)) {

            HttpResponse response = restClient.postJson(RUN_QUERY_PATH, query);
            //ES sends field caps request containing both indices, plugin reduces them to TEST_INDEX_EMPLOYEES_INTERNAL
            //since user lacks search shards on TEST_INDEX_EMPLOYEES_CONTRACTORS, he gets 403
            assertThat(response.getStatusCode(), is(403));
        }
    }

    @Test
    public void testQueryTwoIndicesLookup_usersWithNoRestrictions() throws Exception {
        final String query = String.format(
                "{\"query\": \"FROM %s | LOOKUP JOIN %s ON position_id | KEEP first_name, position_name | SORT first_name DESC | LIMIT 1\"}",
                INDEX_EMPLOYEES_INTERNAL, INDEX_EMPLOYEES_POSITIONS
        );
        for (TestSgConfig.User user : List.of(UNLIMITED_USER, READ_ONLY_USER, READ_ONLY_ALIASES_DS_USER)) {
            try (GenericRestClient restClient = cluster.getRestClient(user)) {

                HttpResponse response = restClient.postJson(RUN_QUERY_PATH, query);

                assertThat(user.getName() + " 200 response expected", response.getStatusCode(), is(200));
                assertThat(
                        "Invalid response body, user: " + user.getName() + ", body: " + response.getBody(),
                        response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 1)
                );
                assertThat(
                        "Invalid response body, user: " + user.getName() + ", body: " + response.getBody(),
                        response.getBodyAsDocNode(), containsList("$.values[0][*]", equalTo("Sarah"), equalTo("HR Specialist"))
                );
            }
        }
    }

    @Test
    public void testQueryTwoIndicesLookup_checkDlsApplies() throws Exception {
        final String query = String.format(
                "{\"query\": \"FROM %s | LOOKUP JOIN %s ON position_id | KEEP first_name, position_name | LIMIT 1000\"}",
                INDEX_EMPLOYEES_INTERNAL, INDEX_EMPLOYEES_POSITIONS
        );
        try (GenericRestClient restClient = cluster.getRestClient(READ_ONLY_USER_WITH_DLS_FLS)) {

            HttpResponse response = restClient.postJson(RUN_QUERY_PATH, query);

            assertThat(response.getStatusCode(), is(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 9));
            //Sarah excluded by DLS
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.values[*][0]", equalTo("Sarah")));
        }
    }

    @Test
    public void testQueryTwoIndicesLookup_checkFlsApplies() throws Exception {
        final String query = String.format(
                "{\"query\": \"FROM %s | LOOKUP JOIN %s ON position_id | SORT first_name DESC | LIMIT 1\"}",
                INDEX_EMPLOYEES_INTERNAL, INDEX_EMPLOYEES_POSITIONS
        );

        try (GenericRestClient restClient = cluster.getRestClient(READ_ONLY_USER_WITH_DLS_FLS)) {

            HttpResponse response = restClient.postJson(RUN_QUERY_PATH, query);

            assertThat(response.getStatusCode(), is(200));
            //without FLS it should return 6 columns, with FLS the hire_date is excluded
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 5));
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.columns[*].name", equalTo("hire_date")));

            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 1));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values[0]", 5));
            //with FLS none of the values contains Olivia's hire_date
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.values[0][*]", equalTo("2020-10-20")));
        }
    }

    @Test
    public void testQueryTwoIndicesLookup_missingPrivileges() throws Exception {
        final String queryInternal = String.format(
                "{\"query\": \"FROM %s | LOOKUP JOIN %s ON position_id\"}",
                INDEX_EMPLOYEES_INTERNAL, INDEX_EMPLOYEES_POSITIONS
        );
        final String queryContractors = String.format(
                "{\"query\": \"FROM %s | LOOKUP JOIN %s ON position_id\"}",
                INDEX_EMPLOYEES_CONTRACTORS, INDEX_EMPLOYEES_POSITIONS
        );

        try (GenericRestClient restClient = cluster.getRestClient(READ_ONLY_EMPLOYEES_INTERNAL_USER)) {

            HttpResponse response = restClient.postJson(RUN_QUERY_PATH, queryInternal);
            //this one ends with 400, since our plugin reduces indices and forces an
            // empty result on the org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest for index TEST_INDEX_POSITIONS
            assertThat(response.getStatusCode(), is(400));
            assertThat(response.getBody(), containsString("Unknown index [" + INDEX_EMPLOYEES_POSITIONS));

            response = restClient.postJson(RUN_QUERY_PATH, queryContractors);
            //this one ends with 400, since our plugin reduces indices and forces an
            // empty result on the org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest for indices TEST_INDEX_EMPLOYEES_CONTRACTORS, TEST_INDEX_POSITIONS
            assertThat(response.getStatusCode(), is(400));
            assertThat(response.getBody(), containsString("Unknown index [" + INDEX_EMPLOYEES_POSITIONS));
            assertThat(response.getBody(), containsString("Unknown index [" + INDEX_EMPLOYEES_CONTRACTORS));
        }
    }

    @Test
    public void testQueryWithWildcard() throws Exception {
        final String queryInternalEmployees = String.format("{\"query\": \"FROM %s\"}", INDEX_EMPLOYEES_PREFIX + "-i*");
        final String queryAllEmployees = String.format("{\"query\": \"FROM %s\"}", INDEX_EMPLOYEES_PREFIX + "-*");

        for (TestSgConfig.User user : List.of(UNLIMITED_USER, READ_ONLY_USER)) {
            try (GenericRestClient restClient = cluster.getRestClient(user)) {

                HttpResponse response = restClient.postJson(RUN_QUERY_PATH, queryInternalEmployees);

                assertThat(response.getStatusCode(), is(200));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 10));

                response = restClient.postJson(RUN_QUERY_PATH, queryAllEmployees);

                assertThat(response.getStatusCode(), is(200));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 15));
            }
        }

        try (GenericRestClient restClient = cluster.getRestClient(READ_ONLY_USER_WITHOUT_ESQL_CLUSTER_PERMS)) {

            HttpResponse response = restClient.postJson(RUN_QUERY_PATH, queryInternalEmployees);

            assertThat(response.getStatusCode(), is(403));
        }

        try (GenericRestClient restClient = cluster.getRestClient(READ_ONLY_EMPLOYEES_USER_WITHOUT_ESQL_INDEX_PERMS)) {

            HttpResponse response = restClient.postJson(RUN_QUERY_PATH, queryInternalEmployees);

            //this one ends with 400, since our plugin reduces indices and forces an
            // empty result on the org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest
            assertThat(response.getStatusCode(), is(400));
            assertThat(response.getBody(), containsString("Unknown index [" + INDEX_EMPLOYEES_PREFIX + "-i*"));

            response = restClient.postJson(RUN_QUERY_PATH, queryAllEmployees);
            //when using a wildcard, both field caps and search shards request can be reduced,
            //in this case user lacks field caps on ALIAS_EMPLOYEES_INTERNAL, and search shard on ALIAS_EMPLOYEES_CONTRACTORS
            assertThat(response.getStatusCode(), is(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 0));
        }
    }

    @Test
    public void testQueryAliases() throws Exception {
        final String queryEmployeesInternal = String.format("{\"query\": \"FROM %s\"}", ALIAS_EMPLOYEES_INTERNAL);
        final String queryEmployeesContractors = String.format("{\"query\": \"FROM %s\"}", ALIAS_EMPLOYEES_CONTRACTORS);
        final String queryBothEmployees = String.format("{\"query\": \"FROM %s, %s\"}", ALIAS_EMPLOYEES_INTERNAL, ALIAS_EMPLOYEES_CONTRACTORS);
        final String queryInternalEmployeesLookupAliasPositions = String.format(
                "{\"query\": \"FROM %s | LOOKUP JOIN %s ON position_id\"}", ALIAS_EMPLOYEES_INTERNAL, ALIAS_EMPLOYEES_POSITIONS
        );
        final String queryInternalEmployeesLookupIndexPositions = String.format(
                "{\"query\": \"FROM %s | LOOKUP JOIN %s ON position_id\"}", ALIAS_EMPLOYEES_INTERNAL, INDEX_EMPLOYEES_POSITIONS
        );
        final String queryEmployeesWildcard = String.format(
                "{\"query\": \"FROM %s\"}", ALIAS_PREFIX + "-" + INDEX_EMPLOYEES_PREFIX + "-*"
        );

        for (TestSgConfig.User user : List.of(UNLIMITED_USER, READ_ONLY_ALIASES_DS_USER)) {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                HttpResponse response = client.postJson(RUN_QUERY_PATH, queryEmployeesInternal);
                assertThat(response.getStatusCode(), equalTo(200));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 5));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 10));

                response = client.postJson(RUN_QUERY_PATH, queryEmployeesContractors);
                assertThat(response.getStatusCode(), equalTo(200));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 5));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 5));

                response = client.postJson(RUN_QUERY_PATH, queryBothEmployees);
                assertThat(response.getStatusCode(), equalTo(200));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 5));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 15));

                response = client.postJson(RUN_QUERY_PATH, queryInternalEmployeesLookupAliasPositions);
                assertThat(response.getStatusCode(), equalTo(400));
                assertThat(response.getBody(), containsString("Aliases and index patterns are not allowed for LOOKUP JOIN"));

                response = client.postJson(RUN_QUERY_PATH, queryInternalEmployeesLookupIndexPositions);
                assertThat(response.getStatusCode(), equalTo(200));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 6));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 10));

                response = client.postJson(RUN_QUERY_PATH, queryEmployeesWildcard);
                assertThat(response.getStatusCode(), equalTo(200));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 5));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 15));
            }
        }

        try (GenericRestClient client = cluster.getRestClient(READ_ONLY_USER_WITH_DLS_FLS)) {
            HttpResponse response = client.postJson(RUN_QUERY_PATH, queryEmployeesInternal);
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 4));
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.columns[*].name", equalTo("hire_date")));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 9));

            response = client.postJson(RUN_QUERY_PATH, queryEmployeesContractors);
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 4));
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.columns[*].name", equalTo("hire_date")));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 4));

            response = client.postJson(RUN_QUERY_PATH, queryBothEmployees);
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 4));
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.columns[*].name", equalTo("hire_date")));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 13));

            response = client.postJson(RUN_QUERY_PATH, queryInternalEmployeesLookupIndexPositions);
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 5));
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.columns[*].name", equalTo("hire_date")));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 9));

            response = client.postJson(RUN_QUERY_PATH, queryEmployeesWildcard);
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 4));
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.columns[*].name", equalTo("hire_date")));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 13));
        }

        try (GenericRestClient client = cluster.getRestClient(READ_ONLY_EMPLOYEES_USER_WITHOUT_ESQL_INDEX_PERMS)) {
            HttpResponse response = client.postJson(RUN_QUERY_PATH, queryEmployeesInternal);
            //user lacks field caps permission
            assertThat(response.getStatusCode(), equalTo(400));
            assertThat(response.getBody(), containsString("Unknown index [" + ALIAS_EMPLOYEES_INTERNAL));

            response = client.postJson(RUN_QUERY_PATH, queryEmployeesContractors);
            //user lacks search shards permission
            assertThat(response.getStatusCode(), equalTo(403));

            response = client.postJson(RUN_QUERY_PATH, queryBothEmployees);
            //user lacks search shards permission on ALIAS_EMPLOYEES_CONTRACTORS
            assertThat(response.getStatusCode(), equalTo(403));

            response = client.postJson(RUN_QUERY_PATH, queryInternalEmployeesLookupIndexPositions);
            //user lacks field caps permission on ALIAS_EMPLOYEES_INTERNAL and INDEX_EMPLOYEES_POSITIONS
            assertThat(response.getStatusCode(), equalTo(400));
            assertThat(response.getBody(), containsString("Unknown index [" + ALIAS_EMPLOYEES_INTERNAL));
            assertThat(response.getBody(), containsString("Unknown index [" + INDEX_EMPLOYEES_POSITIONS));

            response = client.postJson(RUN_QUERY_PATH, queryEmployeesWildcard);
            //when using a wildcard, both field caps and search shards request can be reduced,
            //in this case user lacks field caps on ALIAS_EMPLOYEES_INTERNAL, and search shard on ALIAS_EMPLOYEES_CONTRACTORS
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 0));
        }

        try (GenericRestClient client = cluster.getRestClient(READ_ONLY_EMPLOYEES_INTERNAL_USER)) {
            HttpResponse response = client.postJson(RUN_QUERY_PATH, queryEmployeesInternal);
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 5));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 10));

            response = client.postJson(RUN_QUERY_PATH, queryEmployeesContractors);
            assertThat(response.getStatusCode(), equalTo(400));
            assertThat(response.getBody(), containsString("Unknown index [" + ALIAS_EMPLOYEES_CONTRACTORS));

            response = client.postJson(RUN_QUERY_PATH, queryBothEmployees);
            assertThat(response.getStatusCode(), equalTo(403));

            response = client.postJson(RUN_QUERY_PATH, queryInternalEmployeesLookupIndexPositions);
            assertThat(response.getStatusCode(), equalTo(400));
            assertThat(response.getBody(), containsString("Unknown index [" + INDEX_EMPLOYEES_POSITIONS));

            response = client.postJson(RUN_QUERY_PATH, queryEmployeesWildcard);
            assertThat(response.getStatusCode(), equalTo(200));
            //when using a wildcard, both field caps and search shards request can be reduced,
            //user has access to one of aliases, so records from ALIAS_EMPLOYEES_INTERNAL are returned
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 10));
        }
    }

    @Test
    public void testQueryDataStreams() throws Exception {
        final String queryEmployeesInternalLogin = String.format("{\"query\": \"FROM %s\"}", DS_EMPLOYEES_INTERNAL_LOGIN);
        final String queryEmployeesContractorsLogin = String.format("{\"query\": \"FROM %s\"}", DS_EMPLOYEES_CONTRACTORS_LOGIN);
        final String queryBothEmployeesLogins = String.format("{\"query\": \"FROM %s, %s\"}", DS_EMPLOYEES_INTERNAL_LOGIN, DS_EMPLOYEES_CONTRACTORS_LOGIN);
        final String queryEmployeesLoginsWildcard = String.format(
                "{\"query\": \"FROM %s\"}", DS_PREFIX + "-" + INDEX_EMPLOYEES_PREFIX + "-*"
        );

        for (TestSgConfig.User user : List.of(UNLIMITED_USER, READ_ONLY_ALIASES_DS_USER)) {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                HttpResponse response = client.postJson(RUN_QUERY_PATH, queryEmployeesInternalLogin);
                assertThat(response.getStatusCode(), equalTo(200));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 3));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 3));

                response = client.postJson(RUN_QUERY_PATH, queryEmployeesContractorsLogin);
                assertThat(response.getStatusCode(), equalTo(200));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 3));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 2));

                response = client.postJson(RUN_QUERY_PATH, queryBothEmployeesLogins);
                assertThat(response.getStatusCode(), equalTo(200));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 3));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 5));

                response = client.postJson(RUN_QUERY_PATH, queryEmployeesLoginsWildcard);
                assertThat(response.getStatusCode(), equalTo(200));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 3));
                assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 5));
            }
        }

        try (GenericRestClient client = cluster.getRestClient(READ_ONLY_USER_WITH_DLS_FLS)) {
            HttpResponse response = client.postJson(RUN_QUERY_PATH, queryEmployeesInternalLogin);
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 2));
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.columns[*].name", equalTo("ip")));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 2));
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.values[*][*]", equalTo(1)));

            response = client.postJson(RUN_QUERY_PATH, queryEmployeesContractorsLogin);
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 2));
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.columns[*].name", equalTo("ip")));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 1));
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.values[*][*]", equalTo(1001)));

            response = client.postJson(RUN_QUERY_PATH, queryBothEmployeesLogins);
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 2));
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.columns[*].name", equalTo("ip")));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 3));
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.values[*][*]", equalTo(1), equalTo(1001)));

            response = client.postJson(RUN_QUERY_PATH, queryEmployeesLoginsWildcard);
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 2));
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.columns[*].name", equalTo("ip")));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 3));
            assertThat(response.getBodyAsDocNode(), listDoesNotContain("$.values[*][*]", equalTo(1), equalTo(1001)));
        }

        try (GenericRestClient client = cluster.getRestClient(READ_ONLY_EMPLOYEES_USER_WITHOUT_ESQL_INDEX_PERMS)) {
            HttpResponse response = client.postJson(RUN_QUERY_PATH, queryEmployeesInternalLogin);
            //user lacks field caps permission
            assertThat(response.getStatusCode(), equalTo(400));
            assertThat(response.getBody(), containsString("Unknown index [" + DS_EMPLOYEES_INTERNAL_LOGIN));

            response = client.postJson(RUN_QUERY_PATH, queryEmployeesContractorsLogin);
            //user lacks search shards permission
            assertThat(response.getStatusCode(), equalTo(403));

            response = client.postJson(RUN_QUERY_PATH, queryBothEmployeesLogins);
            //user lacks search shards permission
            assertThat(response.getStatusCode(), equalTo(403));

            response = client.postJson(RUN_QUERY_PATH, queryEmployeesLoginsWildcard);
            //when using a wildcard, both field caps and search shards request can be reduced,
            //in this case user lacks field caps on DS_EMPLOYEE_INTERNAL_LOGIN, and search shard on DS_EMPLOYEE_CONTRACTORS_LOGIN
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 0));
        }

        try (GenericRestClient client = cluster.getRestClient(READ_ONLY_EMPLOYEES_INTERNAL_USER)) {
            HttpResponse response = client.postJson(RUN_QUERY_PATH, queryEmployeesInternalLogin);
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 3));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 3));

            response = client.postJson(RUN_QUERY_PATH, queryEmployeesContractorsLogin);
            assertThat(response.getStatusCode(), equalTo(400));
            assertThat(response.getBody(), containsString("Unknown index [" + DS_EMPLOYEES_CONTRACTORS_LOGIN));

            response = client.postJson(RUN_QUERY_PATH, queryBothEmployeesLogins);
            //user lacks search shards permission on DS_EMPLOYEES_CONTRACTORS_LOGIN
            assertThat(response.getStatusCode(), equalTo(403));

            response = client.postJson(RUN_QUERY_PATH, queryEmployeesLoginsWildcard);
            assertThat(response.getStatusCode(), equalTo(200));
            //when using a wildcard, both field caps and search shards request can be reduced,
            //user has access to one of data streams, so records from DS_EMPLOYEE_INTERNAL_LOGIN are returned
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 3));
        }
    }

    @Test
    public void testQueryRowCommand() throws Exception {
        final String query = "{\"query\": \"row employee_id = 1, @timestamp = NOW(), ip = TO_IP(\\\"127.0.0.1\\\")\"}";

        for (TestSgConfig.User user : List.of(UNLIMITED_USER, READ_ONLY_ALIASES_DS_USER, READ_ONLY_USER_WITH_DLS_FLS)) {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                HttpResponse response = client.postJson(RUN_QUERY_PATH, query);
                assertThat(response.getStatusCode(), equalTo(200));
                assertThat(response.getBodyAsDocNode(), containsList("$.columns[*].name", equalTo("employee_id"), equalTo("@timestamp"), equalTo("ip")));
            }
        }
    }

    @Test
    public void testAsyncQuery() throws Exception {
        final DocNode queryEmployeesInternal = DocNode.of(
                "wait_for_completion_timeout", "1ms",
                "keep_on_completion", true,
                "query", String.format("FROM %s", INDEX_EMPLOYEES_INTERNAL)

        );
        final DocNode queryEmployeesContractors = queryEmployeesInternal.with(
                "query", String.format("FROM %s", INDEX_EMPLOYEES_CONTRACTORS)
        );
        for (TestSgConfig.User user : List.of(UNLIMITED_USER, READ_ONLY_USER)) {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                HttpResponse response = client.postJson(RUN_ASYNC_QUERY_PATH, queryEmployeesInternal);
                assertThat(response.getStatusCode(), equalTo(200));
                assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath("$", "id"));

                String id = response.getBodyAsDocNode().getAsString("id");

                response = client.get("/_query/async/" + id);
                assertThat(response.getStatusCode(), equalTo(200));

                response = client.post("/_query/async/" + id + "/stop");
                assertThat(response.getStatusCode(), equalTo(200));

                response = client.delete("/_query/async/" + id);
                assertThat(response.getStatusCode(), equalTo(200));

                response = client.get("/_query/async/" + id);
                assertThat(response.getStatusCode(), equalTo(404));
            }
        }

        //user cannot access an async query created by another user
        try (GenericRestClient unlimitedClient = cluster.getRestClient(UNLIMITED_USER);
             GenericRestClient readOnlyClient = cluster.getRestClient(READ_ONLY_USER)) {
            HttpResponse response = unlimitedClient.postJson(RUN_ASYNC_QUERY_PATH, queryEmployeesInternal);
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath("$", "id"));
            String id = response.getBodyAsDocNode().getAsString("id");

            response = readOnlyClient.get("/_query/async/" + id);
            assertThat(response.getStatusCode(), equalTo(403));
        }

        try (GenericRestClient client = cluster.getRestClient(READ_ONLY_USER_WITHOUT_ESQL_CLUSTER_PERMS)) {
            HttpResponse response = client.postJson(RUN_ASYNC_QUERY_PATH, queryEmployeesInternal);
            assertThat(response.getStatusCode(), equalTo(403));
        }

        try (GenericRestClient client = cluster.getRestClient(READ_ONLY_EMPLOYEES_USER_WITHOUT_ESQL_INDEX_PERMS)) {
            HttpResponse response = client.postJson(RUN_ASYNC_QUERY_PATH, queryEmployeesInternal);
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath("$", "id"));

            String id = response.getBodyAsDocNode().getAsString("id");

            response = client.get("/_query/async/" + id);
            //user lacks field caps permission
            assertThat(response.getStatusCode(), equalTo(400));
            assertThat(response.getBody(), containsString("Unknown index [" + INDEX_EMPLOYEES_INTERNAL));

            response = client.postJson(RUN_ASYNC_QUERY_PATH, queryEmployeesContractors);
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath("$", "id"));

            id = response.getBodyAsDocNode().getAsString("id");

            response = client.get("/_query/async/" + id);
            //user lacks search shards permission
            assertThat(response.getStatusCode(), equalTo(403));

            response = client.post("/_query/async/" + id + "/stop");
            //user lacks search shards permission
            assertThat(response.getStatusCode(), equalTo(403));

            response = client.delete("/_query/async/" + id);
            assertThat(response.getStatusCode(), equalTo(200));

        }

        try (GenericRestClient client = cluster.getRestClient(READ_ONLY_USER_WITH_DLS_FLS)) {
            HttpResponse response = client.postJson(RUN_ASYNC_QUERY_PATH, queryEmployeesInternal);
            assertThat(response.getStatusCode(), equalTo(200));
            assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath("$", "id"));

            String id = response.getBodyAsDocNode().getAsString("id");

            Awaitility.await("Query is not running, DLS and FLS applied")
                    .atMost(Duration.ofSeconds(3))
                    .pollInterval(Duration.ofMillis(200))
                    .pollDelay(Duration.ofMillis(200))
                    .untilAsserted(() -> {
                        HttpResponse resp = client.get("/_query/async/" + id);
                        assertThat(resp.getStatusCode(), equalTo(200));
                        assertThat(resp.getBodyAsDocNode(), containsValue("$.is_running", false));
                        assertThat(resp.getBodyAsDocNode(), docNodeSizeEqualTo("$.values", 9));
                        assertThat(resp.getBodyAsDocNode(), listDoesNotContain("$.values[*][*]", equalTo("Sarah")));
                        assertThat(resp.getBodyAsDocNode(), docNodeSizeEqualTo("$.columns", 4));
                        assertThat(resp.getBodyAsDocNode(), listDoesNotContain("$.columns[*].name", equalTo("hire_date")));
                    });

            response = client.post("/_query/async/" + id + "/stop");
            assertThat(response.getStatusCode(), equalTo(200));

            response = client.delete("/_query/async/" + id);
            assertThat(response.getStatusCode(), equalTo(200));

            response = client.get("/_query/async/" + id);
            assertThat(response.getStatusCode(), equalTo(404));
        }
    }
}
