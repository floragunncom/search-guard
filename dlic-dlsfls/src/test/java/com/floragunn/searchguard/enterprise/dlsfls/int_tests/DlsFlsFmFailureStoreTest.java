/*
 * Copyright 2026 floragunn GmbH
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

package com.floragunn.searchguard.enterprise.dlsfls.int_tests;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestComponentTemplate;
import com.floragunn.searchguard.test.TestDataStream;
import com.floragunn.searchguard.test.TestIndexTemplate;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import static com.floragunn.searchguard.test.RestMatchers.distinctNodesAt;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.valueSatisfiesMatcher;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

@RunWith(Parameterized.class)
public class DlsFlsFmFailureStoreTest {

    static TestSgConfig.User USER_WITHOUT_RULES = new TestSgConfig.User("user_without_rules")
            .roles(
                    new TestSgConfig.Role("role_without_rules")
                            .clusterPermissions("*")
                            .dataStreamPermissions("*")
                            .on("*")
            );

    static TestSgConfig.User USER_WITH_DLS_RULE = new TestSgConfig.User("user_with_dls_rule")
            .roles(
                    new TestSgConfig.Role("role_with_dls_rule")
                            .clusterPermissions("*")
                            .dataStreamPermissions("*")
                            .dls("{ \"bool\": { \"must\": { \"match\": { \"dept\": \"dept_d\" }}}}")
                            .on("*")
            );

    static TestSgConfig.User USER_WITH_DLS_RULE_FAILURE_STRUCTURE = new TestSgConfig.User("user_with_dls_rule_failure_structure")
            .roles(
                    new TestSgConfig.Role("role_with_dls_rule_failure_structure")
                            .clusterPermissions("*")
                            .dataStreamPermissions("*")
                            .dls("{ \"bool\": { \"must\": { \"match\": { \"document.source.dept\": \"dept_d\" }}}}")
                            .on("*")
            );

    static TestSgConfig.User USER_WITH_FLS_RULE = new TestSgConfig.User("user_with_fls_rule")
            .roles(
                    new TestSgConfig.Role("role_with_fls_rule")
                            .clusterPermissions("*")
                            .dataStreamPermissions("*")
                            .fls("~source_ip")
                            .on("*")
            );

    static TestSgConfig.User USER_WITH_FLS_RULE_FAILURE_STRUCTURE = new TestSgConfig.User("user_with_fls_rule_failure_structure")
            .roles(
                    new TestSgConfig.Role("role_with_fls_rule_failure_structure")
                            .clusterPermissions("*")
                            .dataStreamPermissions("*")
                            .fls("~document.source.source_ip")
                            .on("*")
            );

    static TestSgConfig.User USER_WITH_FM_RULE = new TestSgConfig.User("user_with_fm_rule")
            .roles(
                    new TestSgConfig.Role("role_with_fm_rule")
                            .clusterPermissions("*")
                            .dataStreamPermissions("*")
                            .maskedFields("dest_ip")
                            .on("*")
            );

    static TestSgConfig.User USER_WITH_FM_RULE_FAILURE_STRUCTURE = new TestSgConfig.User("user_with_fm_rule_failure_structure")
            .roles(
                    new TestSgConfig.Role("role_with_fm_rule_failure_structure")
                            .clusterPermissions("*")
                            .dataStreamPermissions("*")
                            .maskedFields("document.source.dest_ip")
                            .on("*")
            );

    static TestDataStream ds_a1 = TestDataStream.name("ds_a1").documentCount(22).rolloverAfter(10).build();

    static List<TestSgConfig.User> USERS = List.of(USER_WITHOUT_RULES, USER_WITH_DLS_RULE, USER_WITH_FLS_RULE, USER_WITH_FM_RULE,
            USER_WITH_DLS_RULE_FAILURE_STRUCTURE, USER_WITH_FLS_RULE_FAILURE_STRUCTURE, USER_WITH_FM_RULE_FAILURE_STRUCTURE
    );

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
            .singleNode()//
            .sslEnabled()//
            .users(USERS)//
            .indexTemplates(new TestIndexTemplate("ds_test", "ds_*").dataStream().composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL))//
            .dataStreams(ds_a1)//
            .authzDebug(true)//
            .enterpriseModulesEnabled()//
            .useExternalProcessCluster().build();

    static List<String> FAILED_DOCS_DEPTS = List.of("dept_d", "dept_c");

    private final String dlsMode;

    @Parameterized.Parameters(name = "{0}")
    public static Object[] parameters() {
        return new Object[] { "FILTER_LEVEL", "LUCENE_LEVEL"};
    }

    public DlsFlsFmFailureStoreTest(String dlsMode) {
        this.dlsMode = dlsMode;
    }

    @Before
    public void setUp() throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse response = adminCertClient.putJson(
                    "/_searchguard/config/authz_dlsfls",
                    DocNode.of("dls.mode", dlsMode, "field_anonymization.prefix", "anonymized_")
            );
            assertThat(response, isOk());
        }
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
            for (String dept : FAILED_DOCS_DEPTS) {
                DocNode docFromDs = DocNode.wrap(ds_a1.getTestData().anyDocument().getContent());
                DocNode docWithInvalidTimestamp = docFromDs.with("@timestamp", "asd").with("dept", dept);
                GenericRestClient.HttpResponse response = adminCertClient.postJson("/"+ ds_a1.getName() + "/_doc/?refresh=true", docWithInvalidTimestamp);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(201));
                assertThat(response.getBody(), response.getBodyAsDocNode(), containsValue("$.failure_store", "used"));
                assertThat(response.getBody(), response.getBodyAsDocNode(), valueSatisfiesMatcher("$._index", String.class, startsWith(".fs")));
            }
        }
    }

    @Test
    public void getDataComponent() throws Exception {
        for (TestSgConfig.User user : USERS) {
            try (GenericRestClient client = cluster.getRestClient(user)) {

                String dsName = ds_a1.getName();

                GenericRestClient.HttpResponse response = client.get(dsName + "/_search?size=1000");
                assertThat(user.getName(), response, isOk());

                if (user == USER_WITH_DLS_RULE) {
                    int docsWithDeptD = Math.toIntExact(ds_a1.getTestData().getRetainedDocuments().values().stream().filter(doc -> doc.get("dept").equals("dept_d")).count());
                    assertThat(user.getName(), response, json(nodeAt("$.hits.total.value", equalTo(docsWithDeptD))));
                } else if (user == USER_WITH_DLS_RULE_FAILURE_STRUCTURE) {
                    assertThat(user.getName(), response, json(nodeAt("$.hits.total.value", equalTo(0))));
                    continue;
                }

                if (user != USER_WITH_DLS_RULE) {
                    assertThat(user.getName(), response, json(distinctNodesAt("$.hits.hits[*]._source.dept", allOf(hasItem("dept_d"), hasItem(anyOf(startsWith("dept_a"), startsWith("dept_b"), startsWith("dept_c")))))));
                } else {
                    assertThat(user.getName(), response, json(distinctNodesAt("$.hits.hits[*]._source.dept", everyItem(equalTo("dept_d")))));
                }

                if (user != USER_WITH_FLS_RULE) {
                    assertThat(user.getName(), response, json(distinctNodesAt("$.hits.hits[*]._source", everyItem(hasKey("source_ip")))));
                } else {
                    assertThat(user.getName(), response, json(distinctNodesAt("$.hits.hits[*]._source", everyItem(not(hasKey("source_ip"))))));
                }

                assertThat(user.getName(), response, json(distinctNodesAt("$.hits.hits[*]._source", everyItem(hasKey("dest_ip")))));

                if (user != USER_WITH_FM_RULE) {
                    assertThat(user.getName(), response, json(distinctNodesAt("$.hits.hits[*]._source.dest_ip", everyItem(not(startsWith("anonymized_"))))));
                } else {
                    assertThat(user.getName(), response, json(distinctNodesAt("$.hits.hits[*]._source.dest_ip", everyItem(startsWith("anonymized_")))));
                }
            }
        }

    }

    @Test
    public void getFailureComponent() throws Exception {
        for (TestSgConfig.User user : USERS) {
            try (GenericRestClient client = cluster.getRestClient(user)) {

                String dsNameWithFailuresSuffix = ds_a1.getName() + "::failures";
                GenericRestClient.HttpResponse response = client.get(dsNameWithFailuresSuffix + "/_search?size=1000");
                assertThat(user.getName(), response, isOk());

                if (user == USER_WITH_DLS_RULE || user == USER_WITH_DLS_RULE_FAILURE_STRUCTURE) {
                    assertThat(user.getName(), response, json(nodeAt("$.hits.total.value", equalTo(0))));
                    continue;
                }
                assertThat(user.getName(), response, json(nodeAt("$.hits.total.value", equalTo(FAILED_DOCS_DEPTS.size()))));

                assertThat(user.getName(), response, json(distinctNodesAt("$.hits.hits[*]._source.document.source.dept", containsInAnyOrder(FAILED_DOCS_DEPTS.toArray()))));

                if (user != USER_WITH_FLS_RULE_FAILURE_STRUCTURE) {
                    assertThat(user.getName(), response, json(distinctNodesAt("$.hits.hits[*]._source.document.source", everyItem(hasKey("source_ip")))));
                } else {
                    assertThat(user.getName(), response, json(distinctNodesAt("$.hits.hits[*]._source.document.source", everyItem(not(hasKey("source_ip"))))));
                }

                assertThat(user.getName(), response, json(distinctNodesAt("$.hits.hits[*]._source.document.source", everyItem(hasKey("dest_ip")))));

                if (user != USER_WITH_FM_RULE_FAILURE_STRUCTURE) {
                    assertThat(user.getName(), response, json(distinctNodesAt("$.hits.hits[*]._source.document.source.dest_ip", everyItem(not(startsWith("anonymized_"))))));
                } else {
                    assertThat(user.getName(), response, json(distinctNodesAt("$.hits.hits[*]._source.document.source.dest_ip", everyItem(startsWith("anonymized_")))));
                }
            }
        }
    }
}
