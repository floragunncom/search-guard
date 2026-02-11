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

package com.floragunn.searchguard.authz.int_tests;

import static com.floragunn.searchguard.test.IndexApiMatchers.containsExactly;
import static com.floragunn.searchguard.test.IndexApiMatchers.esInternalIndices;
import static com.floragunn.searchguard.test.IndexApiMatchers.limitedTo;
import static com.floragunn.searchguard.test.IndexApiMatchers.limitedToNone;
import static com.floragunn.searchguard.test.IndexApiMatchers.searchGuardIndices;
import static com.floragunn.searchguard.test.IndexApiMatchers.unlimitedIncludingEsInternalIndices;
import static com.floragunn.searchguard.test.IndexApiMatchers.unlimitedIncludingSearchGuardIndices;
import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isNotFound;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestComponentTemplate;
import com.floragunn.searchguard.test.TestDataStream;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestIndexTemplate;
import com.floragunn.searchguard.test.TestSgConfig;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * The test is based on {@link DataStreamAuthorizationReadOnlyIntTests}, but data streams used in the test have enabled failure store.
 * Therefore, more test cases are possible.
 */
@RunWith(Parameterized.class)
public class DataStreamFailureStoreAuthorizationReadOnlyIntTests {

    private static final Logger log = LogManager.getLogger(DataStreamFailureStoreAuthorizationReadOnlyIntTests.class);

    static TestDataStream ds_a1 = TestDataStream.name("ds_a1").documentCount(20).rolloverAfter(5).seed(1).failureStoreEnabled(true).failureDocumentCount(2).attr("prefix", "a").build();
    static TestDataStream ds_a2 = TestDataStream.name("ds_a2").documentCount(20).rolloverAfter(5).seed(2).failureStoreEnabled(true).failureDocumentCount(2).attr("prefix", "a").build();
    static TestDataStream ds_a3 = TestDataStream.name("ds_a3").documentCount(20).rolloverAfter(5).seed(3).failureStoreEnabled(true).failureDocumentCount(2).attr("prefix", "a").build();
    static TestDataStream ds_ax = TestDataStream.name("ds_ax").failureStoreEnabled(true).failureDocumentCount(2).build(); // Not existing data stream
    static TestDataStream ds_b1 = TestDataStream.name("ds_b1").documentCount(10).rolloverAfter(5).seed(4).failureStoreEnabled(true).failureDocumentCount(2).attr("prefix", "b").build();
    static TestDataStream ds_b2 = TestDataStream.name("ds_b2").documentCount(20).rolloverAfter(5).seed(5).failureStoreEnabled(true).failureDocumentCount(2).attr("prefix", "b").build();
    static TestDataStream ds_b3 = TestDataStream.name("ds_b3").documentCount(20).rolloverAfter(5).seed(6).failureStoreEnabled(true).failureDocumentCount(2).attr("prefix", "b").build();
    static TestDataStream ds_hidden = TestDataStream.name("ds_hidden").documentCount(20).seed(8).failureStoreEnabled(true).failureDocumentCount(2).attr("prefix", "h").build(); // This is hidden via the ds_hidden index template
    static TestIndex index_c1 = TestIndex.name("index_c1").documentCount(5).seed(7).attr("prefix", "c").build();

    static TestAlias alias_ab1 = new TestAlias("alias_ab1", ds_a1, ds_a2, ds_a3, ds_b1);

    /*
     * This alias points to the index. This means that ES does not support expressions like index_c1::failures.
     * Furthermore, invocation alias_c1.failureOnly() throws exceptions.
     */
    static TestAlias alias_c1 = new TestAlias("alias_c1", index_c1);

    static TestSgConfig.User LIMITED_USER_A = new TestSgConfig.User("limited_user_A")//
            .description("ds_a*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "special:failure_store").on("ds_a*"))//
            .indexMatcher("read", limitedTo(ds_a1, ds_a2, ds_a3, ds_ax))//
            .indexMatcher("read_top_level", limitedTo(ds_a1, ds_a2, ds_a3))//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_B = new TestSgConfig.User("limited_user_B")//
            .description("ds_b*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "special:failure_store").on("ds_b*"))//
            .indexMatcher("read", limitedTo(ds_b1, ds_b2, ds_b3))//
            .indexMatcher("read_top_level", limitedTo(ds_b1, ds_b2, ds_b3))//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_B1 = new TestSgConfig.User("limited_user_B1")//
            .description("ds_b1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "special:failure_store").on("ds_b1"))//
            .indexMatcher("read", limitedTo(ds_b1))//
            .indexMatcher("read_top_level", limitedTo(ds_b1))//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_ALIAS_AB1 = new TestSgConfig.User("limited_user_alias_AB1")//
            .description("alias_ab1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .aliasPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/aliases/get", "special:failure_store").on("alias_ab1*"))//
            .indexMatcher("read", limitedTo(ds_a1, ds_a2, ds_a3, ds_b1, alias_ab1))//
            .indexMatcher("read_top_level", limitedTo(alias_ab1))//
            .indexMatcher("get_alias", limitedTo(ds_a1, ds_a2, ds_a3, ds_b1, alias_ab1));

    static TestSgConfig.User LIMITED_USER_A_HIDDEN = new TestSgConfig.User("limited_user_A_hidden")//
            .description("ds_a*, ds_hidden*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "special:failure_store").on("ds_a*", "ds_hidden*"))//
            .indexMatcher("read", limitedTo(ds_a1, ds_a2, ds_a3, ds_ax, ds_hidden))//
            .indexMatcher("read_top_level", limitedTo(ds_a1, ds_a2, ds_a3, ds_ax, ds_hidden))//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_NONE = new TestSgConfig.User("limited_user_none")//
            .description("no privileges for existing indices")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_CRUD", "SGS_INDICES_MONITOR", "special:failure_store").on("ds_does_not_exist_*"))//
            .indexMatcher("read", limitedToNone())//
            .indexMatcher("read_top_level", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User INVALID_USER_INDEX_PERMISSIONS_FOR_ALIAS = new TestSgConfig.User("invalid_user_index_permissions_for_alias")//
            .description("invalid: index permissions for alias")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR", "special:failure_store").on("alias_ab1"))//
            .indexMatcher("read", limitedToNone())//
            .indexMatcher("read_top_level", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User UNLIMITED_USER = new TestSgConfig.User("unlimited_user")//
            .description("unlimited complete")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("*").on("*")//
                            .aliasPermissions("*").on("*")//
                            .dataStreamPermissions("*").on("*")

            )//
            .indexMatcher("read", unlimitedIncludingEsInternalIndices())//
            .indexMatcher("read_top_level", unlimitedIncludingEsInternalIndices())//
            .indexMatcher("get_alias", unlimitedIncludingEsInternalIndices());

    /**
     * The SUPER_UNLIMITED_USER authenticates with an admin cert, which will cause all access control code to be skipped.
     * This serves as a base for comparison with the default behavior.
     */
    static TestSgConfig.User SUPER_UNLIMITED_USER = new TestSgConfig.User("super_unlimited_user")//
            .description("super unlimited (admin cert)")//
            .adminCertUser()//
            .indexMatcher("read", unlimitedIncludingSearchGuardIndices())//
            .indexMatcher("read_top_level", unlimitedIncludingSearchGuardIndices())//
            .indexMatcher("get_alias", unlimitedIncludingSearchGuardIndices());

    static List<TestSgConfig.User> USERS = ImmutableList.of(LIMITED_USER_A, LIMITED_USER_B, LIMITED_USER_B1, LIMITED_USER_ALIAS_AB1,
            LIMITED_USER_A_HIDDEN, LIMITED_USER_NONE, INVALID_USER_INDEX_PERMISSIONS_FOR_ALIAS, UNLIMITED_USER, SUPER_UNLIMITED_USER);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().users(USERS)//
            .indexTemplates(new TestIndexTemplate("ds_test", "ds_*").dataStream().composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL))//
            .indexTemplates(new TestIndexTemplate("ds_hidden", "ds_hidden*").priority(10).dataStream("hidden", true)
                    .composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL))//
            .dataStreams(ds_a1, ds_a2, ds_a3, ds_b1, ds_b2, ds_b3, ds_hidden)//
            .indices(index_c1)//
            .aliases(alias_ab1, alias_c1)//
            .authzDebug(true)//
            //     .logRequests()//
            .useExternalProcessCluster()//
            .build();

    final TestSgConfig.User user;

    @Test
    public void search_noPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_search?size=1000");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly(), index_c1).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_noPattern_noWildcards() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_search?size=1000&expand_wildcards=none");
            assertThat(httpResponse, containsExactly().at("hits.hits[*]._index").whenEmpty(200));
        }
    }

    @Test
    public void search_noPattern_allowNoIndicesFalse() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_search?size=1000&allow_no_indices=false");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly(), index_c1).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(404));
        }
    }

    @Test
    public void search_all() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_all/_search?size=1000");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly(), index_c1).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_all_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_all::failures/_search?size=1000");
            assertThat(httpResponse, containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly(), ds_b3.failureOnly()).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_all_noWildcards() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_all/_search?size=1000&expand_wildcards=none");
            assertThat(httpResponse, containsExactly().at("hits.hits[*]._index").whenEmpty(200));
        }
    }

    @Test
    public void search_all_noWildcards_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_all::failures/_search?size=1000&expand_wildcards=none");
            assertThat(httpResponse, containsExactly().at("hits.hits[*]._index").whenEmpty(200));
        }
    }

    @Test
    public void search_all_includeHidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_all/_search?size=1000&expand_wildcards=all");
            if (user == SUPER_UNLIMITED_USER) { //todo COMPONENT SELECTORS - this indicates problem with index resolution
                // failure stores are included - expected
                assertThat(httpResponse,
                        containsExactly(ds_a1, ds_a2, ds_a3, ds_b1, ds_b2, ds_b3,
                                index_c1, ds_hidden, searchGuardIndices(), esInternalIndices()).at("hits.hits[*]._index")
                                .but(user.indexMatcher("read")).whenEmpty(200));
            } else {
                // failure stores are excluded
                assertThat(httpResponse,
                        containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly(),
                                index_c1, ds_hidden.dataOnly(), searchGuardIndices(), esInternalIndices()).at("hits.hits[*]._index")
                                .but(user.indexMatcher("read")).whenEmpty(200));
            }
        }
    }

    @Test
    public void search_all_includeHidden_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_all::failures/_search?size=1000&expand_wildcards=all");
            if (user == SUPER_UNLIMITED_USER) { //todo COMPONENT SELECTORS - this indicates problem with index resolution
                // failure stores are included - expected
                assertThat(httpResponse,
                        containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly(), ds_b3.failureOnly(),
                                ds_hidden.failureOnly(), esInternalIndices()).at("hits.hits[*]._index")
                                .but(user.indexMatcher("read")).whenEmpty(200));
            } else {
                // failure stores are excluded
                assertThat(httpResponse,
                        containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly(), ds_b3.failureOnly(),
                                ds_hidden.failureOnly(), esInternalIndices()).at("hits.hits[*]._index")
                                .but(user.indexMatcher("read")).whenEmpty(200));
            }
        }
    }

    @Test
    public void search_wildcard() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/*/_search?size=1000");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly(), index_c1).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_wildcard_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/*::failures/_search?size=1000");
            assertThat(httpResponse, containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly(), ds_b3.failureOnly()).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_wildcard_includeHidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/*/_search?size=1000&expand_wildcards=all");
            // todo COMPONENT SELECTORS - this indicates problem with index resolution
            if (user == SUPER_UNLIMITED_USER) {
                assertThat(httpResponse,
                        containsExactly(ds_a1, ds_a2, ds_a3, ds_b1, ds_b2, ds_b3, index_c1, ds_hidden, searchGuardIndices(), esInternalIndices()).at(
                                "hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
            } else {

                assertThat(httpResponse,
                        containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly(),
                                index_c1, ds_hidden.dataOnly(), searchGuardIndices(), esInternalIndices()).at("hits.hits[*]._index")
                                .but(user.indexMatcher("read")).whenEmpty(200));
            }
        }
    }

    @Test
    public void search_wildcard_includeHidden_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/*::failures/_search?size=1000&expand_wildcards=all");
            if (user == SUPER_UNLIMITED_USER) { //todo COMPONENT SELECTORS - this indicates problem with index resolution
                assertThat(httpResponse,
                        containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly(), ds_b3.failureOnly(),
                                ds_hidden.failureOnly(), esInternalIndices()).at("hits.hits[*]._index")
                                .but(user.indexMatcher("read")).whenEmpty(200));
            } else {
                assertThat(httpResponse,
                        containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly(), ds_b3.failureOnly(),
                                ds_hidden.failureOnly(), esInternalIndices()).at("hits.hits[*]._index")
                                .but(user.indexMatcher("read")).whenEmpty(200));
            }
        }
    }

    @Test
    public void search_staticIndicies_noIgnoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a1,ds_a2,ds_b1/_search?size=1000");
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_b1.dataOnly()).at("hits.hits[*]._index").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void search_staticIndicies_noIgnoreUnavailable_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a1::failures,ds_a2::failures,ds_b1::failures/_search?size=1000");
            assertThat(httpResponse,
                    containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_b1.failureOnly()).at("hits.hits[*]._index").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void search_staticIndicies_exceptFailureStore_noIgnoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a1::data,ds_a2::data,ds_b1::data/_search?size=1000");
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_b1.dataOnly()).at("hits.hits[*]._index").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void search_staticIndicies_exceptFailureStore_noIgnoreUnavailable_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a1::failures,ds_a2::failures,ds_b1::failures/_search?size=1000");
            assertThat(httpResponse,
                    containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_b1.failureOnly()).at("hits.hits[*]._index").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void search_staticIndicies_ignoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a1,ds_a2,ds_b1/_search?size=1000&ignore_unavailable=true");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_b1.dataOnly()).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }


    @Test
    public void search_staticIndicies_ignoreUnavailable_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a1::failures,ds_a2::failures,ds_b1::failures/_search?size=1000&ignore_unavailable=true");
            assertThat(httpResponse, containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_b1.failureOnly()).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }


    @Test
    public void search_staticIndicies_negation() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a1,ds_a2,ds_b1,-ds_b1/_search?size=1000");
            if (containsExactly(ds_a1, ds_a2, ds_b1).at("hits.hits[*]._index").isCoveredBy(user.indexMatcher("read"))) {
                // A 404 error is also acceptable if we get ES complaining about -ds_b1. This will be the case for users with full permissions
                assertThat(httpResponse, isNotFound());
                assertThat(httpResponse, json(nodeAt("error.type", equalTo("index_not_found_exception"))));
                assertThat(httpResponse, json(nodeAt("error.reason", containsString("no such index [-ds_b1]"))));
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void search_staticIndicies_negation_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a1::failures,ds_a2::failures,ds_b1::failures,-ds_b1::failures/_search?size=1000");
            if (containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_b1.failureOnly()).at("hits.hits[*]._index").isCoveredBy(user.indexMatcher("read"))) {
                // A 404 error is also acceptable if we get ES complaining about -ds_b1. This will be the case for users with full permissions
                assertThat(httpResponse, isNotFound());
                assertThat(httpResponse, json(nodeAt("error.type", equalTo("index_not_found_exception"))));
                assertThat(httpResponse, json(nodeAt("error.reason", containsString("no such index [-ds_b1]"))));
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void search_staticIndicies_negation_backingIndices() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a1,ds_a2,ds_b1,-.ds-ds_b1*/_search?size=1000");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_b1.dataOnly()).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_staticIndicies_negation_backingIndices_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a1::failures,ds_a2::failures,ds_b1::failures,-.ds-ds_b1*/_search?size=1000");
            assertThat(httpResponse, containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_b1.failureOnly()).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_staticIndicies_hidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_hidden/_search?size=1000");
            assertThat(httpResponse, containsExactly(ds_hidden.dataOnly()).at("hits.hits[*]._index").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void search_staticIndicies_hidden_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_hidden::failures/_search?size=1000");
            assertThat(httpResponse, containsExactly(ds_hidden.failureOnly()).at("hits.hits[*]._index").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void search_indexPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a*,ds_b*/_search?size=1000");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly()).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_indexPattern_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a*::failures,ds_b*::failures/_search?size=1000");
            assertThat(httpResponse, containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly(), ds_b3.failureOnly()).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_indexPattern_minus() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a*,ds_b*,-ds_b2,-ds_b3/_search?size=1000");
            // Elasticsearch does not handle the expression ds_a*,ds_b*,-ds_b2,-ds_b3 in a way that excludes the data streams. See search_indexPattern_minus_backingIndices for an alternative.
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly()).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_indexPattern_minus_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a*::failures,ds_b*::failures,-ds_b2::failures,-ds_b3::failures/_search?size=1000");
            // Elasticsearch does not handle the expression ds_a*,ds_b*,-ds_b2,-ds_b3 in a way that excludes the data streams. See search_indexPattern_minus_backingIndices for an alternative.
            assertThat(httpResponse,
                    containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly(), ds_b3.failureOnly()).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_indexPattern_minus_backingIndices() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a*,ds_b*,-.ds-ds_b2*,-.ds-ds_b3*/_search?size=1000");
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly()).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_indexPattern_minus_backingIndices_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a*::failures,ds_b*::failures,-.fs-ds_b2*,-.fs-ds_b3*/_search?size=1000");
            assertThat(httpResponse,
                    containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly()).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_indexPattern_nonExistingIndex_ignoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a*,ds_b*,xxx_non_existing/_search?size=1000&ignore_unavailable=true");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly()).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_indexPattern_nonExistingIndex_ignoreUnavailable_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a*::failures,ds_b*::failures,xxx_non_existing::failures/_search?size=1000&ignore_unavailable=true");
            assertThat(httpResponse, containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly(), ds_b3.failureOnly()).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_indexPattern_noWildcards() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a*,ds_b*/_search?size=1000&expand_wildcards=none&ignore_unavailable=true");
            assertThat(httpResponse, containsExactly().at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_indexPattern_noWildcards_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a*::failures,ds_b*::failures/_search?size=1000&expand_wildcards=none&ignore_unavailable=true");
            assertThat(httpResponse, containsExactly().at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_alias_ignoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1/_search?size=1000&ignore_unavailable=true");
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly()).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_alias_ignoreUnavailable_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1::failures/_search?size=1000&ignore_unavailable=true");
            assertThat(httpResponse,
                    containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly()).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_alias_noIgnoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1/_search?size=1000");
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly()).at("hits.hits[*]._index").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void search_alias_noIgnoreUnavailable_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1::failures/_search?size=1000");
            assertThat(httpResponse,
                    containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly()).at("hits.hits[*]._index").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void search_aliasPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1*/_search?size=1000");
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly()).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_aliasPattern_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1*::failures/_search?size=1000");
            assertThat(httpResponse,
                    containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly()).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_aliasAndIndex_ignoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1,ds_b2/_search?size=1000&ignore_unavailable=true");
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly()).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_aliasAndIndex_ignoreUnavailable_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1::failures,ds_b2::failures/_search?size=1000&ignore_unavailable=true");
            assertThat(httpResponse,
                    containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly()).at("hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_aliasAndIndex_noIgnoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1,ds_b2/_search?size=1000");
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly()).at("hits.hits[*]._index").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void search_aliasAndIndex_noIgnoreUnavailable_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1::failures,ds_b2::failures/_search?size=1000");
            assertThat(httpResponse,
                    containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly()).at("hits.hits[*]._index").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void search_nonExisting_static() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("x_does_not_exist/_search?size=1000");

            if (user == UNLIMITED_USER || user == SUPER_UNLIMITED_USER) {
                assertThat(httpResponse, isNotFound());
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void search_nonExisting_static_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("x_does_not_exist::failures/_search?size=1000");

            if (user == UNLIMITED_USER || user == SUPER_UNLIMITED_USER) {
                assertThat(httpResponse, isNotFound());
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void search_nonExisting_indexPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("x_does_not_exist*/_search?size=1000");

            assertThat(httpResponse, containsExactly().at("hits.hits[*]._index").whenEmpty(200));
        }
    }

    @Test
    public void search_nonExisting_indexPattern_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("x_does_not_exist*::failures/_search?size=1000");

            assertThat(httpResponse, containsExactly().at("hits.hits[*]._index").whenEmpty(200));
        }
    }

    @Test
    public void search_termsAggregation_index() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.postJson("/_search",
                    "{\"size\":0,\"aggs\":{\"indices\":{\"terms\":{\"field\":\"_index\",\"size\":1000}}}}");

            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly(), index_c1).at("aggregations.indices.buckets[*].key")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_pit() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.post("/ds_a*/_pit?keep_alive=1m");
            assertThat(httpResponse, isOk());

            String pitId = httpResponse.getBodyAsDocNode().getAsString("id");
            httpResponse = restClient.postJson("/_search?size=1000", DocNode.of("pit.id", pitId));
            assertThat(httpResponse, isOk());
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly()).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void search_pit_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.post("/ds_a*::failures/_pit?keep_alive=1m");
            assertThat(httpResponse, isOk());

            String pitId = httpResponse.getBodyAsDocNode().getAsString("id");
            httpResponse = restClient.postJson("/_search?size=1000", DocNode.of("pit.id", pitId));
            assertThat(httpResponse, isOk());
            assertThat(httpResponse, containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly()).at("hits.hits[*]._index")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }


    @Test
    public void msearch_staticIndices() throws Exception {
        String msearchBody = "{\"index\":\"ds_b1\"}\n" //
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}\n" //
                + "{\"index\":\"ds_b2\"}\n" //
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}\n";

        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.postJson("/_msearch", msearchBody);
            assertThat(httpResponse,
                    containsExactly(ds_b1.dataOnly(), ds_b2.dataOnly()).at("responses[*].hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void msearch_staticIndices_fsAccess() throws Exception {
        String msearchBody = "{\"index\":\"ds_b1::failures\"}\n" //
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}\n" //
                + "{\"index\":\"ds_b2::failures\"}\n" //
                + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}\n";

        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.postJson("/_msearch", msearchBody);
            assertThat(httpResponse,
                    containsExactly(ds_b1.failureOnly(), ds_b2.failureOnly()).at("responses[*].hits.hits[*]._index").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void index_stats_all() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_stats");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly(), index_c1, esInternalIndices()).at("indices.keys()")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void index_stats_pattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_b*/_stats");
            assertThat(httpResponse, containsExactly(ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly()).at("indices.keys()").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void index_stats_pattern_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_b*::failures/_stats?pretty");
            log.info("Rest response status code '{}' and body {}", httpResponse.getStatusCode(), httpResponse.getBody());
            assertThat(httpResponse, containsExactly(ds_b1.failureOnly(), ds_b2.failureOnly(), ds_b3.failureOnly()).at("indices.keys()").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void getAlias_all() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("_alias");//todo COMPONENT SELECTORS - check if test related to failure store is possible
            assertThat(httpResponse,
                    containsExactly(alias_ab1.dataOnly(), alias_c1.dataOnly()).at("$.*.aliases.keys()").but(user.indexMatcher("get_alias")).whenEmpty(200));
            // Interestingly, this API does not return data streams without aliases - while it returns indices without aliases
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), index_c1, searchGuardIndices()).at("$.keys()")
                    .but(user.indexMatcher("get_alias")).whenEmpty(200));
        }
    }

    @Test
    public void getAlias_staticAlias() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            // request with alias_ab*::failures return empty response
            HttpResponse httpResponse = restClient.get("_alias/alias_c1"); // alias "alias_c1" is related to the index; therefore, test with failure store impossible.
            // RestGetAliasesAction does some further post processing on the results, thus we get 404 errors in case a non wildcard alias was removed
            assertThat(httpResponse, containsExactly(alias_c1).at("$.*.aliases.keys()").but(user.indexMatcher("get_alias")).whenEmpty(404));
            assertThat(httpResponse, containsExactly(index_c1).at("$.keys()").but(user.indexMatcher("get_alias")).whenEmpty(404));
        }
    }

    @Test
    public void getAlias_aliasPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            // request with _alias/alias_ab*:: returns empty response
            HttpResponse httpResponse = restClient.get("_alias/alias_ab*?pretty");
            log.info("Rest response status code '{}' and body {}", httpResponse.getStatusCode(), httpResponse.getBody());
            assertThat(httpResponse, containsExactly(alias_ab1.dataOnly()).at("$.*.aliases.keys()").but(user.indexMatcher("get_alias")).whenEmpty(200));
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly()).at("$.keys()").but(user.indexMatcher("get_alias")).whenEmpty(200));
        }
    }

    @Test
    public void getAlias_mixed() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("_alias/alias_ab1,alias_c*");
            // RestGetAliasesAction does some further post processing on the results, thus we get 404 errors in case a non wildcard alias was removed

            assertThat(httpResponse,
                    containsExactly(alias_ab1.dataOnly(), alias_c1.dataOnly()).at("$.*.aliases.keys()").but(user.indexMatcher("get_alias")).whenEmpty(404));
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), index_c1).at("$.keys()").but(user.indexMatcher("get_alias")).whenEmpty(404));
        }
    }

    @Test
    @Ignore
    public void getAlias_mixed_fsAccess() throws Exception {//todo COMPONENT SELECTORS - Causes an error in privileges evaluator "Null elements are not supported [java.lang.IllegalArgumentException: Null elements are not supported]"
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            // This is an edge case. Alias alias_c1 is related to the index, expressions like alias_c1::failures are invalid
            HttpResponse httpResponse = restClient.get("/_alias/alias_ab1::failures,alias_c*::failures");
            log.info("Rest response status code '{}' and body {}", httpResponse.getStatusCode(), httpResponse.getBody());
            // For failure stores, aliases may not be present, but we still need to check
            assertThat(httpResponse,
                    containsExactly(alias_ab1.failureOnly()).at("$.*.aliases.keys()").but(user.indexMatcher("get_alias")).whenEmpty(404));
            assertThat(httpResponse,
                    containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly()).at("$.keys()").but(user.indexMatcher("get_alias")).whenEmpty(404));
        }
    }

    @Test
    public void getDataStream_all() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("_data_stream");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly()).at("$.data_streams[*].name")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void getDataStream_wildcard() throws Exception {
        // API requests, such as GET /_data_stream/{data_stream_name}, do not support component selectors; ES returns a 400 code.
        // Therefore, methods with a postfix _fsAccess do not exist
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("_data_stream/*");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly()).at("$.data_streams[*].name")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void getDataStream_pattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("_data_stream/ds_a*");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly()).at("$.data_streams[*].name").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void getDataStream_pattern_negation() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("_data_stream/ds_*,-ds_b*");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly()).at("$.data_streams[*].name").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void getDataStream_static() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("_data_stream/ds_a1,ds_a2");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly()).at("$.data_streams[*].name").but(user.indexMatcher("read")).whenEmpty(403));
        }
    }

    @Test
    public void getDataStreamStats_all() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("_data_stream/_stats");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly()).at("$.data_streams[*].data_stream")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void getDataStreamStats_wildcard() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("_data_stream/*/_stats");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly()).at("$.data_streams[*].data_stream")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void getDataStreamStats_pattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("_data_stream/ds_a*/_stats");
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly()).at("$.data_streams[*].data_stream").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void getDataStreamStats_pattern2() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("_data_stream/ds_*/_stats");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly()).at("$.data_streams[*].data_stream")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void getDataStreamStats_static() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("_data_stream/ds_a1,ds_a2/_stats");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly()).at("$.data_streams[*].data_stream").but(user.indexMatcher("read")).whenEmpty(403));
        }
    }

    @Test
    public void resolve_wildcard() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_resolve/index/*");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly(), index_c1.dataOnly(), alias_ab1.dataOnly(), alias_c1.dataOnly()).at("$.*[*].name")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void resolve_wildcard_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_resolve/index/*::failures?pretty");
            log.info("Rest response status code '{}' and body {}", httpResponse.getStatusCode(), httpResponse.getBody());
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly(), alias_ab1.dataOnly(), alias_c1).at("$.*[*].name")
                    .but(user.indexMatcher("read")).whenEmpty(200));
            assertThat(httpResponse, containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly()).at("$.aliases[*].indices")
                    .but(user.indexMatcher("get_alias")).whenEmpty(200));
            assertThat(httpResponse, containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly(), ds_b3.failureOnly()).at("$.data_streams[*].backing_indices")
                    .but(user.indexMatcher("read")).whenEmpty(200));

        }
    }

    @Test
    public void resolve_wildcard_includeHidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_resolve/index/*?expand_wildcards=all");
            if (user == SUPER_UNLIMITED_USER) { //todo COMPONENT SELECTORS - indicates problems related to index resolution
                assertThat(httpResponse,
                        containsExactly(ds_a1, ds_a2, ds_a3, ds_b1, ds_b2, ds_b3,
                                index_c1, alias_ab1.dataOnly(), //todo COMPONENT SELECTORS - alias_ab1.dataOnly() - why dataOnly invocation is needed here
                                alias_c1, ds_hidden, searchGuardIndices(),
                                esInternalIndices()).at("$.*[*].name").but(user.indexMatcher("read")).whenEmpty(200));
            } else {
                assertThat(httpResponse,
                        containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly(),
                                index_c1, alias_ab1.dataOnly(), alias_c1.dataOnly(), ds_hidden.dataOnly(), searchGuardIndices(),
                                esInternalIndices()).at("$.*[*].name").but(user.indexMatcher("read")).whenEmpty(200));
            }
        }
    }

    @Test
    public void resolve_wildcard_includeHidden_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_resolve/index/*::failures?expand_wildcards=all&pretty");
            log.info("Rest response status code '{}' and body {}", httpResponse.getStatusCode(), httpResponse.getBody());
            if ((user == SUPER_UNLIMITED_USER) || (user == UNLIMITED_USER)) { //todo COMPONENT SELECTORS - indicates problems related to index resolution
                assertThat(httpResponse,
                        containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly(),
                                ds_hidden.dataOnly(), alias_ab1.dataOnly(), alias_c1, esInternalIndices()).at("$.*[*].name").but(user.indexMatcher("read")).whenEmpty(200));
                assertThat(httpResponse, containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly()).at("$.aliases[*].indices")
                        .but(user.indexMatcher("get_alias")).whenEmpty(200));
                assertThat(httpResponse, containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly(), ds_b3.failureOnly(), ds_hidden.failureOnly()).at("$.data_streams[*].backing_indices")
                        .but(user.indexMatcher("read")).whenEmpty(200));
            } else {
                assertThat(httpResponse,
                        containsExactly(ds_a1, ds_a2, ds_a3, ds_b1, ds_b2, ds_b3,
                                ds_hidden, alias_ab1.dataOnly(), alias_c1, esInternalIndices()).at("$.*[*].name").but(user.indexMatcher("read")).whenEmpty(200));
                assertThat(httpResponse, containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly()).at("$.aliases[*].indices")
                        .but(user.indexMatcher("get_alias")).whenEmpty(200));
                assertThat(httpResponse, containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly(), ds_b3.failureOnly(), ds_hidden.failureOnly()).at("$.data_streams[*].backing_indices")
                        .but(user.indexMatcher("read")).whenEmpty(200));
            }
        }
    }

    @Test
    public void resolve_indexPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_resolve/index/ds_a*,ds_b*");
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly()).at("$.*[*].name").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void resolve_indexPattern_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_resolve/index/ds_a*::failures,ds_b*::failures?pretty");
            log.info("Rest response status code '{}' and body {}", httpResponse.getStatusCode(), httpResponse.getBody());
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly()).at("$.*[*].name").but(user.indexMatcher("read")).whenEmpty(200));
            assertThat(httpResponse, containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly(), ds_b3.failureOnly()).at("$.data_streams[*].backing_indices")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_all() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("/_field_caps?fields=*");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly(), index_c1).at("indices")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_indexPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a*,ds_b*/_field_caps?fields=*");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly()).at("indices")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_indexPattern_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a*::failures,ds_b*::failures/_field_caps?fields=*");
            assertThat(httpResponse, containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly(), ds_b3.failureOnly()).at("indices")
                    .but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_staticIndices_noIgnoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a1,ds_a2,ds_b1/_field_caps?fields=*");
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_b1.dataOnly()).at("indices").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void field_caps_staticIndices_noIgnoreUnavailable_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a1::failures,ds_a2::failures,ds_b1::failures/_field_caps?fields=*");
            assertThat(httpResponse,
                    containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_b1.failureOnly()).at("indices").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void field_caps_staticIndices_ignoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a1,ds_a2,ds_b1/_field_caps?fields=*&ignore_unavailable=true");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_b1.dataOnly()).at("indices").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_staticIndices_ignoreUnavailable_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a1::failures,ds_a2::failures,ds_b1::failures/_field_caps?fields=*&ignore_unavailable=true");
            assertThat(httpResponse, containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_b1.failureOnly()).at("indices").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_staticIndices_hidden() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_hidden/_field_caps?fields=*");
            assertThat(httpResponse, containsExactly(ds_hidden.dataOnly()).at("indices").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void field_caps_staticIndices_hidden_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_hidden::failures/_field_caps?fields=*");
            assertThat(httpResponse, containsExactly(ds_hidden.failureOnly()).at("indices").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void field_caps_alias_ignoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1/_field_caps?fields=*&ignore_unavailable=true");
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly()).at("indices").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_alias_ignoreUnavailable_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1::failures/_field_caps?fields=*&ignore_unavailable=true");
            assertThat(httpResponse,
                    containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly()).at("indices").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_alias_noIgnoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1/_field_caps?fields=*");
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly()).at("indices").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void field_caps_alias_noIgnoreUnavailable_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1::failures/_field_caps?fields=*");
            assertThat(httpResponse,
                    containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly()).at("indices").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void field_caps_aliasPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1*/_field_caps?fields=*");
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly()).at("indices").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_aliasPattern_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1*::failures/_field_caps?fields=*");
            assertThat(httpResponse,
                    containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly()).at("indices").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_nonExisting_static() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("x_does_not_exist/_field_caps?fields=*");

            if (user == UNLIMITED_USER || user == SUPER_UNLIMITED_USER) {
                assertThat(httpResponse, isNotFound());
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void field_caps_nonExisting_static_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("x_does_not_exist::failures/_field_caps?fields=*");

            if (user == UNLIMITED_USER || user == SUPER_UNLIMITED_USER) {
                assertThat(httpResponse, isNotFound());
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void field_caps_nonExisting_indexPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("x_does_not_exist*/_field_caps?fields=*");

            assertThat(httpResponse, containsExactly().at("indices").whenEmpty(200));
        }
    }

    @Test
    public void field_caps_nonExisting_indexPattern_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("x_does_not_exist*::failures/_field_caps?fields=*");

            assertThat(httpResponse, containsExactly().at("indices").whenEmpty(200));
        }
    }

    @Test
    public void field_caps_aliasAndDataStream_ignoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1,ds_b2/_field_caps?fields=*&ignore_unavailable=true");
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly()).at("indices").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_aliasAndDataStream_ignoreUnavailable_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1::failures,ds_b2::failures/_field_caps?fields=*&ignore_unavailable=true");
            assertThat(httpResponse,
                    containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly()).at("indices").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_aliasAndDataStream_noIgnoreUnavailable() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1,ds_b2/_field_caps?fields=*");
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly()).at("indices").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void field_caps_aliasAndDataStream_noIgnoreUnavailable_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("alias_ab1::failures,ds_b2::failures/_field_caps?fields=*");
            assertThat(httpResponse,
                    containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly()).at("indices").butForbiddenIfIncomplete(user.indexMatcher("read")));
        }
    }

    @Test
    public void field_caps_staticIndices_negation() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a1,ds_a2,ds_b1,-ds_b1/_field_caps?fields=*");
            if (containsExactly(ds_a1, ds_a2, ds_b1).at("indices").isCoveredBy(user.indexMatcher("read"))) {
                // A 404 error is also acceptable if we get ES complaining about -ds_b1. This will be the case for users with full permissions
                assertThat(httpResponse, isNotFound());
                assertThat(httpResponse, json(nodeAt("error.type", equalTo("index_not_found_exception"))));
                assertThat(httpResponse, json(nodeAt("error.reason", containsString("no such index [-ds_b1]"))));
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void field_caps_staticIndices_negation_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a1::failures,ds_a2::failures,ds_b1::failures,-ds_b1::failures/_field_caps?fields=*");
            if (containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_b1.failureOnly()).at("indices").isCoveredBy(user.indexMatcher("read"))) {
                // A 404 error is also acceptable if we get ES complaining about -ds_b1. This will be the case for users with full permissions
                assertThat(httpResponse, isNotFound());
                assertThat(httpResponse, json(nodeAt("error.type", equalTo("index_not_found_exception"))));
                assertThat(httpResponse, json(nodeAt("error.reason", containsString("no such index [-ds_b1]"))));
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void field_caps_indexPattern_minus() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a*,ds_b*,-ds_b2,-ds_b3/_field_caps?fields=*");
            // Elasticsearch does not handle the expression ds_a*,ds_b*,-ds_b2,-ds_b3 in a way that excludes the data streams. See field_caps_indexPattern_minus_backingIndices for an alternative.
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly(), ds_b2.dataOnly(), ds_b3.dataOnly()).at("indices").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_indexPattern_minus_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a*::failures,ds_b*::failures,-ds_b2::failures,-ds_b3::failures/_field_caps?fields=*");
            // Elasticsearch does not handle the expression ds_a*,ds_b*,-ds_b2,-ds_b3 in a way that excludes the data streams. See field_caps_indexPattern_minus_backingIndices for an alternative.
            assertThat(httpResponse,
                    containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly(), ds_b2.failureOnly(), ds_b3.failureOnly()).at("indices").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_indexPattern_minus_backingIndices() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a*,ds_b*,-.ds-ds_b2*,-.ds-ds_b3*/_field_caps?fields=*");
            assertThat(httpResponse,
                    containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_a3.dataOnly(), ds_b1.dataOnly()).at("indices").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_indexPattern_minus_backingIndices_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            //.fs-ds_b2*,.fs-ds_b3* - patterns related to data stream backing indices (failure store) - indices does not accept selectors like ::failures
            HttpResponse httpResponse = restClient.get("ds_a*::failures,ds_b*::failures,-.fs-ds_b2*,-.fs-ds_b3*/_field_caps?fields=*");
            assertThat(httpResponse,
                    containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_a3.failureOnly(), ds_b1.failureOnly()).at("indices").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_staticIndices_negation_backingIndices() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.get("ds_a1,ds_a2,ds_b1,-.ds-ds_b1*/_field_caps?fields=*");
            assertThat(httpResponse, containsExactly(ds_a1.dataOnly(), ds_a2.dataOnly(), ds_b1.dataOnly()).at("indices").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Test
    public void field_caps_staticIndices_negation_backingIndices_fsAccess() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            //-.fs-ds_b1* - patterns related to data stream backing indices (failure store) - indices does not accept selectors like ::failures
            HttpResponse httpResponse = restClient.get("ds_a1::failures,ds_a2::failures,ds_b1::failures,-.fs-ds_b1*/_field_caps?fields=*");
            assertThat(httpResponse, containsExactly(ds_a1.failureOnly(), ds_a2.failureOnly(), ds_b1.failureOnly()).at("indices").but(user.indexMatcher("read")).whenEmpty(200));
        }
    }

    @Parameters(name = "{1}")
    public static Collection<Object[]> params() {
        List<Object[]> result = new ArrayList<>();

        for (TestSgConfig.User user : USERS) {
            result.add(new Object[] { user, user.getDescription() });
        }

        return result;
    }

    public DataStreamFailureStoreAuthorizationReadOnlyIntTests(TestSgConfig.User user, String description) throws Exception {
        this.user = user;
    }

}
