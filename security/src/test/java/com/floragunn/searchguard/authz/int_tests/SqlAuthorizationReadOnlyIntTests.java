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
import com.floragunn.searchguard.test.IndexApiMatchers;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestComponentTemplate;
import com.floragunn.searchguard.test.TestDataStream;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestIndexTemplate;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import static com.floragunn.searchguard.test.IndexApiMatchers.sqlLimitedTo;
import static com.floragunn.searchguard.test.RestMatchers.distinctNodesAt;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@RunWith(Suite.class)
@Suite.SuiteClasses({ SqlAuthorizationReadOnlyIntTests.SqlAuthorizationTests.class, SqlAuthorizationReadOnlyIntTests.SqlAuthorizationFlsFmTests.class })
public class SqlAuthorizationReadOnlyIntTests {

    static final Pattern HEX_HASH_PATTERN = Pattern.compile("[0-9a-f]{64}");

    static TestIndex index_a1 = TestIndex.name("index_a1").documentCount(100).attr("index_or_ds", "index_a1").seed(2).build();
    static TestIndex index_a2 = TestIndex.name("index_a2").documentCount(100).attr("index_or_ds", "index_a2").seed(3).build();
    static TestIndex index_b1 = TestIndex.name("index_b1").documentCount(100).attr("index_or_ds", "index_b1").seed(4).build();
    static TestIndex index_b2 = TestIndex.name("index_b2").documentCount(100).attr("index_or_ds", "index_b2").seed(5).build();
    static TestAlias alias_a = new TestAlias("alias_a", index_a1, index_a2);
    static TestAlias alias_b = new TestAlias("alias_b", index_b1, index_b2);
    static TestDataStream ds_a = TestDataStream.name("ds_a").documentCount(100).attr("index_or_ds", "ds_a").seed(6).rolloverAfter(10).build();
    static TestDataStream ds_b = TestDataStream.name("ds_b").documentCount(100).attr("index_or_ds", "ds_b").seed(7).rolloverAfter(10).build();

    static TestSgConfig.User USER_INDEX_A1 = new TestSgConfig.User("user_index_a1")
            .description("user with access to index a1")
            .roles(
                    new TestSgConfig.Role("r1")
                            .clusterPermissions("indices:data/read/sql", "indices:data/read/sql/close_cursor", "indices:data/read/close_point_in_time")
                            .indexPermissions("indices:data/read/field_caps", "indices:data/read/open_point_in_time", "indices:data/read/search").on("index_a1")

            )
            .indexMatcher("read", sqlLimitedTo(index_a1));

    static TestSgConfig.User USER_INDEX_A1_WITH_DLS = new TestSgConfig.User("user_index_a1_with_dls")
            .description("user with access to index a1, and dls excluding dept_d")
            .roles(
                    new TestSgConfig.Role("r1")
                            .clusterPermissions("indices:data/read/sql", "indices:data/read/sql/close_cursor", "indices:data/read/close_point_in_time")
                            .indexPermissions("indices:data/read/field_caps", "indices:data/read/open_point_in_time", "indices:data/read/search")
                            .dls("{ \"bool\": { \"must_not\": { \"match\": { \"dept\": \"dept_d\" }}}}")
                            .on("index_a1")

            )
            .indexMatcher("read", sqlLimitedTo(index_a1.filteredBy(doc -> ! doc.getAsString("dept").equals("dept_d"))));

    static TestSgConfig.User USER_INDEX_A2 = new TestSgConfig.User("user_index_a2")
            .description("user with access to index a2")
            .roles(
                    new TestSgConfig.Role("r1")
                            .clusterPermissions("indices:data/read/sql", "indices:data/read/sql/close_cursor", "indices:data/read/close_point_in_time")
                            .indexPermissions("indices:data/read/field_caps", "indices:data/read/open_point_in_time", "indices:data/read/search").on("index_a2")

            )
            .indexMatcher("read", sqlLimitedTo(index_a2));

    static TestSgConfig.User USER_INDEX_B1 = new TestSgConfig.User("user_index_b1")
            .description("user with access to index b1")
            .roles(
                    new TestSgConfig.Role("r1")
                            .clusterPermissions("indices:data/read/sql", "indices:data/read/sql/close_cursor", "indices:data/read/close_point_in_time")
                            .indexPermissions("indices:data/read/field_caps", "indices:data/read/open_point_in_time", "indices:data/read/search").on("index_b1")

            )
            .indexMatcher("read", sqlLimitedTo(index_b1));

    static TestSgConfig.User USER_INDEX_B2 = new TestSgConfig.User("user_index_b2")
            .description("user with access to index b2")
            .roles(
                    new TestSgConfig.Role("r1")
                            .clusterPermissions("indices:data/read/sql", "indices:data/read/sql/close_cursor", "indices:data/read/close_point_in_time")
                            .indexPermissions("indices:data/read/field_caps", "indices:data/read/open_point_in_time", "indices:data/read/search").on("index_b2")

            )
            .indexMatcher("read", sqlLimitedTo(index_b2));

    static TestSgConfig.User USER_ALL_INDICES = new TestSgConfig.User("user_all_indices")
            .description("user with access to all indices")
            .roles(
                    new TestSgConfig.Role("r1")
                            .clusterPermissions("indices:data/read/sql", "indices:data/read/sql/close_cursor", "indices:data/read/close_point_in_time")
                            .indexPermissions("indices:data/read/field_caps", "indices:data/read/open_point_in_time", "indices:data/read/search").on("index_*")

            )
            .indexMatcher("read", sqlLimitedTo(index_a1, index_a2, index_b1, index_b2));

    static TestSgConfig.User USER_ALIAS_A = new TestSgConfig.User("user_alias_a")
            .description("user with access to alias a")
            .roles(
                    new TestSgConfig.Role("r1")
                            .clusterPermissions("indices:data/read/sql", "indices:data/read/sql/close_cursor", "indices:data/read/close_point_in_time")
                            .aliasPermissions("indices:data/read/field_caps", "indices:data/read/open_point_in_time", "indices:data/read/search").on("alias_a")

            )
            .indexMatcher("read", sqlLimitedTo(index_a1, index_a2, alias_a));

    static TestSgConfig.User USER_ALIAS_A_WITH_DLS = new TestSgConfig.User("user_alias_a_with_dls")
            .description("user with access to alias a, and dls excluding dept_d")
            .roles(
                    new TestSgConfig.Role("r1")
                            .clusterPermissions("indices:data/read/sql", "indices:data/read/sql/close_cursor", "indices:data/read/close_point_in_time")
                            .aliasPermissions("indices:data/read/field_caps", "indices:data/read/open_point_in_time", "indices:data/read/search")
                            .dls("{ \"bool\": { \"must_not\": { \"match\": { \"dept\": \"dept_d\" }}}}")
                            .on("alias_a")

            )
            .indexMatcher("read", sqlLimitedTo(
                    index_a1.filteredBy(doc -> ! doc.getAsString("dept").equals("dept_d")),
                    index_a2.filteredBy(doc -> ! doc.getAsString("dept").equals("dept_d")),
                    alias_a.filteredBy(doc -> ! doc.getAsString("dept").equals("dept_d"))
            ));

    static TestSgConfig.User USER_ALIAS_B = new TestSgConfig.User("user_alias_b")
            .description("user with access to alias b")
            .roles(
                    new TestSgConfig.Role("r1")
                            .clusterPermissions("indices:data/read/sql", "indices:data/read/sql/close_cursor", "indices:data/read/close_point_in_time")
                            .aliasPermissions("indices:data/read/field_caps", "indices:data/read/open_point_in_time", "indices:data/read/search").on("alias_b")

            )
            .indexMatcher("read", sqlLimitedTo(index_b1, index_b2, alias_b));

    static TestSgConfig.User USER_DS_A = new TestSgConfig.User("user_ds_a")
            .description("user with access to data stream a")
            .roles(
                    new TestSgConfig.Role("r1")
                            .clusterPermissions("indices:data/read/sql", "indices:data/read/sql/close_cursor", "indices:data/read/close_point_in_time")
                            .dataStreamPermissions("indices:data/read/field_caps", "indices:data/read/open_point_in_time", "indices:data/read/search").on("ds_a")

            )
            .indexMatcher("read", sqlLimitedTo(ds_a));

    static TestSgConfig.User USER_DS_B = new TestSgConfig.User("user_ds_b")
            .description("user with access to data stream b")
            .roles(
                    new TestSgConfig.Role("r1")
                            .clusterPermissions("indices:data/read/sql", "indices:data/read/sql/close_cursor", "indices:data/read/close_point_in_time")
                            .dataStreamPermissions("indices:data/read/field_caps", "indices:data/read/open_point_in_time", "indices:data/read/search").on("ds_b")

            )
            .indexMatcher("read", sqlLimitedTo(ds_b));

    static TestSgConfig.User USER_DS_B_WITH_DLS = new TestSgConfig.User("user_ds_b_with_dls")
            .description("user with access to data stream b, and dls excluding dept_d")
            .roles(
                    new TestSgConfig.Role("r1")
                            .clusterPermissions("indices:data/read/sql", "indices:data/read/sql/close_cursor", "indices:data/read/close_point_in_time")
                            .dataStreamPermissions("indices:data/read/field_caps", "indices:data/read/open_point_in_time", "indices:data/read/search")
                            .dls("{ \"bool\": { \"must_not\": { \"match\": { \"dept\": \"dept_d\" }}}}")
                            .on("ds_b")

            )
            .indexMatcher("read", sqlLimitedTo(ds_b.filteredBy(doc -> ! doc.getAsString("dept").equals("dept_d"))));

    static TestSgConfig.User USER_ALL_DS = new TestSgConfig.User("user_all_ds")
            .description("user with access to all data streams")
            .roles(
                    new TestSgConfig.Role("r1")
                            .clusterPermissions("indices:data/read/sql", "indices:data/read/sql/close_cursor", "indices:data/read/close_point_in_time")
                            .dataStreamPermissions("indices:data/read/field_caps", "indices:data/read/open_point_in_time", "indices:data/read/search").on("ds_*")

            )
            .indexMatcher("read", sqlLimitedTo(ds_a, ds_b));

    static TestSgConfig.User UNLIMITED_USER = new TestSgConfig.User("unlimited_user")
            .description("unlimited")
            .roles(
                    new TestSgConfig.Role("r1")
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")
                            .indexPermissions("*").on("*")
                            .aliasPermissions("*").on("*")
                            .dataStreamPermissions("*").on("*")

            )
            .indexMatcher("read", sqlLimitedTo(index_a1, index_a2, index_b1, index_b2, alias_a, alias_b, ds_a, ds_b));

    static TestSgConfig.User UNLIMITED_USER_WITH_FLS = new TestSgConfig.User("unlimited_user_with_fls")
            .description("unlimited, but has FLS on *_loc fields")
            .roles(
                    new TestSgConfig.Role("r1")
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")
                            .aliasPermissions("*").fls("~*_loc").on("*")
                            .dataStreamPermissions("*").fls("~*_loc").on("*")

            )
            .addFieldValueMatcher("$.columns[*].name", false, everyItem(not(endsWith("_loc"))))
            .addFieldValueMatcher("$.rows[*]", true, contains(not(matchesPattern(HEX_HASH_PATTERN)), not(matchesPattern(HEX_HASH_PATTERN))));

    static TestSgConfig.User UNLIMITED_USER_WITH_FM = new TestSgConfig.User("unlimited_user_with_fm")
            .description("unlimited, but has fm on *_ip fields")
            .roles(
                    new TestSgConfig.Role("r1")
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")
                            .aliasPermissions("*").maskedFields("*_ip").on("*")
                            .dataStreamPermissions("*").maskedFields("*_ip").on("*")

            )
            .addFieldValueMatcher("$.columns[*].name", false, hasItem(endsWith("_loc")))
            .addFieldValueMatcher("$.rows[*]", true, contains(not(matchesPattern(HEX_HASH_PATTERN)), matchesPattern(HEX_HASH_PATTERN)));

    /**
     * The SUPER_UNLIMITED_USER authenticates with an admin cert, which will cause all access control code to be skipped.
     * This serves as a base for comparison with the default behavior.
     */
    static TestSgConfig.User SUPER_UNLIMITED_USER = new TestSgConfig.User("super_unlimited_user")
            .description("super unlimited (admin cert)")
            .adminCertUser()
            .indexMatcher("read", sqlLimitedTo(index_a1, index_a2, index_b1, index_b2, alias_a, alias_b, ds_a, ds_b))
            .addFieldValueMatcher("$.columns[*].name", false, hasItem(endsWith("_loc")))
            .addFieldValueMatcher("$.rows[*]", true, contains(not(matchesPattern(HEX_HASH_PATTERN)), not(matchesPattern(HEX_HASH_PATTERN))));

    static List<TestSgConfig.User> USERS = ImmutableList.of(
            USER_INDEX_A1, USER_INDEX_A1_WITH_DLS, USER_INDEX_A2, USER_INDEX_B1, USER_INDEX_B2, USER_ALL_INDICES,
            USER_ALIAS_A, USER_ALIAS_A_WITH_DLS, USER_ALIAS_B, USER_DS_A, USER_DS_B, USER_DS_B_WITH_DLS, USER_ALL_DS,
            UNLIMITED_USER, UNLIMITED_USER_WITH_FLS, UNLIMITED_USER_WITH_FM, SUPER_UNLIMITED_USER
    );

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().users(USERS)
            .indexTemplates(new TestIndexTemplate("ds_test", "ds_*").dataStream().composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL))
            .indices(index_a1, index_a2, index_b1, index_b2)
            .aliases(alias_a, alias_b)
            .dataStreams(ds_a, ds_b)
            .authzDebug(true)
            .useExternalProcessCluster()
            .enterpriseModulesEnabled()
            .build();

    @RunWith(Parameterized.class)
    public static class SqlAuthorizationTests {

        final TestSgConfig.User user;

        static List<TestSgConfig.User> USERS = ImmutableList.of(
                USER_INDEX_A1, USER_INDEX_A1_WITH_DLS, USER_INDEX_A2, USER_INDEX_B1, USER_INDEX_B2, USER_ALL_INDICES,
                USER_ALIAS_A, USER_ALIAS_A_WITH_DLS, USER_ALIAS_B, USER_DS_A, USER_DS_B, USER_DS_B_WITH_DLS, USER_ALL_DS, UNLIMITED_USER, SUPER_UNLIMITED_USER
        );

        public SqlAuthorizationTests(TestSgConfig.User user, String description) {
            this.user = user;
        }

        @Parameterized.Parameters(name = "{1}")
        public static Collection<Object[]> params() {
            List<Object[]> result = new ArrayList<>();

            for (TestSgConfig.User user : SqlAuthorizationTests.USERS) {
                result.add(new Object[] { user, user.getDescription() });
            }

            return result;
        }

        @Test
        public void queryIndicesWildcard() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                GenericRestClient.HttpResponse response = client.postSql("SELECT * FROM \"index_a*\"");
                //todo 400 due to missing field caps permissions, we should return 403 instead, please see https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/issues/507
                assertThat(response, sqlLimitedTo(index_a1, index_a2).but(user.indexMatcher("read")).whenEmpty(400));
            }
        }

        @Test
        public void queryOneIndex() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                GenericRestClient.HttpResponse response = client.postSql("SELECT * FROM \"index_a1\"");
                //todo 400 due to missing field caps permissions, we should return 403 instead, please see https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/issues/507
                assertThat(response, sqlLimitedTo(index_a1).but(user.indexMatcher("read")).whenEmpty(400));
            }

            try (GenericRestClient client = cluster.getRestClient(user)) {
                GenericRestClient.HttpResponse response = client.postSql("SELECT * FROM \"index_a2\"");
                //todo 400 due to missing field caps permissions, we should return 403 instead, please see https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/issues/507
                assertThat(response, sqlLimitedTo(index_a2).but(user.indexMatcher("read")).whenEmpty(400));
            }

            try (GenericRestClient client = cluster.getRestClient(user)) {
                GenericRestClient.HttpResponse response = client.postSql("SELECT * FROM \"index_b1\"");
                //todo 400 due to missing field caps permissions, we should return 403 instead, please see https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/issues/507
                assertThat(response, sqlLimitedTo(index_b1).but(user.indexMatcher("read")).whenEmpty(400));
            }
        }

        @Test
        public void queryAliasesWildcard() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                GenericRestClient.HttpResponse response = client.postSql("SELECT * FROM \"alias_*\"");
                //todo 400 due to missing field caps permissions, we should return 403 instead, please see https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/issues/507
                assertThat(response, sqlLimitedTo(index_a1, index_a2, index_b1, index_b2, alias_a, alias_b).but(user.indexMatcher("read")).whenEmpty(400));
            }
        }

        @Test
        public void queryOneAlias() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                //user needs direct access to an alias or access any of member indices
                GenericRestClient.HttpResponse response = client.postSql("SELECT * FROM \"alias_a\"");
                //todo 400 due to missing field caps permissions, we should return 403 instead, please see https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/issues/507
                assertThat(response, sqlLimitedTo(index_a1, index_a2, alias_a).but(user.indexMatcher("read")).whenEmpty(400));
            }
            try (GenericRestClient client = cluster.getRestClient(user)) {
                //user needs direct access to an alias or access to any of member indices
                GenericRestClient.HttpResponse response = client.postSql("SELECT * FROM \"alias_b\"");
                //todo 400 due to missing field caps permissions, we should return 403 instead, please see https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/issues/507
                assertThat(response, sqlLimitedTo(index_b1, index_b2, alias_b).but(user.indexMatcher("read")).whenEmpty(400));
            }
        }

        @Test
        public void queryDataStreamsWildcard() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                GenericRestClient.HttpResponse response = client.postSql("SELECT * FROM \"ds_*\"");
                //todo 400 due to missing field caps permissions, we should return 403 instead, please see https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/issues/507
                assertThat(response, sqlLimitedTo(ds_a, ds_b).but(user.indexMatcher("read")).whenEmpty(400));
            }
        }

        @Test
        public void queryOneDataStream() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                GenericRestClient.HttpResponse response = client.postSql("SELECT * FROM \"ds_a\"");
                //todo 400 due to missing field caps permissions, we should return 403 instead, please see https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/issues/507
                assertThat(response, sqlLimitedTo(ds_a).but(user.indexMatcher("read")).whenEmpty(400));
            }
            try (GenericRestClient client = cluster.getRestClient(user)) {
                GenericRestClient.HttpResponse response = client.postSql("SELECT * FROM \"ds_b\"");
                //todo 400 due to missing field caps permissions, we should return 403 instead, please see https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/issues/507
                assertThat(response, sqlLimitedTo(ds_b).but(user.indexMatcher("read")).whenEmpty(400));
            }
        }

        @Test
        public void queryIndicesWildcard_withCursor() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                DocNode body = DocNode.of(
                        "query", "SELECT * FROM \"index_a*\"",
                        "fetch_size", 5
                );
                GenericRestClient.HttpResponse response = client.postSql(body);

                IndexApiMatchers.SqlLimitedToMatcher indexAliasMatcher = (IndexApiMatchers.SqlLimitedToMatcher) sqlLimitedTo(index_a1, index_a2)
                        .but(user.indexMatcher("read"));

                //todo 400 due to missing field caps permissions, we should return 403 instead, please see https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/issues/507
                assertThat(response, indexAliasMatcher.whenEmpty(400));
                if (! indexAliasMatcher.but(user.indexMatcher("read")).isEmpty()) {
                    assertThat(response.getBody(), response, json(nodeAt("$.cursor", notNullValue())));
                    String cursor = response.getBodyAsDocNode().getAsString("cursor");
                    response = client.postSql(DocNode.of("cursor", cursor));
                    assertThat(response, indexAliasMatcher.shouldContainColumns(false));

                    //close cursor
                    response = client.postJson("/_sql/close", DocNode.of("cursor", cursor));
                    assertThat(response, isOk());
                }
            }
        }

        @Test
        public void queryAliasesWildcard_withCursor() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                DocNode body = DocNode.of(
                        "query", "SELECT * FROM \"alias_*\"",
                        "fetch_size", 5
                );
                GenericRestClient.HttpResponse response = client.postSql(body);

                IndexApiMatchers.SqlLimitedToMatcher indexAliasMatcher = (IndexApiMatchers.SqlLimitedToMatcher) sqlLimitedTo(index_a1, index_a2, index_b1, index_b2, alias_a, alias_b)
                        .but(user.indexMatcher("read"));

                //todo 400 due to missing field caps permissions, we should return 403 instead, please see https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/issues/507
                assertThat(response, indexAliasMatcher.whenEmpty(400));
                if (! indexAliasMatcher.but(user.indexMatcher("read")).isEmpty()) {
                    assertThat(response.getBody(), response, json(nodeAt("$.cursor", notNullValue())));
                    String cursor = response.getBodyAsDocNode().getAsString("cursor");
                    response = client.postSql(DocNode.of("cursor", cursor));
                    assertThat(response, indexAliasMatcher.shouldContainColumns(false));

                    //close cursor
                    response = client.postJson("/_sql/close", DocNode.of("cursor", cursor));
                    assertThat(response, isOk());
                }
            }
        }

        @Test
        public void queryDataStreamsWildcard_withCursor() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                DocNode body = DocNode.of(
                        "query", "SELECT * FROM \"ds_*\"",
                        "fetch_size", 5
                );
                GenericRestClient.HttpResponse response = client.postSql(body);

                IndexApiMatchers.SqlLimitedToMatcher dsMatcher = (IndexApiMatchers.SqlLimitedToMatcher) sqlLimitedTo(ds_a, ds_b)
                        .but(user.indexMatcher("read"));

                //todo 400 due to missing field caps permissions, we should return 403 instead, please see https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/issues/507
                assertThat(response, dsMatcher.whenEmpty(400));
                if (! dsMatcher.but(user.indexMatcher("read")).isEmpty()) {
                    assertThat(response.getBody(), response, json(nodeAt("$.cursor", notNullValue())));
                    String cursor = response.getBodyAsDocNode().getAsString("cursor");
                    response = client.postSql(DocNode.of("cursor", cursor));
                    assertThat(response, dsMatcher.shouldContainColumns(false));

                    //close cursor
                    response = client.postJson("/_sql/close", DocNode.of("cursor", cursor));
                    assertThat(response, isOk());
                }
            }
        }

    }

    @RunWith(Parameterized.class)
    public static class SqlAuthorizationFlsFmTests {

        final TestSgConfig.User user;

        static List<TestSgConfig.User> USERS = ImmutableList.of(
            UNLIMITED_USER_WITH_FLS, UNLIMITED_USER_WITH_FM, SUPER_UNLIMITED_USER
        );

        public SqlAuthorizationFlsFmTests(TestSgConfig.User user, String description) {
            this.user = user;
        }

        @Parameterized.Parameters(name = "{1}")
        public static Collection<Object[]> params() {
            List<Object[]> result = new ArrayList<>();

            for (TestSgConfig.User user : SqlAuthorizationFlsFmTests.USERS) {
                result.add(new Object[]{user, user.getDescription()});
            }

            return result;
        }

        @Test
        public void queryIndicesWildcard_fls() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                GenericRestClient.HttpResponse response = client.postSql("SELECT * FROM \"index_a*\"");
                assertThat(response.getBody(), response, isOk());
                assertThat(response, user.matcherForField("$.columns[*].name"));
            }
        }

        @Test
        public void queryAliasesWildcard_fls() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                GenericRestClient.HttpResponse response = client.postSql("SELECT * FROM \"alias_*\"");
                assertThat(response.getBody(), response, isOk());
                assertThat(response, user.matcherForField("$.columns[*].name"));
            }
        }

        @Test
        public void queryDataStreamsWildcard_fls() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                GenericRestClient.HttpResponse response = client.postSql("SELECT * FROM \"ds_*\"");
                assertThat(response.getBody(), response, isOk());
                assertThat(response, user.matcherForField("$.columns[*].name"));
            }
        }

        @Test
        public void queryIndicesWildcard_fm() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                GenericRestClient.HttpResponse response = client.postSql("SELECT dept, source_ip FROM \"index_a*\"");
                assertThat(response.getBody(), response, isOk());
                assertThat(response, user.matcherForField("$.rows[*]"));
            }
        }

        @Test
        public void queryAliasesWildcard_fm() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                GenericRestClient.HttpResponse response = client.postSql("SELECT dept, source_ip FROM \"alias_*\"");
                assertThat(response.getBody(), response, isOk());
                assertThat(response, user.matcherForField("$.rows[*]"));
            }
        }

        @Test
        public void queryDataStreamsWildcard_fm() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                GenericRestClient.HttpResponse response = client.postSql("SELECT dept, source_ip FROM \"ds_*\"");
                assertThat(response.getBody(), response, isOk());
                assertThat(response, user.matcherForField("$.rows[*]"));
            }
        }

        @Test
        public void queryIndicesWildcard_withCursor_fls() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                DocNode body = DocNode.of(
                        "query", "SELECT * FROM \"index_a*\"",
                        "fetch_size", 5
                );
                GenericRestClient.HttpResponse response = client.postSql(body);
                assertThat(response, isOk());
                assertThat(response, user.matcherForField("$.columns[*].name"));
                final int numberOfValuesInRow = response.getBodyAsDocNode().findByJsonPath("$.columns[*]").size();

                assertThat(response.getBody(), response, json(nodeAt("$.cursor", notNullValue())));
                String cursor = response.getBodyAsDocNode().getAsString("cursor");
                response = client.postSql(DocNode.of("cursor", cursor));
                assertThat(response, isOk());
                //in this case response doesn't contain columns
                assertThat(response, json(distinctNodesAt("$.rows[*]", everyItem(hasSize(numberOfValuesInRow)))));

                //close cursor
                response = client.postJson("/_sql/close", DocNode.of("cursor", cursor));
                assertThat(response, isOk());
            }
        }

        @Test
        public void queryIndicesWildcard_withCursor_fm() throws Exception {
            try (GenericRestClient client = cluster.getRestClient(user)) {
                DocNode body = DocNode.of(
                        "query", "SELECT dept, source_ip FROM \"index_a*\"",
                        "fetch_size", 5
                );
                GenericRestClient.HttpResponse response = client.postSql(body);
                assertThat(response, isOk());
                assertThat(response, user.matcherForField("$.rows[*]"));

                assertThat(response.getBody(), response, json(nodeAt("$.cursor", notNullValue())));
                String cursor = response.getBodyAsDocNode().getAsString("cursor");
                response = client.postSql(DocNode.of("cursor", cursor));
                assertThat(response, isOk());
                assertThat(response, user.matcherForField("$.rows[*]"));

                //close cursor
                response = client.postJson("/_sql/close", DocNode.of("cursor", cursor));
                assertThat(response, isOk());
            }
        }
    }
}
