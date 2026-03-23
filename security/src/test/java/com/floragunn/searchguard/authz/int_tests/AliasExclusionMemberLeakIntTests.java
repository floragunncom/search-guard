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
import static com.floragunn.searchguard.test.IndexApiMatchers.searchGuardIndices;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * Reproduces a bug where excluding an alias member independently (e.g. -index_a1) causes other
 * members of the same alias (e.g. index_b1) to disappear from the result when the alias is also
 * excluded (e.g. -alias_ab).
 *
 * <h3>Root cause</h3>
 *
 * Expression: {@code *,-index_a1,-alias_ab} with {@code expand_wildcards=all}
 *
 * <ol>
 *   <li>Backwards pass processes {@code -alias_ab}: {@code resolveNegationUpAndDown("alias_ab", ...)}
 *       adds {@code alias_ab}, {@code index_a1}, and {@code index_b1} to {@code excludeNames}.</li>
 *   <li>Backwards pass processes {@code -index_a1}: {@code resolveNegationUpAndDown("index_a1", ...)}
 *       discovers that {@code index_a1} is a member of {@code alias_ab} and adds {@code alias_ab}
 *       to {@code partiallyExcludedObjects}.</li>
 *   <li>Forward pass: {@code *} matches {@code alias_ab}. Because {@code alias_ab} is in
 *       {@code partiallyExcludedObjects}, the partial-exclusion branch iterates its members.
 *       {@code index_b1} is in {@code excludeNames} (put there by step 1) and is therefore
 *       skipped — even though it was never independently excluded.</li>
 * </ol>
 *
 * ES natively keeps {@code index_b1} because {@code *} matched it directly as a concrete index,
 * and neither {@code -index_a1} nor {@code -alias_ab} names it explicitly.
 *
 */
public class AliasExclusionMemberLeakIntTests {

    private static final Logger log = LogManager.getLogger(AliasExclusionMemberLeakIntTests.class);

    static TestIndex index_a1 = TestIndex.name("index_a1").documentCount(5).seed(1).attr("prefix", "ia").build();
    static TestIndex index_b1 = TestIndex.name("index_b1").documentCount(5).seed(2).attr("prefix", "ib").build();

    // alias_ab covers both regular indices — mirrors alias_b from IndexResolutionConsistencyNoComponentSelectorsIntTests
    static TestAlias alias_ab = new TestAlias("alias_ab", index_a1, index_b1);

    static TestSgConfig.User UNLIMITED_USER = new TestSgConfig.User("unlimited_user")
            .description("unlimited")
            .roles(new Role("r1")
                    .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")
                    .indexPermissions("*").on("*")
                    .aliasPermissions("*").on("*"));

    static TestSgConfig.User SUPER_UNLIMITED_USER = new TestSgConfig.User("super_unlimited_user")
            .description("super unlimited (admin cert)")
            .adminCertUser();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
            .singleNode().sslEnabled()
            .users(ImmutableList.of(UNLIMITED_USER, SUPER_UNLIMITED_USER))
            .indices(index_a1, index_b1)
            .aliases(alias_ab)
            .build();

    /**
     * Expression: {@code *,-index_a1,-alias_ab} with {@code expand_wildcards=all}.
     *
     * <p>Expected: {@code index_b1} is present — it was matched directly by {@code *} and was
     * never independently excluded. {@code -alias_ab} removes the alias abstraction;
     * {@code -index_a1} removes one concrete index. Neither should remove {@code index_b1}.
     *
     * <p>Bug: {@code index_b1} is absent from the SG (UNLIMITED_USER) result because
     * {@code resolveNegationUpAndDown} for {@code -alias_ab} added it to {@code excludeNames},
     * and the partial-exclusion branch (triggered because {@code -index_a1} put {@code alias_ab}
     * into {@code partiallyExcludedObjects}) filters it out.
     */
    @Test
    public void search_excludeAliasMemberAndAlias_otherMemberSurvives() throws Exception {
        String path = "*,-index_a1,-alias_ab/_search?size=1000&pretty&expand_wildcards=all";

        try (GenericRestClient client = cluster.getRestClient(SUPER_UNLIMITED_USER)) {
            HttpResponse response = client.get(path);
            log.info("Admin response status={} body={}", response.getStatusCode(), response.getBody());
            assertThat(response, containsExactly(index_b1, searchGuardIndices(), esInternalIndices()).at("hits.hits[*]._index").whenEmpty(200));
        }

        try (GenericRestClient client = cluster.getRestClient(UNLIMITED_USER)) {
            HttpResponse response = client.get(path);
            log.info("User response status={} body={}", response.getStatusCode(), response.getBody());
            assertThat(response, containsExactly(index_b1, esInternalIndices()).at("hits.hits[*]._index").whenEmpty(200));
        }
    }
}