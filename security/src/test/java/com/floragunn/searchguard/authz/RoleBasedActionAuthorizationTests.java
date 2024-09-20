/*
 * Copyright 2022-2024 floragunn GmbH
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

package com.floragunn.searchguard.authz;

import static com.floragunn.searchsupport.meta.Meta.Mock.dataStream;
import static com.floragunn.searchsupport.meta.Meta.Mock.indices;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.searchguard.authz.config.MultiTenancyConfigurationProvider;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import com.floragunn.codova.config.text.Pattern;
import com.floragunn.searchguard.authz.config.MultiTenancyConfigurationProvider;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.cstate.metrics.TimeAggregation;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Suite;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.actions.ResolvedIndices;
import com.floragunn.searchguard.authz.config.ActionGroup;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.meta.Meta;

@RunWith(Suite.class)
@Suite.SuiteClasses({ RoleBasedActionAuthorizationTests.ClusterPermissions.class, RoleBasedActionAuthorizationTests.IndexPermissions.class,
        RoleBasedActionAuthorizationTests.IndexPermissionsSpecial.class, RoleBasedActionAuthorizationTests.AliasPermissions.class,
        RoleBasedActionAuthorizationTests.AliasPermissionsSpecial.class, RoleBasedActionAuthorizationTests.DataStreamPermissions.class,
        RoleBasedActionAuthorizationTests.WriteResolveDataStreamIndicesTest.class })
public class RoleBasedActionAuthorizationTests {

    private static final Actions actions = new Actions(null);
    private static final ByteSizeValue STATEFUL_SIZE = new ByteSizeValue(10, ByteSizeUnit.MB);

    public static class ClusterPermissions {

        @Test
        public void clusterAction_wellKnown() throws Exception {

            Action nodesStatsAction = actions.get("cluster:monitor/nodes/stats");
            Action otherAction = actions.get("cluster:monitor/nodes/usage");

            Assert.assertTrue(nodesStatsAction.toString(), nodesStatsAction instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration
                    .fromMap(DocNode.of("test_role", DocNode.of("cluster_permissions", Arrays.asList("cluster:monitor/nodes/stats*"))), CType.ROLES,
                            null)
                    .get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants,
                    STATEFUL_SIZE);

            User user = User.forUser("test").build();

            Assert.assertTrue(subject.hasClusterPermission(ctx(user, "test_role"), nodesStatsAction).isOk());
            Assert.assertFalse(subject.hasClusterPermission(ctx(user, "other_role"), nodesStatsAction).isOk());
            Assert.assertFalse(subject.hasClusterPermission(ctx(user, "test_role"), otherAction).isOk());
        }

        @Test
        public void clusterAction_notWellKnown() throws Exception {

            Action nodesStatsAction = actions.get("cluster:monitor/nodes/stats/somethingnotwellknown");
            Action otherAction = actions.get("cluster:monitor/nodes/usage/somethingnotwellknown");

            Assert.assertFalse(nodesStatsAction.toString(), nodesStatsAction instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration
                    .fromMap(DocNode.of("test_role", DocNode.of("cluster_permissions", Arrays.asList("cluster:monitor/nodes/stats*"))), CType.ROLES,
                            null)
                    .get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants,
                    STATEFUL_SIZE);

            User user = User.forUser("test").build();

            Assert.assertTrue(subject.hasClusterPermission(ctx(user, "test_role"), nodesStatsAction).isOk());
            Assert.assertFalse(subject.hasClusterPermission(ctx(user, "other_role"), nodesStatsAction).isOk());
            Assert.assertFalse(subject.hasClusterPermission(ctx(user, "test_role"), otherAction).isOk());
        }

        @Test
        public void clusterAction_exclusion() throws Exception {

            Action nodesStatsAction = actions.get("cluster:monitor/nodes/stats");
            Action nodesUsageAction = actions.get("cluster:monitor/nodes/usage");
            Action nodesStatsActionNotWellKnown = actions.get("cluster:monitor/nodes/stats/not_well_known");

            Assert.assertTrue(nodesStatsAction.toString(), nodesStatsAction instanceof WellKnownAction);
            Assert.assertTrue(nodesUsageAction.toString(), nodesUsageAction instanceof WellKnownAction);
            Assert.assertFalse(nodesStatsActionNotWellKnown.toString(), nodesStatsActionNotWellKnown instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role1:\n" + //
                            "  cluster_permissions:\n" + //
                            "  - 'cluster:monitor/*'\n" + //
                            "  exclude_cluster_permissions:\n" + //
                            "  - 'cluster:monitor/nodes/stats*'\n"),
                    CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants,
                    STATEFUL_SIZE);

            User user = User.forUser("test").build();

            Assert.assertTrue(subject.hasClusterPermission(ctx(user, "test_role1"), nodesUsageAction).isOk());
            Assert.assertFalse(subject.hasClusterPermission(ctx(user, "test_role1"), nodesStatsAction).isOk());
            Assert.assertFalse(subject.hasClusterPermission(ctx(user, "test_role1"), nodesStatsActionNotWellKnown).isOk());
        }

    }

    @RunWith(Parameterized.class)
    public static class IndexPermissions {

        final ActionSpec actionSpec;
        final IndexSpec indexSpec;
        final SgDynamicConfiguration<Role> roles;
        final Action primaryAction;
        final ImmutableSet<Action> requiredActions;
        final ImmutableSet<Action> otherActions;
        final RoleBasedActionAuthorization subject;
        final User user = User.forUser("test").attribute("dept_no", "a11").build();

        @Test
        public void positive_full() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), primaryAction, requiredActions, ResolvedIndices.of(BASIC, "index_a11"),
                            Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
        }

        @Test
        public void positive_partial() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), primaryAction, requiredActions,
                            ResolvedIndices.of(BASIC, "index_a11", "index_a12"));

            if (this.indexSpec.wildcardPrivs || this.indexSpec.givenIndexPrivs.contains("index_*")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
            } else if ((this.indexSpec.givenIndexPrivs.contains("index_a1*") || this.indexSpec.givenIndexPrivs.contains("/index_(?!b.*).*/"))
                    && !this.indexSpec.givenIndexPrivs.contains("-index_a12")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
            } else {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));
            }
        }

        @Test
        public void positive_partial2() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), primaryAction, requiredActions,
                            ResolvedIndices.of(BASIC, "index_a11", "index_a12", "index_b1"));

            if (this.indexSpec.wildcardPrivs || this.indexSpec.givenIndexPrivs.contains("index_*")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
            } else if ((this.indexSpec.givenIndexPrivs.contains("index_a1*") || this.indexSpec.givenIndexPrivs.contains("/index_(?!b.*).*/"))
                    && !this.indexSpec.givenIndexPrivs.contains("-index_a12")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11", "index_a12")));
            } else {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));
            }
        }

        @Test
        public void positive_alias() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), primaryAction, requiredActions, ResolvedIndices.of(BASIC, "alias_a1"),
                            Action.Scope.INDEX_LIKE);

            if ((this.indexSpec.wildcardPrivs || this.indexSpec.givenIndexPrivs.contains("index_*")
                    || this.indexSpec.givenIndexPrivs.contains("index_a1*") //
                    || this.indexSpec.givenIndexPrivs.contains("/index_(?!b.*).*/")) //
                    && !this.indexSpec.givenIndexPrivs.contains("-index_a12")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK_WHEN_RESOLVED);

                if (this.actionSpec.primaryAction.name().equals("indices:data/write/index")) {
                    // For the write action, resolution is only done to the write target                    
                    Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));
                } else {
                    Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11", "index_a12")));
                }

            } else {
                if (this.actionSpec.primaryAction.name().equals("indices:data/write/index")) {
                    // For the write action, resolution is only done to the write target       
                    Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK_WHEN_RESOLVED);
                    Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));
                } else {
                    Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                    Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));
                }
            }
        }

        @Test
        public void negative_wrongRole() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "other_role"), primaryAction, requiredActions, ResolvedIndices.of(BASIC, "index_a11"),
                            Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void negative_wrongAction() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), otherActions.any(), otherActions, ResolvedIndices.of(BASIC, "index_a11"),
                            Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void negative_wrongRole_alias() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "other_role"), primaryAction, requiredActions, ResolvedIndices.of(BASIC, "alias_a1"),
                            Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void negative_wrongAction_alias() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), otherActions.any(), otherActions, ResolvedIndices.of(BASIC, "alias_a1"),
                            Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        final static Meta BASIC = indices("index_a11", "index_a12", "index_a21", "index_a22", "index_b1", "index_b2")//
                .alias("alias_a").of("index_a11", "index_a12", "index_a21", "index_a22") //
                .alias("alias_a1").of(">index_a11", "index_a12") // index_a11 is the write index of the alias
                .alias("alias_a2").of("index_a21", "index_a22") // 
                .alias("alias_b").of("index_b1", "index_b2");

        @Parameters(name = "{0};  actions: {1};  {2}")
        public static Collection<Object[]> params() {
            List<Object[]> result = new ArrayList<>();

            for (IndexSpec indexSpec : Arrays.asList(//
                    new IndexSpec().givenIndexPrivs("*"), //
                    new IndexSpec().givenIndexPrivs("index_*"), //
                    new IndexSpec().givenIndexPrivs("index_a11"), //
                    new IndexSpec().givenIndexPrivs("index_a1*"), // 
                    new IndexSpec().givenIndexPrivs("index_${user.attrs.dept_no}"), //
                    new IndexSpec().givenIndexPrivs("index_a1*", "-index_a12"), //
                    new IndexSpec().givenIndexPrivs("index_${user.attrs.dept_no}", "-index_a12"), //
                    new IndexSpec().givenIndexPrivs("/index_(?!b.*).*/"))// negative lookahead pattern: indices that do not match index_b*, but everything else that matches index_*

            ) {
                for (ActionSpec actionSpec : Arrays.asList(//
                        new ActionSpec("constant, well known")//
                                .givenPrivs("indices:data/read/search").requiredPrivs("indices:data/read/search"), //
                        new ActionSpec("pattern, well known")//
                                .givenPrivs("indices:data/read/*").requiredPrivs("indices:data/read/search"), //
                        new ActionSpec("pattern, well known, two required privs")//
                                .givenPrivs("indices:data/read/*").requiredPrivs("indices:data/read/search", "indices:data/read/get"), //
                        new ActionSpec("constant, well known, index action (uses write index of alias)")//
                                .givenPrivs("indices:data/write/index").requiredPrivs("indices:data/write/index"), //
                        new ActionSpec("constant, non well known")//
                                .givenPrivs("indices:unknown/unwell").requiredPrivs("indices:unknown/unwell"), //
                        new ActionSpec("pattern, non well known")//
                                .givenPrivs("indices:unknown/*").requiredPrivs("indices:unknown/unwell"), //
                        new ActionSpec("pattern, non well known, two required privs")//
                                .givenPrivs("indices:unknown/*").requiredPrivs("indices:unknown/unwell", "indices:unknown/notatall")//

                )) {
                    for (Statefulness statefulness : Statefulness.values()) {
                        result.add(new Object[] { indexSpec, actionSpec, statefulness });
                    }
                }
            }
            return result;
        }

        public IndexPermissions(IndexSpec indexSpec, ActionSpec actionSpec, Statefulness statefulness) throws Exception {
            this.indexSpec = indexSpec;
            this.actionSpec = actionSpec;
            this.roles = indexSpec.toRolesConfig(actionSpec);

            this.primaryAction = actionSpec.primaryAction;
            this.requiredActions = actionSpec.requiredPrivs;

            this.otherActions = actionSpec.wellKnownActions ? ImmutableSet.of(actions.get("indices:data/write/update"))
                    : ImmutableSet.of(actions.get("indices:foobar/unknown"));

            this.subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions,
                    statefulness == Statefulness.STATEFUL ? BASIC : null, ImmutableSet.empty(), STATEFUL_SIZE);
        }

    }

    public static class IndexPermissionsSpecial {

        final static Meta BASIC = indices("index_a11", "index_a12", "index_a21", "index_a22", "index_b1", "index_b2")//
                .alias("alias_a").of("index_a11", "index_a12", "index_a21", "index_a22")//
                .alias("alias_a1").of("index_a11", "index_a12")//
                .alias("alias_a2").of("index_a21", "index_a22")//
                .alias("alias_b").of("index_b1", "index_b2");

        @Test
        public void indexAction_wellKnown_constantAction_indexTemplate() throws Exception {
            ImmutableSet<Action> indexAction = ImmutableSet.of(actions.get("indices:data/read/search"));
            ImmutableSet<Action> otherAction = ImmutableSet.of(actions.get("indices:data/read/get"));

            Assert.assertTrue(indexAction.toString(), indexAction.only() instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role:\n" + //
                            "  index_permissions:\n" + //
                            "  - index_patterns: ['index_${user.attrs.dept_no}']\n" + //
                            "    allowed_actions: ['indices:data/read/search']"),
                    CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants,
                    STATEFUL_SIZE);

            User user = User.forUser("test").attribute("dept_no", "a11").build();

            PrivilegesEvaluationResult result;

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction.only(), indexAction, ResolvedIndices.of(BASIC, "index_a11"),
                    Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction.only(), indexAction,
                    ResolvedIndices.of(BASIC, "index_a11", "index_a12"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction.only(), indexAction, ResolvedIndices.of(BASIC, "alias_a1"),
                    Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));

            result = subject.hasIndexPermission(ctx(user, "other_role"), indexAction.only(), indexAction, ResolvedIndices.of(BASIC, "index_a11"),
                    Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction.only(), otherAction, ResolvedIndices.of(BASIC, "index_a11"),
                    Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);

            User userWithoutAttributes = User.forUser("no_attributes").build();

            result = subject.hasIndexPermission(ctx(userWithoutAttributes, "test_role"), indexAction.only(), indexAction,
                    ResolvedIndices.of(BASIC, "index_a11"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
            Assert.assertTrue(result.toString(), result.getErrors().toString().contains("No value for ${user.attrs.dept_no}"));
        }

        @Test
        public void indexAction_twoRequiredPrivileges_actionPattern_indexPattern() throws Exception {
            Action indexAction = actions.get("indices:data/write/index");
            Action indexActionNotWellKnown = actions.get("indices:data/write/index/notWellKnown");

            Assert.assertTrue(indexAction.toString(), indexAction instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role1:\n" + //
                            "  index_permissions:\n" + //
                            "  - index_patterns: ['index_a*']\n" + //
                            "    allowed_actions: ['indices:data/write/index']\n" + //
                            "test_role2:\n" + //
                            "  index_permissions:\n" + //
                            "  - index_patterns: ['index_a1']\n" + //
                            "    allowed_actions: ['indices:data/write/index/notWell*']\n" + //
                            "test_role3:\n" + //
                            "  index_permissions:\n" + //
                            "  - index_patterns: ['index_a2']\n" + //
                            "    allowed_actions: ['indices:data/write/index/notWell*']\n" //
            ), CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants,
                    STATEFUL_SIZE);

            User user = User.forUser("test").build();
            Meta meta = indices("index_a1", "index_a2", "index_b");

            PrivilegesEvaluationResult result = subject.hasIndexPermission(ctx(user, "test_role1", "test_role2", "test_role3"), indexAction,
                    ImmutableSet.of(indexAction, indexActionNotWellKnown), ResolvedIndices.of(meta, "index_a1", "index_a2"),
                    Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role1", "test_role2", "test_role3"), indexAction,
                    ImmutableSet.of(indexAction, indexActionNotWellKnown), ResolvedIndices.of(meta, "index_a1", "index_a2", "index_b"),
                    Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a1", "index_a2")));

            result = subject.hasIndexPermission(ctx(user, "test_role1", "test_role2"), indexAction,
                    ImmutableSet.of(indexAction, indexActionNotWellKnown), ResolvedIndices.of(meta, "index_a1", "index_a2", "index_b"),
                    Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a1")));

            result = subject.hasIndexPermission(ctx(user, "test_role2", "test_role3"), indexAction,
                    ImmutableSet.of(indexAction, indexActionNotWellKnown), ResolvedIndices.of(meta, "index_a1", "index_a2"),
                    Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }
    }

    @RunWith(Parameterized.class)
    public static class AliasPermissions {

        final ActionSpec actionSpec;
        final IndexSpec indexSpec;
        final SgDynamicConfiguration<Role> roles;
        final Action primaryAction;
        final ImmutableSet<Action> requiredActions;
        final ImmutableSet<Action> otherActions;
        final RoleBasedActionAuthorization subject;
        final User user = User.forUser("test").attribute("dept_no", "a1").build();

        @Test
        public void positive_alias_full() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), primaryAction, requiredActions, ResolvedIndices.of(BASIC, "alias_a1"),
                            Action.Scope.INDEX_LIKE);

            if (!this.indexSpec.givenAliasPrivs.isEmpty()) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
            } else if (this.indexSpec.wildcardPrivs) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK_WHEN_RESOLVED);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11", "index_a12")));
            } else {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));
            }
        }

        @Test
        public void positive_index_full() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), primaryAction, requiredActions, ResolvedIndices.of(BASIC, "index_a11"),
                            Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
        }

        @Test
        public void positive_alias_partial() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), primaryAction, requiredActions,
                            ResolvedIndices.of(BASIC, "alias_a1", "alias_a2", "alias_b"));

            if (this.indexSpec.aliasWildcardPrivs) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
            } else if (this.indexSpec.wildcardPrivs) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK_WHEN_RESOLVED);
                Assert.assertTrue(result.toString(), result.getAvailableIndices()
                        .equals(ImmutableSet.of("index_b1", "index_b2", "index_a12", "index_a11", "index_a22", "index_a21")));
            } else if (this.indexSpec.givenAliasPrivs.contains("alias_a*") && this.indexSpec.givenAliasPrivs.contains("-alias_a2")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("alias_a1")));
            } else if (this.indexSpec.givenAliasPrivs.contains("alias_a*")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("alias_a1", "alias_a2")));
            } else if (this.indexSpec.givenAliasPrivs.isEmpty() && this.indexSpec.givenIndexPrivs.equals(ImmutableList.of("index_a11"))) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));
            } else {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("alias_a1")));
            }
        }

        @Test
        public void positive_index_partial() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), primaryAction, requiredActions,
                            ResolvedIndices.of(BASIC, "index_a11", "index_a12", "index_a21", "index_b1"), Action.Scope.INDEX_LIKE);

            if (this.indexSpec.wildcardPrivs || this.indexSpec.aliasWildcardPrivs) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
            } else if (this.indexSpec.givenAliasPrivs.contains("alias_a*") && !this.indexSpec.givenAliasPrivs.contains("-alias_a")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11", "index_a12", "index_a21")));
            } else if (this.indexSpec.givenAliasPrivs.isEmpty() && this.indexSpec.givenIndexPrivs.equals(ImmutableList.of("index_a11"))) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));
            } else {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11", "index_a12")));
            }
        }

        @Test
        public void negative_wrongRole() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "other_role"), primaryAction, requiredActions, ResolvedIndices.of(BASIC, "index_a11"),
                            Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void negative_wrongAction() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), otherActions.any(), otherActions, ResolvedIndices.of(BASIC, "index_a11"),
                            Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void negative_wrongRole_alias() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "other_role"), primaryAction, requiredActions, ResolvedIndices.of(BASIC, "alias_a1"),
                            Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void negative_wrongAction_alias() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), otherActions.any(), otherActions, ResolvedIndices.of(BASIC, "alias_a1"),
                            Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        final static Meta BASIC = indices("index_a11", "index_a12", "index_a21", "index_a22", "index_b1", "index_b2")//
                .alias("alias_a").of("index_a11", "index_a12", "index_a21", "index_a22")//
                .alias("alias_a1").of("index_a11", "index_a12")//
                .alias("alias_a2").of("index_a21", "index_a22")//
                .alias("alias_b").of("index_b1", "index_b2");

        @Parameters(name = "{0};  actions: {1};  {2}")
        public static Collection<Object[]> params() {
            List<Object[]> result = new ArrayList<>();

            for (IndexSpec indexSpec : Arrays.asList(//
                    new IndexSpec().givenIndexPrivs("*"), //
                    new IndexSpec().givenAliasPrivs("*"), //
                    new IndexSpec().givenAliasPrivs("alias_a1"), //
                    new IndexSpec().givenAliasPrivs("alias_a*"), // 
                    new IndexSpec().givenAliasPrivs("alias_${user.attrs.dept_no}"), //
                    new IndexSpec().givenAliasPrivs("alias_a*", "-alias_a2", "-alias_a"), //
                    new IndexSpec().givenIndexPrivs("index_a11"))) {
                for (ActionSpec actionSpec : Arrays.asList(//
                        new ActionSpec("constant, well known")//
                                .givenPrivs("indices:data/read/search").requiredPrivs("indices:data/read/search"), //
                        new ActionSpec("pattern, well known")//
                                .givenPrivs("indices:data/read/*").requiredPrivs("indices:data/read/search"), //
                        new ActionSpec("pattern, well known, two required privs")//
                                .givenPrivs("indices:data/read/*").requiredPrivs("indices:data/read/search", "indices:data/read/get"), //
                        new ActionSpec("constant, non well known")//
                                .givenPrivs("indices:unknown/unwell").requiredPrivs("indices:unknown/unwell"), //
                        new ActionSpec("pattern, non well known")//
                                .givenPrivs("indices:unknown/*").requiredPrivs("indices:unknown/unwell"), //
                        new ActionSpec("pattern, non well known, two required privs")//
                                .givenPrivs("indices:unknown/*").requiredPrivs("indices:unknown/unwell", "indices:unknown/notatall")//

                )) {
                    for (Statefulness statefulness : Statefulness.values()) {
                        result.add(new Object[] { indexSpec, actionSpec, statefulness });
                    }
                }
            }
            return result;
        }

        public AliasPermissions(IndexSpec indexSpec, ActionSpec actionSpec, Statefulness statefulness) throws Exception {
            this.indexSpec = indexSpec;
            this.actionSpec = actionSpec;
            this.roles = indexSpec.toRolesConfig(actionSpec);

            this.primaryAction = actionSpec.primaryAction;
            this.requiredActions = actionSpec.requiredPrivs;
            this.otherActions = actionSpec.wellKnownActions ? ImmutableSet.of(actions.get("indices:data/write/update"))
                    : ImmutableSet.of(actions.get("indices:foobar/unknown"));

            this.subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions,
                    statefulness == Statefulness.STATEFUL ? BASIC : null, ImmutableSet.empty(), STATEFUL_SIZE);
        }

    }

    public static class AliasPermissionsSpecial {
        @Test
        public void wellKnown_constantAction_constantAlias_statefulIndices() throws Exception {
            Action indexAction = actions.get("indices:data/write/index");

            Assert.assertTrue(indexAction.toString(), indexAction instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role:\n" + //
                            "  alias_permissions:\n" + //
                            "  - alias_patterns: ['alias_constant_a']\n" + //
                            "    allowed_actions: ['indices:data/write/index']"),
                    CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            Meta indexMetadata = Meta.Mock //
                    .indices("index_a1", "index_a2", "index_b1", "index_b2") //
                    .alias("alias_constant_a").of("index_a1", "index_a2");

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, indexMetadata,
                    tenants, STATEFUL_SIZE);

            User user = User.forUser("test").build();
            ResolvedIndices aliasConstantA = ResolvedIndices.of(indexMetadata, "alias_constant_a");

            PrivilegesEvaluationResult result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ImmutableSet.of(indexAction),
                    aliasConstantA);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            ResolvedIndices indexA1 = ResolvedIndices.of(indexMetadata, "index_a1");
            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ImmutableSet.of(indexAction), indexA1, Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ImmutableSet.of(indexAction),
                    ResolvedIndices.of(indexMetadata, "alias_constant_a", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("alias_constant_a")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ImmutableSet.of(indexAction),
                    ResolvedIndices.of(indexMetadata, "index_a1", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a1")));

            result = subject.hasIndexPermission(ctx(user, "other_role"), indexAction, ImmutableSet.of(indexAction),
                    ResolvedIndices.of(indexMetadata, "alias_constant_a", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void wellKnown_constantAction_indexPattern_statefulIndices() throws Exception {
            Action indexAction = actions.get("indices:data/read/search");

            Assert.assertTrue(indexAction.toString(), indexAction instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role:\n" + //
                            "  alias_permissions:\n" + //
                            "  - alias_patterns: ['alias_a*']\n" + //
                            "    allowed_actions: ['indices:data/read/search']"),
                    CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();
            Meta indexMetadata = indices("index_a11", "index_a12", "index_a21", "index_a22", "index_b1", "index_b2")//
                    .alias("alias_a1").of("index_a11", "index_a12")//
                    .alias("alias_a2").of("index_a21", "index_a22")//
                    .alias("alias_b").of("index_b1", "index_b2");

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, indexMetadata,
                    tenants, STATEFUL_SIZE);

            User user = User.forUser("test").build();
            ResolvedIndices aliasA1 = ResolvedIndices.of(indexMetadata, "alias_a1");

            PrivilegesEvaluationResult result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ImmutableSet.of(indexAction), aliasA1,
                    Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            ResolvedIndices indexA1 = ResolvedIndices.of(indexMetadata, "index_a11");
            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ImmutableSet.of(indexAction), indexA1, Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ImmutableSet.of(indexAction),
                    ResolvedIndices.of(indexMetadata, "alias_a1", "alias_b"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("alias_a1")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ImmutableSet.of(indexAction),
                    ResolvedIndices.of(indexMetadata, "alias_a1", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("alias_a1")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ImmutableSet.of(indexAction),
                    ResolvedIndices.of(indexMetadata, "index_a11", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));

            result = subject.hasIndexPermission(ctx(user, "other_role"), indexAction, ImmutableSet.of(indexAction),
                    ResolvedIndices.of(indexMetadata, "alias_a1", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

    }

    @RunWith(Parameterized.class)
    public static class DataStreamPermissions {

        final ActionSpec actionSpec;
        final IndexSpec indexSpec;
        final SgDynamicConfiguration<Role> roles;
        final Action primaryAction;
        final ImmutableSet<Action> requiredActions;
        final ImmutableSet<Action> otherActions;
        final RoleBasedActionAuthorization subject;
        final User user = User.forUser("test").attribute("dept_no", "a1").build();

        @Test
        public void positive_datastream_full() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), primaryAction, requiredActions, ResolvedIndices.of(BASIC, "datastream_a1"),
                            Action.Scope.INDEX_LIKE);

            if (!this.indexSpec.givenDataStreamPrivs.isEmpty() || this.indexSpec.dataStreamWildcardPrivs || this.indexSpec.aliasWildcardPrivs
                    || this.indexSpec.givenAliasPrivs.contains("datastream_a")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
            } else {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK_WHEN_RESOLVED);
                Assert.assertTrue(result.toString(),
                        result.getAvailableIndices().equals(ImmutableSet.of(".ds-datastream_a1-xyz-0001", ".ds-datastream_a1-xyz-0002")));
            }
        }

        @Test
        public void positive_index_full() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), primaryAction, requiredActions,
                            ResolvedIndices.of(BASIC, ".ds-datastream_a1-xyz-0002"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
        }

        @Test
        public void positive_alias_full() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), primaryAction, requiredActions, ResolvedIndices.of(BASIC, "datastream_a"),
                            Action.Scope.INDEX_LIKE);

            if (this.indexSpec.aliasWildcardPrivs || this.indexSpec.givenAliasPrivs.contains("datastream_a")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
            } else if (this.indexSpec.givenDataStreamPrivs.contains("datastream_a1")
                    || this.indexSpec.givenDataStreamPrivs.contains("datastream_${user.attrs.dept_no}")
                    || (this.indexSpec.givenDataStreamPrivs.contains("datastream_a*")
                            && this.indexSpec.givenDataStreamPrivs.contains("-datastream_a2"))) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("datastream_a1")));
            } else if (this.indexSpec.dataStreamWildcardPrivs || this.indexSpec.givenDataStreamPrivs.contains("datastream_a*")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK_WHEN_RESOLVED);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("datastream_a1", "datastream_a2")));
            } else if (this.indexSpec.givenIndexPrivs.contains(".ds-datastream_a1*")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(),
                        result.getAvailableIndices().equals(ImmutableSet.of(".ds-datastream_a1-xyz-0001", ".ds-datastream_a1-xyz-0002")));
            } else {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK_WHEN_RESOLVED);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of(".ds-datastream_a1-xyz-0001",
                        ".ds-datastream_a1-xyz-0002", ".ds-datastream_a2-xyz-0001", ".ds-datastream_a2-xyz-0002")));
            }
        }

        @Test
        public void positive_datastream_partial() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), primaryAction, requiredActions,
                            ResolvedIndices.of(BASIC, "datastream_a1", "datastream_a2", "datastream_b1"), Action.Scope.INDEX_LIKE);

            if (this.indexSpec.dataStreamWildcardPrivs) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
            } else if (this.indexSpec.wildcardPrivs) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK_WHEN_RESOLVED);
                Assert.assertTrue(result.toString(),
                        result.getAvailableIndices()
                                .equals(ImmutableSet.of(".ds-datastream_a1-xyz-0001", ".ds-datastream_a1-xyz-0002", ".ds-datastream_a2-xyz-0001",
                                        ".ds-datastream_a2-xyz-0002", ".ds-datastream_b1-xyz-0001", ".ds-datastream_b1-xyz-0002")));
            } else if (this.indexSpec.givenDataStreamPrivs.contains("datastream_a*")
                    && this.indexSpec.givenDataStreamPrivs.contains("-datastream_a2")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("datastream_a1")));
            } else if (this.indexSpec.givenDataStreamPrivs.contains("datastream_a*") || this.indexSpec.aliasWildcardPrivs
                    || this.indexSpec.givenAliasPrivs.contains("datastream_a")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("datastream_a1", "datastream_a2")));
            } else if (this.indexSpec.givenIndexPrivs.contains(".ds-datastream_a1*")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(),
                        result.getAvailableIndices().equals(ImmutableSet.of(".ds-datastream_a1-xyz-0001", ".ds-datastream_a1-xyz-0002")));
            } else {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("datastream_a1")));
            }
        }

        @Test
        public void positive_index_partial() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), primaryAction, requiredActions,
                            ResolvedIndices.of(BASIC, ".ds-datastream_a1-xyz-0001", ".ds-datastream_b1-xyz-0001"), Action.Scope.INDEX_LIKE);

            if (this.indexSpec.wildcardPrivs || this.indexSpec.dataStreamWildcardPrivs) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
            } else {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of(".ds-datastream_a1-xyz-0001")));
            }
        }

        @Test
        public void negative_wrongRole() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "other_role"), primaryAction, requiredActions, ResolvedIndices.of(BASIC, "datastream_a1"),
                            Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void negative_wrongAction() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), otherActions.any(), otherActions, ResolvedIndices.of(BASIC, "datastream_a1"),
                            Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void negative_wrongRole_alias() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "other_role"), primaryAction, requiredActions, ResolvedIndices.of(BASIC, "datastream_a"),
                            Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void negative_wrongAction_alias() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), otherActions.any(), otherActions, ResolvedIndices.of(BASIC, "datastream_a"),
                            Action.Scope.INDEX_LIKE);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        final static Meta BASIC = dataStream("datastream_a1").of(".ds-datastream_a1-xyz-0001", ".ds-datastream_a1-xyz-0002")//
                .dataStream("datastream_a2").of(".ds-datastream_a2-xyz-0001", ".ds-datastream_a2-xyz-0002")//
                .dataStream("datastream_b1").of(".ds-datastream_b1-xyz-0001", ".ds-datastream_b1-xyz-0002")//
                .dataStream("datastream_b2").of(".ds-datastream_b2-xyz-0001", ".ds-datastream_b2-xyz-0002")//
                .alias("datastream_a").of("datastream_a1", "datastream_a2");

        @Parameters(name = "{0};  actions: {1};  {2}")
        public static Collection<Object[]> params() {
            List<Object[]> result = new ArrayList<>();

            for (IndexSpec indexSpec : Arrays.asList(//
                    new IndexSpec().givenIndexPrivs("*"), //
                    new IndexSpec().givenAliasPrivs("*"), //
                    new IndexSpec().givenDataStreamPrivs("*"), //
                    new IndexSpec().givenDataStreamPrivs("datastream_a1"), //
                    new IndexSpec().givenDataStreamPrivs("datastream_a*"), //
                    new IndexSpec().givenDataStreamPrivs("datastream_${user.attrs.dept_no}"), //
                    new IndexSpec().givenDataStreamPrivs("datastream_a*", "-datastream_a2"), //
                    new IndexSpec().givenAliasPrivs("datastream_a"), //
                    new IndexSpec().givenIndexPrivs(".ds-datastream_a1*"))) {
                for (ActionSpec actionSpec : Arrays.asList(//
                        new ActionSpec("constant, well known")//
                                .givenPrivs("indices:data/read/search").requiredPrivs("indices:data/read/search"), //
                        new ActionSpec("pattern, well known")//
                                .givenPrivs("indices:data/read/*").requiredPrivs("indices:data/read/search"), //
                        new ActionSpec("pattern, well known, two required privs")//
                                .givenPrivs("indices:data/read/*").requiredPrivs("indices:data/read/search", "indices:data/read/get"), //
                        new ActionSpec("constant, non well known")//
                                .givenPrivs("indices:unknown/unwell").requiredPrivs("indices:unknown/unwell"), //
                        new ActionSpec("pattern, non well known")//
                                .givenPrivs("indices:unknown/*").requiredPrivs("indices:unknown/unwell"), //
                        new ActionSpec("pattern, non well known, two required privs")//
                                .givenPrivs("indices:unknown/*").requiredPrivs("indices:unknown/unwell", "indices:unknown/notatall")//

                )) {
                    for (Statefulness statefulness : Statefulness.values()) {
                        result.add(new Object[] { indexSpec, actionSpec, statefulness });
                    }
                }
            }
            return result;
        }

        public DataStreamPermissions(IndexSpec indexSpec, ActionSpec actionSpec, Statefulness statefulness) throws Exception {
            this.indexSpec = indexSpec;
            this.actionSpec = actionSpec;
            this.roles = indexSpec.toRolesConfig(actionSpec);

            this.primaryAction = actionSpec.primaryAction;
            this.requiredActions = actionSpec.requiredPrivs;
            this.otherActions = actionSpec.wellKnownActions ? ImmutableSet.of(actions.get("indices:data/write/update"))
                    : ImmutableSet.of(actions.get("indices:foobar/unknown"));

            this.subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions,
                    statefulness == Statefulness.STATEFUL ? BASIC : null, ImmutableSet.empty(), STATEFUL_SIZE);
        }

    }

    @RunWith(Parameterized.class)
    public static class WriteResolveDataStreamIndicesTest {

        private record IndexSpecAndInstance(IndexSpec indexSpec, String actualIndexName) {

        }

        private final static User USER = User.forUser("my_test").build();

        public static final String DATA_STREAM_ONE = "my_data_stream_one";
        public static final String DATA_STREAM_TWO = "my_data_stream_two";
        final static Meta BASIC = dataStream(DATA_STREAM_ONE).of(".ds-my_data_stream_one-xyz-0001", ".ds-my_data_stream_one-xyz-0002")//
                .dataStream(DATA_STREAM_TWO).of(".ds-my_data_stream_two-xyz-0001", ".ds-my_data_stream_two-xyz-0002")//
                .alias("prohibited_alias").of(DATA_STREAM_ONE, DATA_STREAM_TWO);

        private final ActionSpec actionSpec;
        private final String indexName;

        // under test
        private final RoleBasedActionAuthorization subject;

        public WriteResolveDataStreamIndicesTest(ActionSpec actionSpec, IndexSpec indexSpec, String indexName) {
            this.actionSpec = actionSpec;
            SgDynamicConfiguration<Role> role = indexSpec.toRolesConfig(actionSpec);
            this.indexName = indexName;
            this.subject = new RoleBasedActionAuthorization(role, ActionGroup.FlattenedIndex.EMPTY, actions, BASIC, ImmutableSet.empty(), STATEFUL_SIZE,
                    Pattern.blank(), MetricsLevel.DETAILED, MultiTenancyConfigurationProvider.DEFAULT);
        }

        @Parameters(name = "action {0} index like {1} actual index {2}")
        public static Collection<Object[]> params() {
            List<Object[]> parameters = new ArrayList<>();

            List<ActionSpec> actions = ImmutableList.of(new ActionSpec("constant, index document")//
                            .givenPrivs("indices:data/write/index").requiredPrivs("indices:data/write/index"), //
                    new ActionSpec("pattern, index document") //
                            .givenPrivs("indices:data/write*").requiredPrivs("indices:data/write/index"));
            List<IndexSpecAndInstance> indices = ImmutableList.of(
                    new IndexSpecAndInstance(new IndexSpec().givenDataStreamPrivs(DATA_STREAM_ONE), DATA_STREAM_ONE), //
                    new IndexSpecAndInstance(new IndexSpec().givenDataStreamPrivs("my_data_stream_t*"), DATA_STREAM_TWO)//
            );

            for (ActionSpec actionSpec : actions) {
                for (IndexSpecAndInstance indexSpecAndInstance : indices) {
                    parameters.add(new Object[] { actionSpec, indexSpecAndInstance.indexSpec, indexSpecAndInstance.actualIndexName });
                }
            }
            return parameters;
        }

        @Test
        public void shouldNotResolveDataStreamBackingIndices() throws PrivilegesEvaluationException {
            ResolvedIndices resolvedIndices = ResolvedIndices.of(BASIC, indexName);

            PrivilegesEvaluationResult result = subject.hasIndexPermission(ctx(USER, "test_role"), actionSpec.primaryAction, //
                    actionSpec.requiredPrivs, resolvedIndices, Action.Scope.INDEX_LIKE);

            MatcherAssert.assertThat(result.getStatus(), equalTo(PrivilegesEvaluationResult.Status.OK));
            MatcherAssert.assertThat(getNumberOfPerformedDataStreamIndicesResolutions(), equalTo(0L));
        }

        @Test
        public void shouldResolveDataStreamBackingIndices() throws PrivilegesEvaluationException {
            ResolvedIndices resolvedIndices = ResolvedIndices.of(BASIC, "prohibited_alias");

            PrivilegesEvaluationResult result = subject.hasIndexPermission(ctx(USER, "test_role"), actionSpec.primaryAction, //
                    actionSpec.requiredPrivs, resolvedIndices, Action.Scope.INDEX_LIKE);

            MatcherAssert.assertThat(result.getStatus(), equalTo(PrivilegesEvaluationResult.Status.INSUFFICIENT));
            MatcherAssert.assertThat(getNumberOfPerformedDataStreamIndicesResolutions(), equalTo(1L));
        }

        private long getNumberOfPerformedDataStreamIndicesResolutions() {
            return ((TimeAggregation.Nanoseconds) subject.getComponentState().getMetrics().get("index_action_checks")).getSubAggregation(
                    "resolve_all_aliases").getCount();
        }

    }

    private static PrivilegesEvaluationContext ctx(User user, String... roles) {
        return new PrivilegesEvaluationContext(user, false, ImmutableSet.ofArray(roles), null, roles, true, null, null);
    }

    static class IndexSpec {

        ImmutableList<String> givenIndexPrivs = ImmutableList.empty();
        ImmutableList<String> givenAliasPrivs = ImmutableList.empty();
        ImmutableList<String> givenDataStreamPrivs = ImmutableList.empty();

        boolean wildcardPrivs;
        boolean aliasWildcardPrivs;
        boolean dataStreamWildcardPrivs;

        IndexSpec() {

        }

        IndexSpec givenIndexPrivs(String... indexPatterns) {
            this.givenIndexPrivs = ImmutableList.ofArray(indexPatterns);
            this.wildcardPrivs = this.givenIndexPrivs.contains("*");
            return this;
        }

        IndexSpec givenAliasPrivs(String... aliasPatterns) {
            this.givenAliasPrivs = ImmutableList.ofArray(aliasPatterns);
            this.aliasWildcardPrivs = this.givenAliasPrivs.contains("*");
            return this;
        }

        IndexSpec givenDataStreamPrivs(String... dataStreamPatterns) {
            this.givenDataStreamPrivs = ImmutableList.ofArray(dataStreamPatterns);
            this.dataStreamWildcardPrivs = this.givenDataStreamPrivs.contains("*");
            return this;
        }

        @Override
        public String toString() {
            StringBuilder result = new StringBuilder();

            if (!this.givenIndexPrivs.isEmpty()) {
                result.append("indices: ").append(this.givenIndexPrivs.stream().collect(Collectors.joining(",")));
            }

            if (!this.givenAliasPrivs.isEmpty()) {
                if (result.length() != 0) {
                    result.append("; ");
                    ;
                }
                result.append("aliases: ").append(this.givenAliasPrivs.stream().collect(Collectors.joining(",")));
            }

            if (!this.givenDataStreamPrivs.isEmpty()) {
                if (result.length() != 0) {
                    result.append("; ");
                    ;
                }
                result.append("data_streams: ").append(this.givenDataStreamPrivs.stream().collect(Collectors.joining(",")));
            }

            return result.toString();
        }

        public SgDynamicConfiguration<Role> toRolesConfig(ActionSpec actionSpec) {
            try {
                return SgDynamicConfiguration.fromMap(DocNode.of("test_role", //
                        DocNode.of(//
                                "index_permissions", DocNode.array(//
                                        DocNode.of("index_patterns", this.givenIndexPrivs, "allowed_actions", actionSpec.givenPrivs)),
                                "alias_permissions", DocNode.array(//
                                        DocNode.of("alias_patterns", this.givenAliasPrivs, "allowed_actions", actionSpec.givenPrivs)),
                                "data_stream_permissions", DocNode.array(//
                                        DocNode.of("data_stream_patterns", this.givenDataStreamPrivs, "allowed_actions", actionSpec.givenPrivs))//
                        )), CType.ROLES, null).get();
            } catch (ConfigValidationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class ActionSpec {
        String name;
        ImmutableList<String> givenPrivs;
        ImmutableSet<Action> requiredPrivs;
        Action primaryAction;
        boolean wellKnownActions;

        ActionSpec(String name) {
            super();
            this.name = name;
        }

        ActionSpec givenPrivs(String... actions) {
            this.givenPrivs = ImmutableList.ofArray(actions);
            return this;
        }

        ActionSpec requiredPrivs(String... requiredPrivs) {
            this.requiredPrivs = ImmutableSet.ofArray(requiredPrivs).map((a) -> actions.get(a));
            this.primaryAction = actions.get(requiredPrivs[0]);
            this.wellKnownActions = this.requiredPrivs.forAnyApplies((a) -> a instanceof WellKnownAction);

            return this;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    static enum Statefulness {
        STATEFUL, NON_STATEFUL
    }
}
