/*
 * Copyright 2022 floragunn GmbH
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

import static com.floragunn.searchsupport.meta.Meta.Mock.indices;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

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
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.config.ActionGroup;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.meta.Meta;

/**
 * TODO index exclusions
 *
 * TODO int test IndicesAliasesAction
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ RoleBasedActionAuthorizationTests.ClusterPermissions.class, RoleBasedActionAuthorizationTests.IndexPermissions.class,
        RoleBasedActionAuthorizationTests.IndexPermissionsSpecial.class, RoleBasedActionAuthorizationTests.AliasPermissions.class,
        RoleBasedActionAuthorizationTests.AliasPermissionsSpecial.class })
public class RoleBasedActionAuthorizationTests {

    private static final Actions actions = new Actions(null);

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

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants);

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

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants);

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

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants);

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
        final ImmutableSet<Action> requiredActions;
        final ImmutableSet<Action> otherActions;
        final RoleBasedActionAuthorization subject;
        final User user = User.forUser("test").attribute("dept_no", "a11").build();

        @Test
        public void positive_full() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), requiredActions, ResolvedIndices.of(BASIC, "index_a11"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
        }

        @Test
        public void positive_partial() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), requiredActions, ResolvedIndices.of(BASIC, "index_a11", "index_a12"));

            if (this.indexSpec.wildcardPrivs || this.indexSpec.givenIndexPrivs.contains("index_*")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
            } else if (this.indexSpec.givenIndexPrivs.contains("index_a1*") && !this.indexSpec.givenIndexPrivs.contains("-index_a12")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
            } else {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));
            }
        }

        @Test
        public void positive_partial2() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), requiredActions,
                            ResolvedIndices.of(BASIC, "index_a11", "index_a12", "index_b1"));

            if (this.indexSpec.wildcardPrivs || this.indexSpec.givenIndexPrivs.contains("index_*")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
            } else if (this.indexSpec.givenIndexPrivs.contains("index_a1*") && !this.indexSpec.givenIndexPrivs.contains("-index_a12")) {
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
                    = subject.hasIndexPermission(ctx(user, "test_role"), requiredActions, ResolvedIndices.of(BASIC, "alias_a1"));

            if ((this.indexSpec.wildcardPrivs || this.indexSpec.givenIndexPrivs.contains("index_*") || this.indexSpec.givenIndexPrivs.contains("index_a1*")) && !this.indexSpec.givenIndexPrivs.contains("-index_a12")) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11", "index_a12")));
            } else {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));
            }
        }

        @Test
        public void negative_wrongRole() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "other_role"), requiredActions, ResolvedIndices.of(BASIC, "index_a11"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void negative_wrongAction() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), otherActions, ResolvedIndices.of(BASIC, "index_a11"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void negative_wrongRole_alias() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "other_role"), requiredActions, ResolvedIndices.of(BASIC, "alias_a1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void negative_wrongAction_alias() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), otherActions, ResolvedIndices.of(BASIC, "alias_a1"));
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
                    new IndexSpec().givenIndexPrivs("index_*"), //
                    new IndexSpec().givenIndexPrivs("index_a11"), //
                    new IndexSpec().givenIndexPrivs("index_a1*"), // 
                    new IndexSpec().givenIndexPrivs("index_${user.attrs.dept_no}"), //
                    new IndexSpec().givenIndexPrivs("index_a1*", "-index_a12"), //
                    new IndexSpec().givenIndexPrivs("index_${user.attrs.dept_no}", "-index_a12"))

            ) {
                for (ActionSpec actionSpec : Arrays.asList(//
                        new ActionSpec("constant, well known")//
                                .givenPrivs("indices:data/write/index").requiredPrivs("indices:data/write/index"), //
                        new ActionSpec("pattern, well known")//
                                .givenPrivs("indices:data/write/*").requiredPrivs("indices:data/write/index"), //
                        new ActionSpec("pattern, well known, two required privs")//
                                .givenPrivs("indices:data/write/*").requiredPrivs("indices:data/write/index", "indices:data/write/delete"), //
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

            this.requiredActions = actionSpec.requiredPrivs;
            this.otherActions = actionSpec.wellKnownActions ? ImmutableSet.of(actions.get("indices:data/read/get"))
                    : ImmutableSet.of(actions.get("indices:foobar/unknown"));

            this.subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions,
                    statefulness == Statefulness.STATEFUL ? BASIC : null, ImmutableSet.empty());
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
            ImmutableSet<Action> indexAction = ImmutableSet.of(actions.get("indices:data/write/index"));
            ImmutableSet<Action> otherAction = ImmutableSet.of(actions.get("indices:data/write/delete"));

            Assert.assertTrue(indexAction.toString(), indexAction.only() instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role:\n" + //
                            "  index_permissions:\n" + //
                            "  - index_patterns: ['index_${user.attrs.dept_no}']\n" + //
                            "    allowed_actions: ['indices:data/write/index']"),
                    CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants);

            User user = User.forUser("test").attribute("dept_no", "a11").build();

            PrivilegesEvaluationResult result;

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "index_a11"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "index_a11", "index_a12"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "alias_a1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));

            result = subject.hasIndexPermission(ctx(user, "other_role"), indexAction, ResolvedIndices.of(BASIC, "index_a11"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);

            result = subject.hasIndexPermission(ctx(user, "test_role"), otherAction, ResolvedIndices.of(BASIC, "index_a11"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);

            User userWithoutAttributes = User.forUser("no_attributes").build();

            result = subject.hasIndexPermission(ctx(userWithoutAttributes, "test_role"), indexAction, ResolvedIndices.of(BASIC, "index_a11"));
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

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants);

            User user = User.forUser("test").build();

            PrivilegesEvaluationResult result = subject.hasIndexPermission(ctx(user, "test_role1", "test_role2", "test_role3"),
                    ImmutableSet.of(indexAction, indexActionNotWellKnown), ResolvedIndices.empty().localIndices("index_a1", "index_a2"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role1", "test_role2", "test_role3"),
                    ImmutableSet.of(indexAction, indexActionNotWellKnown), ResolvedIndices.empty().localIndices("index_a1", "index_a2", "index_b"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a1", "index_a2")));

            result = subject.hasIndexPermission(ctx(user, "test_role1", "test_role2"), ImmutableSet.of(indexAction, indexActionNotWellKnown),
                    ResolvedIndices.empty().localIndices("index_a1", "index_a2", "index_b"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a1")));

            result = subject.hasIndexPermission(ctx(user, "test_role2", "test_role3"), ImmutableSet.of(indexAction, indexActionNotWellKnown),
                    ResolvedIndices.empty().localIndices("index_a1", "index_a2"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }
    }

    @RunWith(Parameterized.class)
    public static class AliasPermissions {

        final ActionSpec actionSpec;
        final IndexSpec indexSpec;
        final SgDynamicConfiguration<Role> roles;
        final ImmutableSet<Action> requiredActions;
        final ImmutableSet<Action> otherActions;
        final RoleBasedActionAuthorization subject;
        final User user = User.forUser("test").attribute("dept_no", "a1").build();

        @Test
        public void positive_alias_full() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), requiredActions, ResolvedIndices.of(BASIC, "alias_a1"));
            
            if (!this.indexSpec.givenAliasPrivs.isEmpty() || this.indexSpec.wildcardPrivs) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
            } else {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
                Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));
            }
        }

        @Test
        public void positive_index_full() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), requiredActions, ResolvedIndices.of(BASIC, "index_a11"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
        }

        @Test
        public void positive_alias_partial() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), requiredActions,
                            ResolvedIndices.of(BASIC, "alias_a1", "alias_a2", "alias_b"));

            if (this.indexSpec.wildcardPrivs || this.indexSpec.aliasWildcardPrivs) {
                Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);
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
                    = subject.hasIndexPermission(ctx(user, "test_role"), requiredActions,
                            ResolvedIndices.of(BASIC, "index_a11", "index_a12", "index_a21", "index_b1"));

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
                    = subject.hasIndexPermission(ctx(user, "other_role"), requiredActions, ResolvedIndices.of(BASIC, "index_a11"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void negative_wrongAction() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), otherActions, ResolvedIndices.of(BASIC, "index_a11"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void negative_wrongRole_alias() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "other_role"), requiredActions, ResolvedIndices.of(BASIC, "alias_a1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void negative_wrongAction_alias() throws Exception {
            PrivilegesEvaluationResult result//
                    = subject.hasIndexPermission(ctx(user, "test_role"), otherActions, ResolvedIndices.of(BASIC, "alias_a1"));
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
                    new IndexSpec().givenIndexPrivs("index_a11"))
            ) {
                for (ActionSpec actionSpec : Arrays.asList(//
                        new ActionSpec("constant, well known")//
                                .givenPrivs("indices:data/write/index").requiredPrivs("indices:data/write/index"), //
                        new ActionSpec("pattern, well known")//
                                .givenPrivs("indices:data/write/*").requiredPrivs("indices:data/write/index"), //
                        new ActionSpec("pattern, well known, two required privs")//
                                .givenPrivs("indices:data/write/*").requiredPrivs("indices:data/write/index", "indices:data/write/delete"), //
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

            this.requiredActions = actionSpec.requiredPrivs;
            this.otherActions = actionSpec.wellKnownActions ? ImmutableSet.of(actions.get("indices:data/read/get"))
                    : ImmutableSet.of(actions.get("indices:foobar/unknown"));

            this.subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions,
                    statefulness == Statefulness.STATEFUL ? BASIC : null, ImmutableSet.empty());
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
                            "  - index_patterns: ['alias_constant_a']\n" + //
                            "    allowed_actions: ['indices:data/write/index']"),
                    CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            Meta indexMetadata = Meta.Mock //
                    .indices("index_a1", "index_a2", "index_b1", "index_b2") //
                    .alias("alias_constant_a").of("index_a1", "index_a2");

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, indexMetadata,
                    tenants);

            User user = User.forUser("test").build();
            ResolvedIndices aliasConstantA = ResolvedIndices.of(indexMetadata, "alias_constant_a");

            PrivilegesEvaluationResult result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction), aliasConstantA);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            ResolvedIndices indexA1 = ResolvedIndices.of(indexMetadata, "index_a1");
            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction), indexA1);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction),
                    ResolvedIndices.of(indexMetadata, "alias_constant_a", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("alias_constant_a")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction),
                    ResolvedIndices.of(indexMetadata, "index_a1", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a1")));

            result = subject.hasIndexPermission(ctx(user, "other_role"), ImmutableSet.of(indexAction),
                    ResolvedIndices.of(indexMetadata, "alias_constant_a", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void wellKnown_constantAction_indexPattern_statefulIndices() throws Exception {
            Action indexAction = actions.get("indices:data/write/index");

            Assert.assertTrue(indexAction.toString(), indexAction instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role:\n" + //
                            "  alias_permissions:\n" + //
                            "  - index_patterns: ['alias_a*']\n" + //
                            "    allowed_actions: ['indices:data/write/index']"),
                    CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();
            Meta indexMetadata = indices("index_a11", "index_a12", "index_a21", "index_a22", "index_b1", "index_b2")//
                    .alias("alias_a1").of("index_a11", "index_a12")//
                    .alias("alias_a2").of("index_a21", "index_a22")//
                    .alias("alias_b").of("index_b1", "index_b2");

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, indexMetadata,
                    tenants);

            User user = User.forUser("test").build();
            ResolvedIndices aliasA1 = ResolvedIndices.of(indexMetadata, "alias_a1");

            PrivilegesEvaluationResult result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction), aliasA1);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            ResolvedIndices indexA1 = ResolvedIndices.of(indexMetadata, "index_a11");
            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction), indexA1);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction),
                    ResolvedIndices.of(indexMetadata, "alias_a1", "alias_b"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("alias_a1")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction),
                    ResolvedIndices.of(indexMetadata, "alias_a1", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("alias_a1")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction),
                    ResolvedIndices.of(indexMetadata, "index_a11", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));

            result = subject.hasIndexPermission(ctx(user, "other_role"), ImmutableSet.of(indexAction),
                    ResolvedIndices.of(indexMetadata, "alias_a1", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

    }

    private static PrivilegesEvaluationContext ctx(User user, String... roles) {
        return new PrivilegesEvaluationContext(user, ImmutableSet.ofArray(roles), null, roles, true, null, null);
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
                                        DocNode.of("index_patterns", this.givenAliasPrivs, "allowed_actions", actionSpec.givenPrivs)),
                                "data_stream_permissions", DocNode.array(//
                                        DocNode.of("index_patterns", this.givenDataStreamPrivs, "allowed_actions", actionSpec.givenPrivs))//
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
            this.wellKnownActions = this.requiredPrivs.forAnyApplies((a) -> a.name().equals("indices:data/write/index"));

            Assert.assertEquals(this.wellKnownActions, this.requiredPrivs.forAnyApplies((a) -> a instanceof WellKnownAction));

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
