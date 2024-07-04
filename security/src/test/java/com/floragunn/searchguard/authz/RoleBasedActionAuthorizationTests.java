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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
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

@RunWith(Suite.class)
@Suite.SuiteClasses({ RoleBasedActionAuthorizationTests.ClusterPermissions.class, RoleBasedActionAuthorizationTests.IndexPermissions.class,
        RoleBasedActionAuthorizationTests.AliasPermissions.class })
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

    public static class IndexPermissions {

        final static Meta BASIC = indices("index_a11", "index_a12", "index_a21", "index_a22", "index_b1", "index_b2")//
                .alias("alias_a").of("index_a11", "index_a12", "index_a21", "index_a22")//
                .alias("alias_a1").of("index_a11", "index_a12")//
                .alias("alias_a2").of("index_a21", "index_a22")//
                .alias("alias_b").of("index_b1", "index_b2");

        @Test
        public void indexAction_wellKnown_constantAction_constantIndex() throws Exception {
            ImmutableSet<Action> indexAction = ImmutableSet.of(actions.get("indices:data/write/index"));
            ImmutableSet<Action> otherAction = ImmutableSet.of(actions.get("indices:data/write/delete"));

            Assert.assertTrue(indexAction.toString(), indexAction.only() instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role:\n" + //
                            "  index_permissions:\n" + //
                            "  - index_patterns: ['index_a11']\n" + //
                            "    allowed_actions: ['indices:data/write/index']"),
                    CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants);

            User user = User.forUser("test").build();

            PrivilegesEvaluationResult result;

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction), ResolvedIndices.of(BASIC, "index_a11"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "index_a11", "index_a12"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "alias_a1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));

            result = subject.hasIndexPermission(ctx(user, "other_role"), indexAction, ResolvedIndices.of(BASIC, "index_a1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);

            result = subject.hasIndexPermission(ctx(user, "test_role"), otherAction, ResolvedIndices.of(BASIC, "index_a1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);

            result = subject.hasIndexPermission(ctx(user, "other_role"), indexAction, ResolvedIndices.of(BASIC, "alias_a1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);

            result = subject.hasIndexPermission(ctx(user, "test_role"), otherAction, ResolvedIndices.of(BASIC, "alias_a1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "alias_b"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void indexAction_wellKnown_constantAction_indexPattern() throws Exception {
            ImmutableSet<Action> indexAction = ImmutableSet.of(actions.get("indices:data/write/index"));
            ImmutableSet<Action> otherAction = ImmutableSet.of(actions.get("indices:data/write/delete"));

            Assert.assertTrue(indexAction.toString(), indexAction.only() instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role:\n" + //
                            "  index_permissions:\n" + //
                            "  - index_patterns: ['index_a1*']\n" + //
                            "    allowed_actions: ['indices:data/write/index']"),
                    CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants);

            User user = User.forUser("test").build();

            PrivilegesEvaluationResult result;

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "index_a11", "index_a12"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "alias_a1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "index_a11", "index_a12", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11", "index_a12")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "alias_a"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11", "index_a12")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "alias_a1", "alias_a2"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11", "index_a12")));

            result = subject.hasIndexPermission(ctx(user, "other_role"), indexAction, ResolvedIndices.of(BASIC, "index_a11", "index_a12"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);

            result = subject.hasIndexPermission(ctx(user, "test_role"), otherAction, ResolvedIndices.of(BASIC, "index_a11", "index_a12"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

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
        public void indexAction_wellKnown_constantAction_indexPattern_statefulIndices() throws Exception {
            ImmutableSet<Action> indexAction = ImmutableSet.of(actions.get("indices:data/write/index"));
            ImmutableSet<Action> otherAction = ImmutableSet.of(actions.get("indices:data/write/delete"));

            Assert.assertTrue(indexAction.toString(), indexAction.only() instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role:\n" + //
                            "  index_permissions:\n" + //
                            "  - index_patterns: ['index_a1*']\n" + //
                            "    allowed_actions: ['indices:data/write/index']"),
                    CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, BASIC, tenants);

            User user = User.forUser("test").build();

            PrivilegesEvaluationResult result;

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "index_a11", "index_a12"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "alias_a1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "index_a11", "index_a12", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11", "index_a12")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "alias_a1", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11", "index_a12")));

            result = subject.hasIndexPermission(ctx(user, "other_role"), indexAction, ResolvedIndices.of(BASIC, "index_a11", "index_a12"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);

            result = subject.hasIndexPermission(ctx(user, "test_role"), otherAction, ResolvedIndices.of(BASIC, "index_a11", "index_a12"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void indexAction_notWellKnown_constantAction_indexPattern() throws Exception {
            ImmutableSet<Action> indexAction = ImmutableSet.of(actions.get("indices:data/write/index/notwellknown"));
            ImmutableSet<Action> otherAction = ImmutableSet.of(actions.get("indices:data/write/delete/notwellknown"));

            Assert.assertTrue(indexAction.toString(), !(indexAction.only() instanceof WellKnownAction));

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role:\n" + //
                            "  index_permissions:\n" + //
                            "  - index_patterns: ['index_a1*']\n" + //
                            "    allowed_actions: ['indices:data/write/index/notwellknown']"),
                    CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants);

            User user = User.forUser("test").build();

            PrivilegesEvaluationResult result;

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "index_a11", "index_a12"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "alias_a1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "index_a11", "index_a12", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11", "index_a12")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "alias_a1", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11", "index_a12")));

            result = subject.hasIndexPermission(ctx(user, "other_role"), indexAction, ResolvedIndices.of(BASIC, "index_a11", "index_a12"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);

            result = subject.hasIndexPermission(ctx(user, "test_role"), otherAction, ResolvedIndices.of(BASIC, "index_a11", "index_a12"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void indexAction_notWellKnown_actionPattern_indexPattern() throws Exception {
            ImmutableSet<Action> indexAction = ImmutableSet.of(actions.get("indices:data/write/index/notwellknown"));
            ImmutableSet<Action> otherAction = ImmutableSet.of(actions.get("indices:data/write/delete/notwellknown"));

            Assert.assertTrue(indexAction.toString(), !(indexAction.only() instanceof WellKnownAction));

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role:\n" + //
                            "  index_permissions:\n" + //
                            "  - index_patterns: ['index_a1*']\n" + //
                            "    allowed_actions: ['indices:data/write/index/*']"),
                    CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants);

            User user = User.forUser("test").build();

            PrivilegesEvaluationResult result;

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "index_a11", "index_a12"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "alias_a1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "index_a11", "index_a12", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11", "index_a12")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), indexAction, ResolvedIndices.of(BASIC, "alias_a1", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11", "index_a12")));

            result = subject.hasIndexPermission(ctx(user, "other_role"), indexAction, ResolvedIndices.of(BASIC, "index_a11", "index_a12"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);

            result = subject.hasIndexPermission(ctx(user, "test_role"), otherAction, ResolvedIndices.of(BASIC, "index_a11", "index_a12"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void indexAction_actionPattern_constantIndex() throws Exception {
            Action indexAction = actions.get("indices:data/write/index");
            Action indexActionNotWellKnown = actions.get("indices:data/write/index/notWellKnown");
            Action otherAction = actions.get("indices:data/write/delete");

            Assert.assertTrue(indexAction.toString(), indexAction instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role:\n" + //
                            "  index_permissions:\n" + //
                            "  - index_patterns: ['index_a11']\n" + //
                            "    allowed_actions: ['indices:data/write/index*']"),
                    CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants);

            User user = User.forUser("test").build();
            PrivilegesEvaluationResult result;

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction), ResolvedIndices.of(BASIC, "index_a11"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexActionNotWellKnown),
                    ResolvedIndices.of(BASIC, "index_a11"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction),
                    ResolvedIndices.of(BASIC, "index_a11", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexActionNotWellKnown),
                    ResolvedIndices.of(BASIC, "index_a11", "index_b1"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a11")));

            result = subject.hasIndexPermission(ctx(user, "other_role"), ImmutableSet.of(indexAction), ResolvedIndices.of(BASIC, "index_a11"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(otherAction), ResolvedIndices.of(BASIC, "index_a11"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void indexAction_actionPattern_indexPattern() throws Exception {
            Action indexAction = actions.get("indices:data/write/index");
            Action indexActionNotWellKnown = actions.get("indices:data/write/index/notWellKnown");
            Action otherAction = actions.get("indices:data/write/delete");

            Assert.assertTrue(indexAction.toString(), indexAction instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role:\n" + //
                            "  index_permissions:\n" + //
                            "  - index_patterns: ['index_a*']\n" + //
                            "    allowed_actions: ['indices:data/write/index*']"),
                    CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants);

            User user = User.forUser("test").build();
            ResolvedIndices indexA12 = ResolvedIndices.empty().localIndices("index_a1", "index_a2");

            PrivilegesEvaluationResult result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction), indexA12);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexActionNotWellKnown), indexA12);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction),
                    ResolvedIndices.empty().localIndices("index_a", "index_b"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexActionNotWellKnown),
                    ResolvedIndices.empty().localIndices("index_a", "index_b"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a")));

            result = subject.hasIndexPermission(ctx(user, "other_role"), ImmutableSet.of(indexAction), indexA12);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(otherAction), indexA12);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void indexAction_actionPattern_indexPattern_statefulIndices() throws Exception {
            Action indexAction = actions.get("indices:data/write/index");
            Action indexActionNotWellKnown = actions.get("indices:data/write/index/notWellKnown");
            Action otherAction = actions.get("indices:data/write/delete");

            Assert.assertTrue(indexAction.toString(), indexAction instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role:\n" + //
                            "  index_permissions:\n" + //
                            "  - index_patterns: ['index_a*']\n" + //
                            "    allowed_actions: ['indices:data/write/index*']"),
                    CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions,
                    indices("index_a1", "index_b"), tenants);

            User user = User.forUser("test").build();
            ResolvedIndices indexA12 = ResolvedIndices.empty().localIndices("index_a1", "index_a2");

            PrivilegesEvaluationResult result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction), indexA12);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexActionNotWellKnown), indexA12);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction),
                    ResolvedIndices.empty().localIndices("index_a", "index_b"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexActionNotWellKnown),
                    ResolvedIndices.empty().localIndices("index_a", "index_b"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_a")));

            result = subject.hasIndexPermission(ctx(user, "other_role"), ImmutableSet.of(indexAction), indexA12);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(otherAction), indexA12);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void indexAction_actionPattern_indexWildcard() throws Exception {
            Action indexAction = actions.get("indices:data/write/index");
            Action indexActionNotWellKnown = actions.get("indices:data/write/index/notWellKnown");
            Action otherAction = actions.get("indices:data/write/delete");

            Assert.assertTrue(indexAction.toString(), indexAction instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role:\n" + //
                            "  index_permissions:\n" + //
                            "  - index_patterns: ['*']\n" + //
                            "    allowed_actions: ['indices:data/write/index*']"),
                    CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants);

            User user = User.forUser("test").build();
            ResolvedIndices indexA12 = ResolvedIndices.empty().localIndices("index_a1", "index_a2");

            PrivilegesEvaluationResult result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction), indexA12);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexActionNotWellKnown), indexA12);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction),
                    ResolvedIndices.empty().localIndices("index_a", "index_b"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexActionNotWellKnown),
                    ResolvedIndices.empty().localIndices("index_a", "index_b"));
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.OK);

            result = subject.hasIndexPermission(ctx(user, "other_role"), ImmutableSet.of(indexAction), indexA12);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(otherAction), indexA12);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
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

        @Test
        public void indexAction_actionPattern_indexPatternWithNegation() throws Exception {
            Action indexAction = actions.get("indices:data/write/index");
            Action otherAction = actions.get("indices:data/write/delete");

            Assert.assertTrue(indexAction.toString(), indexAction instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role:\n" + //
                            "  index_permissions:\n" + //
                            "  - index_patterns: ['index_abc*', '-index_abcd']\n" + //
                            "    allowed_actions: ['indices:data/write/index*']"),
                    CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants);

            User user = User.forUser("test").build();
            ResolvedIndices indices = ResolvedIndices.empty().localIndices("index_abc", "index_abcd");

            PrivilegesEvaluationResult result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction), indices);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_abc")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(otherAction), indices);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

        @Test
        public void indexAction_actionPattern_indexPatternWithNegationAndTemplate() throws Exception {
            Action indexAction = actions.get("indices:data/write/index");
            Action otherAction = actions.get("indices:data/write/delete");

            Assert.assertTrue(indexAction.toString(), indexAction instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role:\n" + //
                            "  index_permissions:\n" + //
                            "  - index_patterns: ['index_${user.attrs.a}*', '-index_abcd']\n" + //
                            "    allowed_actions: ['indices:data/write/index*']"),
                    CType.ROLES, null).get();

            ImmutableSet<String> tenants = ImmutableSet.empty();

            RoleBasedActionAuthorization subject = new RoleBasedActionAuthorization(roles, ActionGroup.FlattenedIndex.EMPTY, actions, null, tenants);

            User user = User.forUser("test").attribute("a", "abc").build();
            ResolvedIndices indices = ResolvedIndices.empty().localIndices("index_abc", "index_abcd", "index_foo");

            PrivilegesEvaluationResult result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(indexAction), indices);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.PARTIALLY_OK);
            Assert.assertTrue(result.toString(), result.getAvailableIndices().equals(ImmutableSet.of("index_abc")));

            result = subject.hasIndexPermission(ctx(user, "test_role"), ImmutableSet.of(otherAction), indices);
            Assert.assertTrue(result.toString(), result.getStatus() == PrivilegesEvaluationResult.Status.INSUFFICIENT);
        }

    }

    public static class AliasPermissions {
        // TODO _all

        @Test
        public void wellKnown_constantAction_constantAlias_statefulIndices() throws Exception {
            Action indexAction = actions.get("indices:data/write/index");

            Assert.assertTrue(indexAction.toString(), indexAction instanceof WellKnownAction);

            SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                    "test_role:\n" + //
                            "  index_permissions:\n" + //
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
                            "  index_permissions:\n" + //
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

}
