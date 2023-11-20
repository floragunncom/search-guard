/*
 * Copyright 2022-2024 by floragunn GmbH - All rights reserved
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

import static com.floragunn.searchsupport.meta.Meta.Mock.indices;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.meta.Meta;

/**
 * Most coverage is already achieved via RoleBasedAuthorizationBase and RoleBasedDocumentAuthorizationTest. We only test here the rule implementation and basic functionality.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ RoleBasedFieldAuthorizationTest.FlsRule.class, RoleBasedFieldAuthorizationTest.IndicesAndAliases_getRestriction.class })
public class RoleBasedFieldAuthorizationTest {

    public static class FlsRule {
        @Test
        public void singleRole_empty() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").on("*").toActualRole();
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role.getIndexPermissions().get(0));

            assertTrue(subject.toString(), subject.isAllowAll());
        }

        @Test
        public void singleRole_simple_positive() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").fls("a", "b").on("*").toActualRole();
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role.getIndexPermissions().get(0));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertTrue(subject.toString(), subject.isAllowed("a"));
            assertTrue(subject.toString(), subject.isAllowed("b"));
            assertFalse(subject.toString(), subject.isAllowed("c"));
            assertFalse(subject.toString(), subject.isAllowed("aa"));
        }

        @Test
        public void singleRole_pattern_positive() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").fls("a*", "b*").on("*").toActualRole();
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role.getIndexPermissions().get(0));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertTrue(subject.toString(), subject.isAllowed("a"));
            assertTrue(subject.toString(), subject.isAllowed("b"));
            assertFalse(subject.toString(), subject.isAllowed("c"));
            assertTrue(subject.toString(), subject.isAllowed("aa"));
        }

        @Test
        public void singleRole_pattern_negation() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").fls("~a*", "~b*").on("*").toActualRole();
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role.getIndexPermissions().get(0));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertFalse(subject.toString(), subject.isAllowed("a"));
            assertFalse(subject.toString(), subject.isAllowed("b"));
            assertTrue(subject.toString(), subject.isAllowed("c"));
            assertFalse(subject.toString(), subject.isAllowed("aa"));
        }

        @Test
        public void singleRole_full_negation() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").fls("~*").on("*").toActualRole();
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role.getIndexPermissions().get(0));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertFalse(subject.toString(), subject.isAllowed("a"));
            assertFalse(subject.toString(), subject.isAllowed(""));
        }

        @Test
        public void singleRole_mixed() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").fls("a*", "~a1*").on("*").toActualRole();
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role.getIndexPermissions().get(0));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertTrue(subject.toString(), subject.isAllowed("a"));
            assertTrue(subject.toString(), subject.isAllowed("a2"));
            assertFalse(subject.toString(), subject.isAllowed("a1"));
            assertFalse(subject.toString(), subject.isAllowed("b"));
        }

        @Test
        public void multiRole_simple_positive() throws Exception {
            Role role1 = new TestSgConfig.Role("role1").indexPermissions("*").fls("a").on("*").toActualRole();
            Role role2 = new TestSgConfig.Role("role2").indexPermissions("*").fls("b").on("*").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2.getIndexPermissions().get(0)) //
            ));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertTrue(subject.toString(), subject.isAllowed("a"));
            assertTrue(subject.toString(), subject.isAllowed("b"));
            assertFalse(subject.toString(), subject.isAllowed("c"));
            assertFalse(subject.toString(), subject.isAllowed("aa"));
        }

        @Test
        public void multiRole_simple_negative_distinct() throws Exception {
            // This is effectively a allowAll rule
            Role role1 = new TestSgConfig.Role("role1").indexPermissions("*").fls("~a").on("*").toActualRole();
            Role role2 = new TestSgConfig.Role("role2").indexPermissions("*").fls("~b").on("*").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2.getIndexPermissions().get(0)) //
            ));

            assertTrue(subject.toString(), subject.isAllowed("a"));
            assertTrue(subject.toString(), subject.isAllowed("b"));
            assertTrue(subject.toString(), subject.isAllowed("c"));
            assertTrue(subject.toString(), subject.isAllowed("aa"));
        }

        @Test
        public void multiRole_pattern_negative_overlapping() throws Exception {
            Role role1 = new TestSgConfig.Role("role1").indexPermissions("*").fls("~a*").on("*").toActualRole();
            Role role2 = new TestSgConfig.Role("role2").indexPermissions("*").fls("~a1*").on("*").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2.getIndexPermissions().get(0)) //
            ));

            assertTrue(subject.toString(), subject.isAllowed("a"));
            assertTrue(subject.toString(), subject.isAllowed("a2"));
            assertFalse(subject.toString(), subject.isAllowed("a1"));
            assertFalse(subject.toString(), subject.isAllowed("a12"));
        }

    }

    public static class IndicesAndAliases_getRestriction {
        final static Meta META = indices("index_a1");

        final static Meta.Index index_a1 = (Meta.Index) META.getIndexOrLike("index_a1");

        @Test
        public void wildcard() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = TestSgConfig.Role.toActualRole(//                    
                    new TestSgConfig.Role("restricted_role_1").indexPermissions("*").fls("a").on("*"),
                    new TestSgConfig.Role("restricted_role_2").indexPermissions("*").fls("b").on("*"),
                    new TestSgConfig.Role("non_restricted_role").indexPermissions("*").on("*"));
            RoleBasedFieldAuthorization subject = new RoleBasedFieldAuthorization(roleConfig, META, MetricsLevel.NONE);

            RoleBasedFieldAuthorization.FlsRule flsRule = subject.getRestriction(ctx("restricted_role_1"), index_a1, Meter.NO_OP);
            assertTrue(flsRule.toString(), flsRule.isAllowed("a"));
            assertFalse(flsRule.toString(), flsRule.isAllowed("b"));

            flsRule = subject.getRestriction(ctx("restricted_role_2"), index_a1, Meter.NO_OP);
            assertFalse(flsRule.toString(), flsRule.isAllowed("a"));
            assertTrue(flsRule.toString(), flsRule.isAllowed("b"));

            flsRule = subject.getRestriction(ctx("restricted_role_1", "restricted_role_2"), index_a1, Meter.NO_OP);
            assertTrue(flsRule.toString(), flsRule.isAllowed("a"));
            assertTrue(flsRule.toString(), flsRule.isAllowed("b"));

            flsRule = subject.getRestriction(ctx("non_restricted_role"), index_a1, Meter.NO_OP);
            assertTrue(flsRule.toString(), flsRule.isAllowed("a"));
            assertTrue(flsRule.toString(), flsRule.isAllowed("b"));

            flsRule = subject.getRestriction(ctx("restricted_role_1", "non_restricted_role"), index_a1, Meter.NO_OP);
            assertTrue(flsRule.toString(), flsRule.isAllowed("a"));
            assertTrue(flsRule.toString(), flsRule.isAllowed("b"));
        }

        private static PrivilegesEvaluationContext ctx(String... roles) {
            return new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(), false, ImmutableSet.ofArray(roles), null, null, true, null,
                    null);
        }
    }
}
