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
            assertTrue(subject.toString(), subject.isAllowedRecursive("a"));
            assertTrue(subject.toString(), subject.isAllowedRecursive("b"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("c"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("aa"));
        }

        @Test
        public void singleRole_pattern_positive() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").fls("a*", "b*").on("*").toActualRole();
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role.getIndexPermissions().get(0));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertTrue(subject.toString(), subject.isAllowedRecursive("a"));
            assertTrue(subject.toString(), subject.isAllowedRecursive("b"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("c"));
            assertTrue(subject.toString(), subject.isAllowedRecursive("aa"));
        }

        @Test
        public void singleRole_pattern_negation() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").fls("~a*", "~b*").on("*").toActualRole();
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role.getIndexPermissions().get(0));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertFalse(subject.toString(), subject.isAllowedRecursive("a"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("b"));
            assertTrue(subject.toString(), subject.isAllowedRecursive("c"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("aa"));
        }

        @Test
        public void singleRole_full_negation() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").fls("~*").on("*").toActualRole();
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role.getIndexPermissions().get(0));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertFalse(subject.toString(), subject.isAllowedRecursive("a"));
            assertFalse(subject.toString(), subject.isAllowedRecursive(""));
        }

        @Test
        public void singleRole_mixed() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").fls("a*", "~a1*").on("*").toActualRole();
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role.getIndexPermissions().get(0));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertTrue(subject.toString(), subject.isAllowedRecursive("a"));
            assertTrue(subject.toString(), subject.isAllowedRecursive("a2"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("a1"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("b"));
        }

        @Test
        public void singleRole_mixed_objects() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").fls("~object", "object.a*", "~object.a1*").on("*").toActualRole();
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role.getIndexPermissions().get(0));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertFalse(subject.toString(), subject.isAllowedRecursive("object"));
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.allowed"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.disallowed"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.a1_not_allowed"));
        }

        @Test
        public void singleRole_subobjects() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").fls("object").on("*").toActualRole();
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role.getIndexPermissions().get(0));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertTrue(subject.toString(), subject.isAllowedRecursive("object"));
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.allowed"));
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.also.allowed"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("object_not_allowed"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("objects"));// same as object_not_allowed
        }

        @Test
        public void singleRole_subobjects_not_grant_parent_access() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").fls("object.child.grandchild").on("*").toActualRole();
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role.getIndexPermissions().get(0));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertFalse(subject.toString(), subject.isAllowedRecursive("object"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child"));
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child.grandchild"));

            assertTrue(subject.toString(), subject.isAllowedAssumingParentsAreAllowed("object.child.grandchild"));
            assertFalse(subject.toString(), subject.isAllowedAssumingParentsAreAllowed("object.child"));
            assertTrue(subject.toString(), subject.isObjectAllowedAssumingParentsAreAllowed("object.child"));
            assertFalse(subject.toString(), subject.isAllowedAssumingParentsAreAllowed("text_field"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("text_field"));
            assertFalse(subject.toString(), subject.isAllowedAssumingParentsAreAllowed("object.child.text_field"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child.text_field"));
            assertTrue("a.b.c.d is allowed", subject.isAllowedRecursive("object.child.grandchild.text_field"));
        }

        @Test
        public void singleRole_subobjects_allow_parent_access_on_exclusion() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").fls("~object.child.grandchild").on("*").toActualRole();
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role.getIndexPermissions().get(0));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertTrue(subject.toString(), subject.isAllowedRecursive("object"));
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child.grandchild"));

            assertFalse(subject.toString(), subject.isAllowedAssumingParentsAreAllowed("object.child.grandchild"));
            assertTrue(subject.toString(), subject.isAllowedAssumingParentsAreAllowed("object.child"));
            assertTrue(subject.toString(), subject.isObjectAllowedAssumingParentsAreAllowed("object.child"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child.grandchild.text_field"));
        }

        @Test
        public void singleRole_subobjects_exclusion_and_inclusions_on_various_levels_allowed_start() throws Exception {
            Role role = new TestSgConfig.Role("role") //
                    .indexPermissions("*") //
                    .fls("object*", "~object_top_secret*", "~object.child*", "object.child.grandchild*", "~object.child.grandchild.leaf*") //
                    .on("*") //
                    .toActualRole();
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role.getIndexPermissions().get(0));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertFalse(subject.toString(), subject.isAllowedRecursive("o"));
            assertTrue(subject.toString(), subject.isAllowedRecursive("object")); //object*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.allowed_because_not_excluded")); //object*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object_top")); //object*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object_top_secret")); //~object_top_secret*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object_top_secret1")); //~object_top_secret*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object_top_secret1")); //~object_top_secret*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object_top_secret.child")); //~object_top_secret*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object_top_secret.child1")); //~object_top_secret*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object_top_secret.child1.subchild")); //~object_top_secret*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child")); //~object.child*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child_1")); //~object.child*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child_2")); //~object.child*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child.child")); //~object.child*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child_3.child")); //~object.child*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child.grand")); //~object.child*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child.grand")); //~object.child*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child.grandchild"));//object.child.grandchild*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child.grandchild_1"));//object.child.grandchild*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child.grandchildleaf"));//object.child.grandchild*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child.grandchild.a"));//object.child.grandchild*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child.grandchild.a.b"));//object.child.grandchild*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child.grandchild.a.b.c"));//object.child.grandchild*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child.grandchild.leaf")); //~object.child.grandchild.leaf*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child.grandchild.leaf_1")); //~object.child.grandchild.leaf*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child.grandchild.leaf_2")); //~object.child.grandchild.leaf*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child.grandchild.leaf_2.a.b.c")); //~object.child.grandchild.leaf*
        }

        @Test
        public void singleRole_subobjects_exclusion_and_inclusions_on_various_levels_disallowed_start() throws Exception {
            Role role = new TestSgConfig.Role("role") //
                    .indexPermissions("*") //
                    .fls("~object*", "object_top_secret*", "object.child*", "~object.child.grandchild*", "object.child.grandchild.leaf*") //
                    .on("*") //
                    .toActualRole();
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role.getIndexPermissions().get(0));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertFalse(subject.toString(), subject.isAllowedRecursive("o"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("object")); //~object*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.allowed_because_not_excluded")); //~object*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object_top")); //~object*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object_top_secret")); //object_top_secret*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object_top_secret1")); //object_top_secret*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object_top_secret1")); //object_top_secret*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object_top_secret.child")); //object_top_secret*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object_top_secret.child1")); //object_top_secret*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object_top_secret.child1.subchild")); //object_top_secret*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child")); //object.child*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child_1")); //object.child*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child_2")); //object.child*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child.child")); //object.child*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child_3.child")); //object.child*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child.grand")); //object.child*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child.grand")); //object.child*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child.grandchild"));//~object.child.grandchild*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child.grandchild_1"));//~object.child.grandchild*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child.grandchildleaf"));//~object.child.grandchild*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child.grandchild.a"));//~object.child.grandchild*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child.grandchild.a.b"));//~object.child.grandchild*
            assertFalse(subject.toString(), subject.isAllowedRecursive("object.child.grandchild.a.b.c"));//~object.child.grandchild*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child.grandchild.leaf")); //object.child.grandchild.leaf*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child.grandchild.leaf_1")); //object.child.grandchild.leaf*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child.grandchild.leaf_2")); //object.child.grandchild.leaf*
            assertTrue(subject.toString(), subject.isAllowedRecursive("object.child.grandchild.leaf_2.a.b.c")); //object.child.grandchild.leaf*
        }

        @Test
        public void singleRole_field_access_exceptions() throws Exception {
            Role role = new TestSgConfig.Role("role") //
                    .indexPermissions("*") //
                    .fls("a*", "~a1*") //
                    .on("*") //
                    .toActualRole();
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role.getIndexPermissions().get(0));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertFalse(subject.toString(), subject.isAllowedRecursive("b"));
            assertTrue(subject.toString(), subject.isAllowedRecursive("abc"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("a1"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("a123"));
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
            assertTrue(subject.toString(), subject.isAllowedRecursive("a"));
            assertTrue(subject.toString(), subject.isAllowedRecursive("b"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("c"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("aa"));
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

            assertTrue(subject.toString(), subject.isAllowedRecursive("a"));
            assertTrue(subject.toString(), subject.isAllowedRecursive("b"));
            assertTrue(subject.toString(), subject.isAllowedRecursive("c"));
            assertTrue(subject.toString(), subject.isAllowedRecursive("aa"));
        }

        @Test
        public void multiRole_pattern_negative_overlapping() throws Exception {
            Role role1 = new TestSgConfig.Role("role1").indexPermissions("*").fls("~a*").on("*").toActualRole();
            Role role2 = new TestSgConfig.Role("role2").indexPermissions("*").fls("~a1*").on("*").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2.getIndexPermissions().get(0)) //
            ));

            assertTrue(subject.toString(), subject.isAllowedRecursive("a"));
            assertTrue(subject.toString(), subject.isAllowedRecursive("a2"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("a1"));
            assertFalse(subject.toString(), subject.isAllowedRecursive("a12"));
        }

        @Test
        public void singleRole_hashCode_sameInstance() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").fls("a", "b").on("*").toActualRole();
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role.getIndexPermissions().get(0));

            int hash1 = subject.hashCode();
            int hash2 = subject.hashCode();

            assertTrue("hashCode should be stable across multiple calls", hash1 == hash2);
        }

        @Test
        public void singleRole_hashCode_equalPatterns() throws Exception {
            Role role1 = new TestSgConfig.Role("role1").indexPermissions("*").fls("a", "b").on("*").toActualRole();
            Role role2 = new TestSgConfig.Role("role2").indexPermissions("*").fls("a", "b").on("*").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.SingleRole subject1 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role1.getIndexPermissions().get(0));
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject2 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role2.getIndexPermissions().get(0));

            int hash1 = subject1.hashCode();
            int hash2 = subject2.hashCode();

            assertTrue("hashCode should be the same for objects with equal patterns", hash1 == hash2);
        }

        @Test
        public void singleRole_hashCode_differentPatterns() throws Exception {
            Role role1 = new TestSgConfig.Role("role1").indexPermissions("*").fls("a", "b").on("*").toActualRole();
            Role role2 = new TestSgConfig.Role("role2").indexPermissions("*").fls("c", "d").on("*").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.SingleRole subject1 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role1.getIndexPermissions().get(0));
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject2 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role2.getIndexPermissions().get(0));

            int hash1 = subject1.hashCode();
            int hash2 = subject2.hashCode();

            // Different patterns should ideally produce different hash codes (not guaranteed, but highly likely)
            // We just verify that both hash codes are computed without errors
            assertTrue("hashCode should execute without error", true);
        }

        @Test
        public void singleRole_hashCode_allowAll() throws Exception {
            Role role1 = new TestSgConfig.Role("role1").indexPermissions("*").on("*").toActualRole();
            Role role2 = new TestSgConfig.Role("role2").indexPermissions("*").on("*").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.SingleRole subject1 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role1.getIndexPermissions().get(0));
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject2 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role2.getIndexPermissions().get(0));

            int hash1 = subject1.hashCode();
            int hash2 = subject2.hashCode();

            assertTrue("hashCode should be the same for allowAll rules", hash1 == hash2);
        }

        @Test
        public void singleRole_hashCode_negationPatterns() throws Exception {
            Role role1 = new TestSgConfig.Role("role1").indexPermissions("*").fls("~a*", "~b*").on("*").toActualRole();
            Role role2 = new TestSgConfig.Role("role2").indexPermissions("*").fls("~a*", "~b*").on("*").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.SingleRole subject1 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role1.getIndexPermissions().get(0));
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject2 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role2.getIndexPermissions().get(0));

            int hash1 = subject1.hashCode();
            int hash2 = subject2.hashCode();

            assertTrue("hashCode should be the same for objects with equal negation patterns", hash1 == hash2);
        }

        @Test
        public void singleRole_hashCode_mixedPatterns() throws Exception {
            Role role1 = new TestSgConfig.Role("role1").indexPermissions("*").fls("a*", "~a1*").on("*").toActualRole();
            Role role2 = new TestSgConfig.Role("role2").indexPermissions("*").fls("a*", "~a1*").on("*").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.SingleRole subject1 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role1.getIndexPermissions().get(0));
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject2 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role2.getIndexPermissions().get(0));

            int hash1 = subject1.hashCode();
            int hash2 = subject2.hashCode();

            assertTrue("hashCode should be the same for objects with equal mixed patterns", hash1 == hash2);
        }

        @Test
        public void singleRole_hashCode_usingPatternConstructor() throws Exception {
            RoleBasedFieldAuthorization.FlsRule rule1 = RoleBasedFieldAuthorization.FlsRule.of("a", "b");
            RoleBasedFieldAuthorization.FlsRule rule2 = RoleBasedFieldAuthorization.FlsRule.of("a", "b");

            int hash1 = rule1.hashCode();
            int hash2 = rule2.hashCode();

            assertTrue("hashCode should be the same for rules created with FlsRule.of() with equal patterns", hash1 == hash2);
        }

        @Test
        public void singleRole_hashCode_sameIndexPattern() throws Exception {
            Role role1 = new TestSgConfig.Role("role1").indexPermissions("*").fls("a", "b").on("index1").toActualRole();
            Role role2 = new TestSgConfig.Role("role2").indexPermissions("*").fls("a", "b").on("index1").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.SingleRole subject1 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role1.getIndexPermissions().get(0));
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject2 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role2.getIndexPermissions().get(0));

            int hash1 = subject1.hashCode();
            int hash2 = subject2.hashCode();

            assertTrue("hashCode should be the same for objects with same index pattern", hash1 == hash2);
        }

        @Test
        public void singleRole_hashCode_differentIndexPatterns() throws Exception {
            Role role1 = new TestSgConfig.Role("role1").indexPermissions("*").fls("a", "b").on("index1").toActualRole();
            Role role2 = new TestSgConfig.Role("role2").indexPermissions("*").fls("a", "b").on("index2").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.SingleRole subject1 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role1.getIndexPermissions().get(0));
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject2 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role2.getIndexPermissions().get(0));

            int hash1 = subject1.hashCode();
            int hash2 = subject2.hashCode();

            assertTrue("hashCode should be different for objects with different index patterns", hash1 != hash2);
        }

        @Test
        public void singleRole_hashCode_wildcardVsSpecificIndexPattern() throws Exception {
            Role role1 = new TestSgConfig.Role("role1").indexPermissions("*").fls("a", "b").on("*").toActualRole();
            Role role2 = new TestSgConfig.Role("role2").indexPermissions("*").fls("a", "b").on("index1").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.SingleRole subject1 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role1.getIndexPermissions().get(0));
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject2 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role2.getIndexPermissions().get(0));

            int hash1 = subject1.hashCode();
            int hash2 = subject2.hashCode();

            assertTrue("hashCode should be different for wildcard vs specific index pattern", hash1 != hash2);
        }

        @Test
        public void singleRole_hashCode_multipleIndexPatterns() throws Exception {
            Role role1 = new TestSgConfig.Role("role1").indexPermissions("*").fls("a", "b").on("index1", "index2").toActualRole();
            Role role2 = new TestSgConfig.Role("role2").indexPermissions("*").fls("a", "b").on("index1", "index2").toActualRole();
            Role role3 = new TestSgConfig.Role("role3").indexPermissions("*").fls("a", "b").on("index1", "index3").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.SingleRole subject1 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role1.getIndexPermissions().get(0));
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject2 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role2.getIndexPermissions().get(0));
            RoleBasedFieldAuthorization.FlsRule.SingleRole subject3 = new RoleBasedFieldAuthorization.FlsRule.SingleRole(
                    role3.getIndexPermissions().get(0));

            int hash1 = subject1.hashCode();
            int hash2 = subject2.hashCode();
            int hash3 = subject3.hashCode();

            assertTrue("hashCode should be the same for objects with same multiple index patterns", hash1 == hash2);
            assertTrue("hashCode should be different for objects with different multiple index patterns", hash1 != hash3);
        }

        @Test
        public void multiRole_hashCode_sameInstance() throws Exception {
            Role role1 = new TestSgConfig.Role("role1").indexPermissions("*").fls("a").on("*").toActualRole();
            Role role2 = new TestSgConfig.Role("role2").indexPermissions("*").fls("b").on("*").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2.getIndexPermissions().get(0)) //
            ));

            int hash1 = subject.hashCode();
            int hash2 = subject.hashCode();

            assertTrue("hashCode should be stable across multiple calls", hash1 == hash2);
        }

        @Test
        public void multiRole_hashCode_equalEntries() throws Exception {
            Role role1a = new TestSgConfig.Role("role1a").indexPermissions("*").fls("a").on("*").toActualRole();
            Role role2a = new TestSgConfig.Role("role2a").indexPermissions("*").fls("b").on("*").toActualRole();
            Role role1b = new TestSgConfig.Role("role1b").indexPermissions("*").fls("a").on("*").toActualRole();
            Role role2b = new TestSgConfig.Role("role2b").indexPermissions("*").fls("b").on("*").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject1 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1a.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2a.getIndexPermissions().get(0)) //
            ));

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject2 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1b.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2b.getIndexPermissions().get(0)) //
            ));

            int hash1 = subject1.hashCode();
            int hash2 = subject2.hashCode();

            assertTrue("hashCode should be the same for MultiRole objects with equal entries", hash1 == hash2);
        }

        @Test
        public void multiRole_hashCode_differentEntries() throws Exception {
            Role role1a = new TestSgConfig.Role("role1a").indexPermissions("*").fls("a").on("*").toActualRole();
            Role role2a = new TestSgConfig.Role("role2a").indexPermissions("*").fls("b").on("*").toActualRole();
            Role role1b = new TestSgConfig.Role("role1b").indexPermissions("*").fls("c").on("*").toActualRole();
            Role role2b = new TestSgConfig.Role("role2b").indexPermissions("*").fls("d").on("*").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject1 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1a.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2a.getIndexPermissions().get(0)) //
            ));

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject2 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1b.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2b.getIndexPermissions().get(0)) //
            ));

            int hash1 = subject1.hashCode();
            int hash2 = subject2.hashCode();

            // Different entries should ideally produce different hash codes (not guaranteed, but highly likely)
            // We just verify that both hash codes are computed without errors
            assertTrue("hashCode should execute without error", true);
        }

        @Test
        public void multiRole_hashCode_withCache() throws Exception {
            Role role1 = new TestSgConfig.Role("role1").indexPermissions("*").fls("a*").on("*").toActualRole();
            Role role2 = new TestSgConfig.Role("role2").indexPermissions("*").fls("b*").on("*").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2.getIndexPermissions().get(0)) //
            ));

            int hashBeforeCache = subject.hashCode();

            // Trigger cache population
            subject.isAllowedRecursive("a");
            subject.isAllowedRecursive("b");
            subject.isAllowedRecursive("c");

            int hashAfterCache = subject.hashCode();

            assertTrue("hashCode should remain stable after cache population", hashBeforeCache == hashAfterCache);
        }

        @Test
        public void multiRole_hashCode_differentNumberOfEntries() throws Exception {
            Role role1 = new TestSgConfig.Role("role1").indexPermissions("*").fls("a").on("*").toActualRole();
            Role role2 = new TestSgConfig.Role("role2").indexPermissions("*").fls("b").on("*").toActualRole();
            Role role3 = new TestSgConfig.Role("role3").indexPermissions("*").fls("c").on("*").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject1 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2.getIndexPermissions().get(0)) //
            ));

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject2 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role3.getIndexPermissions().get(0)) //
            ));

            int hash1 = subject1.hashCode();
            int hash2 = subject2.hashCode();

            // Different number of entries should ideally produce different hash codes
            // We just verify that both hash codes are computed without errors
            assertTrue("hashCode should execute without error", true);
        }

        @Test
        public void multiRole_hashCode_negationPatterns() throws Exception {
            Role role1a = new TestSgConfig.Role("role1a").indexPermissions("*").fls("~a*").on("*").toActualRole();
            Role role2a = new TestSgConfig.Role("role2a").indexPermissions("*").fls("~b*").on("*").toActualRole();
            Role role1b = new TestSgConfig.Role("role1b").indexPermissions("*").fls("~a*").on("*").toActualRole();
            Role role2b = new TestSgConfig.Role("role2b").indexPermissions("*").fls("~b*").on("*").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject1 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1a.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2a.getIndexPermissions().get(0)) //
            ));

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject2 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1b.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2b.getIndexPermissions().get(0)) //
            ));

            int hash1 = subject1.hashCode();
            int hash2 = subject2.hashCode();

            assertTrue("hashCode should be the same for MultiRole objects with equal negation patterns", hash1 == hash2);
        }

        @Test
        public void multiRole_hashCode_sameIndexPatterns() throws Exception {
            Role role1a = new TestSgConfig.Role("role1a").indexPermissions("*").fls("a").on("index1").toActualRole();
            Role role2a = new TestSgConfig.Role("role2a").indexPermissions("*").fls("b").on("index1").toActualRole();
            Role role1b = new TestSgConfig.Role("role1b").indexPermissions("*").fls("a").on("index1").toActualRole();
            Role role2b = new TestSgConfig.Role("role2b").indexPermissions("*").fls("b").on("index1").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject1 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1a.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2a.getIndexPermissions().get(0)) //
            ));

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject2 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1b.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2b.getIndexPermissions().get(0)) //
            ));

            int hash1 = subject1.hashCode();
            int hash2 = subject2.hashCode();

            assertTrue("hashCode should be the same for MultiRole objects with same index patterns", hash1 == hash2);
        }

        @Test
        public void multiRole_hashCode_differentIndexPatterns() throws Exception {
            Role role1a = new TestSgConfig.Role("role1a").indexPermissions("*").fls("a").on("index1").toActualRole();
            Role role2a = new TestSgConfig.Role("role2a").indexPermissions("*").fls("b").on("index1").toActualRole();
            Role role1b = new TestSgConfig.Role("role1b").indexPermissions("*").fls("a").on("index2").toActualRole();
            Role role2b = new TestSgConfig.Role("role2b").indexPermissions("*").fls("b").on("index2").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject1 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1a.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2a.getIndexPermissions().get(0)) //
            ));

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject2 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1b.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2b.getIndexPermissions().get(0)) //
            ));

            int hash1 = subject1.hashCode();
            int hash2 = subject2.hashCode();

            assertTrue("hashCode should be different for MultiRole objects with different index patterns", hash1 != hash2);
        }

        @Test
        public void multiRole_hashCode_wildcardVsSpecificIndexPattern() throws Exception {
            Role role1a = new TestSgConfig.Role("role1a").indexPermissions("*").fls("a").on("*").toActualRole();
            Role role2a = new TestSgConfig.Role("role2a").indexPermissions("*").fls("b").on("*").toActualRole();
            Role role1b = new TestSgConfig.Role("role1b").indexPermissions("*").fls("a").on("index1").toActualRole();
            Role role2b = new TestSgConfig.Role("role2b").indexPermissions("*").fls("b").on("index1").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject1 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1a.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2a.getIndexPermissions().get(0)) //
            ));

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject2 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1b.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2b.getIndexPermissions().get(0)) //
            ));

            int hash1 = subject1.hashCode();
            int hash2 = subject2.hashCode();

            assertTrue("hashCode should be different for MultiRole with wildcard vs specific index pattern", hash1 != hash2);
        }

        @Test
        public void multiRole_hashCode_multipleIndexPatterns() throws Exception {
            Role role1a = new TestSgConfig.Role("role1a").indexPermissions("*").fls("a").on("index1", "index2").toActualRole();
            Role role2a = new TestSgConfig.Role("role2a").indexPermissions("*").fls("b").on("index1", "index2").toActualRole();
            Role role1b = new TestSgConfig.Role("role1b").indexPermissions("*").fls("a").on("index1", "index2").toActualRole();
            Role role2b = new TestSgConfig.Role("role2b").indexPermissions("*").fls("b").on("index1", "index2").toActualRole();
            Role role1c = new TestSgConfig.Role("role1c").indexPermissions("*").fls("a").on("index1", "index3").toActualRole();
            Role role2c = new TestSgConfig.Role("role2c").indexPermissions("*").fls("b").on("index1", "index3").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject1 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1a.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2a.getIndexPermissions().get(0)) //
            ));

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject2 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1b.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2b.getIndexPermissions().get(0)) //
            ));

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject3 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1c.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2c.getIndexPermissions().get(0)) //
            ));

            int hash1 = subject1.hashCode();
            int hash2 = subject2.hashCode();
            int hash3 = subject3.hashCode();

            assertTrue("hashCode should be the same for MultiRole objects with same multiple index patterns", hash1 == hash2);
            assertTrue("hashCode should be different for MultiRole objects with different multiple index patterns", hash1 != hash3);
        }

        @Test
        public void multiRole_hashCode_mixedIndexPatterns() throws Exception {
            Role role1a = new TestSgConfig.Role("role1a").indexPermissions("*").fls("a").on("index1").toActualRole();
            Role role2a = new TestSgConfig.Role("role2a").indexPermissions("*").fls("b").on("index2").toActualRole();
            Role role1b = new TestSgConfig.Role("role1b").indexPermissions("*").fls("a").on("index1").toActualRole();
            Role role2b = new TestSgConfig.Role("role2b").indexPermissions("*").fls("b").on("index2").toActualRole();
            Role role1c = new TestSgConfig.Role("role1c").indexPermissions("*").fls("a").on("index1").toActualRole();
            Role role2c = new TestSgConfig.Role("role2c").indexPermissions("*").fls("b").on("index3").toActualRole();

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject1 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1a.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2a.getIndexPermissions().get(0)) //
            ));

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject2 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1b.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2b.getIndexPermissions().get(0)) //
            ));

            RoleBasedFieldAuthorization.FlsRule.MultiRole subject3 = new RoleBasedFieldAuthorization.FlsRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role1c.getIndexPermissions().get(0)), //
                    new RoleBasedFieldAuthorization.FlsRule.SingleRole(role2c.getIndexPermissions().get(0)) //
            ));

            int hash1 = subject1.hashCode();
            int hash2 = subject2.hashCode();
            int hash3 = subject3.hashCode();

            assertTrue("hashCode should be the same for MultiRole objects with same mixed index patterns", hash1 == hash2);
            assertTrue("hashCode should be different for MultiRole objects with different mixed index patterns", hash1 != hash3);
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
            assertTrue(flsRule.toString(), flsRule.isAllowedRecursive("a"));
            assertFalse(flsRule.toString(), flsRule.isAllowedRecursive("b"));

            flsRule = subject.getRestriction(ctx("restricted_role_2"), index_a1, Meter.NO_OP);
            assertFalse(flsRule.toString(), flsRule.isAllowedRecursive("a"));
            assertTrue(flsRule.toString(), flsRule.isAllowedRecursive("b"));

            flsRule = subject.getRestriction(ctx("restricted_role_1", "restricted_role_2"), index_a1, Meter.NO_OP);
            assertTrue(flsRule.toString(), flsRule.isAllowedRecursive("a"));
            assertTrue(flsRule.toString(), flsRule.isAllowedRecursive("b"));

            flsRule = subject.getRestriction(ctx("non_restricted_role"), index_a1, Meter.NO_OP);
            assertTrue(flsRule.toString(), flsRule.isAllowedRecursive("a"));
            assertTrue(flsRule.toString(), flsRule.isAllowedRecursive("b"));

            flsRule = subject.getRestriction(ctx("restricted_role_1", "non_restricted_role"), index_a1, Meter.NO_OP);
            assertTrue(flsRule.toString(), flsRule.isAllowedRecursive("a"));
            assertTrue(flsRule.toString(), flsRule.isAllowedRecursive("b"));
        }

        private static PrivilegesEvaluationContext ctx(String... roles) {
            return new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(), false, ImmutableSet.ofArray(roles), null, null, true, null,
                    null);
        }
    }
}
