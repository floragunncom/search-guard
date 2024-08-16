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

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchsupport.meta.Meta;
import org.junit.Test;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;


import static com.floragunn.searchsupport.meta.Meta.Mock.indices;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Most coverage is already achieved via {@link RoleBasedAuthorizationBase} and {@link RoleBasedDocumentAuthorizationTest}.
 * We only test here the rule implementation and basic functionality.
 */
@RunWith(Suite.class)
@SuiteClasses({ RoleBasedFieldMaskingTest.FieldMaskingRule.class, RoleBasedFieldMaskingTest.IndicesAndAliases_getRestriction.class })
public class RoleBasedFieldMaskingTest {

    public static class FieldMaskingRule {
        @Test
        public void singleRole_empty() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").on("*").toActualRole();
            RoleBasedFieldMasking.FieldMaskingRule.SingleRole subject = new RoleBasedFieldMasking.FieldMaskingRule.SingleRole(
                    role.getIndexPermissions().get(0), DlsFlsConfig.FieldMasking.DEFAULT);

            assertTrue(subject.toString(), subject.isAllowAll());
        }

        @Test
        public void singleRole_simple_positive() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").maskedFields("a", "b").on("*").toActualRole();
            RoleBasedFieldMasking.FieldMaskingRule.SingleRole subject = new RoleBasedFieldMasking.FieldMaskingRule.SingleRole(
                    role.getIndexPermissions().get(0), DlsFlsConfig.FieldMasking.DEFAULT);

            assertFalse(subject.toString(), subject.isAllowAll());
            assertNotNull(subject.toString(), subject.get("a"));
            assertNotNull(subject.toString(), subject.get("b"));
            assertNull(subject.toString(), subject.get("c"));
            assertNull(subject.toString(), subject.get("aa"));
        }

        @Test
        public void singleRole_pattern_positive() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").maskedFields("a*", "b*").on("*").toActualRole();
            RoleBasedFieldMasking.FieldMaskingRule.SingleRole subject = new RoleBasedFieldMasking.FieldMaskingRule.SingleRole(
                    role.getIndexPermissions().get(0), DlsFlsConfig.FieldMasking.DEFAULT);

            assertFalse(subject.toString(), subject.isAllowAll());
            assertNotNull(subject.toString(), subject.get("a"));
            assertNotNull(subject.toString(), subject.get("b"));
            assertNull(subject.toString(), subject.get("c"));
            assertNotNull(subject.toString(), subject.get("aa"));
        }

        @Test
        public void multiRole_simple_positive() throws Exception {
            Role role1 = new TestSgConfig.Role("role1").indexPermissions("*").maskedFields("a", "c").on("*").toActualRole();
            Role role2 = new TestSgConfig.Role("role2").indexPermissions("*").maskedFields("b", "c").on("*").toActualRole();

            RoleBasedFieldMasking.FieldMaskingRule.MultiRole subject = new RoleBasedFieldMasking.FieldMaskingRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldMasking.FieldMaskingRule.SingleRole(role1.getIndexPermissions().get(0), DlsFlsConfig.FieldMasking.DEFAULT), //
                    new RoleBasedFieldMasking.FieldMaskingRule.SingleRole(role2.getIndexPermissions().get(0), DlsFlsConfig.FieldMasking.DEFAULT) //
            ));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertNull(subject.toString(), subject.get("a"));
            assertNull(subject.toString(), subject.get("b"));
            assertNotNull(subject.toString(), subject.get("c"));
            assertNull(subject.toString(), subject.get("aa"));
        }

        @Test
        public void multiRole_pattern_positive() throws Exception {
            Role role1 = new TestSgConfig.Role("role1").indexPermissions("*").maskedFields("a*", "c*").on("*").toActualRole();
            Role role2 = new TestSgConfig.Role("role2").indexPermissions("*").maskedFields("b*", "c*").on("*").toActualRole();

            RoleBasedFieldMasking.FieldMaskingRule.MultiRole subject = new RoleBasedFieldMasking.FieldMaskingRule.MultiRole(ImmutableList.of(//
                    new RoleBasedFieldMasking.FieldMaskingRule.SingleRole(role1.getIndexPermissions().get(0), DlsFlsConfig.FieldMasking.DEFAULT), //
                    new RoleBasedFieldMasking.FieldMaskingRule.SingleRole(role2.getIndexPermissions().get(0), DlsFlsConfig.FieldMasking.DEFAULT) //
            ));

            assertFalse(subject.toString(), subject.isAllowAll());
            assertNull(subject.toString(), subject.get("a"));
            assertNull(subject.toString(), subject.get("b"));
            assertNotNull(subject.toString(), subject.get("c"));
            assertNull(subject.toString(), subject.get("aa"));
        }
    }

    public static class IndicesAndAliases_getRestriction {
        final static Meta META = indices("index_a1");

        final static Meta.Index index_a1 = (Meta.Index) META.getIndexOrLike("index_a1");

        @Test
        public void wildcard() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = TestSgConfig.Role.toActualRole(//
                    new TestSgConfig.Role("restricted_role_1").indexPermissions("*").maskedFields("a").on("*"),
                    new TestSgConfig.Role("restricted_role_2").indexPermissions("*").maskedFields("b").on("*"),
                    new TestSgConfig.Role("non_restricted_role").indexPermissions("*").on("*"));
            RoleBasedFieldMasking subject = new RoleBasedFieldMasking(roleConfig, DlsFlsConfig.FieldMasking.DEFAULT, META, MetricsLevel.NONE);

            RoleBasedFieldMasking.FieldMaskingRule fieldMaskingRule = subject.getRestriction(ctx("restricted_role_1"), index_a1, Meter.NO_OP);
            assertNotNull(fieldMaskingRule.toString(), fieldMaskingRule.get("a"));
            assertNull(fieldMaskingRule.toString(), fieldMaskingRule.get("b"));

            fieldMaskingRule = subject.getRestriction(ctx("restricted_role_2"), index_a1, Meter.NO_OP);
            assertNull(fieldMaskingRule.toString(), fieldMaskingRule.get("a"));
            assertNotNull(fieldMaskingRule.toString(), fieldMaskingRule.get("b"));

            fieldMaskingRule = subject.getRestriction(ctx("restricted_role_1", "restricted_role_2"), index_a1, Meter.NO_OP);
            assertNull(fieldMaskingRule.toString(), fieldMaskingRule.get("a"));
            assertNull(fieldMaskingRule.toString(), fieldMaskingRule.get("b"));

            fieldMaskingRule = subject.getRestriction(ctx("non_restricted_role"), index_a1, Meter.NO_OP);
            assertNull(fieldMaskingRule.toString(), fieldMaskingRule.get("a"));
            assertNull(fieldMaskingRule.toString(), fieldMaskingRule.get("b"));

            fieldMaskingRule = subject.getRestriction(ctx("restricted_role_1", "non_restricted_role"), index_a1, Meter.NO_OP);
            assertNull(fieldMaskingRule.toString(), fieldMaskingRule.get("a"));
            assertNull(fieldMaskingRule.toString(), fieldMaskingRule.get("b"));
        }

        private static PrivilegesEvaluationContext ctx(String... roles) {
            return new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(), false, ImmutableSet.ofArray(roles), null, null, true, null,
                    null);
        }
    }
}
