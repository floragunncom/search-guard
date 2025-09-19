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

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchsupport.meta.Meta;
import com.floragunn.searchsupport.util.EsLogging;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import static com.floragunn.searchsupport.meta.Meta.Mock.indices;
import static java.util.Objects.isNull;
import static org.hamcrest.MatcherAssert.assertThat;
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
    public static final DlsFlsConfig.FieldMasking CUSTOM_CONFIG = new DlsFlsConfig.FieldMasking(DocNode.EMPTY,
            new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5 }, new byte[] { 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1, -1, -1, -1, -1, 8 },
            "confidential");

    public static class FieldMaskingRule {
        @Test
        public void singleRole_empty() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").on("*").toActualRole();
            RoleBasedFieldMasking.FieldMaskingRule.SingleRole subject = new RoleBasedFieldMasking.FieldMaskingRule.SingleRole(
                    role.getIndexPermissions().get(0), DlsFlsConfig.FieldMasking.DEFAULT);

            assertTrue(subject.toString(), subject.isAllowAll());
        }

        @Test
        public void singleRole_simple() throws Exception {
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
        public void singleRole_pattern() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").maskedFields("a*::/[0-9]{1,3}$/::XXX::/^[0-9]{1,3}/::***", "b*").on("*").toActualRole();
            RoleBasedFieldMasking.FieldMaskingRule.SingleRole subject = new RoleBasedFieldMasking.FieldMaskingRule.SingleRole(
                    role.getIndexPermissions().get(0), DlsFlsConfig.FieldMasking.DEFAULT);

            assertFalse(subject.toString(), subject.isAllowAll());
            assertNotNull(subject.toString(), subject.get("a"));
            assertNotNull(subject.toString(), subject.get("b"));
            assertNull(subject.toString(), subject.get("c"));
            assertNotNull(subject.toString(), subject.get("aa"));
        }

        @Test
        public void singleRole_pattern_alg() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").maskedFields("a*::SHA-1", "b*::SHA-512").on("*").toActualRole();
            RoleBasedFieldMasking.FieldMaskingRule.SingleRole subject = new RoleBasedFieldMasking.FieldMaskingRule.SingleRole(
                    role.getIndexPermissions().get(0), DlsFlsConfig.FieldMasking.DEFAULT);

            assertFalse(subject.toString(), subject.isAllowAll());
            assertNotNull(subject.toString(), subject.get("a"));
            assertNotNull(subject.toString(), subject.get("b"));
            assertNull(subject.toString(), subject.get("c"));
            assertNotNull(subject.toString(), subject.get("aa"));
        }

        @Test
        public void singleRole_pattern_customConfig() throws Exception {
            Role role = new TestSgConfig.Role("role").indexPermissions("*").maskedFields("a*", "b*").on("*").toActualRole();
            RoleBasedFieldMasking.FieldMaskingRule.SingleRole subject = new RoleBasedFieldMasking.FieldMaskingRule.SingleRole(
                    role.getIndexPermissions().get(0), CUSTOM_CONFIG);

            assertFalse(subject.toString(), subject.isAllowAll());
            assertNotNull(subject.toString(), subject.get("a"));
            assertNotNull(subject.toString(), subject.get("b"));
            assertNull(subject.toString(), subject.get("c"));
            assertNotNull(subject.toString(), subject.get("aa"));
        }

        @Test
        public void multiRole_simple() throws Exception {
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
        public void multiRole_pattern() throws Exception {
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

    @RunWith(Parameterized.class)
    public static class IndicesAndAliases_getRestriction {
        @ClassRule
        public static EsLogging esLogging = new EsLogging();
        final static Meta META = indices("index_a1", "hr", "it");

        final static Meta.Index INDEX_A1 = (Meta.Index) META.getIndexOrLike("index_a1");
        final static Meta.Index INDEX_HR = (Meta.Index) META.getIndexOrLike("hr");

        private final DlsFlsConfig.FieldMasking fieldMaskingConfig;

        public IndicesAndAliases_getRestriction(DlsFlsConfig.FieldMasking fieldMaskingConfig) {
            this.fieldMaskingConfig = fieldMaskingConfig;
        }

        @Parameterized.Parameters
        public static Object[] data() {
            return new Object[] { new Object[] { DlsFlsConfig.FieldMasking.DEFAULT }, new Object[] { CUSTOM_CONFIG } };
        }

        @Test
        public void wildcard() throws Exception {
            RoleBasedFieldMasking subject = buildRoleBasedFieldMasking( //
                    new TestSgConfig.Role("restricted_role_1").indexPermissions("*").maskedFields("a").on("*"), //
                    new TestSgConfig.Role("restricted_role_2").indexPermissions("*").maskedFields("b").on("*"), //
                    new TestSgConfig.Role("non_restricted_role").indexPermissions("*").on("*"));

            RoleBasedFieldMasking.FieldMaskingRule fieldMaskingRule = subject.getRestriction(ctx("restricted_role_1"), INDEX_A1, Meter.NO_OP);
            assertNotNull(fieldMaskingRule.toString(), fieldMaskingRule.get("a"));
            assertNull(fieldMaskingRule.toString(), fieldMaskingRule.get("b"));

            fieldMaskingRule = subject.getRestriction(ctx("restricted_role_2"), INDEX_A1, Meter.NO_OP);
            assertNull(fieldMaskingRule.toString(), fieldMaskingRule.get("a"));
            assertNotNull(fieldMaskingRule.toString(), fieldMaskingRule.get("b"));

            fieldMaskingRule = subject.getRestriction(ctx("restricted_role_1", "restricted_role_2"), INDEX_A1, Meter.NO_OP);
            assertNull(fieldMaskingRule.toString(), fieldMaskingRule.get("a"));
            assertNull(fieldMaskingRule.toString(), fieldMaskingRule.get("b"));

            fieldMaskingRule = subject.getRestriction(ctx("non_restricted_role"), INDEX_A1, Meter.NO_OP);
            assertNull(fieldMaskingRule.toString(), fieldMaskingRule.get("a"));
            assertNull(fieldMaskingRule.toString(), fieldMaskingRule.get("b"));

            fieldMaskingRule = subject.getRestriction(ctx("restricted_role_1", "non_restricted_role"), INDEX_A1, Meter.NO_OP);
            assertNull(fieldMaskingRule.toString(), fieldMaskingRule.get("a"));
            assertNull(fieldMaskingRule.toString(), fieldMaskingRule.get("b"));
        }

        private RoleBasedFieldMasking buildRoleBasedFieldMasking(TestSgConfig.Role... roles) throws ConfigValidationException {
            SgDynamicConfiguration<Role> roleConfig = TestSgConfig.Role.toActualRole(roles);
            return new RoleBasedFieldMasking(roleConfig, fieldMaskingConfig, META, MetricsLevel.NONE);
        }

        @Test
        public void combine() throws ConfigValidationException, PrivilegesEvaluationException {
            RoleBasedFieldMasking subject = buildRoleBasedFieldMasking( //
                    new TestSgConfig.Role("masked_a_on_hr").indexPermissions("*").maskedFields("a").on("hr"), //
                    new TestSgConfig.Role("masked_b_on_hr").indexPermissions("*").maskedFields("b").on("hr"), //
                    new TestSgConfig.Role("masked_c_on_hr").indexPermissions("*").maskedFields("c").on("hr"), //
                    new TestSgConfig.Role("masked_ab_on_hr").indexPermissions("*").maskedFields("a", "b").on("hr"), //
                    new TestSgConfig.Role("masked_bc_on_hr").indexPermissions("*").maskedFields("b", "c").on("hr"), //
                    new TestSgConfig.Role("masked_ac_on_hr").indexPermissions("*").maskedFields("a", "c").on("hr"), //
                    new TestSgConfig.Role("masked_abc_on_hr").indexPermissions("*").maskedFields("a", "b", "c").on("hr"), //
                    new TestSgConfig.Role("masked_all_on_hr").indexPermissions("*").maskedFields("*").on("hr"), //
                    new TestSgConfig.Role("non_masked_on_hr").indexPermissions("*").on("hr"), //
                    new TestSgConfig.Role("masked_salary_on_it").indexPermissions("*").maskedFields("salary*").on("it"), //
                    new TestSgConfig.Role("non_restricted_role").indexPermissions("*").on("*"));

            var fieldMaskingRule = subject.getRestriction(ctx("masked_abc_on_hr", "masked_salary_on_it"), INDEX_HR, Meter.NO_OP);
            assertThat(fieldMaskingRule, maskedFields("a", "b", "c"));

            fieldMaskingRule = subject.getRestriction(ctx("masked_abc_on_hr", "non_masked_on_hr"), INDEX_HR, Meter.NO_OP);
            assertThat(fieldMaskingRule, notMaskedFields("a", "b", "c"));

            fieldMaskingRule = subject.getRestriction(ctx("masked_abc_on_hr", "non_masked_on_hr"), INDEX_HR, Meter.NO_OP);
            assertThat(fieldMaskingRule, notMaskedFields("a", "b", "c"));

            fieldMaskingRule = subject.getRestriction(ctx("masked_abc_on_hr", "masked_bc_on_hr"), INDEX_HR, Meter.NO_OP);
            assertThat(fieldMaskingRule, notMaskedFields("a"));
            assertThat(fieldMaskingRule, maskedFields("b", "c"));

            fieldMaskingRule = subject.getRestriction(ctx("masked_abc_on_hr", "masked_a_on_hr"), INDEX_HR, Meter.NO_OP);
            assertThat(fieldMaskingRule, maskedFields("a"));
            assertThat(fieldMaskingRule, notMaskedFields("b", "c"));

            fieldMaskingRule = subject.getRestriction(ctx("masked_ab_on_hr", "masked_b_on_hr", "masked_bc_on_hr"), INDEX_HR, Meter.NO_OP);
            assertThat(fieldMaskingRule, notMaskedFields("a","c"));
            assertThat(fieldMaskingRule, maskedFields("b"));

            fieldMaskingRule = subject.getRestriction(ctx("masked_ab_on_hr", "masked_bc_on_hr"), INDEX_HR, Meter.NO_OP);
            assertThat(fieldMaskingRule, notMaskedFields("a", "c"));
            assertThat(fieldMaskingRule, maskedFields("b"));

            fieldMaskingRule = subject.getRestriction(ctx("masked_abc_on_hr", "masked_c_on_hr"), INDEX_HR, Meter.NO_OP);
            assertThat(fieldMaskingRule, notMaskedFields("a", "b"));
            assertThat(fieldMaskingRule, maskedFields("c"));

            fieldMaskingRule = subject.getRestriction(ctx("masked_abc_on_hr", "masked_bc_on_hr"), INDEX_HR, Meter.NO_OP);
            assertThat(fieldMaskingRule, notMaskedFields("a"));
            assertThat(fieldMaskingRule, maskedFields("b", "c"));

            fieldMaskingRule = subject.getRestriction(ctx("masked_ab_on_hr", "masked_abc_on_hr", "masked_bc_on_hr", "masked_ac_on_hr", "masked_a_on_hr", "masked_b_on_hr"), INDEX_HR, Meter.NO_OP);
            assertThat(fieldMaskingRule, notMaskedFields("a", "b", "c"));

            fieldMaskingRule = subject.getRestriction(ctx("masked_all_on_hr", "non_masked_on_hr"), INDEX_HR, Meter.NO_OP);
            assertThat(fieldMaskingRule, notMaskedFields("a", "b", "c"));

            fieldMaskingRule = subject.getRestriction(ctx("masked_abc_on_hr", "non_masked_on_hr"), INDEX_HR, Meter.NO_OP);
            assertThat(fieldMaskingRule, notMaskedFields("a", "b", "c"));

            fieldMaskingRule = subject.getRestriction(ctx("masked_ab_on_hr", "masked_ac_on_hr"), INDEX_HR, Meter.NO_OP);
            assertThat(fieldMaskingRule, notMaskedFields("b", "c"));
            assertThat(fieldMaskingRule, maskedFields("a"));

            fieldMaskingRule = subject.getRestriction(ctx("masked_ab_on_hr", "masked_ac_on_hr", "masked_salary_on_it"), INDEX_HR, Meter.NO_OP);
            assertThat(fieldMaskingRule, notMaskedFields("b", "c"));
            assertThat(fieldMaskingRule, maskedFields("a"));

            // Test case 9
            fieldMaskingRule = subject.getRestriction(ctx("masked_a_on_hr", "masked_b_on_hr", "masked_c_on_hr"), INDEX_HR, Meter.NO_OP);
            assertThat(fieldMaskingRule, notMaskedFields("a", "b", "c"));

            fieldMaskingRule = subject.getRestriction(ctx("masked_a_on_hr", "masked_ab_on_hr"), INDEX_HR, Meter.NO_OP);
            assertThat(fieldMaskingRule, notMaskedFields("b", "c"));
            assertThat(fieldMaskingRule, maskedFields("a"));

            fieldMaskingRule = subject.getRestriction(ctx("masked_b_on_hr", "masked_bc_on_hr", "masked_salary_on_it"), INDEX_HR, Meter.NO_OP);
            assertThat(fieldMaskingRule, notMaskedFields("a", "c"));
            assertThat(fieldMaskingRule, maskedFields("b"));

            fieldMaskingRule = subject.getRestriction(ctx("masked_c_on_hr", "masked_ac_on_hr"), INDEX_HR, Meter.NO_OP);
            assertThat(fieldMaskingRule, notMaskedFields("a", "b"));
            assertThat(fieldMaskingRule, maskedFields("c"));
        }

        private static PrivilegesEvaluationContext ctx(String... roles) {
            return new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(), false, ImmutableSet.ofArray(roles), null, null, true, null,
                    null);
        }
    }

    public static Matcher<RoleBasedFieldMasking.FieldMaskingRule> maskedFields(String...fields) {
        return new TypeSafeDiagnosingMatcher<>() {
            {
                if (fields == null || fields.length == 0) {
                    throw new IllegalArgumentException("At least one field must be provided");
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Fields ");
                for (String fieldName : fields) {
                    description.appendValue(fieldName).appendText(" ");
                }
                description.appendValue("should be masked");
            }

            @Override
            protected boolean matchesSafely(RoleBasedFieldMasking.FieldMaskingRule rule, Description description) {
                boolean result = true;
                for (String fieldName : fields) {
                    if (isNull(rule.get(fieldName))) {
                        description.appendText("Field ").appendValue(fieldName).appendText(" is not masked. ");
                        result = false;
                    }
                }
                return result;
            }
        };
    }

    public static Matcher<RoleBasedFieldMasking.FieldMaskingRule> notMaskedFields(String...fields) {
        return new TypeSafeDiagnosingMatcher<>() {
            {
                if (fields == null || fields.length == 0) {
                    throw new IllegalArgumentException("At least one field must be provided");
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Fields ");
                for (String fieldName : fields) {
                    description.appendValue(fieldName).appendText(" ");
                }
                description.appendValue("should not be masked");
            }

            @Override
            protected boolean matchesSafely(RoleBasedFieldMasking.FieldMaskingRule rule, Description description) {
                boolean result = true;
                for (String fieldName : fields) {
                    if (rule.get(fieldName) != null) {
                        description.appendText("Field ").appendValue(fieldName).appendText(" is masked. ");
                        result = false;
                    }
                }
                return result;
            }
        };
    }
}
