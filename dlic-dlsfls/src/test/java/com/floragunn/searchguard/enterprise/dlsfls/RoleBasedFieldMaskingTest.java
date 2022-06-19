/*
 * Copyright 2022 by floragunn GmbH - All rights reserved
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

import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldMasking.FieldMaskingRule;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;

public class RoleBasedFieldMaskingTest {
    @Test
    public void getFieldMaskingRule_template() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration.of(CType.ROLES, "role",
                Role.parse(DocNode.of("index_permissions",
                        DocNode.array(DocNode.of("index_patterns", "index_${user.attrs.a}", "masked_fields", DocNode.array("masked_a", "masked_b")))),
                        null).get());

        RoleBasedFieldMasking subject = new RoleBasedFieldMasking(roleConfig, DlsFlsConfig.FieldMasking.DEFAULT,
                ImmutableSet.of("index_value_of_a", "another_index"), MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(
                new User.Builder().name("test_user").attribute("a", "value_of_a").build(), ImmutableSet.of("role"), null, subject, false, null, null);

        FieldMaskingRule fmRule = subject.getFieldMaskingRule(context, "index_value_of_a", Meter.NO_OP);

        Assert.assertNotNull(fmRule.toString(), fmRule.get("masked_a"));
        Assert.assertNotNull(fmRule.toString(), fmRule.get("masked_b"));
        Assert.assertNull(fmRule.toString(), fmRule.get("masked_c"));

        fmRule = subject.getFieldMaskingRule(context, "another_index", Meter.NO_OP);

        Assert.assertNull(fmRule.toString(), fmRule.get("masked_a"));
        Assert.assertNull(fmRule.toString(), fmRule.get("masked_b"));
        Assert.assertNull(fmRule.toString(), fmRule.get("masked_c"));
    }

    @Test
    public void getFieldMaskingRule_wildcardRule() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration.of(CType.ROLES, "role_with_wildcard_fm",
                Role.parse(
                        DocNode.of("index_permissions",
                                DocNode.array(
                                        DocNode.of("index_patterns", "*", "masked_fields", DocNode.array("wildcard_masked_a", "wildcard_masked_b")))),
                        null).get());

        RoleBasedFieldMasking subject = new RoleBasedFieldMasking(roleConfig, DlsFlsConfig.FieldMasking.DEFAULT,
                ImmutableSet.of("one_index", "another_index"), MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(),
                ImmutableSet.of("role_with_wildcard_fm"), null, subject, false, null, null);

        FieldMaskingRule fmRule = subject.getFieldMaskingRule(context, "one_index", Meter.NO_OP);

        Assert.assertNotNull(fmRule.toString(), fmRule.get("wildcard_masked_a"));
        Assert.assertNotNull(fmRule.toString(), fmRule.get("wildcard_masked_b"));
        Assert.assertNull(fmRule.toString(), fmRule.get("wildcard_masked_c"));
    }

    @Test
    public void hasFieldMaskingRestriction_template() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration.of(CType.ROLES, "role",
                Role.parse(DocNode.of("index_permissions",
                        DocNode.array(DocNode.of("index_patterns", "index_${user.attrs.a}", "masked_fields", DocNode.array("masked_a", "masked_b")))),
                        null).get());

        RoleBasedFieldMasking subject = new RoleBasedFieldMasking(roleConfig, DlsFlsConfig.FieldMasking.DEFAULT,
                ImmutableSet.of("index_value_of_a", "another_index"), MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(
                new User.Builder().name("test_user").attribute("a", "value_of_a").build(), ImmutableSet.of("role"), null, subject, false, null, null);

        Assert.assertTrue(subject.toString(), subject.hasFieldMaskingRestrictions(context, "index_value_of_a", Meter.NO_OP));
        Assert.assertFalse(subject.toString(), subject.hasFieldMaskingRestrictions(context, "another_index", Meter.NO_OP));
    }

    @Test
    public void hasFieldMaskingRestriction_wildcardRule() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration.of(CType.ROLES, "role_with_wildcard_fm",
                Role.parse(
                        DocNode.of("index_permissions",
                                DocNode.array(
                                        DocNode.of("index_patterns", "*", "masked_fields", DocNode.array("wildcard_masked_a", "wildcard_masked_b")))),
                        null).get());

        RoleBasedFieldMasking subject = new RoleBasedFieldMasking(roleConfig, DlsFlsConfig.FieldMasking.DEFAULT,
                ImmutableSet.of("one_index", "another_index"), MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(),
                ImmutableSet.of("role_with_wildcard_fm"), null, subject, false, null, null);

        Assert.assertTrue(subject.toString(), subject.hasFieldMaskingRestrictions(context, "one_index", Meter.NO_OP));
        Assert.assertTrue(subject.toString(), subject.hasFieldMaskingRestrictions(context, "another_index", Meter.NO_OP));

        context = new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(), ImmutableSet.of("role_without_wildcard_fm"), null,
                subject, false, null, null);

        Assert.assertFalse(subject.toString(), subject.hasFieldMaskingRestrictions(context, "one_index", Meter.NO_OP));
        Assert.assertFalse(subject.toString(), subject.hasFieldMaskingRestrictions(context, "another_index", Meter.NO_OP));

    }
}
