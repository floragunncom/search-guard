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
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldAuthorization.FlsRule;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;

public class RoleBasedFieldAuthorizationTest {

    /*
    @Test
    public void getFlsRule_template() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration.of(CType.ROLES, "role", Role
                .parse(DocNode.of("index_permissions",
                        DocNode.array(DocNode.of("index_patterns", "index_${user.attrs.a}", "fls", DocNode.array("allowed_a", "allowed_b")))), null)
                .get());

        RoleBasedFieldAuthorization subject = new RoleBasedFieldAuthorization(roleConfig, ImmutableSet.of("index_value_of_a", "another_index"),
                MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(
                new User.Builder().name("test_user").attribute("a", "value_of_a").build(), ImmutableSet.of("role"), null, subject, false, null, null);

        FlsRule flsRule = subject.getFlsRule(context, "index_value_of_a", Meter.NO_OP);

        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_a"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_b"));
        Assert.assertFalse(flsRule.toString(), flsRule.isAllowed("allowed_c"));

        flsRule = subject.getFlsRule(context, "another_index", Meter.NO_OP);

        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_a"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_b"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_c"));
    }

    @Test
    public void getFlsRule_negation() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration.of(CType.ROLES, "role",
                Role.parse(DocNode.of("index_permissions", DocNode.array(DocNode.of("index_patterns",
                        DocNode.array("index_abc*", "-index_abcd"), "fls", DocNode.array("allowed_a", "allowed_b")))), null).get());

        RoleBasedFieldAuthorization subject = new RoleBasedFieldAuthorization(roleConfig, ImmutableSet.of("index_abc", "index_abcd"),
                MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(),
                ImmutableSet.of("role"), null, subject, false, null, null);

        FlsRule flsRule = subject.getFlsRule(context, "index_abc", Meter.NO_OP);

        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_a"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_b"));
        Assert.assertFalse(flsRule.toString(), flsRule.isAllowed("allowed_c"));

        flsRule = subject.getFlsRule(context, "index_abcd", Meter.NO_OP);

        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_a"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_b"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_c"));
    }
    
    @Test
    public void getFlsRule_templateAndNegation() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration.of(CType.ROLES, "role",
                Role.parse(DocNode.of("index_permissions", DocNode.array(DocNode.of("index_patterns",
                        DocNode.array("index_${user.attrs.a}*", "-index_abcd"), "fls", DocNode.array("allowed_a", "allowed_b")))), null).get());

        RoleBasedFieldAuthorization subject = new RoleBasedFieldAuthorization(roleConfig, ImmutableSet.of("index_abc", "index_abcd"),
                MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(new User.Builder().name("test_user").attribute("a", "abc").build(),
                ImmutableSet.of("role"), null, subject, false, null, null);

        FlsRule flsRule = subject.getFlsRule(context, "index_abc", Meter.NO_OP);

        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_a"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_b"));
        Assert.assertFalse(flsRule.toString(), flsRule.isAllowed("allowed_c"));

        flsRule = subject.getFlsRule(context, "index_abcd", Meter.NO_OP);

        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_a"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_b"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_c"));
    }

    
    @Test
    public void getFlsRule_wildcardRule() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration
                .of(CType.ROLES, "role_with_wildcard_fls",
                        Role.parse(
                                DocNode.of("index_permissions",
                                        DocNode.array(
                                                DocNode.of("index_patterns", "*", "fls", DocNode.array("wildcard_allowed_a", "wildcard_allowed_b")))),
                                null).get(),
                        "role_without_wildcard_fls",
                        Role.parse(
                                DocNode.of("index_permissions",
                                        DocNode.array(DocNode.of("index_patterns", "another_index", "fls", DocNode.array("allowed_x", "allowed_y")))),
                                null).get());

        RoleBasedFieldAuthorization subject = new RoleBasedFieldAuthorization(roleConfig, ImmutableSet.of("one_index", "another_index"),
                MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(),
                ImmutableSet.of("role_with_wildcard_fls"), null, subject, false, null, null);

        FlsRule flsRule = subject.getFlsRule(context, "one_index", Meter.NO_OP);

        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("wildcard_allowed_a"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("wildcard_allowed_b"));
        Assert.assertFalse(flsRule.toString(), flsRule.isAllowed("allowed_x"));

        flsRule = subject.getFlsRule(context, "another_index", Meter.NO_OP);

        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("wildcard_allowed_a"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("wildcard_allowed_b"));
        Assert.assertFalse(flsRule.toString(), flsRule.isAllowed("allowed_x"));

        context = new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(), ImmutableSet.of("role_without_wildcard_fls"), null,
                subject, false, null, null);

        flsRule = subject.getFlsRule(context, "one_index", Meter.NO_OP);

        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("wildcard_allowed_a"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("wildcard_allowed_b"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_x"));

        flsRule = subject.getFlsRule(context, "another_index", Meter.NO_OP);

        Assert.assertFalse(flsRule.toString(), flsRule.isAllowed("wildcard_allowed_a"));
        Assert.assertFalse(flsRule.toString(), flsRule.isAllowed("wildcard_allowed_b"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_x"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_y"));

    }

    @Test
    public void getFlsRule_multiRule() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration.of(CType.ROLES, "role_a",
                Role.parse(DocNode.of("index_permissions",
                        DocNode.array(DocNode.of("index_patterns", "one_index", "fls", DocNode.array("allowed_a", "allowed_b")))), null).get(),
                "role_b",
                Role.parse(
                        DocNode.of("index_permissions", DocNode.array(DocNode.of("index_patterns", "one_index", "fls", DocNode.array("allowed_c")))),
                        null).get());

        RoleBasedFieldAuthorization subject = new RoleBasedFieldAuthorization(roleConfig, ImmutableSet.of("one_index", "another_index"),
                MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(),
                ImmutableSet.of("role_a", "role_b"), null, subject, false, null, null);

        FlsRule flsRule = subject.getFlsRule(context, "one_index", Meter.NO_OP);

        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_a"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_b"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("allowed_c"));
        Assert.assertFalse(flsRule.toString(), flsRule.isAllowed("allowed_d"));
    }

    @Test
    public void getFlsRule_multiRule_exclusion() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration.of(CType.ROLES, "role_a",
                Role.parse(DocNode.of("index_permissions",
                        DocNode.array(DocNode.of("index_patterns", "one_index", "fls", DocNode.array("~denied_a", "~denied_b")))), null).get(),
                "role_b", Role
                        .parse(DocNode.of("index_permissions",
                                DocNode.array(DocNode.of("index_patterns", "one_index", "fls", DocNode.array("~denied_b", "~denied_c")))), null)
                        .get());

        RoleBasedFieldAuthorization subject = new RoleBasedFieldAuthorization(roleConfig, ImmutableSet.of("one_index", "another_index"),
                MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(),
                ImmutableSet.of("role_a", "role_b"), null, subject, false, null, null);

        FlsRule flsRule = subject.getFlsRule(context, "one_index", Meter.NO_OP);

        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("denied_a"));
        Assert.assertFalse(flsRule.toString(), flsRule.isAllowed("denied_b"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("denied_c"));

        context = new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(), ImmutableSet.of("role_a"), null, subject, false, null,
                null);

        flsRule = subject.getFlsRule(context, "one_index", Meter.NO_OP);

        Assert.assertFalse(flsRule.toString(), flsRule.isAllowed("denied_a"));
        Assert.assertFalse(flsRule.toString(), flsRule.isAllowed("denied_b"));
        Assert.assertTrue(flsRule.toString(), flsRule.isAllowed("denied_c"));

    }

    @Test
    public void hasFlsRestriction_template() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration.of(CType.ROLES, "role", Role
                .parse(DocNode.of("index_permissions",
                        DocNode.array(DocNode.of("index_patterns", "index_${user.attrs.a}", "fls", DocNode.array("allowed_a", "allowed_b")))), null)
                .get());

        RoleBasedFieldAuthorization subject = new RoleBasedFieldAuthorization(roleConfig, ImmutableSet.of("index_value_of_a", "another_index"),
                MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(
                new User.Builder().name("test_user").attribute("a", "value_of_a").build(), ImmutableSet.of("role"), null, subject, false, null, null);

        Assert.assertTrue(subject.toString(), subject.hasFlsRestrictions(context, "index_value_of_a", Meter.NO_OP));
        Assert.assertFalse(subject.toString(), subject.hasFlsRestrictions(context, "another_index", Meter.NO_OP));
    }

    @Test
    public void hasFlsRestriction_templateAndNegation() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration
                .of(CType.ROLES, "role", Role
                        .parse(DocNode.of("index_permissions", DocNode.array(DocNode.of("index_patterns",
                                DocNode.array("index_${user.attrs.a}*", "-index_abcd"), "fls", DocNode.array("allowed_a", "allowed_b")))), null)
                        .get());

        RoleBasedFieldAuthorization subject = new RoleBasedFieldAuthorization(roleConfig, ImmutableSet.of("index_abc", "index_abcd"),
                MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(
                new User.Builder().name("test_user").attribute("a", "abc").build(), ImmutableSet.of("role"), null, subject, false, null, null);

        Assert.assertTrue(subject.toString(), subject.hasFlsRestrictions(context, "index_abc", Meter.NO_OP));
        Assert.assertFalse(subject.toString(), subject.hasFlsRestrictions(context, "index_abcd", Meter.NO_OP));
    }
    
    @Test
    public void hasFlsRestriction_negation() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration
                .of(CType.ROLES, "role", Role
                        .parse(DocNode.of("index_permissions", DocNode.array(DocNode.of("index_patterns",
                                DocNode.array("index_abc*", "-index_abcd"), "fls", DocNode.array("allowed_a", "allowed_b")))), null)
                        .get());

        RoleBasedFieldAuthorization subject = new RoleBasedFieldAuthorization(roleConfig, ImmutableSet.of("index_abc", "index_abcd"),
                MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(
                new User.Builder().name("test_user").build(), ImmutableSet.of("role"), null, subject, false, null, null);

        Assert.assertTrue(subject.toString(), subject.hasFlsRestrictions(context, "index_abc", Meter.NO_OP));
        Assert.assertFalse(subject.toString(), subject.hasFlsRestrictions(context, "index_abcd", Meter.NO_OP));
    }


    @Test
    public void hasFlsRestriction_wildcardRule() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration
                .of(CType.ROLES, "role_with_wildcard_fls",
                        Role.parse(
                                DocNode.of("index_permissions",
                                        DocNode.array(
                                                DocNode.of("index_patterns", "*", "fls", DocNode.array("wildcard_allowed_a", "wildcard_allowed_b")))),
                                null).get(),
                        "role_without_wildcard_fls",
                        Role.parse(
                                DocNode.of("index_permissions",
                                        DocNode.array(DocNode.of("index_patterns", "another_index", "fls", DocNode.array("allowed_x", "allowed_y")))),
                                null).get());

        RoleBasedFieldAuthorization subject = new RoleBasedFieldAuthorization(roleConfig, ImmutableSet.of("one_index", "another_index"),
                MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(),
                ImmutableSet.of("role_with_wildcard_fls"), null, subject, false, null, null);

        Assert.assertTrue(subject.toString(), subject.hasFlsRestrictions(context, "one_index", Meter.NO_OP));
        Assert.assertTrue(subject.toString(), subject.hasFlsRestrictions(context, "another_index", Meter.NO_OP));

        context = new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(), ImmutableSet.of("role_without_wildcard_fls"), null,
                subject, false, null, null);

        Assert.assertFalse(subject.toString(), subject.hasFlsRestrictions(context, "one_index", Meter.NO_OP));
        Assert.assertTrue(subject.toString(), subject.hasFlsRestrictions(context, "another_index", Meter.NO_OP));

    }
    */
}
