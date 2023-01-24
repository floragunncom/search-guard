/*
  * Copyright 2022 by floragunn GmbH - All rights reserved
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
import com.floragunn.codova.documents.Parser;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import java.io.IOException;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Assert;
import org.junit.Test;

public class RoleBasedDocumentAuthorizationTest {

    static NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
            ImmutableList.of(new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField(TermQueryBuilder.NAME),
                    (CheckedFunction<XContentParser, TermQueryBuilder, IOException>) (p) -> TermQueryBuilder.fromXContent(p))));
    static Parser.Context context = new ConfigurationRepository.Context(null, null, null, xContentRegistry);

    @Test
    public void getDlsRestriction_template() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration.of(CType.ROLES, "role",
                Role.parse(DocNode.of("index_permissions", DocNode
                        .array(DocNode.of("index_patterns", "index_${user.attrs.a}", "dls", DocNode.of("term.dept.value", "dept_d").toJsonString()))),
                        context).get());

        RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, ImmutableSet.of("index_value_of_a", "another_index"),
                MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(
                new User.Builder().name("test_user").attribute("a", "value_of_a").build(), ImmutableSet.of("role"), null, subject, false, null, null);

        DlsRestriction dlsRestriction = subject.getDlsRestriction(context, "index_value_of_a", Meter.NO_OP);
        Assert.assertTrue(dlsRestriction.toString(), dlsRestriction.getQueries().size() == 1);

        dlsRestriction = subject.getDlsRestriction(context, "another_index", Meter.NO_OP);
        Assert.assertTrue(dlsRestriction.toString(), dlsRestriction.getQueries().size() == 0);
    }

    @Test
    public void getDlsRestriction_wildcardRule() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration
                .of(CType.ROLES, "role_with_wildcard_dls",
                        Role.parse(
                                DocNode.of("index_permissions",
                                        DocNode.array(
                                                DocNode.of("index_patterns", "*", "dls", DocNode.of("term.dept.value", "dept_d").toJsonString()))),
                                context).get(),
                        "role_without_wildcard_dls",
                        Role.parse(DocNode.of("index_permissions", DocNode
                                .array(DocNode.of("index_patterns", "another_index", "dls", DocNode.of("term.dept.value", "dept_d").toJsonString()))),
                                context).get());

        RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, ImmutableSet.of("one_index", "another_index"),
                MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(),
                ImmutableSet.of("role_with_wildcard_dls"), null, subject, false, null, null);

        DlsRestriction dlsRestriction = subject.getDlsRestriction(context, "one_index", Meter.NO_OP);
        Assert.assertTrue(dlsRestriction.toString(), dlsRestriction.getQueries().size() == 1);

        dlsRestriction = subject.getDlsRestriction(context, "another_index", Meter.NO_OP);
        Assert.assertTrue(dlsRestriction.toString(), dlsRestriction.getQueries().size() == 1);

        context = new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(), ImmutableSet.of("role_without_wildcard_dls"), null,
                subject, false, null, null);

        dlsRestriction = subject.getDlsRestriction(context, "one_index", Meter.NO_OP);
        Assert.assertTrue(dlsRestriction.toString(), dlsRestriction.getQueries().size() == 0);

        dlsRestriction = subject.getDlsRestriction(context, "another_index", Meter.NO_OP);
        Assert.assertTrue(dlsRestriction.toString(), dlsRestriction.getQueries().size() == 1);

    }

    @Test
    public void getDlsRestriction_wildcardWithoutQuery() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration.of(CType.ROLES, "role_with_dls", Role.parse(
                DocNode.of("index_permissions",
                        DocNode.array(
                                DocNode.of("index_patterns", "protected_index", "dls", DocNode.of("term.dept.value", "dept_d").toJsonString()))),
                context).get(), "role_without_dls",
                Role.parse(DocNode.of("index_permissions", DocNode.array(DocNode.of("index_patterns", "*"))), context).get());

        RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, ImmutableSet.of("protected_index", "another_index"),
                MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(),
                ImmutableSet.of("role_with_dls"), null, subject, false, null, null);

        DlsRestriction dlsRestriction = subject.getDlsRestriction(context, "protected_index", Meter.NO_OP);
        Assert.assertTrue(dlsRestriction.toString(), dlsRestriction.getQueries().size() == 1);

        context = new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(), ImmutableSet.of("role_with_dls", "role_without_dls"),
                null, subject, false, null, null);

        dlsRestriction = subject.getDlsRestriction(context, "protected_index", Meter.NO_OP);
        Assert.assertEquals(dlsRestriction.toString(), 0, dlsRestriction.getQueries().size());

    }

    @Test
    public void hasDlsRestriction_template() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration.of(CType.ROLES, "role",
                Role.parse(DocNode.of("index_permissions", DocNode
                        .array(DocNode.of("index_patterns", "index_${user.attrs.a}", "dls", DocNode.of("term.dept.value", "dept_d").toJsonString()))),
                        context).get());

        RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, ImmutableSet.of("index_value_of_a", "another_index"),
                MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(
                new User.Builder().name("test_user").attribute("a", "value_of_a").build(), ImmutableSet.of("role"), null, subject, false, null, null);

        Assert.assertTrue(subject.toString(), subject.hasDlsRestrictions(context, ImmutableList.of("index_value_of_a"), Meter.NO_OP));
        Assert.assertFalse(subject.toString(), subject.hasDlsRestrictions(context, ImmutableList.of("another_index"), Meter.NO_OP));
    }

    @Test
    public void hasDlsRestriction_wildcardRule() throws Exception {
        SgDynamicConfiguration<Role> roleConfig = SgDynamicConfiguration
                .of(CType.ROLES, "role_with_wildcard_dls",
                        Role.parse(
                                DocNode.of("index_permissions",
                                        DocNode.array(
                                                DocNode.of("index_patterns", "*", "dls", DocNode.of("term.dept.value", "dept_d").toJsonString()))),
                                context).get(),
                        "role_without_wildcard_dls",
                        Role.parse(DocNode.of("index_permissions", DocNode
                                .array(DocNode.of("index_patterns", "another_index", "dls", DocNode.of("term.dept.value", "dept_d").toJsonString()))),
                                context).get());

        RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, ImmutableSet.of("one_index", "another_index"),
                MetricsLevel.NONE);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(),
                ImmutableSet.of("role_with_wildcard_dls"), null, subject, false, null, null);

        Assert.assertTrue(subject.toString(), subject.hasDlsRestrictions(context, ImmutableList.of("one_index"), Meter.NO_OP));
        Assert.assertTrue(subject.toString(), subject.hasDlsRestrictions(context, ImmutableList.of("another_index"), Meter.NO_OP));

        context = new PrivilegesEvaluationContext(new User.Builder().name("test_user").build(), ImmutableSet.of("role_without_wildcard_dls"), null,
                subject, false, null, null);

        Assert.assertFalse(subject.toString(), subject.hasDlsRestrictions(context, ImmutableList.of("one_index"), Meter.NO_OP));
        Assert.assertTrue(subject.toString(), subject.hasDlsRestrictions(context, ImmutableList.of("another_index"), Meter.NO_OP));

    }
}
