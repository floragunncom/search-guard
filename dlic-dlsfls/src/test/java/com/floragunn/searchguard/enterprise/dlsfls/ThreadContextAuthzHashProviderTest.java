/*
 * Copyright 2024 by floragunn GmbH - All rights reserved
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

import java.io.IOException;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.StaticSettings;

public class ThreadContextAuthzHashProviderTest {

    final static StaticSettings ENABLED_SETTINGS = StaticSettings.ofBoolean(DlsFlsModule.PROVIDE_THREAD_CONTEXT_AUTHZ_HASH, true);
    final static User TEST_USER = User.forUser("test_user").attribute("x", 1).build();
    final static NamedXContentRegistry X_CONTENT_REGISTRY = new NamedXContentRegistry(
            ImmutableList.of(new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField(TermQueryBuilder.NAME),
                    (CheckedFunction<XContentParser, TermQueryBuilder, IOException>) (p) -> TermQueryBuilder.fromXContent(p))));
    final static ConfigurationRepository.Context PARSER_CONTEXT = new ConfigurationRepository.Context(null, null, null, X_CONTENT_REGISTRY, null);

    @Test
    public void restrictionsInfo_basic() throws Exception {
        ThreadContextAuthzHashProvider subject = new ThreadContextAuthzHashProvider(ENABLED_SETTINGS, new ThreadContext(Settings.EMPTY));
        PrivilegesEvaluationContext ctx = ctx(TEST_USER, "role_1");
        SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                "role_1:\n" + //
                        "  index_permissions:\n" + //
                        "  - index_patterns: ['protected_index_*']\n" + //
                        "    dls: '{\"term\" : {\"department\" : 1}}}'\n" + //
                        "  - index_patterns: ['unprotected_index_*']\n"),
                CType.ROLES, PARSER_CONTEXT).get();

        String info = subject.restrictionsInfo(ctx, roles);

        Assert.assertEquals("[unprotected_index_*]::{protected_index_*=[dls:{\"term\" : {\"department\" : 1}}}, fls: [], fm: []]}", info);
    }

    @Test
    public void restrictionsInfo_indexPermissionTemplate() throws Exception {
        ThreadContextAuthzHashProvider subject = new ThreadContextAuthzHashProvider(ENABLED_SETTINGS, new ThreadContext(Settings.EMPTY));
        PrivilegesEvaluationContext ctx = ctx(TEST_USER, "role_1");
        SgDynamicConfiguration<Role> roles = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                "role_1:\n" + //
                        "  index_permissions:\n" + //
                        "  - index_patterns: ['protected_index_${user.attr.x}']\n" + //
                        "    dls: '{\"term\" : {\"department\" : 1}}}'\n" + //
                        "  - index_patterns: ['unprotected_index_*']\n"),
                CType.ROLES, PARSER_CONTEXT).get();

        String info = subject.restrictionsInfo(ctx, roles);

        Assert.assertEquals("test_user::{\"x\":1}::[role_1]", info);
    }

    private static PrivilegesEvaluationContext ctx(User user, String... roles) {
        return new PrivilegesEvaluationContext(user, false, ImmutableSet.ofArray(roles), null, roles, true, null, null);
    }

}
