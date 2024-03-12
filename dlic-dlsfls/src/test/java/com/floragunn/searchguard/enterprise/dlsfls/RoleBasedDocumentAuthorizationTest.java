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

import static com.floragunn.searchsupport.meta.Meta.Mock.dataStream;
import static com.floragunn.searchsupport.meta.Meta.Mock.indices;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.query.BaseTermQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Suite;

import com.floragunn.codova.config.templates.ExpressionEvaluationException;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.meta.Meta;

@RunWith(Suite.class)
@Suite.SuiteClasses({ RoleBasedDocumentAuthorizationTest.IndicesAndAliases.class,  RoleBasedDocumentAuthorizationTest.DataStreams.class })
public class RoleBasedDocumentAuthorizationTest {

    static NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
            ImmutableList.of(new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField(TermQueryBuilder.NAME),
                    (CheckedFunction<XContentParser, TermQueryBuilder, IOException>) (p) -> TermQueryBuilder.fromXContent(p))));
    static Parser.Context parserContext = new ConfigurationRepository.Context(null, null, null, xContentRegistry, null);

    @RunWith(Parameterized.class)
    public static class IndicesAndAliases {
        final static Meta BASIC = indices("index_a1", "index_a2", "index_b1", "index_b2")//
                .alias("alias_a").of("index_a1", "index_a2");

        final static Meta.Index index_a1 = (Meta.Index) BASIC.getIndexOrLike("index_a1");
        final static Meta.Index index_a2 = (Meta.Index) BASIC.getIndexOrLike("index_a2");
        final static Meta.Index index_b1 = (Meta.Index) BASIC.getIndexOrLike("index_b1");

        final UserSpec userSpec;
        final User user;
        final IndexSpec indexSpec;
        final Meta.Index index;
        final PrivilegesEvaluationContext context;

        @Test
        public void getDlsRestriction_wildcard() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("*"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("*"),
                    new TestSgConfig.Role("non_dls_role").indexPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, BASIC, MetricsLevel.NONE);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
            } else if (userSpec.roles.contains("dls_role_1")) {
                assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
            } else if (userSpec.roles.contains("dls_role_2")) {
                assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
            }
            // TODO no roles
        }

        @Test
        public void getDlsRestriction_wildcard_negation() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("*", "-index_b*"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("*", "-index_a*"),
                    new TestSgConfig.Role("non_dls_role").indexPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, BASIC, MetricsLevel.NONE);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (index == index_a1 || index == index_a2) {
                if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == index_b1) {
                if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            }
        }

        @Test
        public void getDlsRestriction_indexPattern() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("index_a*"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_b*"),
                    new TestSgConfig.Role("non_dls_role").indexPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, BASIC, MetricsLevel.NONE);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (index == index_a1 || index == index_a2) {
                if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == index_b1) {
                if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            }
            // TODO no roles
        }

        @Test
        public void getDlsRestriction_indexPattern_negation() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("index_*",
                            "-index_b*"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_*",
                            "-index_a*"),
                    new TestSgConfig.Role("non_dls_role").indexPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, BASIC, MetricsLevel.NONE);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (index == index_a1 || index == index_a2) {
                if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == index_b1) {
                if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            }
            // TODO no roles
        }

        @Test
        public void getDlsRestriction_template() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1"))
                            .on("index_${user.attrs.attr_a}"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_a*"),
                    new TestSgConfig.Role("non_dls_role").indexPermissions("*").on("index_${user.attrs.attr_a}"));

            RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, BASIC, MetricsLevel.NONE);

            try {
                DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

                if (userSpec.roles.contains("non_dls_role")) {
                    assertThat(dlsRestriction, isUnrestricted());
                } else if (index == index_a1) {
                    if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
                    } else if (userSpec.roles.contains("dls_role_1")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                    } else {
                        assertThat(dlsRestriction, isUnrestricted());
                    }
                } else if (index == index_a2) {
                    if (userSpec.roles.contains("dls_role_2")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                    } else {
                        assertThat(dlsRestriction, isUnrestricted());
                    }
                } else if (index == index_b1) {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } catch (PrivilegesEvaluationException e) {
                if ((userSpec.roles.contains("non_dls_role") || userSpec.roles.contains("dls_role_1"))
                        && !userSpec.attributes.containsKey("attr_a")) {
                    assertThat(e.getCause(), is(instanceOf((ExpressionEvaluationException.class))));
                } else {
                    fail("Unexpected exception: " + e);
                }
            }
        }

        @Test
        public void getDlsRestriction_alias_static() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").aliasPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("alias_a"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_a2"),
                    new TestSgConfig.Role("non_dls_role").aliasPermissions("*").on("alias_a"));
            RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, BASIC, MetricsLevel.NONE);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (index == index_a1) {
                if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == index_a2) {
                if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
                } else if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == index_b1) {
                assertThat(dlsRestriction, isUnrestricted());
            }
            // TODO no roles
        }

        @Test
        public void getDlsRestriction_alias_static_wildcardNonDls() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").aliasPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("alias_a"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_a2"),
                    new TestSgConfig.Role("non_dls_role").aliasPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, BASIC, MetricsLevel.NONE);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (index == index_a1) {
                if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == index_a2) {
                if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
                } else if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == index_b1) {
                assertThat(dlsRestriction, isUnrestricted());
            }
            // TODO no roles
        }

        @Test
        public void getDlsRestriction_alias_wildcard() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").aliasPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("alias_a*"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_a2"),
                    new TestSgConfig.Role("non_dls_role").aliasPermissions("*").on("alias_a*"));
            RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, BASIC, MetricsLevel.NONE);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (index == index_a1) {
                if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == index_a2) {
                if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
                } else if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == index_b1) {
                assertThat(dlsRestriction, isUnrestricted());
            }
            // TODO no roles
        }

        @Parameters(name = "{0}; {1}")
        public static Collection<Object[]> params() {
            List<Object[]> result = new ArrayList<>();

            for (UserSpec userSpec : Arrays.asList(//
                    new UserSpec("non_dls_role", "non_dls_role"), //
                    new UserSpec("dls_role_1", "dls_role_1"), //
                    new UserSpec("dls_role_1 and dls_role_2", "dls_role_1", "dls_role_2"), //
                    new UserSpec("dls_role_1 and non_dls_role", "dls_role_1", "non_dls_role"), //
                    new UserSpec("non_dls_role, attributes", "non_dls_role").attribute("attr_a", "a1"), //
                    new UserSpec("dls_role_1, attributes", "dls_role_1").attribute("attr_a", "a1"), //
                    new UserSpec("dls_role_1 and dls_role_2, attributes", "dls_role_1", "dls_role_2").attribute("attr_a", "a1"), //
                    new UserSpec("dls_role_1 and non_dls_role, attributes", "dls_role", "non_dls_role").attribute("attr_a", "a1"), //
                    new UserSpec("no roles")//
            )) {
                for (IndexSpec indexSpec : Arrays.asList(//
                        new IndexSpec("index_a1"), //
                        new IndexSpec("index_a2"), //
                        new IndexSpec("index_b1"))) {
                    result.add(new Object[] { userSpec, indexSpec });
                }
            }
            return result;
        }

        public IndicesAndAliases(UserSpec userSpec, IndexSpec indexSpec) {
            this.userSpec = userSpec;
            this.indexSpec = indexSpec;
            this.user = userSpec.buildUser();
            this.index = (Meta.Index) BASIC.getIndexOrLike(indexSpec.index);
            this.context = new PrivilegesEvaluationContext(this.user, ImmutableSet.of(userSpec.roles), null, null, true, null, null);
        }

    }
    
    @RunWith(Parameterized.class)
    public static class DataStreams {
        final static Meta BASIC = dataStream("datastream_a1").of(".ds-datastream_a1_backing_0001", ".ds-datastream_a1_backing_0002")//
                .dataStream("datastream_a2").of(".ds-datastream_a2_backing_0001", ".ds-datastream_a2_backing_0002")//
                .dataStream("datastream_b1").of(".ds-datastream_b1_backing_0001", ".ds-datastream_b1_backing_0002")//
                .dataStream("datastream_b2").of(".ds-datastream_b2_backing_0001", ".ds-datastream_b2_backing_0002")//
                .alias("alias_a").of("datastream_a1", "datastream_a2");

        final static Meta.Index datastream_a1_backing = (Meta.Index) BASIC.getIndexOrLike(".ds-datastream_a1_backing_0001");
        final static Meta.Index datastream_a2_backing = (Meta.Index) BASIC.getIndexOrLike(".ds-datastream_a2_backing_0001");
        final static Meta.Index datastream_b1_backing = (Meta.Index) BASIC.getIndexOrLike(".ds-datastream_b1_backing_0001");

        final UserSpec userSpec;
        final User user;
        final IndexSpec indexSpec;
        final Meta.Index index;
        final PrivilegesEvaluationContext context;

        @Test
        public void getDlsRestriction_wildcard() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("*"),
                    new TestSgConfig.Role("dls_role_2").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("*"),
                    new TestSgConfig.Role("non_dls_role").dataStreamPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, BASIC, MetricsLevel.NONE);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
            } else if (userSpec.roles.contains("dls_role_1")) {
                assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
            } else if (userSpec.roles.contains("dls_role_2")) {
                assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
            }
            // TODO no roles
        }
        
        @Test
        public void getDlsRestriction_wildcard_negation() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("*", "-datastream_b*"),
                    new TestSgConfig.Role("dls_role_2").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("*", "-datastream_a*"),
                    new TestSgConfig.Role("non_dls_role").dataStreamPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, BASIC, MetricsLevel.NONE);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (index == datastream_a1_backing || index == datastream_a2_backing) {
                if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == datastream_b1_backing) {
                if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            }
            // TODO no roles
        }
        
        @Test
        public void getDlsRestriction_indexPattern() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("datastream_a*"),
                    new TestSgConfig.Role("dls_role_2").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("datastream_b*"),
                    new TestSgConfig.Role("non_dls_role").dataStreamPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, BASIC, MetricsLevel.NONE);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (index == datastream_a1_backing || index == datastream_a2_backing) {
                if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == datastream_b1_backing) {
                if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            }
            // TODO no roles
        }
                
        @Test
        public void getDlsRestriction_indexPattern_negation() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("datastream_*", "-datastream_b*"),
                    new TestSgConfig.Role("dls_role_2").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("datastream_*", "-datastream_a*"),
                    new TestSgConfig.Role("non_dls_role").dataStreamPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, BASIC, MetricsLevel.NONE);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (index == datastream_a1_backing || index == datastream_a2_backing) {
                if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == datastream_b1_backing) {
                if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            }
            // TODO no roles
        }
        
        @Test
        public void getDlsRestriction_template() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1"))
                            .on("datastream_${user.attrs.attr_a}"),
                    new TestSgConfig.Role("dls_role_2").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("datastream_a*"),
                    new TestSgConfig.Role("non_dls_role").dataStreamPermissions("*").on("datastream_${user.attrs.attr_a}"));

            RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, BASIC, MetricsLevel.NONE);

            try {
                DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

                if (userSpec.roles.contains("non_dls_role")) {
                    assertThat(dlsRestriction, isUnrestricted());
                } else if (index == datastream_a1_backing) {
                    if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
                    } else if (userSpec.roles.contains("dls_role_1")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                    } else {
                        assertThat(dlsRestriction, isUnrestricted());
                    }
                } else if (index == datastream_a2_backing) {
                    if (userSpec.roles.contains("dls_role_2")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                    } else {
                        assertThat(dlsRestriction, isUnrestricted());
                    }
                } else if (index == datastream_b1_backing) {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } catch (PrivilegesEvaluationException e) {
                if ((userSpec.roles.contains("non_dls_role") || userSpec.roles.contains("dls_role_1"))
                        && !userSpec.attributes.containsKey("attr_a")) {
                    assertThat(e.getCause(), is(instanceOf((ExpressionEvaluationException.class))));
                } else {
                    fail("Unexpected exception: " + e);
                }
            }
        }


        @Test
        public void getDlsRestriction_alias_static() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").aliasPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("alias_a"),
                    new TestSgConfig.Role("dls_role_2").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("datastream_a2"),
                    new TestSgConfig.Role("non_dls_role").aliasPermissions("*").on("alias_a"));
            RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, BASIC, MetricsLevel.NONE);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (index == datastream_a1_backing) {
                if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == datastream_a2_backing) {
                if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
                } else if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == datastream_b1_backing) {
                assertThat(dlsRestriction, isUnrestricted());
            }
            // TODO no roles
        }
        
        @Test
        public void getDlsRestriction_wildcardOnIndices() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("*"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("*"),
                    new TestSgConfig.Role("non_dls_role").indexPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, BASIC, MetricsLevel.NONE);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
            } else if (userSpec.roles.contains("dls_role_1")) {
                assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
            } else if (userSpec.roles.contains("dls_role_2")) {
                assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
            }
            // TODO no roles
        }


        @Parameters(name = "{0}; {1}")
        public static Collection<Object[]> params() {
            List<Object[]> result = new ArrayList<>();

            for (UserSpec userSpec : Arrays.asList(//
                    new UserSpec("non_dls_role", "non_dls_role"), //
                    new UserSpec("dls_role_1", "dls_role_1"), //
                    new UserSpec("dls_role_1 and dls_role_2", "dls_role_1", "dls_role_2"), //
                    new UserSpec("dls_role_1 and non_dls_role", "dls_role_1", "non_dls_role"), //
                    new UserSpec("non_dls_role, attributes", "non_dls_role").attribute("attr_a", "a1"), //
                    new UserSpec("dls_role_1, attributes", "dls_role_1").attribute("attr_a", "a1"), //
                    new UserSpec("dls_role_1 and dls_role_2, attributes", "dls_role_1", "dls_role_2").attribute("attr_a", "a1"), //
                    new UserSpec("dls_role_1 and non_dls_role, attributes", "dls_role", "non_dls_role").attribute("attr_a", "a1"), //
                    new UserSpec("no roles")//
            )) {
                for (IndexSpec indexSpec : Arrays.asList(//
                        new IndexSpec(datastream_a1_backing.name()), //
                        new IndexSpec(datastream_a2_backing.name()), //
                        new IndexSpec(datastream_b1_backing.name()))) {
                    result.add(new Object[] { userSpec, indexSpec });
                }
            }
            return result;
        }

        public DataStreams(UserSpec userSpec, IndexSpec indexSpec) {
            this.userSpec = userSpec;
            this.indexSpec = indexSpec;
            this.user = userSpec.buildUser();
            this.index = (Meta.Index) BASIC.getIndexOrLike(indexSpec.index);
            this.context = new PrivilegesEvaluationContext(this.user, ImmutableSet.of(userSpec.roles), null, null, true, null, null);
        }

    }

    static SgDynamicConfiguration<Role> roleConfig(TestSgConfig.Role... roles) throws ConfigValidationException {
        Map<String, Role> parsedRoles = new HashMap<>();

        for (TestSgConfig.Role role : roles) {
            parsedRoles.put(role.getName(), Role.parse(DocNode.wrap(role.toDeepBasicObject()), parserContext).get());
        }

        return SgDynamicConfiguration.of(CType.ROLES, parsedRoles);
    }

    public static class UserSpec {
        final List<String> roles;
        final String description;
        final Map<String, Object> attributes = new HashMap<>();

        UserSpec(String description, String... roles) {
            this.description = description;
            this.roles = Arrays.asList(roles);
        }

        UserSpec attribute(String name, Object value) {
            this.attributes.put(name, value);
            return this;
        }

        User buildUser() {
            return new User.Builder().name("test_user_" + description).attributes(this.attributes).build();
        }

        @Override
        public String toString() {
            return this.description;
        }
    }

    public static class IndexSpec {
        final String index;

        IndexSpec(String index) {
            this.index = index;
        }

        @Override
        public String toString() {
            return this.index;
        }
    }

    static DiagnosingMatcher<DlsRestriction> isUnrestricted() {
        return new DiagnosingMatcher<DlsRestriction>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("A DlsRestriction object that has no restrictions");
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof DlsRestriction)) {
                    mismatchDescription.appendValue(item).appendText(" is not a DlsRestriction object");
                    return false;
                }

                DlsRestriction dlsRestriction = (DlsRestriction) item;

                if (dlsRestriction.isUnrestricted()) {
                    return true;
                } else {
                    mismatchDescription.appendText("The DlsRestriction object is not unrestricted:").appendValue(dlsRestriction);
                    return false;
                }
            }

        };

    }

    static DiagnosingMatcher<DlsRestriction> isRestricted() {
        return new DiagnosingMatcher<DlsRestriction>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("A DlsRestriction object that has at least one restrictions");
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof DlsRestriction)) {
                    mismatchDescription.appendValue(item).appendText(" is not a DlsRestriction object");
                    return false;
                }

                DlsRestriction dlsRestriction = (DlsRestriction) item;

                if (!dlsRestriction.isUnrestricted()) {
                    return true;
                } else {
                    mismatchDescription.appendText("The DlsRestriction object is not restricted:").appendValue(dlsRestriction);
                    return false;
                }
            }

        };

    }

    @SafeVarargs
    static DiagnosingMatcher<DlsRestriction> isRestricted(Matcher<QueryBuilder>... queries) {
        return new DiagnosingMatcher<DlsRestriction>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("A DlsRestriction object that has the restrictions: ").appendList("", "", ", ", Arrays.asList(queries));
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof DlsRestriction)) {
                    mismatchDescription.appendValue(item).appendText(" is not a DlsRestriction object");
                    return false;
                }

                DlsRestriction dlsRestriction = (DlsRestriction) item;

                if (dlsRestriction.isUnrestricted()) {
                    mismatchDescription.appendText("The DlsRestriction object is not restricted:").appendValue(dlsRestriction);
                    return false;

                }

                Set<Matcher<QueryBuilder>> subMatchers = new HashSet<>(Arrays.asList(queries));
                Set<com.floragunn.searchsupport.queries.Query> unmatchedQueries = new HashSet<>(dlsRestriction.getQueries());

                for (com.floragunn.searchsupport.queries.Query query : dlsRestriction.getQueries()) {
                    for (Matcher<QueryBuilder> subMatcher : subMatchers) {
                        if (subMatcher.matches(query.getQueryBuilder())) {
                            unmatchedQueries.remove(query);
                            subMatchers.remove(subMatcher);
                            break;
                        }
                    }
                }

                if (unmatchedQueries.isEmpty() && subMatchers.isEmpty()) {
                    return true;
                }

                if (!unmatchedQueries.isEmpty()) {
                    mismatchDescription.appendText("The DlsRestriction contains unexpected queries:").appendValue(unmatchedQueries).appendText("\n");
                }

                if (!subMatchers.isEmpty()) {
                    mismatchDescription.appendText("The DlsRestriction does not contain expected queries: ").appendValue(subMatchers)
                            .appendText("\n");
                }

                return false;
            }

        };
    }

    static BaseMatcher<QueryBuilder> termQuery(String field, Object value) {
        return new BaseMatcher<QueryBuilder>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("A TermQueryBuilder object with ").appendValue(field).appendText("=").appendValue(value);
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BaseTermQueryBuilder)) {
                    return false;
                }

                BaseTermQueryBuilder<?> queryBuilder = (BaseTermQueryBuilder<?>) item;

                if (queryBuilder.fieldName().equals(field) && queryBuilder.value().equals(value)) {
                    return true;
                } else {
                    return false;
                }
            }

        };

    }
}
