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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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

import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchsupport.util.EsLogging;
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
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.actions.ResolvedIndices;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.meta.Meta;

@RunWith(Suite.class)
@Suite.SuiteClasses({ RoleBasedDocumentAuthorizationTest.IndicesAndAliases_getRestriction.class,
        RoleBasedDocumentAuthorizationTest.IndicesAndAliases_hasRestriction.class,
        RoleBasedDocumentAuthorizationTest.DataStreams_getRestriction.class })
public class RoleBasedDocumentAuthorizationTest {

    static NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
            ImmutableList.of(new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField(TermQueryBuilder.NAME),
                    (CheckedFunction<XContentParser, TermQueryBuilder, IOException>) (p) -> TermQueryBuilder.fromXContent(p))));
    static ConfigurationRepository.Context parserContext = new ConfigurationRepository.Context(null, null, null, xContentRegistry, null, Actions.forTests());

    @RunWith(Parameterized.class)
    public static class IndicesAndAliases_getRestriction {

        final static Meta BASIC = indices("index_a1", "index_a2", "index_b1", "index_b2")//
                .alias("alias_a").of("index_a1", "index_a2");

        final static Meta.Index index_a1 = (Meta.Index) BASIC.getIndexOrLike("index_a1");
        final static Meta.Index index_a2 = (Meta.Index) BASIC.getIndexOrLike("index_a2");
        final static Meta.Index index_b1 = (Meta.Index) BASIC.getIndexOrLike("index_b1");

        final Statefulness statefulness;
        final UserSpec userSpec;
        final User user;
        final IndexSpec indexSpec;
        final Meta.Index index;
        final PrivilegesEvaluationContext context;

        @Test
        public void wildcard() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("*"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("*"),
                    new TestSgConfig.Role("non_dls_role").indexPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
            } else if (userSpec.roles.contains("dls_role_1")) {
                assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
            } else if (userSpec.roles.contains("dls_role_2")) {
                assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
            } else if (userSpec.roles.isEmpty()) {
                assertThat(dlsRestriction, isFullyRestricted());
            } else {
                fail("Missing case for " + userSpec);
            }
        }

        @Test
        public void wildcard_negation() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("*", "-index_b*"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("*", "-index_a*"),
                    new TestSgConfig.Role("non_dls_role").indexPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);
            
            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (userSpec.roles.isEmpty()) {
                assertThat(dlsRestriction, isFullyRestricted());
            } else if (index == index_a1 || index == index_a2) {
                if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isFullyRestricted());
                }
            } else if (index == index_b1) {
                if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isFullyRestricted());
                }
            }
        }

        @Test
        public void indexPattern() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("index_a*"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_b*"),
                    new TestSgConfig.Role("non_dls_role").indexPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (userSpec.roles.isEmpty()) {
                assertThat(dlsRestriction, isFullyRestricted());
            } else if (index == index_a1 || index == index_a2) {
                if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isFullyRestricted());
                }
            } else if (index == index_b1) {
                if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isFullyRestricted());
                }
            }
        }

        @Test
        public void indexPattern_negation() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("index_*",
                            "-index_b*"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_*",
                            "-index_a*"),
                    new TestSgConfig.Role("non_dls_role").indexPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (userSpec.roles.isEmpty()) {
                assertThat(dlsRestriction, isFullyRestricted());
            } else if (index == index_a1 || index == index_a2) {
                if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isFullyRestricted());
                }
            } else if (index == index_b1) {
                if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isFullyRestricted());
                }
            }
        }

        @Test
        public void template() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1"))
                            .on("index_${user.attrs.attr_a}1"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_a*"),
                    new TestSgConfig.Role("non_dls_role").indexPermissions("*").on("index_${user.attrs.attr_a}1"));

            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            try {
                DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

                if (userSpec.roles.isEmpty()) {
                    assertThat(dlsRestriction, isFullyRestricted());
                } else if (index == index_a1) {
                    if (userSpec.roles.contains("non_dls_role")) {
                        assertThat(dlsRestriction, isUnrestricted());
                    } else if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
                    } else if (userSpec.roles.contains("dls_role_1")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                    } else {
                        assertThat(dlsRestriction, isUnrestricted());
                    }
                } else if (index == index_a2) {
                    if (userSpec.roles.contains("non_dls_role")) {
                        assertThat(dlsRestriction, isFullyRestricted());
                    } else if (userSpec.roles.contains("dls_role_2")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                    } else {
                        assertThat(dlsRestriction, isFullyRestricted());
                    }
                } else if (index == index_b1) {
                    assertThat(dlsRestriction, isFullyRestricted());
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
        public void alias_static() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").aliasPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("alias_a"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_a2"),
                    new TestSgConfig.Role("non_dls_role").aliasPermissions("*").on("alias_a"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.isEmpty()) {
                assertThat(dlsRestriction, isFullyRestricted());
            } else if (index == index_a1) {
                if (userSpec.roles.contains("non_dls_role")) {
                    assertThat(dlsRestriction, isUnrestricted());
                } else if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == index_a2) {
                if (userSpec.roles.contains("non_dls_role")) {
                    assertThat(dlsRestriction, isUnrestricted());
                } else if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
                } else if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == index_b1) {
                assertThat(dlsRestriction, isFullyRestricted());
            }
        }

        @Test
        public void alias_static_wildcardNonDls() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").aliasPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("alias_a"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_a2"),
                    new TestSgConfig.Role("non_dls_role").aliasPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.isEmpty()) {
                assertThat(dlsRestriction, isFullyRestricted());
            } else if (index == index_a1) {
                if (userSpec.roles.contains("non_dls_role")) {
                    assertThat(dlsRestriction, isUnrestricted());
                } else if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == index_a2) {
                if (userSpec.roles.contains("non_dls_role")) {
                    assertThat(dlsRestriction, isUnrestricted());
                } else if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
                } else if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == index_b1) {
                assertThat(dlsRestriction, isFullyRestricted());
            }
        }

        @Test
        public void alias_wildcard() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").aliasPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("alias_a*"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_a2"),
                    new TestSgConfig.Role("non_dls_role").aliasPermissions("*").on("alias_a*"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.isEmpty()) {
                assertThat(dlsRestriction, isFullyRestricted());
            } else if (index == index_a1) {
                if (userSpec.roles.contains("non_dls_role")) {
                    assertThat(dlsRestriction, isUnrestricted());
                } else if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == index_a2) {
                if (userSpec.roles.contains("non_dls_role")) {
                    assertThat(dlsRestriction, isUnrestricted());
                } else if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
                } else if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == index_b1) {
                assertThat(dlsRestriction, isFullyRestricted());
            }
        }

        @Test
        public void alias_template() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").aliasPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1"))
                            .on("alias_${user.attrs.attr_a}"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_a2"),
                    new TestSgConfig.Role("non_dls_role").aliasPermissions("*").on("alias_${user.attrs.attr_a}"));

            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            try {
                DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

                if (userSpec.roles.isEmpty()) {
                    assertThat(dlsRestriction, isFullyRestricted());
                } else if (index == index_a1) {
                    if (userSpec.roles.contains("non_dls_role")) {
                        assertThat(dlsRestriction, isUnrestricted());
                    } else if (userSpec.roles.contains("dls_role_1")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                    } else {
                        fail("Unhandled case " + userSpec);
                    }
                } else if (index == index_a2) {
                    if (userSpec.roles.contains("non_dls_role")) {
                        assertThat(dlsRestriction, isUnrestricted());
                    } else if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
                    } else if (userSpec.roles.contains("dls_role_1")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                    } else if (userSpec.roles.contains("dls_role_2")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                    } else {
                        fail("Unhandled case " + userSpec);
                    }
                } else if (index == index_b1) {
                    assertThat(dlsRestriction, isFullyRestricted());
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

        @Parameters(name = "{0}; {1}; {2}")
        public static Collection<Object[]> params() {
            List<Object[]> result = new ArrayList<>();

            for (UserSpec userSpec : Arrays.asList(//
                    new UserSpec("non_dls_role", "non_dls_role"), //
                    new UserSpec("dls_role_1", "dls_role_1"), //
                    new UserSpec("dls_role_1 and dls_role_2", "dls_role_1", "dls_role_2"), //
                    new UserSpec("dls_role_1 and non_dls_role", "dls_role_1", "non_dls_role"), //
                    new UserSpec("non_dls_role, attributes", "non_dls_role").attribute("attr_a", "a"), //
                    new UserSpec("dls_role_1, attributes", "dls_role_1").attribute("attr_a", "a"), //
                    new UserSpec("dls_role_1 and dls_role_2, attributes", "dls_role_1", "dls_role_2").attribute("attr_a", "a"), //
                    new UserSpec("dls_role_1 and non_dls_role, attributes", "dls_role", "non_dls_role").attribute("attr_a", "a"), //
                    new UserSpec("no roles")//
            )) {
                for (IndexSpec indexSpec : Arrays.asList(//
                        new IndexSpec("index_a1"), //
                        new IndexSpec("index_a2"), //
                        new IndexSpec("index_b1"))) {
                    for (Statefulness statefulness : Statefulness.values()) {
                        result.add(new Object[] { userSpec, indexSpec, statefulness });                        
                    }
                }
            }
            return result;
        }

        public IndicesAndAliases_getRestriction(UserSpec userSpec, IndexSpec indexSpec, Statefulness statefulness) {
            this.userSpec = userSpec;
            this.indexSpec = indexSpec;
            this.user = userSpec.buildUser();
            this.index = (Meta.Index) BASIC.getIndexOrLike(indexSpec.index);
            this.context = new PrivilegesEvaluationContext(this.user, false, ImmutableSet.of(userSpec.roles), null, null, true, null, null);
            this.statefulness = statefulness;
        }


        private RoleBasedDocumentAuthorization createSubject(SgDynamicConfiguration<Role> roleConfig) {
            return new RoleBasedDocumentAuthorization(roleConfig, statefulness == Statefulness.STATEFUL ? BASIC : null, MetricsLevel.NONE);
        }
    }

    @RunWith(Parameterized.class)
    public static class IndicesAndAliases_hasRestriction {

        final static Meta BASIC = indices("index_a1", "index_a2", "index_b1", "index_b2")//
                .alias("alias_a").of("index_a1", "index_a2");

        final static Meta.Index index_a1 = (Meta.Index) BASIC.getIndexOrLike("index_a1");
        final static Meta.Index index_a2 = (Meta.Index) BASIC.getIndexOrLike("index_a2");
        final static Meta.Index index_b1 = (Meta.Index) BASIC.getIndexOrLike("index_b1");
        final static Meta.Alias alias_a = (Meta.Alias) BASIC.getIndexOrLike("alias_a");

        final Statefulness statefulness;
        final UserSpec userSpec;
        final User user;
        final IndicesSpec indicesSpec;
        final ResolvedIndices resolvedIndices;
        final PrivilegesEvaluationContext context;

        @Test
        public void wildcard() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("*"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("*"),
                    new TestSgConfig.Role("non_dls_role").indexPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            boolean result = subject.hasRestrictions(context, resolvedIndices, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertFalse(result);
            } else {
                assertTrue(result);
            }
        }

        @Test
        public void wildcard_negation() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("*", "-index_b*"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("*", "-index_a*"),
                    new TestSgConfig.Role("non_dls_role").indexPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            boolean result = subject.hasRestrictions(context, resolvedIndices, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertFalse(result);
            } else {
                assertTrue(result);
            }
        }

        @Test
        public void indexPattern() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("index_a*"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_b*"),
                    new TestSgConfig.Role("non_dls_role").indexPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig, BASIC, MetricsLevel.NONE);

            boolean result = subject.hasRestrictions(context, resolvedIndices, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertFalse(result);
            } else {
                assertTrue(result);
            }
        }

        @Test
        public void indexPattern_negation() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("index_*",
                            "-index_b*"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_*",
                            "-index_a*"),
                    new TestSgConfig.Role("non_dls_role").indexPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            boolean result = subject.hasRestrictions(context, resolvedIndices, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertFalse(result);
            } else {
                assertTrue(result);
            }
        }

        @Test
        public void template() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1"))
                            .on("index_${user.attrs.attr_a}1"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_a*"),
                    new TestSgConfig.Role("non_dls_role").indexPermissions("*").on("index_${user.attrs.attr_a}1"));

            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            try {
                boolean result = subject.hasRestrictions(context, resolvedIndices, Meter.NO_OP);

                if (userSpec.roles.contains("non_dls_role") && resolvedIndices.getLocal().getUnion().equals(ImmutableSet.of(index_a1))
                        && userSpec.attributes.containsKey("attr_a")) {
                    assertFalse(result);
                } else {
                    assertTrue(result);
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
        public void alias_static() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").aliasPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("alias_a"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_a2"),
                    new TestSgConfig.Role("non_dls_role").aliasPermissions("*").on("alias_a"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            boolean result = subject.hasRestrictions(context, resolvedIndices, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")
                    && resolvedIndices.getLocal().getUnion().forAllApplies(i -> i instanceof Meta.Alias || !i.parentAliases().isEmpty())) {
                assertFalse(result);
            } else {
                assertTrue(result);
            }
        }

        @Test
        public void alias_static_wildcardNonDls() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").aliasPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("alias_a"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_a2"),
                    new TestSgConfig.Role("non_dls_role").aliasPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            boolean result = subject.hasRestrictions(context, resolvedIndices, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role") && resolvedIndices.getLocal().getUnion().forAllApplies(i -> !i.parentAliases().isEmpty() || i instanceof Meta.Alias)) {
                assertFalse(result);
            } else {
                assertTrue(result);
            }
        }

        @Test
        public void alias_wildcard() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").aliasPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("alias_a*"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_a2"),
                    new TestSgConfig.Role("non_dls_role").aliasPermissions("*").on("alias_a*"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            boolean result = subject.hasRestrictions(context, resolvedIndices, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")
                    && resolvedIndices.getLocal().getUnion().forAllApplies(i -> i == alias_a || i.parentAliases().contains(alias_a))) {
                assertFalse(result);
            } else {
                assertTrue(result);
            }
        }

        @Test
        public void alias_template() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").aliasPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1"))
                            .on("alias_${user.attrs.attr_a}"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("index_a2"),
                    new TestSgConfig.Role("non_dls_role").aliasPermissions("*").on("alias_${user.attrs.attr_a}"));

            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            try {
                boolean result = subject.hasRestrictions(context, resolvedIndices, Meter.NO_OP);

                if (userSpec.roles.contains("non_dls_role") && userSpec.attributes.containsKey("attr_a")
                        && resolvedIndices.getLocal().getUnion().forAllApplies(i -> i == alias_a || i.parentAliases().contains(alias_a))) {
                    assertFalse(result);
                } else {
                    assertTrue(result);
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

        @Parameters(name = "{0}; {1}; {2}")
        public static Collection<Object[]> params() {
            List<Object[]> result = new ArrayList<>();

            for (UserSpec userSpec : Arrays.asList(//
                    new UserSpec("non_dls_role", "non_dls_role"), //
                    new UserSpec("dls_role_1", "dls_role_1"), //
                    new UserSpec("dls_role_1 and dls_role_2", "dls_role_1", "dls_role_2"), //
                    new UserSpec("dls_role_1 and non_dls_role", "dls_role_1", "non_dls_role"), //
                    new UserSpec("non_dls_role, attributes", "non_dls_role").attribute("attr_a", "a"), //
                    new UserSpec("dls_role_1, attributes", "dls_role_1").attribute("attr_a", "a"), //
                    new UserSpec("dls_role_1 and dls_role_2, attributes", "dls_role_1", "dls_role_2").attribute("attr_a", "a"), //
                    new UserSpec("dls_role_1 and non_dls_role, attributes", "dls_role", "non_dls_role").attribute("attr_a", "a"), //
                    new UserSpec("no roles")//
            )) {
                for (IndicesSpec indicesSpec : Arrays.asList(//
                        new IndicesSpec("index_a1"), //
                        new IndicesSpec("index_a2"), //
                        new IndicesSpec("index_b1"), //
                        new IndicesSpec("alias_a"), //
                        new IndicesSpec("index_a1", "index_a2"), //
                        new IndicesSpec("index_a1", "index_b1"), //
                        new IndicesSpec("alias_a", "index_b1"))) {
                    for (Statefulness statefulness : Statefulness.values()) {
                        result.add(new Object[] { userSpec, indicesSpec, statefulness });
                    }
                }
            }
            return result;
        }

        public IndicesAndAliases_hasRestriction(UserSpec userSpec, IndicesSpec indicesSpec, Statefulness statefulness) {
            this.userSpec = userSpec;
            this.indicesSpec = indicesSpec;
            this.user = userSpec.buildUser();
            this.resolvedIndices = ResolvedIndices.of(BASIC, indicesSpec.indices.toArray(new String[0]));
            this.context = new PrivilegesEvaluationContext(this.user, false, ImmutableSet.of(userSpec.roles), null, null, true, null, null);
            this.statefulness = statefulness;
        }

        private RoleBasedDocumentAuthorization createSubject(SgDynamicConfiguration<Role> roleConfig) {
            return new RoleBasedDocumentAuthorization(roleConfig, statefulness == Statefulness.STATEFUL ? BASIC : null, MetricsLevel.NONE);
        }
    }

    @RunWith(Parameterized.class)
    public static class DataStreams_getRestriction {
        final static Meta BASIC = dataStream("datastream_a1").of(".ds-datastream_a1_backing_0001", ".ds-datastream_a1_backing_0002")//
                .dataStream("datastream_a2").of(".ds-datastream_a2_backing_0001", ".ds-datastream_a2_backing_0002")//
                .dataStream("datastream_b1").of(".ds-datastream_b1_backing_0001", ".ds-datastream_b1_backing_0002")//
                .dataStream("datastream_b2").of(".ds-datastream_b2_backing_0001", ".ds-datastream_b2_backing_0002")//
                .alias("alias_a").of("datastream_a1", "datastream_a2");

        final static Meta.Index datastream_a1_backing = (Meta.Index) BASIC.getIndexOrLike(".ds-datastream_a1_backing_0001");
        final static Meta.Index datastream_a2_backing = (Meta.Index) BASIC.getIndexOrLike(".ds-datastream_a2_backing_0001");
        final static Meta.Index datastream_b1_backing = (Meta.Index) BASIC.getIndexOrLike(".ds-datastream_b1_backing_0001");

        final Statefulness statefulness;
        final UserSpec userSpec;
        final User user;
        final IndexSpec indexSpec;
        final Meta.Index index;
        final PrivilegesEvaluationContext context;

        @Test
        public void wildcard() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("*"),
                    new TestSgConfig.Role("dls_role_2").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("*"),
                    new TestSgConfig.Role("non_dls_role").dataStreamPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (userSpec.roles.isEmpty()) {
                assertThat(dlsRestriction, isFullyRestricted());
            } else if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
            } else if (userSpec.roles.contains("dls_role_1")) {
                assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
            } else if (userSpec.roles.contains("dls_role_2")) {
                assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
            } else {
                fail("Unhandled case " + userSpec);
            }
        }

        @Test
        public void wildcard_negation() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("*",
                            "-datastream_b*"),
                    new TestSgConfig.Role("dls_role_2").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("*",
                            "-datastream_a*"),
                    new TestSgConfig.Role("non_dls_role").dataStreamPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (userSpec.roles.isEmpty()) {
                assertThat(dlsRestriction, isFullyRestricted());
            } else if (index == datastream_a1_backing || index == datastream_a2_backing) {
                if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isFullyRestricted());
                }
            } else if (index == datastream_b1_backing) {
                if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isFullyRestricted());
                }
            }
        }

        @Test
        public void indexPattern() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("datastream_a*"),
                    new TestSgConfig.Role("dls_role_2").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("datastream_b*"),
                    new TestSgConfig.Role("non_dls_role").dataStreamPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (userSpec.roles.isEmpty()) {
                assertThat(dlsRestriction, isFullyRestricted());
            } else if (index == datastream_a1_backing || index == datastream_a2_backing) {
                if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isFullyRestricted());
                }
            } else if (index == datastream_b1_backing) {
                if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isFullyRestricted());
                }
            }
        }

        @Test
        public void indexPattern_negation() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("datastream_*",
                            "-datastream_b*"),
                    new TestSgConfig.Role("dls_role_2").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("datastream_*",
                            "-datastream_a*"),
                    new TestSgConfig.Role("non_dls_role").dataStreamPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (userSpec.roles.isEmpty()) {
                assertThat(dlsRestriction, isFullyRestricted());
            } else if (index == datastream_a1_backing || index == datastream_a2_backing) {
                if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isFullyRestricted());
                }
            } else if (index == datastream_b1_backing) {
                if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isFullyRestricted());
                }
            }
        }

        @Test
        public void template() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1"))
                            .on("datastream_${user.attrs.attr_a}1"),
                    new TestSgConfig.Role("dls_role_2").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("datastream_a*"),
                    new TestSgConfig.Role("non_dls_role").dataStreamPermissions("*").on("datastream_${user.attrs.attr_a}1"));

            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            try {
                DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

                if (userSpec.roles.isEmpty()) {
                    assertThat(dlsRestriction, isFullyRestricted());
                } else if (index == datastream_a1_backing) {
                    if (userSpec.roles.contains("non_dls_role")) {
                        assertThat(dlsRestriction, isUnrestricted());
                    } else if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
                    } else if (userSpec.roles.contains("dls_role_1")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                    } else {
                        assertThat(dlsRestriction, isFullyRestricted());
                    }
                } else if (index == datastream_a2_backing) {
                    if (userSpec.roles.contains("dls_role_2")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                    } else {
                        assertThat(dlsRestriction, isFullyRestricted());
                    }
                } else if (index == datastream_b1_backing) {
                    assertThat(dlsRestriction, isFullyRestricted());
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
        public void alias_static() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").aliasPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("alias_a"),
                    new TestSgConfig.Role("dls_role_2").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("datastream_a2"),
                    new TestSgConfig.Role("non_dls_role").aliasPermissions("*").on("alias_a"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.isEmpty()) {
                assertThat(dlsRestriction, isFullyRestricted());
            } else if (index == datastream_a1_backing) {
                if (userSpec.roles.contains("non_dls_role")) {
                    assertThat(dlsRestriction, isUnrestricted());
                } else if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == datastream_a2_backing) {
                if (userSpec.roles.contains("non_dls_role")) {
                    assertThat(dlsRestriction, isUnrestricted());
                } else if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
                } else if (userSpec.roles.contains("dls_role_1")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                } else if (userSpec.roles.contains("dls_role_2")) {
                    assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                } else {
                    assertThat(dlsRestriction, isUnrestricted());
                }
            } else if (index == datastream_b1_backing) {
                assertThat(dlsRestriction, isFullyRestricted());
            }
        }

        @Test
        public void alias_template() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").aliasPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1"))
                            .on("alias_${user.attrs.attr_a}"),
                    new TestSgConfig.Role("dls_role_2").dataStreamPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("datastream_a2"),
                    new TestSgConfig.Role("non_dls_role").aliasPermissions("*").on("alias_${user.attrs.attr_a}"));

            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            try {
                DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

                if (userSpec.roles.isEmpty()) {
                    assertThat(dlsRestriction, isFullyRestricted());
                } else if (index == datastream_a1_backing) {
                    if (userSpec.roles.contains("non_dls_role")) {
                        assertThat(dlsRestriction, isUnrestricted());
                    } else if (userSpec.roles.contains("dls_role_1")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                    } else {
                        fail("Unhandled case " + userSpec);
                    }
                } else if (index == datastream_a2_backing) {
                    if (userSpec.roles.contains("non_dls_role")) {
                        assertThat(dlsRestriction, isUnrestricted());
                    } else if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
                    } else if (userSpec.roles.contains("dls_role_1")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
                    } else if (userSpec.roles.contains("dls_role_2")) {
                        assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
                    } else {
                        fail("Unhandled case " + userSpec);
                    }
                } else if (index == datastream_b1_backing) {
                    assertThat(dlsRestriction, isFullyRestricted());
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
        public void wildcardOnIndices() throws Exception {
            SgDynamicConfiguration<Role> roleConfig = roleConfig(//                    
                    new TestSgConfig.Role("dls_role_1").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r1")).on("*"),
                    new TestSgConfig.Role("dls_role_2").indexPermissions("*").dls(DocNode.of("term.dept.value", "dept_r2")).on("*"),
                    new TestSgConfig.Role("non_dls_role").indexPermissions("*").on("*"));
            RoleBasedDocumentAuthorization subject = createSubject(roleConfig);

            DlsRestriction dlsRestriction = subject.getRestriction(context, index, Meter.NO_OP);

            if (userSpec.roles.isEmpty()) {
                assertThat(dlsRestriction, isFullyRestricted());
            } else if (userSpec.roles.contains("non_dls_role")) {
                assertThat(dlsRestriction, isUnrestricted());
            } else if (userSpec.roles.contains("dls_role_1") && userSpec.roles.contains("dls_role_2")) {
                assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1"), termQuery("dept", "dept_r2")));
            } else if (userSpec.roles.contains("dls_role_1")) {
                assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r1")));
            } else if (userSpec.roles.contains("dls_role_2")) {
                assertThat(dlsRestriction, isRestricted(termQuery("dept", "dept_r2")));
            }
        }

        @Parameters(name = "{0}; {1}; {2}")
        public static Collection<Object[]> params() {
            List<Object[]> result = new ArrayList<>();

            for (UserSpec userSpec : Arrays.asList(//
                    new UserSpec("non_dls_role", "non_dls_role"), //
                    new UserSpec("dls_role_1", "dls_role_1"), //
                    new UserSpec("dls_role_1 and dls_role_2", "dls_role_1", "dls_role_2"), //
                    new UserSpec("dls_role_1 and non_dls_role", "dls_role_1", "non_dls_role"), //
                    new UserSpec("non_dls_role, attributes", "non_dls_role").attribute("attr_a", "a"), //
                    new UserSpec("dls_role_1, attributes", "dls_role_1").attribute("attr_a", "a"), //
                    new UserSpec("dls_role_1 and dls_role_2, attributes", "dls_role_1", "dls_role_2").attribute("attr_a", "a"), //
                    new UserSpec("dls_role_1 and non_dls_role, attributes", "dls_role", "non_dls_role").attribute("attr_a", "a"), //
                    new UserSpec("no roles")//
            )) {
                for (IndexSpec indexSpec : Arrays.asList(//
                        new IndexSpec(datastream_a1_backing.name()), //
                        new IndexSpec(datastream_a2_backing.name()), //
                        new IndexSpec(datastream_b1_backing.name()))) {
                    for (Statefulness statefulness : Statefulness.values()) {
                        result.add(new Object[] { userSpec, indexSpec, statefulness });
                    }
                }
            }
            return result;
        }

        private RoleBasedDocumentAuthorization createSubject(SgDynamicConfiguration<Role> roleConfig) {
            return new RoleBasedDocumentAuthorization(roleConfig, statefulness == Statefulness.STATEFUL ? BASIC : null, MetricsLevel.NONE);
        }

        public DataStreams_getRestriction(UserSpec userSpec, IndexSpec indexSpec, Statefulness statefulness) {
            this.userSpec = userSpec;
            this.indexSpec = indexSpec;
            this.user = userSpec.buildUser();
            this.index = (Meta.Index) BASIC.getIndexOrLike(indexSpec.index);
            this.context = new PrivilegesEvaluationContext(this.user, false, ImmutableSet.of(userSpec.roles), null, null, true, null, null);
            this.statefulness = statefulness;
        }

    }

    static SgDynamicConfiguration<Role> roleConfig(TestSgConfig.Role... roles) throws ConfigValidationException {
        return TestSgConfig.Role.toActualRole(parserContext, roles);
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

    public static class IndicesSpec {
        final ImmutableList<String> indices;

        IndicesSpec(String... indices) {
            this.indices = ImmutableList.ofArray(indices);
        }

        @Override
        public String toString() {
            return this.indices.toString();
        }
    }

    static enum Statefulness {
        STATEFUL, NON_STATEFUL
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

    static DiagnosingMatcher<DlsRestriction> isFullyRestricted() {
        return new DiagnosingMatcher<DlsRestriction>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("A DlsRestriction object that has full restrictions");
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof DlsRestriction)) {
                    mismatchDescription.appendValue(item).appendText(" is not a DlsRestriction object");
                    return false;
                }

                DlsRestriction dlsRestriction = (DlsRestriction) item;

                if (dlsRestriction.getQueries().size() != 0) {
                    for (com.floragunn.searchsupport.queries.Query query : dlsRestriction.getQueries()) {
                        if (!query.getQueryBuilder().equals(com.floragunn.searchsupport.queries.Query.MATCH_NONE.getQueryBuilder())) {
                            mismatchDescription.appendText("The DlsRestriction object is not fully restricted:").appendValue(dlsRestriction);
                            return false;
                        }
                    }

                    return true;
                } else {
                    mismatchDescription.appendText("The DlsRestriction object is not fully restricted:").appendValue(dlsRestriction);
                    return false;
                }
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
