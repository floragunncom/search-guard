/*
 * Copyright 2024 floragunn GmbH
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.floragunn.searchguard.authz.config;

import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.test.helper.log.LogsRule;
import com.floragunn.searchsupport.util.EsLogging;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.configuration.StaticSgConfig;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

public class RoleTest {

    @ClassRule
    public static EsLogging esLogging = new EsLogging();

    @Rule
    public LogsRule logsRule = new LogsRule("com.floragunn.searchguard.authz.config.Role");

    @Test(expected = ConfigValidationException.class)
    public void indexExclusion_notSupported() throws Exception {
        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                "test_role1:\n" + //
                        "  index_permissions:\n" + //
                        "  - index_patterns: ['index_a*']\n" + //
                        "    allowed_actions: ['indices:data/write/*']\n" + //
                        "  exclude_index_permissions:\n" + //
                        "  - index_patterns: ['index_a1']\n" + //
                        "    actions: ['indices:data/write/delete']\n" //
        ), CType.ROLES, new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests()).withoutLenientValidation()).get();
    }

    @Test
    public void indexExclusion_ignored() throws Exception {
        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from(//
                "test_role1:\n" + //
                        "  index_permissions:\n" + //
                        "  - index_patterns: ['index_a*']\n" + //
                        "    allowed_actions: ['indices:data/write/*']\n" + //
                        "  exclude_index_permissions:\n" + //
                        "  - index_patterns: ['index_a1']\n" + //
                        "    actions: ['indices:data/write/delete']\n" //
        ), CType.ROLES, new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())).get();
    }

    @Test
    public void shouldLogValidationWarnings_indexPrivsAssignedAsClusterPrivs() throws Exception {
        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    cluster_permissions:
                        - "indices:admin/index_as_cluster_priv"
                        - "cluster:monitor/main"
                        - "indices:data/read/search/template" #this one is a cluster priv
                        - "*"
                        - "indices:data/*"
                """
        ), CType.ROLES, new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())).get();

        logsRule.assertThatContainExactly(String.format("The following index permissions are assigned as cluster permissions: [%s]", "indices:admin/index_as_cluster_priv, indices:data/*"));
    }

    @Test
    public void shouldLogValidationWarnings_clusterPrivsAssignedAsIndexPrivs() throws Exception {
        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    index_permissions:
                        - index_patterns: ["*"]
                          allowed_actions:
                            - "indices:data/read/search/template" #this one is a cluster priv
                            - "cluster:admin/*"
                            - "cluster:monitor/cluster_as_index_priv"
                            - "indices:monitor/settings/get"
                            - "*"
                """
        ), CType.ROLES, new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())).get();

        logsRule.assertThatContainExactly(String.format("The following cluster permissions are assigned as index permissions: [%s]", "indices:data/read/search/template, cluster:admin/*, cluster:monitor/cluster_as_index_priv"));
    }

    @Test
    public void shouldLogValidationWarnings_clusterPrivsAssignedAsAliasPrivs() throws Exception {
        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    alias_permissions:
                        - alias_patterns: ["*"]
                          allowed_actions:
                            - "indices:data/read/search/template" #this one is a cluster priv
                            - "cluster:admin/*"
                            - "indices:monitor/settings/get"
                            - "cluster:monitor/cluster_as_index_priv"
                            - "*"
                """
        ), CType.ROLES, new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())).get();

        logsRule.assertThatContainExactly(String.format("The following cluster permissions are assigned as alias permissions: [%s]", "indices:data/read/search/template, cluster:admin/*, cluster:monitor/cluster_as_index_priv"));
    }

    @Test
    public void shouldLogValidationWarnings_clusterPrivsAssignedAsDataStreamPrivs() throws Exception {
        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    data_stream_permissions:
                        - data_stream_patterns: ["*"]
                          allowed_actions:
                            - "indices:data/read/search/template" #this one is a cluster priv
                            - "cluster:admin/*"
                            - "indices:monitor/settings/get"
                            - "cluster:monitor/cluster_as_index_priv"
                            - "*"
                """
        ), CType.ROLES, new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())).get();

        logsRule.assertThatContainExactly(String.format("The following cluster permissions are assigned as data stream permissions: [%s]", "indices:data/read/search/template, cluster:admin/*, cluster:monitor/cluster_as_index_priv"));
    }

    @Test
    public void shouldNotLogValidationWarnings_indexAndClusterPrivsAreNotMisconfigured() throws Exception {
        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    cluster_permissions:
                        - "cluster:monitor/main"
                        - "*"
                    index_permissions:
                        - index_patterns: ["*"]
                          allowed_actions:
                            - "indices:monitor/settings/get"
                            - "*"
                    alias_permissions:
                        - alias_patterns: ["*"]
                          allowed_actions:
                            - "indices:monitor/settings/get"
                            - "*"
                    data_stream_permissions:
                        - data_stream_patterns: ["*"]
                          allowed_actions:
                            - "indices:monitor/settings/get"
                            - "*"
                """
        ), CType.ROLES, new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())).get();

        logsRule.assertThatNotContain("The following index permissions are assigned as cluster permissions:");
        logsRule.assertThatNotContain("The following cluster permissions are assigned as index permissions:");
        logsRule.assertThatNotContain("The following cluster permissions are assigned as alias permissions:");
        logsRule.assertThatNotContain("The following cluster permissions are assigned as data stream permissions:");
    }

    @Test
    public void shouldLogValidationWarnings_dlsAssignedToWildcardPattern() throws Exception {
        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    index_permissions:
                        - index_patterns: ["*", "index1"]
                          allowed_actions:
                            - "*"
                          dls: "{\\"term\\" : {\\"_type\\" : \\"index\\"}}"
                        - index_patterns: ["*"]
                          allowed_actions:
                            - "*"
                          dls: "{\\"term\\" : {\\"_type\\" : \\"index2\\"}}"
                    alias_permissions:
                        - alias_patterns: ["*"]
                          allowed_actions:
                            - "*"
                          dls: "{\\"term\\" : {\\"_type\\" : \\"alias\\"}}"
                    data_stream_permissions:
                        - data_stream_patterns: ["*", "ds1"]
                          allowed_actions:
                            - "*"
                          dls: "{\\"term\\" : {\\"_type\\" : \\"ds\\"}}"
                """
        ), CType.ROLES, new ConfigurationRepository.Context(null, null, null, xContentRegistry(), null, Actions.forTests())).get();

        logsRule.assertThatContainExactly(String.format("Role assigns a DLS rule '%s' to wildcard (*) index_patterns", "{\"term\" : {\"_type\" : \"index\"}}"));
        logsRule.assertThatContainExactly(String.format("Role assigns a DLS rule '%s' to wildcard (*) index_patterns", "{\"term\" : {\"_type\" : \"index2\"}}"));
        logsRule.assertThatContainExactly(String.format("Role assigns a DLS rule '%s' to wildcard (*) alias_patterns", "{\"term\" : {\"_type\" : \"alias\"}}"));
        logsRule.assertThatContainExactly(String.format("Role assigns a DLS rule '%s' to wildcard (*) data_stream_patterns", "{\"term\" : {\"_type\" : \"ds\"}}"));
    }

    @Test
    public void shouldLogValidationWarnings_flsAssignedToWildcardPattern() throws Exception {
        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    index_permissions:
                        - index_patterns: ["*", "index1"]
                          allowed_actions:
                            - "*"
                          fls: ["alias", "ds"]
                    alias_permissions:
                        - alias_patterns: ["*"]
                          allowed_actions:
                            - "*"
                          fls: ["index", "ds"]
                        - alias_patterns: ["*"]
                          allowed_actions:
                            - "*"
                          fls: ["index", "ds2"]
                    data_stream_permissions:
                        - data_stream_patterns: ["*", "ds1"]
                          allowed_actions:
                            - "*"
                          fls: ["index", "alias"]
                """
        ), CType.ROLES, new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())).get();

        logsRule.assertThatContainExactly(String.format("Role assigns a FLS rule '[%s]' to wildcard (*) index_patterns", "alias, ds"));
        logsRule.assertThatContainExactly(String.format("Role assigns a FLS rule '[%s]' to wildcard (*) alias_patterns", "index, ds"));
        logsRule.assertThatContainExactly(String.format("Role assigns a FLS rule '[%s]' to wildcard (*) alias_patterns", "index, ds2"));
        logsRule.assertThatContainExactly(String.format("Role assigns a FLS rule '[%s]' to wildcard (*) data_stream_patterns", "index, alias"));
    }

    @Test
    public void shouldNotLogValidationWarnings_dlsAndFlsIsNotAssignedToWildcardPattern() throws Exception {
        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    index_permissions:
                        - index_patterns: ["index1"]
                          allowed_actions:
                            - "*"
                          fls: ["alias", "ds"]
                          dls: "{\\"term\\" : {\\"_type\\" : \\"index\\"}}"
                    alias_permissions:
                        - alias_patterns: ["alias*"]
                          allowed_actions:
                            - "*"
                          fls: ["index", "ds"]
                          dls: "{\\"term\\" : {\\"_type\\" : \\"alias\\"}}"
                    data_stream_permissions:
                        - data_stream_patterns: ["*ds"]
                          allowed_actions:
                            - "*"
                          fls: ["index", "alias"]
                          dls: "{\\"term\\" : {\\"_type\\" : \\"ds\\"}}"
                """
        ), CType.ROLES, new ConfigurationRepository.Context(null, null, null, xContentRegistry(), null, Actions.forTests())).get();

        logsRule.assertThatNotContain("Role assigns a DLS rule");
        logsRule.assertThatNotContain("Role assigns a FLS rule");
    }

    @Test
    public void shouldLogValidationWarnings_clusterTypeActionGroupAssignedAsIndexPriv() throws Exception {
        SgDynamicConfiguration<ActionGroup> actionGroups = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                CLUSTER_ACTION_GROUP:
                    type: "cluster"
                    allowed_actions:
                        - "cluster:monitor/main"
                        - "cluster:admin/settings/update"
                """
        ), CType.ACTIONGROUPS, new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())).get();

        ConfigurationRepository.Context contextWithActionGroups = new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())
                .withActionGroups(actionGroups);

        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    index_permissions:
                        - index_patterns: ["*"]
                          allowed_actions:
                            - "CLUSTER_ACTION_GROUP"
                            - "indices:data/read/*"
                """
        ), CType.ROLES, contextWithActionGroups).get();

        logsRule.assertThatContainExactly("The following cluster permissions are assigned as index permissions: [CLUSTER_ACTION_GROUP]");
    }

    @Test
    public void shouldLogValidationWarnings_indexTypeActionGroupAssignedAsClusterPriv() throws Exception {
        SgDynamicConfiguration<ActionGroup> actionGroups = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                INDEX_ACTION_GROUP:
                    type: "index"
                    allowed_actions:
                        - "indices:data/read/*"
                        - "indices:data/write/*"
                """
        ), CType.ACTIONGROUPS, new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())).get();

        ConfigurationRepository.Context contextWithActionGroups = new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())
                .withActionGroups(actionGroups);

        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    cluster_permissions:
                        - "INDEX_ACTION_GROUP"
                        - "cluster:monitor/main"
                """
        ), CType.ROLES, contextWithActionGroups).get();

        logsRule.assertThatContainExactly("The following index permissions are assigned as cluster permissions: [INDEX_ACTION_GROUP]");
    }

    @Test
    public void shouldLogValidationWarnings_clusterTypeActionGroupAssignedAsAliasPriv() throws Exception {
        SgDynamicConfiguration<ActionGroup> actionGroups = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                CLUSTER_ACTION_GROUP:
                    type: "cluster"
                    allowed_actions:
                        - "cluster:monitor/main"
                """
        ), CType.ACTIONGROUPS, new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())).get();

        ConfigurationRepository.Context contextWithActionGroups = new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())
                .withActionGroups(actionGroups);

        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    alias_permissions:
                        - alias_patterns: ["*"]
                          allowed_actions:
                            - "CLUSTER_ACTION_GROUP"
                """
        ), CType.ROLES, contextWithActionGroups).get();

        logsRule.assertThatContainExactly("The following cluster permissions are assigned as alias permissions: [CLUSTER_ACTION_GROUP]");
    }

    @Test
    public void shouldLogValidationWarnings_clusterTypeActionGroupAssignedAsDataStreamPriv() throws Exception {
        SgDynamicConfiguration<ActionGroup> actionGroups = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                CLUSTER_ACTION_GROUP:
                    type: "cluster"
                    allowed_actions:
                        - "cluster:monitor/main"
                """
        ), CType.ACTIONGROUPS, new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())).get();

        ConfigurationRepository.Context contextWithActionGroups = new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())
                .withActionGroups(actionGroups);

        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    data_stream_permissions:
                        - data_stream_patterns: ["*"]
                          allowed_actions:
                            - "CLUSTER_ACTION_GROUP"
                """
        ), CType.ROLES, contextWithActionGroups).get();

        logsRule.assertThatContainExactly("The following cluster permissions are assigned as data stream permissions: [CLUSTER_ACTION_GROUP]");
    }

    @Test
    public void shouldNotLogValidationWarnings_actionGroupsUsedCorrectly() throws Exception {
        SgDynamicConfiguration<ActionGroup> actionGroups = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                CLUSTER_ACTION_GROUP:
                    type: "cluster"
                    allowed_actions:
                        - "cluster:monitor/main"
                INDEX_ACTION_GROUP:
                    type: "index"
                    allowed_actions:
                        - "indices:data/read/*"
                """
        ), CType.ACTIONGROUPS, new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())).get();

        ConfigurationRepository.Context contextWithActionGroups = new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())
                .withActionGroups(actionGroups);

        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    cluster_permissions:
                        - "CLUSTER_ACTION_GROUP"
                    index_permissions:
                        - index_patterns: ["*"]
                          allowed_actions:
                            - "INDEX_ACTION_GROUP"
                    alias_permissions:
                        - alias_patterns: ["*"]
                          allowed_actions:
                            - "indices:admin/aliases/*"
                    data_stream_permissions:
                        - data_stream_patterns: ["*"]
                          allowed_actions:
                            - "indices:admin/data_stream/*"
                """
        ), CType.ROLES, contextWithActionGroups).get();

        logsRule.assertThatNotContain("The following index permissions are assigned as cluster permissions:");
        logsRule.assertThatNotContain("The following cluster permissions are assigned as index permissions:");
        logsRule.assertThatNotContain("The following cluster permissions are assigned as alias permissions:");
        logsRule.assertThatNotContain("The following cluster permissions are assigned as data stream permissions:");
    }

    @Test
    public void shouldLogValidationWarnings_multipleActionGroupsMisconfigured() throws Exception {
        SgDynamicConfiguration<ActionGroup> actionGroups = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                CLUSTER_GROUP_1:
                    type: "cluster"
                    allowed_actions:
                        - "cluster:monitor/main"
                CLUSTER_GROUP_2:
                    type: "cluster"
                    allowed_actions:
                        - "cluster:admin/*"
                INDEX_GROUP_1:
                    type: "index"
                    allowed_actions:
                        - "indices:data/read/*"
                """
        ), CType.ACTIONGROUPS, new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())).get();

        ConfigurationRepository.Context contextWithActionGroups = new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())
                .withActionGroups(actionGroups);

        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    cluster_permissions:
                        - "INDEX_GROUP_1"
                    index_permissions:
                        - index_patterns: ["*"]
                          allowed_actions:
                            - "CLUSTER_GROUP_1"
                            - "CLUSTER_GROUP_2"
                """
        ), CType.ROLES, contextWithActionGroups).get();

        logsRule.assertThatContainExactly("The following index permissions are assigned as cluster permissions: [INDEX_GROUP_1]");
        logsRule.assertThatContainExactly("The following cluster permissions are assigned as index permissions: [CLUSTER_GROUP_1, CLUSTER_GROUP_2]");
    }

    private NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(List.of(
                new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField(TermQueryBuilder.NAME), TermQueryBuilder::fromXContent)
        ));
    }

    // Tests for static action groups (loaded from static_action_groups.yml)

    @Test
    public void shouldNotLogValidationWarnings_staticClusterActionGroupUsedInClusterPermissions() throws Exception {
        // SGS_CLUSTER_COMPOSITE_OPS is a static action group with type: "cluster"
        // When used in cluster_permissions, it should NOT trigger a warning
        StaticSgConfig staticSgConfig = new StaticSgConfig(Settings.EMPTY);
        SgDynamicConfiguration<ActionGroup> staticActionGroups = staticSgConfig.addTo(
                SgDynamicConfiguration.empty(CType.ACTIONGROUPS)
        );

        ConfigurationRepository.Context contextWithStaticActionGroups =
                new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())
                        .withActionGroups(staticActionGroups);

        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    cluster_permissions:
                        - "SGS_CLUSTER_COMPOSITE_OPS"
                        - "SGS_CLUSTER_COMPOSITE_OPS_RO"
                        - "SGS_CLUSTER_MONITOR"
                        - "cluster:monitor/main"
                """
        ), CType.ROLES, contextWithStaticActionGroups).get();

        logsRule.assertThatNotContain("The following index permissions are assigned as cluster permissions:");
    }

    @Test
    public void shouldNotLogValidationWarnings_staticIndexActionGroupUsedInIndexPermissions() throws Exception {
        // SGS_CRUD is a static action group with type: "index"
        // When used in index_permissions, it should NOT trigger a warning
        StaticSgConfig staticSgConfig = new StaticSgConfig(Settings.EMPTY);
        SgDynamicConfiguration<ActionGroup> staticActionGroups = staticSgConfig.addTo(
                SgDynamicConfiguration.empty(CType.ACTIONGROUPS)
        );

        ConfigurationRepository.Context contextWithStaticActionGroups =
                new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())
                        .withActionGroups(staticActionGroups);

        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    index_permissions:
                        - index_patterns: ["*"]
                          allowed_actions:
                            - "SGS_CRUD"
                            - "SGS_READ"
                            - "SGS_WRITE"
                            - "SGS_SEARCH"
                """
        ), CType.ROLES, contextWithStaticActionGroups).get();

        logsRule.assertThatNotContain("The following cluster permissions are assigned as index permissions:");
    }

    @Test
    public void shouldLogValidationWarnings_staticIndexActionGroupUsedInClusterPermissions() throws Exception {
        // SGS_CRUD is a static action group with type: "index"
        // When used in cluster_permissions, it SHOULD trigger a warning
        StaticSgConfig staticSgConfig = new StaticSgConfig(Settings.EMPTY);
        SgDynamicConfiguration<ActionGroup> staticActionGroups = staticSgConfig.addTo(
                SgDynamicConfiguration.empty(CType.ACTIONGROUPS)
        );

        ConfigurationRepository.Context contextWithStaticActionGroups =
                new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())
                        .withActionGroups(staticActionGroups);

        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    cluster_permissions:
                        - "SGS_CRUD"
                        - "cluster:monitor/main"
                """
        ), CType.ROLES, contextWithStaticActionGroups).get();

        logsRule.assertThatContainExactly("The following index permissions are assigned as cluster permissions: [SGS_CRUD]");
    }

    @Test
    public void shouldLogValidationWarnings_staticClusterActionGroupUsedInIndexPermissions() throws Exception {
        // SGS_CLUSTER_MONITOR is a static action group with type: "cluster"
        // When used in index_permissions, it SHOULD trigger a warning
        StaticSgConfig staticSgConfig = new StaticSgConfig(Settings.EMPTY);
        SgDynamicConfiguration<ActionGroup> staticActionGroups = staticSgConfig.addTo(
                SgDynamicConfiguration.empty(CType.ACTIONGROUPS)
        );

        ConfigurationRepository.Context contextWithStaticActionGroups =
                new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())
                        .withActionGroups(staticActionGroups);

        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    index_permissions:
                        - index_patterns: ["*"]
                          allowed_actions:
                            - "SGS_CLUSTER_MONITOR"
                            - "indices:data/read/*"
                """
        ), CType.ROLES, contextWithStaticActionGroups).get();

        logsRule.assertThatContainExactly("The following cluster permissions are assigned as index permissions: [SGS_CLUSTER_MONITOR]");
    }

    @Test
    public void shouldNotLogValidationWarnings_mixedStaticAndCustomActionGroupsUsedCorrectly() throws Exception {
        // Test combining static action groups with custom action groups, all used correctly
        StaticSgConfig staticSgConfig = new StaticSgConfig(Settings.EMPTY);

        // First, create custom action groups
        SgDynamicConfiguration<ActionGroup> customActionGroups = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                CUSTOM_CLUSTER_GROUP:
                    type: "cluster"
                    allowed_actions:
                        - "cluster:admin/settings/update"
                CUSTOM_INDEX_GROUP:
                    type: "index"
                    allowed_actions:
                        - "indices:data/read/get"
                """
        ), CType.ACTIONGROUPS, new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())).get();

        // Merge with static action groups
        SgDynamicConfiguration<ActionGroup> allActionGroups = staticSgConfig.addTo(customActionGroups);

        ConfigurationRepository.Context contextWithAllActionGroups =
                new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())
                        .withActionGroups(allActionGroups);

        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    cluster_permissions:
                        - "SGS_CLUSTER_COMPOSITE_OPS"
                        - "CUSTOM_CLUSTER_GROUP"
                    index_permissions:
                        - index_patterns: ["*"]
                          allowed_actions:
                            - "SGS_CRUD"
                            - "CUSTOM_INDEX_GROUP"
                """
        ), CType.ROLES, contextWithAllActionGroups).get();

        logsRule.assertThatNotContain("The following index permissions are assigned as cluster permissions:");
        logsRule.assertThatNotContain("The following cluster permissions are assigned as index permissions:");
    }

    @Test
    public void shouldLogValidationWarnings_mixedStaticAndCustomActionGroupsUsedIncorrectly() throws Exception {
        // Test combining static action groups with custom action groups, with some incorrect usage
        StaticSgConfig staticSgConfig = new StaticSgConfig(Settings.EMPTY);

        // First, create custom action groups
        SgDynamicConfiguration<ActionGroup> customActionGroups = SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                CUSTOM_CLUSTER_GROUP:
                    type: "cluster"
                    allowed_actions:
                        - "cluster:admin/settings/update"
                CUSTOM_INDEX_GROUP:
                    type: "index"
                    allowed_actions:
                        - "indices:data/read/get"
                """
        ), CType.ACTIONGROUPS, new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())).get();

        // Merge with static action groups
        SgDynamicConfiguration<ActionGroup> allActionGroups = staticSgConfig.addTo(customActionGroups);

        ConfigurationRepository.Context contextWithAllActionGroups =
                new ConfigurationRepository.Context(null, null, null, null, null, Actions.forTests())
                        .withActionGroups(allActionGroups);

        SgDynamicConfiguration.fromMap(DocNode.parse(Format.YAML).from("""
                test_role1:
                    cluster_permissions:
                        - "SGS_CRUD"
                        - "CUSTOM_INDEX_GROUP"
                    index_permissions:
                        - index_patterns: ["*"]
                          allowed_actions:
                            - "SGS_CLUSTER_MONITOR"
                            - "CUSTOM_CLUSTER_GROUP"
                """
        ), CType.ROLES, contextWithAllActionGroups).get();

        logsRule.assertThatContainExactly("The following index permissions are assigned as cluster permissions: [SGS_CRUD, CUSTOM_INDEX_GROUP]");
        logsRule.assertThatContainExactly("The following cluster permissions are assigned as index permissions: [SGS_CLUSTER_MONITOR, CUSTOM_CLUSTER_GROUP]");
    }
}
