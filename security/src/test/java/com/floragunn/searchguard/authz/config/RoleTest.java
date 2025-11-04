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
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;

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

        logsRule.assertThatContainExactly(String.format("Following index permissions are assigned as cluster permissions: [%s]", "indices:admin/index_as_cluster_priv, indices:data/*"));
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

        logsRule.assertThatContainExactly(String.format("Following cluster permissions are assigned as index permissions: [%s]", "indices:data/read/search/template, cluster:admin/*, cluster:monitor/cluster_as_index_priv"));
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

        logsRule.assertThatContainExactly(String.format("Following cluster permissions are assigned as alias permissions: [%s]", "indices:data/read/search/template, cluster:admin/*, cluster:monitor/cluster_as_index_priv"));
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

        logsRule.assertThatContainExactly(String.format("Following cluster permissions are assigned as data stream permissions: [%s]", "indices:data/read/search/template, cluster:admin/*, cluster:monitor/cluster_as_index_priv"));
    }
}
