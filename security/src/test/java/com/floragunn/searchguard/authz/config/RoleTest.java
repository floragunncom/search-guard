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
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;

public class RoleTest {
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

}
