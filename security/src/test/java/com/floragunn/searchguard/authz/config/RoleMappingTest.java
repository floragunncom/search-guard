/*
 * Copyright 2026 floragunn GmbH
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
 */

package com.floragunn.searchguard.authz.config;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;

public class RoleMappingTest {

    @Test
    public void fromBackendRolesCaseInsensitive_shouldDefaultToFalse() throws Exception {
        AuthorizationConfig config = AuthorizationConfig.parse(DocNode.EMPTY, null).get();

        assertFalse(config.isFromBackendRolesCaseInsensitive());
    }

    @Test
    public void fromBackendRolesCaseInsensitive_shouldBeConfigurable() throws Exception {
        AuthorizationConfig config = AuthorizationConfig
                .parse(DocNode.of("role_mapping.from_backend_roles_case_insensitive", true), null).get();

        assertTrue(config.isFromBackendRolesCaseInsensitive());
    }

    @Test
    public void backendRoleMappings_shouldRemainCaseSensitiveByDefault() {
        RoleMapping.InvertedIndex subject = roleMappingIndex(false);
        User user = new User("user", Arrays.asList("BACKEND_ROLE", "FIRST_ROLE", "SECOND_ROLE"), null);

        assertThat(subject.evaluate(user, null, RoleMapping.ResolutionMode.MAPPING_ONLY), empty());
    }

    @Test
    public void backendRoleMappings_shouldUseLowerCaseBackendRolesWhenCaseInsensitive() {
        RoleMapping.InvertedIndex subject = roleMappingIndex(true);
        User user = new User("user", Arrays.asList("BACKEND_ROLE", "FIRST_ROLE", "SECOND_ROLE"), null);

        assertThat(subject.evaluate(user, null, RoleMapping.ResolutionMode.MAPPING_ONLY),
                containsInAnyOrder("role_from_backend_roles", "role_from_and_backend_roles"));
    }

    private RoleMapping.InvertedIndex roleMappingIndex(boolean fromBackendRolesCaseInsensitive) {
        RoleMapping fromBackendRoles = new RoleMapping(DocNode.EMPTY, false, false, Pattern.createUnchecked("backend_role"), Pattern.blank(),
                Pattern.blank(), null, null, null);
        RoleMapping fromAndBackendRoles = new RoleMapping(DocNode.EMPTY, false, false, Pattern.blank(), Pattern.blank(), Pattern.blank(), null,
                ImmutableSet.of(Pattern.createUnchecked("first_role"), Pattern.createUnchecked("second_role")), null);
        SgDynamicConfiguration<RoleMapping> roleMappings = SgDynamicConfiguration.of(CType.ROLESMAPPING,
                ImmutableMap.of("role_from_backend_roles", fromBackendRoles, "role_from_and_backend_roles", fromAndBackendRoles));

        return new RoleMapping.InvertedIndex(roleMappings, MetricsLevel.BASIC, fromBackendRolesCaseInsensitive);
    }
}
