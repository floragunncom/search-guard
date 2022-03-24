/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.searchguard.authz;

public class RoleBasedActionAuthorizationTests {
    /* TODO
     * 
    
     
    @Test
    public void testWildcardTenantMapping() throws IOException, ConfigValidationException {
        SgDynamicConfiguration<RoleV7> roles = SgDynamicConfiguration
                .fromMap(
                        ImmutableMap.of("_sg_meta", ImmutableMap.of("type", "roles", "config_version", 2), "all_access",
                                ImmutableMap.of("tenant_permissions",
                                        Arrays.asList(
                                                ImmutableMap.of("tenant_patterns", Arrays.asList("*"), "allowed_actions", Arrays.asList("*"))))),
                        CType.ROLES, -1, -1, -1, null);
        SgDynamicConfiguration<RoleMapping> rolemappings = SgDynamicConfiguration.empty();
        SgDynamicConfiguration<ActionGroupsV7> actiongroups = SgDynamicConfiguration.empty();
        SgDynamicConfiguration<TenantV7> tenants = SgDynamicConfiguration.fromMap(ImmutableMap.of("_sg_meta",
                ImmutableMap.of("type", "tenants", "config_version", 2), "my_tenant", ImmutableMap.of("description", "my tenant")), CType.TENANTS, 
                -1, -1, -1, null);
        SgDynamicConfiguration<BlocksV7> blocks = SgDynamicConfiguration.empty();

        ConfigModel configModel = new ConfigModelV7(roles, rolemappings, actiongroups, tenants, blocks, Settings.EMPTY);

        User user = User.forUser("test").searchGuardRoles("all_access").build();
        SgRoles sgRoles = configModel.getSgRoles();
        SgRoles filteredSgRoles = sgRoles.filter(configModel.mapSgRoles(user, null));
        Assert.assertEquals(ImmutableMap.of("test", true, "my_tenant", true),
                filteredSgRoles.mapTenants(user, configModel.getAllConfiguredTenantNames()));
    }
    
     */
}
