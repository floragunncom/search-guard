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

package com.floragunn.searchguard.enterprise.femt;

import java.util.List;
import java.util.Map;

import com.floragunn.searchguard.authz.TenantManager;
import com.floragunn.searchguard.authz.config.MultiTenancyConfigurationProvider;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.config.ActionGroup;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.user.User;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FeMultiTenancyTenantAccessMapperTest {

    private static final ActionGroup.FlattenedIndex emptyActionGroups = new ActionGroup.FlattenedIndex(
            SgDynamicConfiguration.empty(CType.ACTIONGROUPS));
    private static final Actions actions = new Actions(null);

    @Mock
    private MultiTenancyConfigurationProvider multiTenancyConfigurationProvider;

    @Before
    public void setUp() throws Exception {
        when(multiTenancyConfigurationProvider.isMultiTenancyEnabled()).thenReturn(true);
        when(multiTenancyConfigurationProvider.isGlobalTenantEnabled()).thenReturn(true);
        when(multiTenancyConfigurationProvider.isPrivateTenantEnabled()).thenReturn(true);
    }
    
    @Test
    public void wildcardTenantMapping() throws Exception {
        SgDynamicConfiguration<Role> roles = SgDynamicConfiguration
                .fromMap(
                        DocNode.of("all_access",
                                DocNode.of("tenant_permissions",
                                        List.of(
                                                ImmutableMap.of("tenant_patterns", List.of("*"), "allowed_actions", List.of("*"))))),
                        CType.ROLES, null)
                .get();

        ImmutableSet<String> tenants = ImmutableSet.of("my_tenant", "test");

        TenantManager tenantManager = new TenantManager(tenants, multiTenancyConfigurationProvider);
        RoleBasedTenantAuthorization tenantAuthorization = new RoleBasedTenantAuthorization(roles, emptyActionGroups, actions, tenantManager, MetricsLevel.NONE);
        FeMultiTenancyTenantAccessMapper mapper = new FeMultiTenancyTenantAccessMapper(tenantManager, tenantAuthorization, actions);

        User user = User.forUser("user_name").searchGuardRoles("all_access").build();

        Map<String, Boolean> accessToTenants = mapper.mapTenantsAccess(user, ImmutableSet.of("all_access"));
        assertThat(accessToTenants, Matchers.aMapWithSize(3));
        assertThat(accessToTenants, Matchers.hasEntry("my_tenant", true));
        assertThat(accessToTenants, Matchers.hasEntry("test", true));
        assertThat(accessToTenants, Matchers.hasEntry(user.getName(), true));
    }

    @Test
    public void tenantMappingByName() throws Exception {
        SgDynamicConfiguration<Role> roles = SgDynamicConfiguration
                .fromMap(
                        DocNode.of("access_to_some_tenants",
                                DocNode.of("tenant_permissions",
                                        List.of(
                                                ImmutableMap.of("tenant_patterns", List.of("write_tenant"), "allowed_actions", List.of(KibanaActionsProvider.getKibanaWriteAction(actions).name())),
                                                ImmutableMap.of("tenant_patterns", List.of("read_tenant"), "allowed_actions", List.of(KibanaActionsProvider.getKibanaReadAction(actions).name()))
                                        ))),
                        CType.ROLES, null)
                .get();

        ImmutableSet<String> tenants = ImmutableSet.of("write_tenant", "read_tenant", "another_tenant");

        TenantManager tenantManager = new TenantManager(tenants, multiTenancyConfigurationProvider);
        RoleBasedTenantAuthorization tenantAuthorization = new RoleBasedTenantAuthorization(roles, emptyActionGroups, actions, tenantManager, MetricsLevel.NONE);
        FeMultiTenancyTenantAccessMapper mapper = new FeMultiTenancyTenantAccessMapper(tenantManager, tenantAuthorization, actions);

        User user = User.forUser("user_name").searchGuardRoles("access_to_some_tenants").build();

        Map<String, Boolean> accessToTenants = mapper.mapTenantsAccess(user, ImmutableSet.of("access_to_some_tenants"));
        assertThat(accessToTenants, Matchers.aMapWithSize(3));
        assertThat(accessToTenants, Matchers.hasEntry("write_tenant", true));
        assertThat(accessToTenants, Matchers.hasEntry("read_tenant", false));
        assertThat(accessToTenants, Matchers.hasEntry(user.getName(), true));
    }

    @Test
    public void shouldNotReturnPrivateTenantWhenItsDisabled() throws Exception {
        SgDynamicConfiguration<Role> roles = SgDynamicConfiguration
                .fromMap(
                        DocNode.of("access_to_some_tenants",
                                DocNode.of("tenant_permissions",
                                        List.of(
                                                ImmutableMap.of("tenant_patterns", List.of("write_tenant"), "allowed_actions", List.of(KibanaActionsProvider.getKibanaWriteAction(actions).name())),
                                                ImmutableMap.of("tenant_patterns", List.of("read_tenant"), "allowed_actions", List.of(KibanaActionsProvider.getKibanaReadAction(actions).name()))
                                        ))),
                        CType.ROLES, null)
                .get();

        ImmutableSet<String> tenants = ImmutableSet.of("write_tenant", "read_tenant", "another_tenant");

        when(multiTenancyConfigurationProvider.isPrivateTenantEnabled()).thenReturn(false);

        TenantManager tenantManager = new TenantManager(tenants, multiTenancyConfigurationProvider);
        RoleBasedTenantAuthorization tenantAuthorization = new RoleBasedTenantAuthorization(roles, emptyActionGroups, actions, tenantManager, MetricsLevel.NONE);
        FeMultiTenancyTenantAccessMapper mapper = new FeMultiTenancyTenantAccessMapper(tenantManager, tenantAuthorization, actions);

        User user = User.forUser("user_name").searchGuardRoles("access_to_some_tenants").build();

        Map<String, Boolean> accessToTenants = mapper.mapTenantsAccess(user, ImmutableSet.of("access_to_some_tenants"));
        assertThat(accessToTenants, Matchers.aMapWithSize(2));
        assertThat(accessToTenants, Matchers.hasEntry("write_tenant", true));
        assertThat(accessToTenants, Matchers.hasEntry("read_tenant", false));
    }

    @Test
    public void shouldNotReturnGlobalTenantWhenUserHasAccessButTenantIsDisabled() throws Exception {
        SgDynamicConfiguration<Role> roles = SgDynamicConfiguration
                .fromMap(
                        DocNode.of("access_to_global_tenant",
                                DocNode.of("tenant_permissions",
                                        List.of(
                                                ImmutableMap.of("tenant_patterns", List.of(Tenant.GLOBAL_TENANT_ID), "allowed_actions", List.of(KibanaActionsProvider.getKibanaWriteAction(actions).name()))
                                        ))),
                        CType.ROLES, null)
                .get();

        ImmutableSet<String> tenants = ImmutableSet.of(Tenant.GLOBAL_TENANT_ID);

        when(multiTenancyConfigurationProvider.isGlobalTenantEnabled()).thenReturn(false);

        TenantManager tenantManager = new TenantManager(tenants, multiTenancyConfigurationProvider);
        RoleBasedTenantAuthorization tenantAuthorization = new RoleBasedTenantAuthorization(roles, emptyActionGroups, actions, tenantManager, MetricsLevel.NONE);
        FeMultiTenancyTenantAccessMapper mapper = new FeMultiTenancyTenantAccessMapper(tenantManager, tenantAuthorization, actions);

        User user = User.forUser("user_name").searchGuardRoles("access_to_global_tenant").build();

        Map<String, Boolean> accessToTenants = mapper.mapTenantsAccess(user, ImmutableSet.of("access_to_global_tenant"));
        assertThat(accessToTenants, Matchers.aMapWithSize(1));
        assertThat(accessToTenants, Matchers.hasEntry(user.getName(), true));
    }

}
