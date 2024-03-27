/*
 * Copyright 2023-2024 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.enterprise.femt.tenants;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.user.User;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class DefaultTenantSelectorTest {

    private final DefaultTenantSelector tenantSelector = new DefaultTenantSelector();


    @Test
    public void shouldReturnFirstOfPreferredTenants_toWhichUserHasWriteAccessOrHasReadAccessAndTenantExists() {

        User user = User.forUser("user").build();
        List<String> preferredTenants = ImmutableList.of("tenant-1", "tenant-2", "tenant-3", "tenant-4", "tenant-5", "tenant-6", "tenant-7", "tenant-8");
        ImmutableMap<String, TenantAccessData> tenantAccess = new ImmutableMap.Builder<String, TenantAccessData>()
                .with("tenant-1", new TenantAccessData(false, false, false))
                .with("tenant-2", new TenantAccessData(false, false, true))
                .with("tenant-3", new TenantAccessData(false, true, false))
                .with("tenant-4", new TenantAccessData(false, true, true))
                .with("tenant-5", new TenantAccessData(true, false, false))
                .with("tenant-6", new TenantAccessData(true, false, true))
                .with("tenant-7", new TenantAccessData(true, true, false))
                .with("tenant-8", new TenantAccessData(true, true, true))
                .build();

        Optional<String> selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo("tenant-3"));

        tenantAccess = tenantAccess.without("tenant-3");

        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo("tenant-4"));

        tenantAccess = tenantAccess.without("tenant-4");

        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo("tenant-6"));

        tenantAccess = tenantAccess.without("tenant-6");

        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo("tenant-7"));

        tenantAccess = tenantAccess.without("tenant-7");

        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo("tenant-8"));

        tenantAccess = tenantAccess.without("tenant-8");

        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(false));
    }

    @Test
    public void shouldReturnFirstOfPreferredTenants_toWhichUserHasWriteAccessOrHasReadAccessAndTenantExists_globalTenantPreferred() {

        User user = User.forUser("user").build();
        List<String> preferredTenants = ImmutableList.of("tenant-1", "global", "tenant-2");
        ImmutableMap<String, TenantAccessData> tenantAccess = new ImmutableMap.Builder<String, TenantAccessData>()
                .with("tenant-1", new TenantAccessData(true, false, false))
                .with(Tenant.GLOBAL_TENANT_ID, new TenantAccessData(true, false, true))
                .with("tenant-2", new TenantAccessData(true, true, true))
                .build();

        Optional<String> selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo(Tenant.GLOBAL_TENANT_ID));

        preferredTenants = ImmutableList.of("tenant-1", "GlObAl", "tenant-2");

        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo(Tenant.GLOBAL_TENANT_ID));

        preferredTenants = ImmutableList.of("tenant-1", "__global__", "tenant-2");

        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo(Tenant.GLOBAL_TENANT_ID));

        preferredTenants = ImmutableList.of("tenant-1", "__gLObAl__", "tenant-2");

        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo(Tenant.GLOBAL_TENANT_ID));
    }

    @Test
    public void shouldReturnFirstOfPreferredTenants_toWhichUserHasWriteAccessOrHasReadAccessAndTenantExists_privateTenantPreferred() {

        User user = User.forUser("user").build();
        List<String> preferredTenants = ImmutableList.of("tenant-1", "private", "tenant-2");
        ImmutableMap<String, TenantAccessData> tenantAccess = new ImmutableMap.Builder<String, TenantAccessData>()
                .with("tenant-1", new TenantAccessData(true, false, false))
                .with(user.getName(), new TenantAccessData(true, false, true))
                .with("tenant-2", new TenantAccessData(true, true, true))
                .build();

        Optional<String> selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo(user.getName()));

        preferredTenants = ImmutableList.of("tenant-1", "pRiVaTe", "tenant-2");

        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo(user.getName()));

        preferredTenants = ImmutableList.of("tenant-1", "__user__", "tenant-2");

        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo(user.getName()));

        preferredTenants = ImmutableList.of("tenant-1", "__UsEr__", "tenant-2");

        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo(user.getName()));

        preferredTenants = ImmutableList.of("tenant-1", user.getName(), "tenant-2");

        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo(user.getName()));
    }

    @Test
    public void shouldReturnGlobalTenant_userHasNoAccessToPreferredTenant_butHasWriteAccessToGlobal() {

        User user = User.forUser("user").build();
        List<String> preferredTenants = ImmutableList.of("preferred-1");
        ImmutableMap<String, TenantAccessData> tenantAccess = new ImmutableMap.Builder<String, TenantAccessData>()
                .with(Tenant.GLOBAL_TENANT_ID, new TenantAccessData(false, true, false))
                .with("tenant-2", new TenantAccessData(true, false, false))
                .build();

        Optional<String> selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo(Tenant.GLOBAL_TENANT_ID));

        tenantAccess =  tenantAccess.with(Tenant.GLOBAL_TENANT_ID, new TenantAccessData(false, true, true));
        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo(Tenant.GLOBAL_TENANT_ID));
    }

    @Test
    public void shouldReturnGlobalTenant_userHasNoAccessToPreferredTenant_butHasReadAccessToGlobalAndGlobalExists() {

        User user = User.forUser("user").build();
        List<String> preferredTenants = ImmutableList.of("preferred-1");
        ImmutableMap<String, TenantAccessData> tenantAccess = new ImmutableMap.Builder<String, TenantAccessData>()
                .with(Tenant.GLOBAL_TENANT_ID, new TenantAccessData(true, false, true))
                .with("tenant-2", new TenantAccessData(true, false, false))
                .build();

        Optional<String> selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo(Tenant.GLOBAL_TENANT_ID));

        tenantAccess =  tenantAccess.with(Tenant.GLOBAL_TENANT_ID, new TenantAccessData(true, false, false));
        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(false));

        tenantAccess =  tenantAccess.with(Tenant.GLOBAL_TENANT_ID, new TenantAccessData(false, false, false));
        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(false));

        tenantAccess =  tenantAccess.with(Tenant.GLOBAL_TENANT_ID, new TenantAccessData(false, false, true));
        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(false));
    }


    @Test
    public void shouldReturnPrivateTenant_userHasNoAccessToPreferredAndGlobalTenant_butPrivateTenantIsEnabled() {

        User user = User.forUser("user").build();
        List<String> preferredTenants = ImmutableList.of("preferred-1");
        ImmutableMap<String, TenantAccessData> tenantAccess = new ImmutableMap.Builder<String, TenantAccessData>()
                .with("preferred-1", new TenantAccessData(true, false, false))
                .with(Tenant.GLOBAL_TENANT_ID, new TenantAccessData(true, false, false))
                .with(user.getName(), new TenantAccessData(false, false, false))
                .with("tenant-1", new TenantAccessData(true, false, false))
                .build();

        Optional<String> selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo(User.USER_TENANT));


        tenantAccess =  tenantAccess.without(user.getName());
        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(false));
    }


    @Test
    public void shouldReturnFirstOfTenantsAvailableToUser_toWhichUserHasWriteAccessOrReadAccessAndTenantExists() {

        User user = User.forUser("user").build();
        List<String> preferredTenants = ImmutableList.of("preferred-1");
        ImmutableMap<String, TenantAccessData> tenantAccess = new ImmutableMap.Builder<String, TenantAccessData>()
                .with("preferred-1", new TenantAccessData(true, false, false))
                .with(Tenant.GLOBAL_TENANT_ID, new TenantAccessData(true, false, false))
                .with("tenant-1", new TenantAccessData(false, false, false))
                .with("tenant-2", new TenantAccessData(false, false, true))
                .with("tenant-3", new TenantAccessData(false, true, false))
                .with("tenant-4", new TenantAccessData(false, true, true))
                .with("tenant-5", new TenantAccessData(true, false, false))
                .with("tenant-6", new TenantAccessData(true, false, true))
                .with("tenant-7", new TenantAccessData(true, true, false))
                .with("tenant-8", new TenantAccessData(true, true, true))
                .build();

        Optional<String> selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo("tenant-3"));

        tenantAccess = tenantAccess.without("tenant-3");

        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo("tenant-4"));

        tenantAccess = tenantAccess.without("tenant-4");

        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo("tenant-6"));

        tenantAccess = tenantAccess.without("tenant-6");

        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo("tenant-7"));

        tenantAccess = tenantAccess.without("tenant-7");

        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(true));
        assertThat(selectedTenant.get(), equalTo("tenant-8"));

        tenantAccess = tenantAccess.without("tenant-8");

        selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(false));
    }

    @Test
    public void shouldNotReturnAnyTenant_userDoesNotHaveWriteAccessToAnyTenant_andHasReadAccessOnlyToNotExistingTenants() {

        User user = User.forUser("user").build();
        List<String> preferredTenants = ImmutableList.of("preferred-1");
        ImmutableMap<String, TenantAccessData> tenantAccess = new ImmutableMap.Builder<String, TenantAccessData>()
                .with("preferred-1", new TenantAccessData(true, false, false))
                .with(Tenant.GLOBAL_TENANT_ID, new TenantAccessData(true, false, false))
                .with("tenant-1", new TenantAccessData(true, false, false))
                .with("tenant-2", new TenantAccessData(false, false, true))
                .build();

        Optional<String> selectedTenant = tenantSelector.findDefaultTenantForUser(user, tenantAccess, preferredTenants);
        assertThat(selectedTenant.isPresent(), equalTo(false));
    }
}
