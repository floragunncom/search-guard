package com.floragunn.searchguard.enterprise.femt.tenants;

import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.user.User;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

class DefaultTenantSelector {

    private final Predicate<TenantAccessData> writeAccessOrReadAccessToExistingTenant = tenantAccessData ->
            tenantAccessData.writeAccess() || (tenantAccessData.readAccess() && tenantAccessData.exists());

    Optional<String> findDefaultTenantForUser(
            User user, Map<String, TenantAccessData> tenantsAvailableToUser, List<String> configuredPreferredTenants) {

        Optional<String> preferredGlobalOrPrivateTenant = configuredPreferredTenants
                .stream()
                //return first of preferred tenants to which: user has write access or (user has read access and tenant exists)
                .filter(tenant -> Optional.ofNullable(tenantsAvailableToUser.get(tenant))
                        .map(writeAccessOrReadAccessToExistingTenant::test)
                        .orElse(false))
                .findFirst()
                //return global tenant if user has write access or (user has read access and tenant exists)
                .or(() -> Optional.ofNullable(tenantsAvailableToUser.get(Tenant.GLOBAL_TENANT_ID))
                        .map(writeAccessOrReadAccessToExistingTenant::test)
                        .flatMap(accessible -> accessible? Optional.of(Tenant.GLOBAL_TENANT_ID) : Optional.empty()))
                //return private tenant if it's enabled
                .or(() -> Optional.ofNullable(tenantsAvailableToUser.get(user.getName()))
                        .map(tenantAccessData -> user.getName())
                );

        return preferredGlobalOrPrivateTenant
                //return first of tenants available to user to which: user has write access or (user has read access and tenant exists)
                .or(() -> tenantsAvailableToUser
                        .keySet()
                        .stream()
                        .sorted()
                        .filter(tenantName -> writeAccessOrReadAccessToExistingTenant.test(tenantsAvailableToUser.get(tenantName)))
                        .findFirst()
                );
    }

}
