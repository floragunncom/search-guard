package com.floragunn.searchguard.enterprise.femt.tenants;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.user.User;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

class DefaultTenantSelector {

    private static final Set<String> GLOBAL_TENANT_ALIASES = ImmutableSet.of("global", "__global__");
    private static final Set<String> PRIVATE_TENANT_ALIASES = ImmutableSet.of("private", "__user__");

    private final Predicate<TenantAccessData> writeAccessOrReadAccessToExistingTenant = tenantAccessData ->
            tenantAccessData.writeAccess() || (tenantAccessData.readAccess() && tenantAccessData.exists());

    Optional<String> findDefaultTenantForUser(
            User user, Map<String, TenantAccessData> tenantsAvailableToUser, List<String> configuredPreferredTenants) {


        List<String> preferredTenantsInternalNames = toTenantsInternalNames(user, configuredPreferredTenants);

        Optional<String> preferredGlobalOrPrivateTenant = preferredTenantsInternalNames
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
                        .map(tenantAccessData -> User.USER_TENANT)
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

    private List<String> toTenantsInternalNames(User user, List<String> configuredPreferredTenants) {
        return configuredPreferredTenants.stream()
                .map(tenantName -> {
                    if (GLOBAL_TENANT_ALIASES.contains(tenantName.toLowerCase())) {
                        return Tenant.GLOBAL_TENANT_ID;
                    } else if (PRIVATE_TENANT_ALIASES.contains(tenantName.toLowerCase())) {
                        return user.getName();
                    } else {
                        return tenantName;
                    }
                }).collect(Collectors.toList());
    }

}
