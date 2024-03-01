package com.floragunn.searchguard.enterprise.femt.tenants;

import com.floragunn.searchguard.TenantSelector;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.user.User;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

class DefaultTenantSelector {


    Optional<String> findDefaultTenantForUser(
            User user, Map<String, TenantAccessData> tenantsAvailableToUser, List<String> configuredPreferredTenants) {

        Predicate<String> writeAccessOrReadAccessToExistingTenant = tenantName -> {
            TenantAccessData tenantAccessData = tenantsAvailableToUser.get(tenantName);
            return Objects.nonNull(tenantAccessData) && (tenantAccessData.writeAccess() || (tenantAccessData.readAccess() && tenantAccessData.exists()));
        };

        Optional<String> preferredGlobalOrPrivateTenant = configuredPreferredTenants
                .stream()
                //return first of preferred tenants to which: user has write access or (user has read access and tenant exists)
                .filter(writeAccessOrReadAccessToExistingTenant)
                .findFirst()
                //return global tenant if user has write access or (user has read access and tenant exists)
                .or(() -> Optional.ofNullable(Tenant.GLOBAL_TENANT_ID).filter(writeAccessOrReadAccessToExistingTenant::test))
                //return private tenant if it's enabled
                .or(() -> Optional.ofNullable(tenantsAvailableToUser.get(user.getName()))
                    .map(tenantAccessData -> user.getName()));

        return preferredGlobalOrPrivateTenant
                //return first of tenants available to user to which: user has write access or (user has read access and tenant exists)
                .or(() -> tenantsAvailableToUser
                        .keySet()
                        .stream()
                        .sorted()
                        .filter(tenantName -> writeAccessOrReadAccessToExistingTenant.test(tenantName))
                        .findFirst()
                );
    }

}
