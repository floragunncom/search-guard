package com.floragunn.searchguard.enterprise.femt.tenants;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.TenantSelector;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.authz.config.MultiTenancyConfigurationProvider;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class AvailableTenantService implements TenantSelector {

    private final MultiTenancyConfigurationProvider configProvider;
    private final AuthorizationService authorizationService;
    private final ThreadPool threadPool;
    private final TenantAvailabilityRepository tenantRepository;
    private final DefaultTenantSelector defaultTenantSelector;

    public AvailableTenantService(MultiTenancyConfigurationProvider configProvider, AuthorizationService authorizationService,
        ThreadPool threadPool, TenantAvailabilityRepository tenantRepository) {
        this.configProvider = requireNonNull(configProvider, "Multi tenancy config provider is required");
        this.authorizationService = requireNonNull(authorizationService, "Authorization service is required");
        this.threadPool = requireNonNull(threadPool, "Thread pool is required");
        this.tenantRepository = requireNonNull(tenantRepository, "Tenant availability repository is required");
        this.defaultTenantSelector = new DefaultTenantSelector();
    }

    public Optional<AvailableTenantData> findTenantAvailableForCurrentUser() {
        ThreadContext threadContext = threadPool.getThreadContext();
        return Optional.<User>ofNullable(threadContext.getTransient(ConfigConstants.SG_USER))
            .map(user -> {
                final TransportAddress remoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
                Set<String> internalRoles = authorizationService.getMappedRoles(user, remoteAddress);
                Map<String, Boolean> tenantsWriteAccessMap = configProvider.getTenantAccessMapper().mapTenantsAccess(user, internalRoles);
                ImmutableSet<String> exists = tenantRepository.exists(tenantsWriteAccessMap.keySet().toArray(String[]::new));
                Map<String, TenantAccessData> tenantsAccess = new HashMap<>(tenantsWriteAccessMap.size());
                tenantsWriteAccessMap.forEach((key, value) -> tenantsAccess.put(key, new TenantAccessData(true, value, exists.contains(key))));
                Optional<String> tenantSelectedByDefault = defaultTenantSelector.findDefaultTenantForUser(user, tenantsAccess, configProvider.getPreferredTenants());
                return new AvailableTenantData(
                        configProvider.isMultiTenancyEnabled(), tenantsAccess, user.getName(),
                        tenantSelectedByDefault.orElseThrow(DefaultTenantNotFoundException::new));
            });
    }

    @Override
    public Optional<String> selectTenant(User user) {
        ThreadContext threadContext = threadPool.getThreadContext();
        final TransportAddress remoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
        Set<String> internalRoles = authorizationService.getMappedRoles(user, remoteAddress);
        Map<String, Boolean> tenantsWriteAccessMap = configProvider.getTenantAccessMapper().mapTenantsAccess(user, internalRoles);
        ImmutableSet<String> exists = tenantRepository.exists(tenantsWriteAccessMap.keySet().toArray(String[]::new));
        Map<String, TenantAccessData> tenantsAccess = new HashMap<>(tenantsWriteAccessMap.size());
        tenantsWriteAccessMap.forEach((key, value) -> tenantsAccess.put(key, new TenantAccessData(true, value, exists.contains(key))));
        return defaultTenantSelector.findDefaultTenantForUser(user, tenantsAccess, configProvider.getPreferredTenants());
    }
}
