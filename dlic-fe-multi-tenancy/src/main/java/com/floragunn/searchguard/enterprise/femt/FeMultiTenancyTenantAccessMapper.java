package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.TenantAccessMapper;
import com.floragunn.searchguard.authz.TenantManager;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.user.User;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;

public class FeMultiTenancyTenantAccessMapper  implements TenantAccessMapper {

    private static final Logger log = LogManager.getLogger(FeMultiTenancyTenantAccessMapper.class);
    private final TenantManager tenantManager;
    private final TenantAuthorization tenantAuthorization;
    private final Actions actions;
    private final FeMultiTenancyConfig feMultiTenancyConfig;

    public FeMultiTenancyTenantAccessMapper(TenantManager tenantManager, TenantAuthorization tenantAuthorization,
                                            Actions actions, FeMultiTenancyConfig feMultiTenancyConfig) {
        this.tenantManager = tenantManager;
        this.tenantAuthorization = tenantAuthorization;
        this.actions = actions;
        this.feMultiTenancyConfig = feMultiTenancyConfig;
    }

    @Override
    public Map<String, Boolean> mapTenantsAccess(User user, Set<String> roles) {
        if (user == null) {
            return ImmutableMap.empty();
        }

        ImmutableMap.Builder<String, Boolean> result = new ImmutableMap.Builder<>(roles.size());
        result.put(user.getName(), true);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(user, ImmutableSet.of(roles), null, null, false, null, null);

        for (String tenant : tenantManager.getAllKnownTenantNames()) {
            try {
                boolean hasReadPermission = tenantAuthorization.hasTenantPermission(context, KibanaActionsProvider.getKibanaReadAction(actions), tenant).isOk();
                boolean hasWritePermission = tenantAuthorization.hasTenantPermission(context, KibanaActionsProvider.getKibanaWriteAction(actions), tenant).isOk();

                if (hasWritePermission) {
                    result.put(tenant, true);
                } else if (hasReadPermission) {
                    result.put(tenant, false);
                }
            } catch (PrivilegesEvaluationException e) {
                log.error("Error while evaluating privileges for " + user + " " + tenant, e);
            }
        }

        if (!feMultiTenancyConfig.isGlobalTenantEnabled()) {
            result.remove(Tenant.GLOBAL_TENANT_ID);
        }

        if (! feMultiTenancyConfig.isPrivateTenantEnabled()) {
            result.remove(user.getName());
        }


        return result.build();
    }
}
