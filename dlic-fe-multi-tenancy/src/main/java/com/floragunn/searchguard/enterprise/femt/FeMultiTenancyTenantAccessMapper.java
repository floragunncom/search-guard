/*
 * Copyright 2024 by floragunn GmbH - All rights reserved
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

    public FeMultiTenancyTenantAccessMapper(TenantManager tenantManager, TenantAuthorization tenantAuthorization, Actions actions) {
        this.tenantManager = tenantManager;
        this.tenantAuthorization = tenantAuthorization;
        this.actions = actions;
    }

    @Override
    public Map<String, Boolean> mapTenantsAccess(User user, boolean adminUser, Set<String> roles) {
        if (user == null) {
            return ImmutableMap.empty();
        }

        ImmutableMap.Builder<String, Boolean> result = new ImmutableMap.Builder<>(roles.size());
        result.put(user.getName(), true);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(user, adminUser, ImmutableSet.of(roles), null, null, false, null, null);

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

        if (! tenantManager.isTenantHeaderValid(Tenant.GLOBAL_TENANT_ID)) {
            result.remove(Tenant.GLOBAL_TENANT_ID);
        }

        if (! tenantManager.isTenantHeaderValid(User.USER_TENANT)) {
            result.remove(user.getName());
        }


        return result.build();
    }
}
