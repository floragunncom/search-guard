/*
 * Copyright 2020-2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.authtoken;

import java.util.Set;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.ActionAuthorization;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.PrivilegesEvaluationResult;
import com.floragunn.searchguard.authz.RoleBasedActionAuthorization;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.authz.config.ActionGroup;
import com.floragunn.searchguard.authz.actions.Actions;

public class RestrictedActionAuthorization implements ActionAuthorization {

    private final ActionAuthorization base;
    private final ActionAuthorization restrictionSgRoles;
    private final RequestedPrivileges restriction;

    RestrictedActionAuthorization(ActionAuthorization base, RequestedPrivileges restriction, ActionGroup.FlattenedIndex actionGroups, Actions actions,
            Set<String> indices, Set<String> tenants) {
        this.base = base;
        this.restriction = restriction;
        this.restrictionSgRoles = new RoleBasedActionAuthorization(restriction.toRolesConfig(), actionGroups, actions, indices, tenants);
    }

    @Override
    public PrivilegesEvaluationResult hasClusterPermission(PrivilegesEvaluationContext context, Action action) throws PrivilegesEvaluationException {
        PrivilegesEvaluationResult result = restrictionSgRoles.hasClusterPermission(context.mappedRoles(RequestedPrivileges.RESTRICTION_ROLES),
                action);

        if (result.getStatus() != PrivilegesEvaluationResult.Status.OK) {
            return result.reason("Privilege was not requested for token");
        }

        return base.hasClusterPermission(context, action);
    }

    @Override
    public PrivilegesEvaluationResult hasIndexPermission(PrivilegesEvaluationContext context, ImmutableSet<Action> actions,
            ResolvedIndices resolvedIndices) throws PrivilegesEvaluationException {
        PrivilegesEvaluationResult restrictedPermission = restrictionSgRoles
                .hasIndexPermission(context.mappedRoles(RequestedPrivileges.RESTRICTION_ROLES), actions, resolvedIndices);

        if (restrictedPermission.getStatus() != PrivilegesEvaluationResult.Status.OK) {
            // Don't calculate base permission if we already know we will get an empty set
            return restrictedPermission.reason("Privilege was not requested for token");
        }

        return base.hasIndexPermission(context, actions, resolvedIndices);
    }

    @Override
    public PrivilegesEvaluationResult hasTenantPermission(PrivilegesEvaluationContext context, Action action, String requestedTenant)
            throws PrivilegesEvaluationException {
        PrivilegesEvaluationResult result = restrictionSgRoles.hasTenantPermission(context.mappedRoles(RequestedPrivileges.RESTRICTION_ROLES), action,
                requestedTenant);

        if (result.getStatus() != PrivilegesEvaluationResult.Status.OK) {
            return result.reason("Privilege was not requested for token");
        }

        return base.hasTenantPermission(context, action, requestedTenant);
    }

    @Override
    public String toString() {
        return "RestrictedActionAuthorization [base=" + base + ", restrictionSgRoles=" + restrictionSgRoles + ", restriction=" + restriction + "]";
    }

}
