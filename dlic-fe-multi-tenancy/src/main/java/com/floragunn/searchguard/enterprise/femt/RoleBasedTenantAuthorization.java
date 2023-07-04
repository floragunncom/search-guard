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

package com.floragunn.searchguard.enterprise.femt;

import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.config.templates.ExpressionEvaluationException;
import com.floragunn.codova.config.templates.Template;
import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.PrivilegesEvaluationResult;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.config.ActionGroup;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.metrics.CountAggregation;
import com.floragunn.searchsupport.cstate.metrics.Measurement;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.cstate.metrics.TimeAggregation;

public class RoleBasedTenantAuthorization implements TenantAuthorization, ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(RoleBasedTenantAuthorization.class);
    private static final String USER_TENANT = "__user__";

    private final SgDynamicConfiguration<Role> roles;
    private final ActionGroup.FlattenedIndex actionGroups;
    private final Actions actions;
    private final ImmutableSet<String> tenants;

    private final TenantPermissions tenant;
    private final ComponentState componentState;

    private final MetricsLevel metricsLevel;
    private final Measurement<?> tenantActionChecks;
    private final CountAggregation tenantActionCheckResults;
    private final CountAggregation tenantActionCheckResults_ok;
    private final CountAggregation tenantActionCheckResults_insufficient;

    public RoleBasedTenantAuthorization(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions,
             Set<String> tenants) {
        this(roles, actionGroups, actions, tenants, MetricsLevel.NONE);
    }

    public RoleBasedTenantAuthorization(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions,
             Set<String> tenants, MetricsLevel metricsLevel) {
        this.roles = roles;
        this.actionGroups = actionGroups;
        this.actions = actions;
        this.metricsLevel = metricsLevel;
        this.tenants = ImmutableSet.of(tenants);

        this.tenant = new TenantPermissions(roles, actionGroups, actions, this.tenants);

        this.componentState = new ComponentState("role_based_tenant_authorization");
        this.componentState.addParts(tenant.getComponentState());

       

        this.componentState.updateStateFromParts();
        this.componentState.setConfigVersion(roles.getDocVersion());

        if (metricsLevel.detailedEnabled()) {
            tenantActionChecks = new TimeAggregation.Nanoseconds();
            tenantActionCheckResults = new CountAggregation();
        } else if (metricsLevel.basicEnabled()) {
            tenantActionChecks = new CountAggregation();
            tenantActionCheckResults = new CountAggregation();
        } else {
            tenantActionChecks = CountAggregation.noop();
            tenantActionCheckResults = CountAggregation.noop();
        }

        tenantActionCheckResults_ok = tenantActionCheckResults.getSubCount("ok");
        tenantActionCheckResults_insufficient = tenantActionCheckResults.getSubCount("insufficient");
        
        if (metricsLevel.basicEnabled()) {
            this.componentState.addMetrics("tenant_action_check_results", tenantActionCheckResults);
            this.componentState.addMetrics("tenant_action_checks", tenantActionChecks);
        }
    }

    @Override
    public PrivilegesEvaluationResult hasTenantPermission(PrivilegesEvaluationContext context, Action action, String requestedTenant)
            throws PrivilegesEvaluationException {
        try (Meter meter = Meter.basic(metricsLevel, tenantActionChecks)) {
            User user = context.getUser();
            ImmutableSet<String> mappedRoles = context.getMappedRoles();

            ImmutableList<PrivilegesEvaluationResult.Error> errors = this.tenant.initializationErrors;

            ImmutableMap<String, ImmutableSet<String>> tenantToRoles = tenant.actionToTenantToRoles.get(action);

            if (tenantToRoles != null) {
                ImmutableSet<String> roles = tenantToRoles.get(requestedTenant);

                if (roles != null && roles.containsAny(mappedRoles)) {
                    tenantActionCheckResults_ok.increment();
                    return PrivilegesEvaluationResult.OK;
                }
            }

            if (!isTenantValid(requestedTenant)) {
                log.info("Invalid tenant requested: {}", requestedTenant);
                tenantActionCheckResults_insufficient.increment();
                return PrivilegesEvaluationResult.INSUFFICIENT.reason("Invalid requested tenant");
            }

            try (Meter subMeter = meter.basic("action_tenant_pattern")) {
                for (String role : mappedRoles) {
                    ImmutableMap<Action, ImmutableSet<Template<Pattern>>> actionToTenantPattern = tenant.roleToActionToTenantPattern.get(role);

                    if (actionToTenantPattern != null) {
                        ImmutableSet<Template<Pattern>> tenantTemplates = actionToTenantPattern.get(action);

                        if (tenantTemplates != null) {
                            for (Template<Pattern> tenantTemplate : tenantTemplates) {
                                try (Meter subMeter2 = subMeter.basic("render_tenant_template")) {
                                    Pattern tenantPattern = tenantTemplate.render(user);

                                    if (tenantPattern.matches(requestedTenant)) {
                                        tenantActionCheckResults_ok.increment();
                                        return PrivilegesEvaluationResult.OK;
                                    }
                                } catch (ExpressionEvaluationException e) {
                                    errors = errors.with(new PrivilegesEvaluationResult.Error("Error while evaluating tenant pattern", e, role));
                                    log.error("Error while evaluating tenant privilege", e);
                                    this.componentState.addLastException("has_tenant_permission", e);
                                }
                            }
                        }
                    }
                }
            }

            tenantActionCheckResults_insufficient.increment();
            return PrivilegesEvaluationResult.INSUFFICIENT.with(errors).missingPrivileges(action);
        } finally {
            tenantActionCheckResults.increment();
        }
    }
    
    static class TenantPermissions implements ComponentStateProvider {
        private final ImmutableMap<Action, ImmutableMap<String, ImmutableSet<String>>> actionToTenantToRoles;
        private final ImmutableMap<String, ImmutableMap<Action, ImmutableSet<Template<Pattern>>>> roleToActionToTenantPattern;

        private final ImmutableList<PrivilegesEvaluationResult.Error> initializationErrors;
        private final ComponentState componentState;

        TenantPermissions(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions,
                ImmutableSet<String> tenants) {

            ImmutableMap.Builder<Action, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>> actionToTenantToRoles = //
                    new ImmutableMap.Builder<Action, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<String, ImmutableSet.Builder<String>>()
                                    .defaultValue((k2) -> new ImmutableSet.Builder<String>()));

            ImmutableMap.Builder<String, ImmutableMap.Builder<Action, ImmutableSet.Builder<Template<Pattern>>>> roleToActionToTenantPattern = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<Action, ImmutableSet.Builder<Template<Pattern>>>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<Action, ImmutableSet.Builder<Template<Pattern>>>()
                                    .defaultValue((k2) -> new ImmutableSet.Builder<Template<Pattern>>()));

            ImmutableList.Builder<PrivilegesEvaluationResult.Error> initializationErrors = new ImmutableList.Builder<>();

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();

                    for (Role.Tenant tenantPermissions : role.getTenantPermissions()) {
                        ImmutableSet<String> permissions = actionGroups.resolve(tenantPermissions.getAllowedActions());

                        for (String permission : permissions) {
                            for (Template<Pattern> tenantPatternTemplate : tenantPermissions.getTenantPatterns()) {
                                if (tenantPatternTemplate.isConstant()) {
                                    Pattern tenantPattern = tenantPatternTemplate.getConstantValue();
                                    ImmutableSet<String> matchingTenants = tenants.matching(tenantPattern);

                                    if (Pattern.isConstant(permission)) {
                                        for (String tenant : matchingTenants) {
                                            actionToTenantToRoles.get(actions.get(permission)).get(tenant).add(roleName);
                                        }
                                    } else {
                                        Pattern actionPattern = Pattern.create(permission);

                                        ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.tenantActions()
                                                .matching((a) -> actionPattern.matches(a.name()));

                                        for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                            for (String tenant : matchingTenants) {
                                                actionToTenantToRoles.get(action).get(tenant).add(roleName);
                                            }
                                        }
                                    }
                                } else {
                                    if (Pattern.isConstant(permission)) {
                                        roleToActionToTenantPattern.get(roleName).get(actions.get(permission)).add(tenantPatternTemplate);
                                    } else {
                                        Pattern actionPattern = Pattern.create(permission);

                                        ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.tenantActions()
                                                .matching((a) -> actionPattern.matches(a.name()));

                                        for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                            roleToActionToTenantPattern.get(roleName).get(action).add(tenantPatternTemplate);
                                        }
                                    }
                                }

                            }
                        }

                    }

                } catch (ConfigValidationException e) {
                    log.error("Invalid configuration in role: " + entry + "\nThis should have been caught before. Ignoring role.", e);
                    initializationErrors.with(new PrivilegesEvaluationResult.Error("Invalid configuration in role", e, entry.getKey()));
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                    initializationErrors.with(new PrivilegesEvaluationResult.Error("Unexpected exception while processing role", e, entry.getKey()));
                }
            }

            this.actionToTenantToRoles = actionToTenantToRoles.build((b) -> b.build(ImmutableSet.Builder::build));
            this.roleToActionToTenantPattern = roleToActionToTenantPattern.build((b) -> b.build(ImmutableSet.Builder::build));

            this.initializationErrors = initializationErrors.build();
            this.componentState = new ComponentState("tenant_permissions");
            this.componentState.setConfigVersion(roles.getDocVersion());

            if (this.initializationErrors.isEmpty()) {
                this.componentState.setInitialized();
            } else {
                this.componentState.setState(State.PARTIALLY_INITIALIZED, "contains_invalid_roles");
                this.componentState.addDetail(initializationErrors);
            }
        }

        @Override
        public ComponentState getComponentState() {
            return this.componentState;
        }
    }

    private boolean isTenantValid(String requestedTenant) {
        if (Tenant.GLOBAL_TENANT_ID.equals(requestedTenant) || USER_TENANT.equals(requestedTenant)) {
            return true;
        }

        return tenants.contains(requestedTenant);
    }

    public ImmutableSet<String> getTenants() {
        return tenants;
    }

    public ActionGroup.FlattenedIndex getActionGroups() {
        return actionGroups;
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
}
