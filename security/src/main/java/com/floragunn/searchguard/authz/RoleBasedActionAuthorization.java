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

package com.floragunn.searchguard.authz;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.floragunn.searchguard.authz.config.MultiTenancyConfigurationProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.config.templates.ExpressionEvaluationException;
import com.floragunn.codova.config.templates.Template;
import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.CheckTable;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.config.ActionGroup;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.metrics.Count;
import com.floragunn.searchsupport.cstate.metrics.CountAggregation;
import com.floragunn.searchsupport.cstate.metrics.Measurement;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.cstate.metrics.TimeAggregation;

public class RoleBasedActionAuthorization implements ActionAuthorization, ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(RoleBasedActionAuthorization.class);

    private final SgDynamicConfiguration<Role> roles;
    private final ActionGroup.FlattenedIndex actionGroups;
    private final Actions actions;
    private final TenantManager tenantManager;

    private final ClusterPermissions cluster;
    private final ClusterPermissionExclusions clusterExclusions;
    private final IndexPermissions index;
    private final IndexPermissionExclusions indexExclusions;
    private final TenantPermissions tenant;
    private final ComponentState componentState;

    private final Pattern universallyDeniedIndices;

    private final MetricsLevel metricsLevel;
    private final Measurement<?> indexActionChecks;
    private final CountAggregation indexActionCheckResults;
    private final CountAggregation indexActionCheckResults_ok;
    private final CountAggregation indexActionCheckResults_insufficient;
    private final CountAggregation indexActionCheckResults_partially;
    private final CountAggregation indexActionTypes;
    private final CountAggregation indexActionTypes_wellKnown;
    private final CountAggregation indexActionTypes_nonWellKnown;
    private final Measurement<?> tenantActionChecks;
    private final CountAggregation tenantActionCheckResults;
    private final CountAggregation tenantActionCheckResults_ok;
    private final CountAggregation tenantActionCheckResults_insufficient;

    private final TimeAggregation statefulIndexRebuild = new TimeAggregation.Milliseconds();

    private volatile StatefulIndexPermssions statefulIndex;
    private final ComponentState statefulIndexState = new ComponentState("index_permissions_stateful");

    public RoleBasedActionAuthorization(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions,
            Set<String> indices, Set<String> tenants) {
        this(roles, actionGroups, actions, indices, tenants, Pattern.blank(), MetricsLevel.NONE, MultiTenancyConfigurationProvider.DEFAULT);
    }

    public RoleBasedActionAuthorization(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions,
                                        Set<String> indices, Set<String> tenants, Pattern universallyDeniedIndices, MetricsLevel metricsLevel,
                                        MultiTenancyConfigurationProvider multiTenancyConfigurationProvider) {
        this.roles = roles;
        this.actionGroups = actionGroups;
        this.actions = actions;
        this.metricsLevel = metricsLevel;
        this.tenantManager = new TenantManager(tenants, multiTenancyConfigurationProvider);

        this.cluster = new ClusterPermissions(roles, actionGroups, actions, metricsLevel);
        this.clusterExclusions = new ClusterPermissionExclusions(roles, actionGroups, actions);
        this.index = new IndexPermissions(roles, actionGroups, actions);
        this.indexExclusions = new IndexPermissionExclusions(roles, actionGroups, actions);
        this.tenant = new TenantPermissions(roles, actionGroups, actions, this.tenantManager.getConfiguredTenantNames());
        this.universallyDeniedIndices = universallyDeniedIndices;

        this.componentState = new ComponentState("role_based_action_authorization");
        this.componentState.addParts(cluster.getComponentState(), clusterExclusions.getComponentState(), index.getComponentState(),
                indexExclusions.getComponentState(), tenant.getComponentState(), statefulIndexState);

        if (indices != null) {
            try (Meter meter = Meter.basic(metricsLevel, statefulIndexRebuild)) {
                this.statefulIndex = new StatefulIndexPermssions(roles, actionGroups, actions, indices, universallyDeniedIndices, statefulIndexState);
            }
        } else {
            this.statefulIndexState.setState(State.SUSPENDED, "no_index_information");
        }

        this.componentState.updateStateFromParts();
        this.componentState.setConfigVersion(roles.getDocVersion());

        if (metricsLevel.detailedEnabled()) {
            indexActionChecks = new TimeAggregation.Nanoseconds();
            indexActionCheckResults = new CountAggregation();
            tenantActionChecks = new TimeAggregation.Nanoseconds();
            tenantActionCheckResults = new CountAggregation();
            indexActionTypes = new CountAggregation();
        } else if (metricsLevel.basicEnabled()) {
            indexActionChecks = new CountAggregation();
            indexActionCheckResults = new CountAggregation();
            tenantActionChecks = new CountAggregation();
            tenantActionCheckResults = new CountAggregation();
            indexActionTypes = new CountAggregation();
        } else {
            indexActionChecks = CountAggregation.noop();
            indexActionCheckResults = CountAggregation.noop();
            tenantActionChecks = CountAggregation.noop();
            tenantActionCheckResults = CountAggregation.noop();
            indexActionTypes = CountAggregation.noop();
        }

        indexActionCheckResults_ok = indexActionCheckResults.getSubCount("ok");
        indexActionCheckResults_partially = indexActionCheckResults.getSubCount("partially_ok");
        indexActionCheckResults_insufficient = indexActionCheckResults.getSubCount("insufficient");
        tenantActionCheckResults_ok = tenantActionCheckResults.getSubCount("ok");
        tenantActionCheckResults_insufficient = tenantActionCheckResults.getSubCount("insufficient");
        indexActionTypes_wellKnown = indexActionTypes.getSubCount("well_known");
        indexActionTypes_nonWellKnown = indexActionTypes.getSubCount("non_well_known");

        if (metricsLevel.basicEnabled()) {
            this.componentState.addMetrics("index_action_check_results", indexActionCheckResults);
            this.componentState.addMetrics("tenant_action_check_results", tenantActionCheckResults);

            this.componentState.addMetrics("index_action_checks", indexActionChecks, "tenant_action_checks", tenantActionChecks,
                    "statful_index_rebuilds", statefulIndexRebuild);

            this.componentState.addMetrics("index_action_types", indexActionTypes);
        }
    }

    @Override
    public PrivilegesEvaluationResult hasClusterPermission(PrivilegesEvaluationContext context, Action action) throws PrivilegesEvaluationException {
        PrivilegesEvaluationResult result = clusterExclusions.contains(action, context.getMappedRoles());

        if (result.getStatus() != PrivilegesEvaluationResult.Status.PENDING) {
            return result.missingPrivileges(action);
        }

        return cluster.contains(action, context.getMappedRoles());
    }

    @Override
    public PrivilegesEvaluationResult hasIndexPermission(PrivilegesEvaluationContext context, ImmutableSet<Action> actions, ResolvedIndices resolved)
            throws PrivilegesEvaluationException {
        if (metricsLevel.basicEnabled()) {
            actions.forEach((action) -> {
                indexActionTypes.increment();
                if (action instanceof WellKnownAction) {
                    indexActionTypes_wellKnown.increment();
                } else {
                    indexActionTypes_nonWellKnown.increment();

                    if (metricsLevel.detailedEnabled()) {
                        indexActionTypes_nonWellKnown.getSubCount(action.name()).increment();
                    }
                }
            });
        }

        try (Meter meter = Meter.basic(metricsLevel, indexActionChecks)) {
            User user = context.getUser();
            ImmutableSet<String> mappedRoles = context.getMappedRoles();

            ImmutableList<PrivilegesEvaluationResult.Error> errors = this.index.initializationErrors;

            if (log.isTraceEnabled()) {
                log.trace("hasIndexPermission()\nuser: " + user + "\nactions: " + actions + "\nresolved: " + resolved);
            }

            // TODO this isBlank() creates a performance penalty, because we always skip the following block
            if (resolved.isLocalAll() && universallyDeniedIndices.isBlank()) {
                // If we have a query on all indices, first check for roles which give privileges for *. Thus, we avoid costly index resolutions

                try (Meter subMeter = meter.basic("local_all")) {
                    CheckTable<String, Action> checkTable = CheckTable.create("*", actions);

                    top: for (Action action : actions) {
                        ImmutableSet<String> rolesWithWildcardIndexPrivileges = index.actionToRolesWithWildcardIndexPrivileges.get(action);

                        if (rolesWithWildcardIndexPrivileges != null && rolesWithWildcardIndexPrivileges.containsAny(mappedRoles)) {
                            if (checkTable.check("*", action)) {
                                break top;
                            }
                        }
                    }

                    if (checkTable.isComplete() && !indexExclusions.contains(mappedRoles, actions)) {
                        indexActionCheckResults_ok.increment();
                        return PrivilegesEvaluationResult.OK;
                    }

                    if (!context.isResolveLocalAll()) {
                        indexActionCheckResults_insufficient.increment();

                        if (!checkTable.isComplete()) {
                            return PrivilegesEvaluationResult.INSUFFICIENT.reason("Insufficient privileges").with(checkTable);
                        } else {
                            return PrivilegesEvaluationResult.INSUFFICIENT.reason("Privileges excluded").with(checkTable);
                        }
                    }
                }
            }

            if (resolved.getLocalIndices().isEmpty()) {
                log.debug("No local indices; grant the request");
                indexActionCheckResults_ok.increment();

                return PrivilegesEvaluationResult.OK;
            }

            CheckTable<String, Action> checkTable = CheckTable.create(resolved.getLocalIndices(), actions);

            StatefulIndexPermssions statefulIndex = this.statefulIndex;
            PrivilegesEvaluationResult resultFromStatefulIndex = null;

            if (statefulIndex != null) {
                resultFromStatefulIndex = statefulIndex.hasPermission(user, mappedRoles, actions, resolved, context, checkTable);

                if (resultFromStatefulIndex != null) {
                    if (log.isTraceEnabled()) {
                        log.trace("resultFromStatefulIndex: " + resultFromStatefulIndex);
                    }

                    return resultFromStatefulIndex;
                }

                // Note: statefulIndex.hasPermission() modifies as a side effect the checkTable. 
                // We can carry on using this as an intermediate result and further complete checkTable below.
            }

            try (Meter subMeter = meter.basic("well_known_action_index_pattern")) {
                top: for (String role : mappedRoles) {
                    ImmutableMap<Action, IndexPattern> actionToIndexPattern = index.rolesToActionToIndexPattern.get(role);

                    if (log.isTraceEnabled()) {
                        log.trace("Role " + role + " => " + actionToIndexPattern);
                    }

                    if (actionToIndexPattern != null) {
                        for (Action action : actions) {
                            IndexPattern indexPattern = actionToIndexPattern.get(action);

                            if (indexPattern != null) {
                                for (String index : checkTable.iterateUncheckedRows(action)) {
                                    try {
                                        if (indexPattern.matches(index, user, context, subMeter) && checkTable.check(index, action)) {
                                            break top;
                                        }
                                    } catch (PrivilegesEvaluationException e) {
                                        // We can ignore these errors, as this max leads to fewer privileges than available
                                        log.error("Error while evaluating index pattern of role " + role + ". Ignoring entry", e);
                                        this.componentState.addLastException("has_index_permission", e);
                                        errors = errors.with(new PrivilegesEvaluationResult.Error("Error while evaluating index pattern", e, role));
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // If all actions are well-known, the index.rolesToActionToIndexPattern data structure that was evaluated above,
            // would have contained all the actions if privileges are provided. If there are non-well-known actions among the
            // actions, we also have to evaluate action patterns to check the authorization

            boolean allActionsWellKnown = actions.forAllApplies((a) -> a instanceof WellKnownAction);

            if (!checkTable.isComplete() && !allActionsWellKnown) {
                try (Meter subMeter = meter.basic("non_well_known_actions_index_pattern")) {
                    top: for (String role : mappedRoles) {
                        ImmutableMap<Pattern, IndexPattern> actionPatternToIndexPattern = index.rolesToActionPatternToIndexPattern.get(role);

                        if (log.isTraceEnabled()) {
                            log.trace("Role " + role + " => " + actionPatternToIndexPattern);
                        }

                        if (actionPatternToIndexPattern != null) {
                            for (Action action : actions) {
                                if (action instanceof WellKnownAction) {
                                    continue;
                                }

                                for (Map.Entry<Pattern, IndexPattern> entry : actionPatternToIndexPattern.entrySet()) {
                                    Pattern actionPattern = entry.getKey();
                                    IndexPattern indexPattern = entry.getValue();

                                    if (actionPattern.matches(action.name())) {
                                        for (String index : checkTable.iterateUncheckedRows(action)) {
                                            try {
                                                if (indexPattern.matches(index, user, context, subMeter) && checkTable.check(index, action)) {
                                                    break top;
                                                }
                                            } catch (PrivilegesEvaluationException e) {
                                                // We can ignore these errors, as this max leads to fewer privileges than available
                                                log.error("Error while evaluating index pattern. Ignoring entry", e);
                                                this.componentState.addLastException("has_index_permission", e);
                                                errors = errors
                                                        .with(new PrivilegesEvaluationResult.Error("Error while evaluating index pattern", e, role));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (log.isTraceEnabled()) {
                log.trace("Permissions before exclusions:\n" + checkTable);
            }

            checkTable.uncheckRowIf((i) -> universallyDeniedIndices.matches(i));

            if (log.isTraceEnabled()) {
                log.trace("Permissions after universallyDeniedIndices exclusions:\n" + checkTable);
            }

            indexExclusions.uncheckExclusions(checkTable, user, mappedRoles, actions, resolved, context, meter);

            if (log.isTraceEnabled()) {
                log.trace("Permissions after exclusions:\n" + checkTable);
            }

            if (checkTable.isComplete()) {
                indexActionCheckResults_ok.increment();
                return PrivilegesEvaluationResult.OK;
            }

            ImmutableSet<String> availableIndices = checkTable.getCompleteRows();

            if (!availableIndices.isEmpty()) {
                indexActionCheckResults_partially.increment();
                return PrivilegesEvaluationResult.PARTIALLY_OK.availableIndices(availableIndices, checkTable, errors);
            }

            indexActionCheckResults_insufficient.increment();
            return PrivilegesEvaluationResult.INSUFFICIENT.with(checkTable, errors)
                    .reason(resolved.getLocalIndices().size() == 1 ? "Insufficient permissions for the referenced index"
                            : "None of " + resolved.getLocalIndices().size() + " referenced indices has sufficient permissions");
        } finally {
            indexActionCheckResults.increment();
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

            if (!tenantManager.isTenantHeaderValid(requestedTenant)) {
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

    public void updateIndices(Set<String> indices) {
        StatefulIndexPermssions statefulIndex = this.statefulIndex;

        if (statefulIndex != null && statefulIndex.indices.equals(indices)) {
            return;
        }

        try (Meter meter = Meter.basic(metricsLevel, statefulIndexRebuild)) {
            this.statefulIndex = new StatefulIndexPermssions(roles, actionGroups, actions, indices, universallyDeniedIndices, statefulIndexState);
            this.componentState.updateStateFromParts();
        }
    }

    static class ClusterPermissions implements ComponentStateProvider {
        private final ImmutableMap<Action, ImmutableSet<String>> actionToRoles;
        private final ImmutableSet<String> rolesWithWildcardPermissions;
        private final ImmutableMap<String, Pattern> rolesToActionPattern;
        private final ImmutableList<PrivilegesEvaluationResult.Error> initializationErrors;
        private final ComponentState componentState;
        private final CountAggregation checks;
        private final CountAggregation nonWellKnownChecks;
        private final CountAggregation wildcardChecks;
        private final MetricsLevel metricsLevel;

        ClusterPermissions(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions, MetricsLevel metricsLevel) {
            this.componentState = new ComponentState("cluster_permissions");

            ImmutableMap.Builder<Action, ImmutableSet.Builder<String>> actionToRoles = new ImmutableMap.Builder<Action, ImmutableSet.Builder<String>>()
                    .defaultValue((k) -> new ImmutableSet.Builder<String>());
            ImmutableSet.Builder<String> rolesWithWildcardPermissions = new ImmutableSet.Builder<>();
            ImmutableMap.Builder<String, Pattern> rolesToActionPattern = new ImmutableMap.Builder<>();
            ImmutableList.Builder<PrivilegesEvaluationResult.Error> initializationErrors = new ImmutableList.Builder<>();

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();
                    ImmutableSet<String> permissions = actionGroups.resolve(role.getClusterPermissions());
                    ImmutableSet<String> excludedPermissions = actionGroups.resolve(role.getExcludeClusterPermissions());
                    Pattern excludedPattern = Pattern.createWithoutExclusions(excludedPermissions);
                    List<Pattern> patterns = new ArrayList<>();

                    if (permissions.contains("*") && excludedPermissions.isEmpty()) {
                        rolesWithWildcardPermissions.add(roleName);
                        continue;
                    }

                    for (String permission : permissions) {
                        if (Pattern.isConstant(permission)) {
                            if (!excludedPattern.matches(permission) && isActionName(permission)) {
                                actionToRoles.get(actions.get(permission)).add(roleName);
                            }
                        } else {
                            Pattern pattern = Pattern.create(permission);

                            ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.clusterActions()
                                    .matching((a) -> pattern.matches(a.name()) && !excludedPattern.matches(a.name()));

                            for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                actionToRoles.get(action).add(roleName);
                            }

                            patterns.add(pattern);
                        }
                    }

                    if (!patterns.isEmpty()) {
                        rolesToActionPattern.put(roleName, Pattern.join(patterns).excluding(excludedPattern));
                    }

                } catch (ConfigValidationException e) {
                    log.error("Invalid pattern in role: " + entry + "\nThis should have been caught before. Ignoring role.", e);
                    initializationErrors.with(new PrivilegesEvaluationResult.Error("Invalid pattern in role", e, entry.getKey()));
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                    initializationErrors.with(new PrivilegesEvaluationResult.Error("Unexpected exception while processing role", e, entry.getKey()));
                }
            }

            this.actionToRoles = actionToRoles.build(ImmutableSet.Builder::build);
            this.rolesWithWildcardPermissions = rolesWithWildcardPermissions.build();
            this.rolesToActionPattern = rolesToActionPattern.build();
            this.initializationErrors = initializationErrors.build();

            this.componentState.setConfigVersion(roles.getDocVersion());

            this.checks = CountAggregation.basic(metricsLevel);
            this.nonWellKnownChecks = checks.getSubCount("non_well_known_actions");
            this.wildcardChecks = checks.getSubCount("wildcard");
            this.metricsLevel = metricsLevel;

            if (metricsLevel.basicEnabled()) {
                this.componentState.addMetrics("checks", checks);
                this.componentState.addMetrics("action_to_roles_map", new Count(actionToRoles.size()));
                this.componentState.addMetrics("roles_to_action_pattern_map", new Count(rolesToActionPattern.size()));
            }

            if (this.initializationErrors.isEmpty()) {
                this.componentState.setInitialized();
            } else {
                this.componentState.setState(State.PARTIALLY_INITIALIZED, "contains_invalid_roles");
                this.componentState.addDetail(initializationErrors);
            }
        }

        PrivilegesEvaluationResult contains(Action action, Set<String> roles) {
            checks.increment();

            if (rolesWithWildcardPermissions.containsAny(roles)) {
                wildcardChecks.increment();
                return PrivilegesEvaluationResult.OK;
            }

            ImmutableSet<String> rolesWithPrivileges = this.actionToRoles.get(action);

            if (rolesWithPrivileges != null && rolesWithPrivileges.containsAny(roles)) {
                return PrivilegesEvaluationResult.OK;
            }

            if (!(action instanceof WellKnownAction)) {
                // WellKnownActions are guaranteed to be in the collections above

                try (Meter m = Meter.basic(MetricsLevel.BASIC, nonWellKnownChecks)) {
                    if (metricsLevel.detailedEnabled()) {
                        m.count(action.name());
                    }

                    for (String role : roles) {
                        Pattern pattern = this.rolesToActionPattern.get(role);

                        if (pattern != null && pattern.matches(action.name())) {
                            return PrivilegesEvaluationResult.OK;
                        }
                    }
                }
            }

            return PrivilegesEvaluationResult.INSUFFICIENT.with(initializationErrors).missingPrivileges(action);
        }

        @Override
        public ComponentState getComponentState() {
            return this.componentState;
        }
    }

    static class ClusterPermissionExclusions implements ComponentStateProvider {
        private final ImmutableMap<Action, ImmutableSet<String>> actionToRoles;
        private final ImmutableMap<String, Pattern> rolesToActionPattern;
        private final ImmutableList<PrivilegesEvaluationResult.Error> initializationErrors;
        private final ComponentState componentState;

        ClusterPermissionExclusions(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions) {
            this.componentState = new ComponentState("cluster_permission_exclusions");

            ImmutableMap.Builder<Action, ImmutableSet.Builder<String>> actionToRoles = new ImmutableMap.Builder<Action, ImmutableSet.Builder<String>>()
                    .defaultValue((k) -> new ImmutableSet.Builder<String>());
            ImmutableMap.Builder<String, Pattern> rolesToActionPattern = new ImmutableMap.Builder<>();
            ImmutableList.Builder<PrivilegesEvaluationResult.Error> initializationErrors = new ImmutableList.Builder<>();

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();
                    ImmutableSet<String> permissions = actionGroups.resolve(role.getExcludeClusterPermissions());
                    List<Pattern> patterns = new ArrayList<>();

                    for (String permission : permissions) {
                        if (Pattern.isConstant(permission)) {
                            actionToRoles.get(actions.get(permission)).add(roleName);
                        } else {
                            Pattern pattern = Pattern.create(permission);

                            ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.clusterActions()
                                    .matching((a) -> pattern.matches(a.name()));

                            for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                actionToRoles.get(action).add(roleName);
                            }

                            patterns.add(pattern);
                        }
                    }

                    if (!patterns.isEmpty()) {
                        rolesToActionPattern.put(roleName, Pattern.join(patterns));
                    }

                } catch (ConfigValidationException e) {
                    log.error("Invalid pattern in role: " + entry + "\nThis should have been caught before. Ignoring role.", e);
                    initializationErrors.with(new PrivilegesEvaluationResult.Error("Invalid pattern in role", e, entry.getKey()));
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                    initializationErrors.with(new PrivilegesEvaluationResult.Error("Unexpected exception while processing role", e, entry.getKey()));
                }
            }

            this.actionToRoles = actionToRoles.build(ImmutableSet.Builder::build);
            this.rolesToActionPattern = rolesToActionPattern.build();
            this.initializationErrors = initializationErrors.build();

            this.componentState.setConfigVersion(roles.getDocVersion());

            if (this.initializationErrors.isEmpty()) {
                this.componentState.setInitialized();
            } else {
                this.componentState.setState(State.PARTIALLY_INITIALIZED, "contains_invalid_roles");
                this.componentState.addDetail(this.initializationErrors);
            }
        }

        PrivilegesEvaluationResult contains(Action action, Set<String> roles) {
            ImmutableSet<String> rolesWithPrivileges = this.actionToRoles.get(action);

            if (rolesWithPrivileges != null && rolesWithPrivileges.containsAny(roles)) {
                return PrivilegesEvaluationResult.INSUFFICIENT.reason("Privilege exclusion in role " + rolesWithPrivileges.intersection(roles));
            }

            if (!(action instanceof WellKnownAction)) {
                // WellKnownActions are guaranteed to be in the collections above

                for (String role : roles) {
                    Pattern pattern = this.rolesToActionPattern.get(role);

                    if (pattern != null && pattern.matches(action.name())) {
                        return PrivilegesEvaluationResult.INSUFFICIENT.reason("Privilege exclusion in role " + role);
                    }
                }
            }

            return PrivilegesEvaluationResult.PENDING;
        }

        @Override
        public ComponentState getComponentState() {
            return this.componentState;
        }

    }

    static class IndexPermissions implements ComponentStateProvider {
        private final ImmutableMap<String, ImmutableMap<Action, IndexPattern>> rolesToActionToIndexPattern;
        private final ImmutableMap<String, ImmutableMap<Pattern, IndexPattern>> rolesToActionPatternToIndexPattern;

        private final ImmutableMap<Action, ImmutableSet<String>> actionToRolesWithWildcardIndexPrivileges;

        private final ImmutableList<PrivilegesEvaluationResult.Error> initializationErrors;
        private final ComponentState componentState;

        IndexPermissions(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions) {
            this.componentState = new ComponentState("index_permissions");

            ImmutableMap.Builder<String, ImmutableMap.Builder<Action, IndexPattern.Builder>> rolesToActionToIndexPattern = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<Action, IndexPattern.Builder>>().defaultValue(
                            (k) -> new ImmutableMap.Builder<Action, IndexPattern.Builder>().defaultValue((k2) -> new IndexPattern.Builder()));

            ImmutableMap.Builder<String, ImmutableMap.Builder<Pattern, IndexPattern.Builder>> rolesToActionPatternsToIndexPattern = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<Pattern, IndexPattern.Builder>>().defaultValue(
                            (k) -> new ImmutableMap.Builder<Pattern, IndexPattern.Builder>().defaultValue((k2) -> new IndexPattern.Builder()));

            ImmutableMap.Builder<Action, ImmutableSet.Builder<String>> actionToRolesWithWildcardIndexPrivileges = //
                    new ImmutableMap.Builder<Action, ImmutableSet.Builder<String>>().defaultValue((k) -> new ImmutableSet.Builder<String>());

            ImmutableList.Builder<PrivilegesEvaluationResult.Error> initializationErrors = new ImmutableList.Builder<>();

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();

                    for (Role.Index indexPermissions : role.getIndexPermissions()) {
                        ImmutableSet<String> permissions = actionGroups.resolve(indexPermissions.getAllowedActions());

                        for (String permission : permissions) {
                            if (Pattern.isConstant(permission)) {
                                rolesToActionToIndexPattern.get(roleName).get(actions.get(permission)).add(indexPermissions.getIndexPatterns());

                                if (indexPermissions.getIndexPatterns().getPattern().isWildcard()) {
                                    actionToRolesWithWildcardIndexPrivileges.get(actions.get(permission)).add(roleName);
                                }
                            } else {
                                Pattern actionPattern = Pattern.create(permission);

                                ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.indexActions()
                                        .matching((a) -> actionPattern.matches(a.name()));

                                for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                    rolesToActionToIndexPattern.get(roleName).get(action).add(indexPermissions.getIndexPatterns());

                                    if (indexPermissions.getIndexPatterns().getPattern().isWildcard()) {
                                        actionToRolesWithWildcardIndexPrivileges.get(action).add(roleName);
                                    }
                                }

                                rolesToActionPatternsToIndexPattern.get(roleName).get(actionPattern).add(indexPermissions.getIndexPatterns());
                            }
                        }
                    }

                } catch (ConfigValidationException e) {
                    log.error("Invalid configuration in role: " + entry + "\nThis should have been caught before. Ignoring role.", e);
                    initializationErrors.with(new PrivilegesEvaluationResult.Error("Invalid pattern in role", e, entry.getKey()));
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                    initializationErrors.with(new PrivilegesEvaluationResult.Error("Unexpected exception while processing role", e, entry.getKey()));
                }
            }

            this.rolesToActionToIndexPattern = rolesToActionToIndexPattern.build((b) -> b.build(IndexPattern.Builder::build));
            this.rolesToActionPatternToIndexPattern = rolesToActionPatternsToIndexPattern.build((b) -> b.build(IndexPattern.Builder::build));

            this.actionToRolesWithWildcardIndexPrivileges = actionToRolesWithWildcardIndexPrivileges.build(ImmutableSet.Builder::build);

            this.initializationErrors = initializationErrors.build();

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

    static class IndexPermissionExclusions implements ComponentStateProvider {
        private final ImmutableMap<String, ImmutableMap<Action, IndexPattern>> rolesToActionToIndexPattern;
        private final ImmutableMap<String, ImmutableMap<Pattern, IndexPattern>> rolesToActionPatternToIndexPattern;

        private final ImmutableMap<String, ImmutableList<Exception>> rolesToInitializationErrors;
        private final ComponentState componentState;

        IndexPermissionExclusions(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions) {
            this.componentState = new ComponentState("index_permission_exclusions");

            ImmutableMap.Builder<String, ImmutableMap.Builder<Action, IndexPattern.Builder>> rolesToActionToIndexPattern = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<Action, IndexPattern.Builder>>().defaultValue(
                            (k) -> new ImmutableMap.Builder<Action, IndexPattern.Builder>().defaultValue((k2) -> new IndexPattern.Builder()));

            ImmutableMap.Builder<String, ImmutableMap.Builder<Pattern, IndexPattern.Builder>> rolesToActionPatternsToIndexPattern = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<Pattern, IndexPattern.Builder>>().defaultValue(
                            (k) -> new ImmutableMap.Builder<Pattern, IndexPattern.Builder>().defaultValue((k2) -> new IndexPattern.Builder()));

            ImmutableMap.Builder<String, ImmutableList.Builder<Exception>> rolesToInitializationErrors = new ImmutableMap.Builder<String, ImmutableList.Builder<Exception>>()
                    .defaultValue((k) -> new ImmutableList.Builder<Exception>());

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();

                    for (Role.ExcludeIndex indexPermissions : role.getExcludeIndexPermissions()) {
                        ImmutableSet<String> permissions = actionGroups.resolve(indexPermissions.getActions());

                        for (String permission : permissions) {

                            if (Pattern.isConstant(permission)) {
                                rolesToActionToIndexPattern.get(roleName).get(actions.get(permission)).add(indexPermissions.getIndexPatterns());
                            } else {
                                Pattern actionPattern = Pattern.create(permission);

                                ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.indexActions()
                                        .matching((a) -> actionPattern.matches(a.name()));

                                for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                    rolesToActionToIndexPattern.get(roleName).get(action).add(indexPermissions.getIndexPatterns());
                                }

                                rolesToActionPatternsToIndexPattern.get(roleName).get(actionPattern).add(indexPermissions.getIndexPatterns());
                            }

                        }

                    }

                } catch (ConfigValidationException e) {
                    log.error("Invalid configuration in role: " + entry + "\nThis should have been caught before. Ignoring role.", e);
                    rolesToInitializationErrors.get(entry.getKey()).with(e);
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                    rolesToInitializationErrors.get(entry.getKey()).with(e);
                }
            }

            this.rolesToActionToIndexPattern = rolesToActionToIndexPattern.build((b) -> b.build(IndexPattern.Builder::build));
            this.rolesToActionPatternToIndexPattern = rolesToActionPatternsToIndexPattern.build((b) -> b.build(IndexPattern.Builder::build));

            this.rolesToInitializationErrors = rolesToInitializationErrors.build(ImmutableList.Builder::build);
            this.componentState.setConfigVersion(roles.getDocVersion());

            if (this.rolesToInitializationErrors.isEmpty()) {
                this.componentState.setInitialized();
            } else {
                this.componentState.setState(State.PARTIALLY_INITIALIZED, "contains_invalid_roles");
                this.componentState.setMessage("Roles with initialization errors: " + this.rolesToInitializationErrors.keySet());
                this.componentState.addDetail(rolesToInitializationErrors);
            }
        }

        boolean contains(ImmutableSet<String> mappedRoles, ImmutableSet<Action> actions) {
            boolean allActionsWellKnown = actions.forAllApplies((a) -> a instanceof WellKnownAction);

            for (String role : mappedRoles) {
                ImmutableMap<Action, IndexPattern> actionToIndexPattern = rolesToActionToIndexPattern.get(role);

                if (actionToIndexPattern != null && actionToIndexPattern.containsAny(actions)) {
                    return true;
                }

                if (!allActionsWellKnown) {
                    // We need to check the patterns only if we have non-well-known actions to test

                    ImmutableMap<Pattern, IndexPattern> actionPatternToIndexPattern = rolesToActionPatternToIndexPattern.get(role);

                    if (actionPatternToIndexPattern != null) {
                        for (Pattern pattern : actionPatternToIndexPattern.keySet()) {
                            if (actions.forAnyApplies((a) -> pattern.test(a.name()))) {
                                return true;
                            }
                        }
                    }
                }
            }

            return false;
        }

        void uncheckExclusions(CheckTable<String, Action> checkTable, User user, ImmutableSet<String> mappedRoles, ImmutableSet<Action> actions,
                ResolvedIndices resolved, PrivilegesEvaluationContext context, Meter meter) throws PrivilegesEvaluationException {

            try (Meter subMeter = meter.basic("well_known_actions_uncheck_exclusions")) {
                top: for (String role : mappedRoles) {
                    ImmutableMap<Action, IndexPattern> actionToIndexPattern = rolesToActionToIndexPattern.get(role);

                    if (actionToIndexPattern != null) {
                        for (Action action : actions) {
                            IndexPattern indexPattern = actionToIndexPattern.get(action);

                            if (indexPattern != null) {
                                for (String index : checkTable.iterateCheckedRows(action)) {
                                    if (indexPattern.matches(index, user, context, subMeter)) {
                                        checkTable.uncheck(index, action);
                                    }
                                }

                                if (checkTable.isBlank()) {
                                    break top;
                                }
                            }
                        }
                    }
                }
            }

            boolean allActionsWellKnown = actions.forAllApplies((a) -> a instanceof WellKnownAction);

            if (!checkTable.isBlank() && !allActionsWellKnown) {
                try (Meter subMeter = meter.basic("non_well_known_actions_uncheck_exclusions")) {
                    top: for (String role : mappedRoles) {
                        ImmutableMap<Pattern, IndexPattern> actionPatternToIndexPattern = rolesToActionPatternToIndexPattern.get(role);

                        if (actionPatternToIndexPattern != null) {
                            for (Action action : actions) {
                                if (action instanceof WellKnownAction) {
                                    continue;
                                }

                                for (Map.Entry<Pattern, IndexPattern> entry : actionPatternToIndexPattern.entrySet()) {
                                    Pattern actionPattern = entry.getKey();
                                    IndexPattern indexPattern = entry.getValue();

                                    if (actionPattern.matches(action.name())) {
                                        for (String index : checkTable.iterateCheckedRows(action)) {
                                            if (indexPattern.matches(index, user, context, subMeter)) {
                                                checkTable.uncheck(index, action);
                                            }
                                        }

                                        if (checkTable.isBlank()) {
                                            break top;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        @Override
        public ComponentState getComponentState() {
            return componentState;
        }
    }

    static class StatefulIndexPermssions implements ComponentStateProvider {
        private final ImmutableMap<WellKnownAction<?, ?, ?>, ImmutableMap<String, ImmutableSet<String>>> actionToIndexToRoles;
        private final ImmutableMap<WellKnownAction<?, ?, ?>, ImmutableMap<String, ImmutableSet<String>>> excludedActionToIndexToRoles;
        private final ImmutableSet<String> rolesWithTemplatedExclusions;
        private final ImmutableSet<String> indices;

        private final ImmutableMap<String, ImmutableList<Exception>> rolesToInitializationErrors;
        private final ComponentState componentState;
        private final Pattern universallyDeniedIndices;

        StatefulIndexPermssions(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions, Set<String> indexNames,
                Pattern universallyDeniedIndices, ComponentState componentState) {
            ImmutableMap.Builder<WellKnownAction<?, ?, ?>, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>> actionToIndexToRoles = //
                    new ImmutableMap.Builder<WellKnownAction<?, ?, ?>, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<String, ImmutableSet.Builder<String>>()
                                    .defaultValue((k2) -> new ImmutableSet.Builder<String>()));

            ImmutableMap.Builder<WellKnownAction<?, ?, ?>, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>> excludedActionToIndexToRoles = //
                    new ImmutableMap.Builder<WellKnownAction<?, ?, ?>, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<String, ImmutableSet.Builder<String>>()
                                    .defaultValue((k2) -> new ImmutableSet.Builder<String>()));

            ImmutableSet.Builder<String> rolesWithTemplatedExclusions = new ImmutableSet.Builder<>();

            ImmutableMap.Builder<String, ImmutableList.Builder<Exception>> rolesToInitializationErrors = new ImmutableMap.Builder<String, ImmutableList.Builder<Exception>>()
                    .defaultValue((k) -> new ImmutableList.Builder<Exception>());

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();

                    for (Role.ExcludeIndex excludedIndexPermissions : role.getExcludeIndexPermissions()) {
                        ImmutableSet<String> permissions = actionGroups.resolve(excludedIndexPermissions.getActions());

                        if (excludedIndexPermissions.getIndexPatterns().getPattern().isWildcard()) {
                            // This is handled in the static IndexPermissions object.
                            continue;
                        }

                        if (!excludedIndexPermissions.getIndexPatterns().getPatternTemplates().isEmpty()
                                || !excludedIndexPermissions.getIndexPatterns().getDateMathExpressions().isEmpty()) {
                            // This class can only work on non-templated index patterns. 
                            // If there are templated exclusions (which should be a very rare thing), we cannot do evaluation here
                            // We record the role name to indicate that this class cannot evaluate these roles
                            rolesWithTemplatedExclusions.add(roleName);
                            continue;
                        }

                        for (String permission : permissions) {
                            Pattern indexPattern = excludedIndexPermissions.getIndexPatterns().getPattern();

                            if (Pattern.isConstant(permission)) {
                                Action action = actions.get(permission);

                                if (action instanceof WellKnownAction) {
                                    for (String index : indexPattern.iterateMatching(indexNames)) {
                                        excludedActionToIndexToRoles.get((WellKnownAction<?, ?, ?>) action).get(index).add(roleName);
                                    }
                                }
                            } else {
                                Pattern pattern = Pattern.create(permission);

                                ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.indexActions()
                                        .matching((a) -> pattern.matches(a.name()));

                                for (String index : indexPattern.iterateMatching(indexNames)) {
                                    for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                        excludedActionToIndexToRoles.get(action).get(index).add(roleName);
                                    }
                                }
                            }
                        }
                    }

                    for (Role.Index indexPermissions : role.getIndexPermissions()) {
                        ImmutableSet<String> permissions = actionGroups.resolve(indexPermissions.getAllowedActions());
                        Pattern indexPattern = indexPermissions.getIndexPatterns().getPattern();

                        if (indexPattern.isWildcard()) {
                            // Wildcard index patterns are handled in the static IndexPermissions object.
                            continue;
                        }

                        if (indexPattern.isBlank()) {
                            // The pattern is likely blank because there are only templated patterns. Index patterns with templates are not handled here, but in the static IndexPermissions object
                            continue;
                        }

                        for (String permission : permissions) {
                            if (Pattern.isConstant(permission)) {
                                Action action = actions.get(permission);

                                if (action instanceof WellKnownAction) {
                                    for (String index : indexPattern.iterateMatching(indexNames)) {
                                        actionToIndexToRoles.get((WellKnownAction<?, ?, ?>) action).get(index).add(roleName);
                                    }
                                }
                            } else {
                                Pattern pattern = Pattern.create(permission);

                                ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.indexActions()
                                        .matching((a) -> pattern.matches(a.name()));

                                for (String index : indexPattern.iterateMatching(indexNames)) {
                                    for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                        actionToIndexToRoles.get(action).get(index).add(roleName);
                                    }
                                }
                            }
                        }
                    }

                } catch (ConfigValidationException e) {
                    log.error("Invalid pattern in role: " + entry + "\nThis should have been caught before. Ignoring role.", e);
                    rolesToInitializationErrors.get(entry.getKey()).with(e);
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                    rolesToInitializationErrors.get(entry.getKey()).with(e);
                }
            }

            this.actionToIndexToRoles = actionToIndexToRoles.build((b) -> b.build(ImmutableSet.Builder::build));
            this.excludedActionToIndexToRoles = excludedActionToIndexToRoles.build((b) -> b.build(ImmutableSet.Builder::build));
            this.rolesWithTemplatedExclusions = rolesWithTemplatedExclusions.build();
            this.indices = ImmutableSet.of(indexNames);

            this.universallyDeniedIndices = universallyDeniedIndices;

            this.rolesToInitializationErrors = rolesToInitializationErrors.build(ImmutableList.Builder::build);
            this.componentState = componentState;
            this.componentState.setConfigVersion(roles.getDocVersion());

            if (this.rolesToInitializationErrors.isEmpty()) {
                this.componentState.setInitialized();
                this.componentState.setMessage("Initialized with " + indices.size() + " indices");
            } else {
                this.componentState.setState(State.PARTIALLY_INITIALIZED, "contains_invalid_roles");
                this.componentState.setMessage("Roles with initialization errors: " + this.rolesToInitializationErrors.keySet());
                this.componentState.addDetail(rolesToInitializationErrors);
            }
        }

        PrivilegesEvaluationResult hasPermission(User user, ImmutableSet<String> mappedRoles, ImmutableSet<Action> actions,
                ResolvedIndices resolvedIndices, PrivilegesEvaluationContext context, CheckTable<String, Action> checkTable)
                throws PrivilegesEvaluationException {
            boolean allActionsWellKnown = actions.forAllApplies((a) -> a instanceof WellKnownAction);

            if (!allActionsWellKnown) {
                // This class can operate only on well known actions
                return null;
            }

            if (rolesWithTemplatedExclusions.containsAny(mappedRoles)) {
                // This class can only work on non-templated index patterns. 
                // If there are templated exclusions (which should be a very rare thing), we cannot do evaluation here
                return null;
            }

            top: for (Action action : actions) {
                ImmutableMap<String, ImmutableSet<String>> indexToRoles = actionToIndexToRoles.get(action);

                if (indexToRoles != null) {
                    for (String index : resolvedIndices.getLocalIndices()) {
                        ImmutableSet<String> rolesWithPrivileges = indexToRoles.get(index);

                        if (rolesWithPrivileges != null && rolesWithPrivileges.containsAny(mappedRoles)
                                && !isExcluded(action, index, user, mappedRoles, context)) {

                            if (checkTable.check(index, action)) {
                                break top;
                            }
                        }
                    }
                }
            }

            if (checkTable.isComplete()) {
                return PrivilegesEvaluationResult.OK;
            } else {
                return null;
            }
        }

        private boolean isExcluded(Action action, String index, User user, ImmutableSet<String> mappedRoles, PrivilegesEvaluationContext context) {
            if (universallyDeniedIndices.matches(index)) {
                return true;
            }

            ImmutableMap<String, ImmutableSet<String>> indexToRoles = excludedActionToIndexToRoles.get(action);

            if (indexToRoles == null) {
                return false;
            }

            ImmutableSet<String> rolesWithPrivileges = indexToRoles.get(index);

            if (rolesWithPrivileges == null) {
                return false;
            }

            return rolesWithPrivileges.containsAny(mappedRoles);
        }

        @Override
        public ComponentState getComponentState() {
            return this.componentState;
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

    static class IndexPattern {

        private final Pattern pattern;
        private final ImmutableList<Role.IndexPatterns.IndexPatternTemplate> patternTemplates;
        private final ImmutableList<Role.IndexPatterns.DateMathExpression> dateMathExpressions;

        IndexPattern(Pattern pattern, ImmutableList<Role.IndexPatterns.IndexPatternTemplate> patternTemplates,
                ImmutableList<Role.IndexPatterns.DateMathExpression> dateMathExpressions) {
            this.pattern = pattern;
            this.patternTemplates = patternTemplates;
            this.dateMathExpressions = dateMathExpressions;
        }

        public boolean matches(String index, User user, PrivilegesEvaluationContext context, Meter meter) throws PrivilegesEvaluationException {
            if (pattern.matches(index)) {
                return true;
            }

            if (!patternTemplates.isEmpty()) {
                for (Role.IndexPatterns.IndexPatternTemplate patternTemplate : this.patternTemplates) {
                    try (Meter subMeter = meter.basic("render_index_pattern_template")) {
                        Pattern pattern = context.getRenderedPattern(patternTemplate.getTemplate());

                        if (pattern.matches(index) && !patternTemplate.getExclusions().matches(index)) {
                            return true;
                        }
                    } catch (ExpressionEvaluationException e) {
                        throw new PrivilegesEvaluationException("Error while evaluating dynamic index pattern: " + patternTemplate, e);
                    }
                }
            }

            if (!dateMathExpressions.isEmpty()) {                
                // Note: The use of date math expressions in privileges is deprecated as it conceptually does not fit well. 
                // We need to conceive a replacement
                try (Meter subMeter = meter.basic("render_date_math_expression")) {
                    for (Role.IndexPatterns.DateMathExpression dateMathExpression : this.dateMathExpressions) {
                        try {
                            String resolvedExpression = com.floragunn.searchsupport.queries.DateMathExpressionResolver
                                    .resolveExpression(dateMathExpression.getDateMathExpression());
                            
                            if (!Template.containsPlaceholders(resolvedExpression)) {
                                Pattern pattern = Pattern.create(resolvedExpression);

                                if (pattern.matches(index) && !dateMathExpression.getExclusions().matches(index)) {
                                    return true;
                                }
                            } else {
                                Template<Pattern> patternTemplate = new Template<>(resolvedExpression, Pattern::create);
                                Pattern pattern = patternTemplate.render(user);

                                if (pattern.matches(index) && !dateMathExpression.getExclusions().matches(index)) {
                                    return true;
                                }
                            }
                        } catch (Exception e) {
                            throw new PrivilegesEvaluationException("Error while evaluating date math expression: " + dateMathExpression, e);
                        }
                    }
                }
            }

            return false;
        }

        @Override
        public String toString() {
            if (pattern != null && patternTemplates != null && patternTemplates.size() != 0) {
                return pattern + " " + patternTemplates;
            } else if (pattern != null) {
                return pattern.toString();
            } else if (patternTemplates != null) {
                return patternTemplates.toString();
            } else {
                return "-/-";
            }
        }

        static class Builder {
            private List<Pattern> constantPatterns = new ArrayList<>();
            private List<Role.IndexPatterns.IndexPatternTemplate> patternTemplates = new ArrayList<>();
            private List<Role.IndexPatterns.DateMathExpression> dateMathExpressions = new ArrayList<>();

            void add(Role.IndexPatterns indexPattern) {
                this.constantPatterns.add(indexPattern.getPattern());
                this.patternTemplates.addAll(indexPattern.getPatternTemplates());
                this.dateMathExpressions.addAll(indexPattern.getDateMathExpressions());
            }

            IndexPattern build() {
                return new IndexPattern(Pattern.join(constantPatterns), ImmutableList.of(patternTemplates), ImmutableList.of(dateMathExpressions));
            }
        }

    }

    public ActionGroup.FlattenedIndex getActionGroups() {
        return actionGroups;
    }

    private static boolean isActionName(String actionName) {
        return actionName.indexOf(':') != -1;
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

}
