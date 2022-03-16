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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.config.templates.ExpressionEvaluationException;
import com.floragunn.codova.config.templates.Template;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.CheckTable;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.Action.WellKnownAction;
import com.floragunn.searchguard.privileges.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.privileges.PrivilegesEvaluationContext;
import com.floragunn.searchguard.privileges.PrivilegesEvaluationException;
import com.floragunn.searchguard.privileges.PrivilegesEvaluationResult;
import com.floragunn.searchguard.sgconf.ActionGroups;
import com.floragunn.searchguard.sgconf.ConfigModel;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.support.Pattern;
import com.floragunn.searchguard.user.User;

public class RoleBasedActionAuthorization implements ActionAuthorization {
    private static final Logger log = LogManager.getLogger(RoleBasedActionAuthorization.class);

    private final SgDynamicConfiguration<Role> roles;
    private final ActionGroups actionGroups;
    private final Actions actions;
    private final ImmutableSet<String> tenants;

    private final ClusterPermissions cluster;
    private final ClusterPermissionExclusions clusterExclusions;
    private final IndexPermissions index;
    private final IndexPermissionExclusions indexExclusions;
    private final TenantPermissions tenant;
    private volatile StatefulIndexPermssions statefulIndex;

    public RoleBasedActionAuthorization(SgDynamicConfiguration<Role> roles, ActionGroups actionGroups, Actions actions, Set<String> indices,
            Set<String> tenants) {
        this.roles = roles;
        this.actionGroups = actionGroups;
        this.actions = actions;
        this.tenants = ImmutableSet.of(tenants);

        this.cluster = new ClusterPermissions(roles, actionGroups, actions);
        this.clusterExclusions = new ClusterPermissionExclusions(roles, actionGroups, actions);
        this.index = new IndexPermissions(roles, actionGroups, actions);
        this.indexExclusions = new IndexPermissionExclusions(roles, actionGroups, actions);
        this.tenant = new TenantPermissions(roles, actionGroups, actions, this.tenants);

        if (indices != null) {
            this.statefulIndex = new StatefulIndexPermssions(roles, actionGroups, actions, indices);
        }
    }

    @Override
    public boolean hasClusterPermission(User user, ImmutableSet<String> mappedRoles, Action action) throws PrivilegesEvaluationException {
        if (clusterExclusions.contains(action, mappedRoles)) {
            return false;
        }

        return cluster.contains(action, mappedRoles);
    }

    @Override
    public PrivilegesEvaluationResult hasIndexPermission(User user, ImmutableSet<String> mappedRoles, ImmutableSet<Action> actions,
            ResolvedIndices resolved, PrivilegesEvaluationContext context) throws PrivilegesEvaluationException {
        ImmutableSet<PrivilegesEvaluationResult.Error> errors = ImmutableSet.empty();

        if (resolved.isLocalAll()) {
            // If we have a query on all indices, first check for roles which give privileges for *. Thus, we avoid costly index resolutions

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
                return PrivilegesEvaluationResult.OK;
            }

            if (!context.isResolveLocalAll()) {
                if (!checkTable.isComplete()) {
                    return PrivilegesEvaluationResult.INSUFFICIENT.reason("Insufficient privileges").with(checkTable);
                } else {
                    return PrivilegesEvaluationResult.INSUFFICIENT.reason("Privileges excluded").with(checkTable);
                }
            }
        }

        if (resolved.getLocalIndices().isEmpty()) {
            log.debug("No local indices; grant the request");
            return PrivilegesEvaluationResult.OK;
        }

        CheckTable<String, Action> checkTable = CheckTable.create(resolved.getLocalIndices(), actions);

        StatefulIndexPermssions statefulIndex = this.statefulIndex;
        PrivilegesEvaluationResult resultFromStatefulIndex = null;

        if (statefulIndex != null) {
            resultFromStatefulIndex = statefulIndex.hasPermission(user, mappedRoles, actions, resolved, context, checkTable);

            if (resultFromStatefulIndex != null) {
                return resultFromStatefulIndex;
            }

            // Note: statefulIndex.hasPermission() modifies as a side effect the checkTable. 
            // We can carry on using this as an intermediate result and further complete checkTable below.
        }

        top: for (String role : mappedRoles) {
            ImmutableMap<Action, IndexPattern> actionToIndexPattern = index.rolesToActionToIndexPattern.get(role);

            if (actionToIndexPattern != null) {
                for (Action action : actions) {
                    IndexPattern indexPattern = actionToIndexPattern.get(action);

                    if (indexPattern != null) {
                        for (String index : checkTable.iterateUncheckedRows(action)) {
                            try {
                                if (indexPattern.matches(index, user) && checkTable.check(index, action)) {
                                    break top;
                                }
                            } catch (PrivilegesEvaluationException e) {
                                // We can ignore these errors, as this max leads to fewer privileges than available
                                log.error("Error while evaluating index pattern. Ignoring entry", e);
                            }
                        }
                    }
                }
            }
        }

        boolean allActionsWellKnown = actions.forAllApplies((a) -> a instanceof WellKnownAction);

        if (!checkTable.isComplete() && !allActionsWellKnown) {
            top: for (String role : mappedRoles) {
                ImmutableMap<Pattern, IndexPattern> actionPatternToIndexPattern = index.rolesToActionPatternToIndexPattern.get(role);

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
                                        if (indexPattern.matches(index, user) && checkTable.check(index, action)) {
                                            break top;
                                        }
                                    } catch (PrivilegesEvaluationException e) {
                                        // We can ignore these errors, as this max leads to fewer privileges than available
                                        log.error("Error while evaluating index pattern. Ignoring entry", e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        indexExclusions.uncheckExclusions(checkTable, user, mappedRoles, actions, resolved, context);

        if (checkTable.isComplete()) {
            return PrivilegesEvaluationResult.OK;
        }

        ImmutableSet<String> availableIndices = checkTable.getCompleteRows();

        if (!availableIndices.isEmpty()) {
            return PrivilegesEvaluationResult.PARTIALLY_OK.availableIndices(availableIndices, checkTable);
        }

        return PrivilegesEvaluationResult.INSUFFICIENT.with(checkTable, errors);
    }

    @Override
    public boolean hasTenantPermission(User user, String requestedTenant, ImmutableSet<String> mappedRoles, Action action,
            PrivilegesEvaluationContext context) throws PrivilegesEvaluationException {

        ImmutableMap<String, ImmutableSet<String>> tenantToRoles = tenant.actionToTenantToRoles.get(action);

        if (tenantToRoles != null) {
            ImmutableSet<String> roles = tenantToRoles.get(requestedTenant);

            if (roles != null && roles.containsAny(mappedRoles)) {
                return true;
            }
        }

        if (!isTenantValid(requestedTenant)) {
            log.info("Invalid tenant requested: {}", requestedTenant);
        }
        
        for (String role : mappedRoles) {
            ImmutableMap<Action, ImmutableSet<Template<Pattern>>> actionToTenantPattern = tenant.roleToActionToTenantPattern.get(role);

            if (actionToTenantPattern != null) {
                ImmutableSet<Template<Pattern>> tenantTemplates = actionToTenantPattern.get(action);

                if (tenantTemplates != null) {
                    for (Template<Pattern> tenantTemplate : tenantTemplates) {
                        try {
                            Pattern tenantPattern = tenantTemplate.render(user);
                            
                            if (tenantPattern.matches(requestedTenant)) {
                                return true;
                            }
                        } catch (ExpressionEvaluationException e) {
                            log.error("Error while evaluating tenant privilege", e);
                        }
                    }
                }
            }
        }
        
        return false;
    }

    public void updateIndices(Set<String> indices) {
        StatefulIndexPermssions statefulIndex = this.statefulIndex;

        if (statefulIndex != null && statefulIndex.indices.equals(indices)) {
            return;
        }

        this.statefulIndex = new StatefulIndexPermssions(roles, actionGroups, actions, indices);
    }

    private boolean isTenantValid(String requestedTenant) {

        if ("SGS_GLOBAL_TENANT".equals(requestedTenant) || ConfigModel.USER_TENANT.equals(requestedTenant)) {
            return true;
        }

        return tenants.contains(requestedTenant);
    }
    
    static class ClusterPermissions {
        private final ImmutableMap<Action, ImmutableSet<String>> actionToRoles;
        private final ImmutableSet<String> rolesWithWildcardPermissions;
        private final ImmutableMap<String, Pattern> rolesToActionPattern;

        ClusterPermissions(SgDynamicConfiguration<Role> roles, ActionGroups actionGroups, Actions actions) {
            ImmutableMap.Builder<Action, ImmutableSet.Builder<String>> actionToRoles = new ImmutableMap.Builder<Action, ImmutableSet.Builder<String>>()
                    .defaultValue((k) -> new ImmutableSet.Builder<String>());
            ImmutableSet.Builder<String> rolesWithWildcardPermissions = new ImmutableSet.Builder<>();
            ImmutableMap.Builder<String, Pattern> rolesToActionPattern = new ImmutableMap.Builder<>();

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();
                    ImmutableSet<String> permissions = actionGroups.resolve(role.getClusterPermissions());
                    ImmutableSet<String> excludedPermissions = actionGroups.resolve(role.getExcludeClusterPermissions());
                    Pattern excludedPattern = Pattern.create(excludedPermissions);
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
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                }
            }

            this.actionToRoles = actionToRoles.build(ImmutableSet.Builder::build);
            this.rolesWithWildcardPermissions = rolesWithWildcardPermissions.build();
            this.rolesToActionPattern = rolesToActionPattern.build();
        }

        boolean contains(Action action, Set<String> roles) {
            if (rolesWithWildcardPermissions.containsAny(roles)) {
                return true;
            }

            ImmutableSet<String> rolesWithPrivileges = this.actionToRoles.get(action);

            if (rolesWithPrivileges != null && rolesWithPrivileges.containsAny(roles)) {
                return true;
            }

            if (!(action instanceof WellKnownAction)) {
                // WellKnownActions are guaranteed to be in the collections above

                for (String role : roles) {
                    Pattern pattern = this.rolesToActionPattern.get(role);

                    if (pattern != null && pattern.matches(action.name())) {
                        return true;
                    }
                }
            }

            return false;
        }
    }

    static class ClusterPermissionExclusions {
        private final ImmutableMap<Action, ImmutableSet<String>> actionToRoles;
        private final ImmutableMap<String, Pattern> rolesToActionPattern;

        ClusterPermissionExclusions(SgDynamicConfiguration<Role> roles, ActionGroups actionGroups, Actions actions) {
            ImmutableMap.Builder<Action, ImmutableSet.Builder<String>> actionToRoles = new ImmutableMap.Builder<Action, ImmutableSet.Builder<String>>()
                    .defaultValue((k) -> new ImmutableSet.Builder<String>());
            ImmutableMap.Builder<String, Pattern> rolesToActionPattern = new ImmutableMap.Builder<>();

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
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                }
            }

            this.actionToRoles = actionToRoles.build(ImmutableSet.Builder::build);
            this.rolesToActionPattern = rolesToActionPattern.build();
        }

        boolean contains(Action action, Set<String> roles) {
            ImmutableSet<String> rolesWithPrivileges = this.actionToRoles.get(action);

            if (rolesWithPrivileges != null && rolesWithPrivileges.containsAny(roles)) {
                return true;
            }

            if (!(action instanceof WellKnownAction)) {
                // WellKnownActions are guaranteed to be in the collections above

                for (String role : roles) {
                    Pattern pattern = this.rolesToActionPattern.get(role);

                    if (pattern != null && pattern.matches(action.name())) {
                        return true;
                    }
                }
            }

            return false;
        }

    }

    static class IndexPermissions {
        private final ImmutableMap<String, ImmutableMap<Action, IndexPattern>> rolesToActionToIndexPattern;
        private final ImmutableMap<String, ImmutableMap<Pattern, IndexPattern>> rolesToActionPatternToIndexPattern;

        private final ImmutableMap<Action, ImmutableSet<String>> actionToRolesWithWildcardIndexPrivileges;

        IndexPermissions(SgDynamicConfiguration<Role> roles, ActionGroups actionGroups, Actions actions) {

            ImmutableMap.Builder<String, ImmutableMap.Builder<Action, IndexPattern.Builder>> rolesToActionToIndexPattern = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<Action, IndexPattern.Builder>>().defaultValue(
                            (k) -> new ImmutableMap.Builder<Action, IndexPattern.Builder>().defaultValue((k2) -> new IndexPattern.Builder()));

            ImmutableMap.Builder<String, ImmutableMap.Builder<Pattern, IndexPattern.Builder>> rolesToActionPatternsToIndexPattern = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<Pattern, IndexPattern.Builder>>().defaultValue(
                            (k) -> new ImmutableMap.Builder<Pattern, IndexPattern.Builder>().defaultValue((k2) -> new IndexPattern.Builder()));

            ImmutableMap.Builder<Action, ImmutableSet.Builder<String>> actionToRolesWithWildcardIndexPrivileges = //
                    new ImmutableMap.Builder<Action, ImmutableSet.Builder<String>>().defaultValue((k) -> new ImmutableSet.Builder<String>());

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();

                    for (Role.Index indexPermissions : role.getIndexPermissions()) {
                        ImmutableSet<String> permissions = actionGroups.resolve(indexPermissions.getAllowedActions());

                        for (String permission : permissions) {
                            for (Template<Pattern> indexPattern : indexPermissions.getIndexPatterns()) {

                                if (Pattern.isConstant(permission)) {
                                    rolesToActionToIndexPattern.get(roleName).get(actions.get(permission)).add(indexPattern);

                                    if (indexPattern.isConstant() && indexPattern.getConstantValue().isWildcard()) {
                                        actionToRolesWithWildcardIndexPrivileges.get(actions.get(permission)).add(roleName);
                                    }
                                } else {
                                    Pattern actionPattern = Pattern.create(permission);

                                    ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.indexActions()
                                            .matching((a) -> actionPattern.matches(a.name()));

                                    for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                        rolesToActionToIndexPattern.get(roleName).get(action).add(indexPattern);

                                        if (indexPattern.isConstant() && indexPattern.getConstantValue().isWildcard()) {
                                            actionToRolesWithWildcardIndexPrivileges.get(action).add(roleName);
                                        }
                                    }

                                    rolesToActionPatternsToIndexPattern.get(roleName).get(actionPattern).add(indexPattern);
                                }
                            }
                        }

                    }

                } catch (ConfigValidationException e) {
                    log.error("Invalid configuration in role: " + entry + "\nThis should have been caught before. Ignoring role.", e);
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                }
            }

            this.rolesToActionToIndexPattern = rolesToActionToIndexPattern.build((b) -> b.build(IndexPattern.Builder::build));
            this.rolesToActionPatternToIndexPattern = rolesToActionPatternsToIndexPattern.build((b) -> b.build(IndexPattern.Builder::build));

            this.actionToRolesWithWildcardIndexPrivileges = actionToRolesWithWildcardIndexPrivileges.build(ImmutableSet.Builder::build);
        }

    }

    static class IndexPermissionExclusions {
        private final ImmutableMap<String, ImmutableMap<Action, IndexPattern>> rolesToActionToIndexPattern;
        private final ImmutableMap<String, ImmutableMap<Pattern, IndexPattern>> rolesToActionPatternToIndexPattern;

        IndexPermissionExclusions(SgDynamicConfiguration<Role> roles, ActionGroups actionGroups, Actions actions) {

            ImmutableMap.Builder<String, ImmutableMap.Builder<Action, IndexPattern.Builder>> rolesToActionToIndexPattern = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<Action, IndexPattern.Builder>>().defaultValue(
                            (k) -> new ImmutableMap.Builder<Action, IndexPattern.Builder>().defaultValue((k2) -> new IndexPattern.Builder()));

            ImmutableMap.Builder<String, ImmutableMap.Builder<Pattern, IndexPattern.Builder>> rolesToActionPatternsToIndexPattern = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<Pattern, IndexPattern.Builder>>().defaultValue(
                            (k) -> new ImmutableMap.Builder<Pattern, IndexPattern.Builder>().defaultValue((k2) -> new IndexPattern.Builder()));

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();

                    for (Role.ExcludeIndex indexPermissions : role.getExcludeIndexPermissions()) {
                        ImmutableSet<String> permissions = actionGroups.resolve(indexPermissions.getActions());

                        for (String permission : permissions) {
                            for (Template<Pattern> indexPattern : indexPermissions.getIndexPatterns()) {

                                if (Pattern.isConstant(permission)) {
                                    rolesToActionToIndexPattern.get(roleName).get(actions.get(permission)).add(indexPattern);
                                } else {
                                    Pattern actionPattern = Pattern.create(permission);

                                    ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.indexActions()
                                            .matching((a) -> actionPattern.matches(a.name()));

                                    for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                        rolesToActionToIndexPattern.get(roleName).get(action).add(indexPattern);
                                    }

                                    rolesToActionPatternsToIndexPattern.get(roleName).get(actionPattern).add(indexPattern);
                                }
                            }
                        }

                    }

                } catch (ConfigValidationException e) {
                    log.error("Invalid configuration in role: " + entry + "\nThis should have been caught before. Ignoring role.", e);
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                }
            }

            this.rolesToActionToIndexPattern = rolesToActionToIndexPattern.build((b) -> b.build(IndexPattern.Builder::build));
            this.rolesToActionPatternToIndexPattern = rolesToActionPatternsToIndexPattern.build((b) -> b.build(IndexPattern.Builder::build));

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
                ResolvedIndices resolved, PrivilegesEvaluationContext context) throws PrivilegesEvaluationException {
            boolean allActionsWellKnown = actions.forAllApplies((a) -> a instanceof WellKnownAction);

            top: for (String role : mappedRoles) {
                ImmutableMap<Action, IndexPattern> actionToIndexPattern = rolesToActionToIndexPattern.get(role);

                if (actionToIndexPattern != null) {
                    for (Action action : actions) {
                        IndexPattern indexPattern = actionToIndexPattern.get(action);

                        if (indexPattern != null) {
                            for (String index : checkTable.iterateCheckedRows(action)) {
                                if (indexPattern.matches(index, user)) {
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

            if (!checkTable.isBlank() && !allActionsWellKnown) {
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
                                        if (indexPattern.matches(index, user)) {
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

    static class StatefulIndexPermssions {
        private final ImmutableMap<WellKnownAction<?, ?, ?>, ImmutableMap<String, ImmutableSet<String>>> actionToIndexToRoles;
        private final ImmutableMap<WellKnownAction<?, ?, ?>, ImmutableMap<String, ImmutableSet<String>>> excludedActionToIndexToRoles;
        private final ImmutableSet<String> rolesWithTemplatedExclusions;
        private final ImmutableSet<String> indices;

        StatefulIndexPermssions(SgDynamicConfiguration<Role> roles, ActionGroups actionGroups, Actions actions, Set<String> indexNames) {
            ImmutableMap.Builder<WellKnownAction<?, ?, ?>, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>> actionToIndexToRoles = //
                    new ImmutableMap.Builder<WellKnownAction<?, ?, ?>, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<String, ImmutableSet.Builder<String>>()
                                    .defaultValue((k2) -> new ImmutableSet.Builder<String>()));

            ImmutableMap.Builder<WellKnownAction<?, ?, ?>, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>> excludedActionToIndexToRoles = //
                    new ImmutableMap.Builder<WellKnownAction<?, ?, ?>, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<String, ImmutableSet.Builder<String>>()
                                    .defaultValue((k2) -> new ImmutableSet.Builder<String>()));

            ImmutableSet.Builder<String> rolesWithTemplatedExclusions = new ImmutableSet.Builder<>();

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();

                    for (Role.ExcludeIndex excludedIndexPermissions : role.getExcludeIndexPermissions()) {
                        ImmutableSet<String> permissions = actionGroups.resolve(excludedIndexPermissions.getActions());

                        if (excludedIndexPermissions.getIndexPatterns().forAnyApplies((p) -> p.isConstant() && p.getConstantValue().isWildcard())) {
                            // This is handled in the static IndexPermissions object.
                            continue;
                        }

                        for (String permission : permissions) {
                            for (Template<Pattern> indexPatternTemplate : excludedIndexPermissions.getIndexPatterns()) {
                                if (!indexPatternTemplate.isConstant()) {
                                    rolesWithTemplatedExclusions.add(roleName);
                                    continue;
                                }

                                Pattern indexPattern = indexPatternTemplate.getConstantValue();

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

                    }

                    for (Role.Index indexPermissions : role.getIndexPermissions()) {
                        ImmutableSet<String> permissions = actionGroups.resolve(indexPermissions.getAllowedActions());

                        if (indexPermissions.getIndexPatterns().forAnyApplies((p) -> p.isConstant() && p.getConstantValue().isWildcard())) {
                            // This is handled in the static IndexPermissions object.
                            continue;
                        }

                        for (String permission : permissions) {
                            for (Template<Pattern> indexPatternTemplate : indexPermissions.getIndexPatterns()) {
                                if (!indexPatternTemplate.isConstant()) {
                                    continue;
                                }

                                Pattern indexPattern = indexPatternTemplate.getConstantValue();

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
                    }

                } catch (ConfigValidationException e) {
                    log.error("Invalid pattern in role: " + entry + "\nThis should have been caught before. Ignoring role.", e);
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                }
            }

            this.actionToIndexToRoles = actionToIndexToRoles.build((b) -> b.build(ImmutableSet.Builder::build));
            this.excludedActionToIndexToRoles = excludedActionToIndexToRoles.build((b) -> b.build(ImmutableSet.Builder::build));
            this.rolesWithTemplatedExclusions = rolesWithTemplatedExclusions.build();
            this.indices = ImmutableSet.of(indexNames);
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

    }

    static class TenantPermissions {
        private final ImmutableMap<Action, ImmutableMap<String, ImmutableSet<String>>> actionToTenantToRoles;
        private final ImmutableMap<String, ImmutableMap<Action, ImmutableSet<Template<Pattern>>>> roleToActionToTenantPattern;

        TenantPermissions(SgDynamicConfiguration<Role> roles, ActionGroups actionGroups, Actions actions, ImmutableSet<String> tenants) {

            ImmutableMap.Builder<Action, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>> actionToTenantToRoles = //
                    new ImmutableMap.Builder<Action, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<String, ImmutableSet.Builder<String>>()
                                    .defaultValue((k2) -> new ImmutableSet.Builder<String>()));

            ImmutableMap.Builder<String, ImmutableMap.Builder<Action, ImmutableSet.Builder<Template<Pattern>>>> roleToActionToTenantPattern = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<Action, ImmutableSet.Builder<Template<Pattern>>>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<Action, ImmutableSet.Builder<Template<Pattern>>>()
                                    .defaultValue((k2) -> new ImmutableSet.Builder<Template<Pattern>>()));

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
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                }
            }

            this.actionToTenantToRoles = actionToTenantToRoles.build((b) -> b.build(ImmutableSet.Builder::build));
            this.roleToActionToTenantPattern = roleToActionToTenantPattern.build((b) -> b.build(ImmutableSet.Builder::build));
        }

    }

    static class IndexPattern {

        private final Pattern pattern;
        private final ImmutableList<Template<Pattern>> patternTemplates;

        IndexPattern(Pattern pattern, ImmutableList<Template<Pattern>> patternTemplates) {
            this.pattern = pattern;
            this.patternTemplates = patternTemplates;
        }

        public boolean matches(String index, User user) throws PrivilegesEvaluationException {
            if (pattern.matches(index)) {
                return true;
            }

            if (!patternTemplates.isEmpty()) {
                for (Template<Pattern> patternTemplate : this.patternTemplates) {
                    try {
                        Pattern pattern = patternTemplate.render(user);

                        if (pattern.matches(index)) {
                            return true;
                        }
                    } catch (ExpressionEvaluationException e) {
                        throw new PrivilegesEvaluationException("Error while evaluating dynamic index pattern: " + patternTemplate, e);
                    }
                }
            }

            return false;
        }

        static class Builder {
            private List<Pattern> constantPatterns = new ArrayList<>();
            private List<Template<Pattern>> patternTemplates = new ArrayList<>();

            void add(Template<Pattern> patternTemplate) {
                if (patternTemplate.isConstant()) {
                    constantPatterns.add(patternTemplate.getConstantValue());
                } else {
                    patternTemplates.add(patternTemplate);
                }
            }

            IndexPattern build() {
                return new IndexPattern(Pattern.join(constantPatterns), ImmutableList.of(patternTemplates));
            }
        }

    }

    public ImmutableSet<String> getTenants() {
        return tenants;
    }

    public ActionGroups getActionGroups() {
        return actionGroups;
    }
    
    
    private static boolean isActionName(String actionName) {
        return actionName.indexOf(':') != -1;
    }


}
