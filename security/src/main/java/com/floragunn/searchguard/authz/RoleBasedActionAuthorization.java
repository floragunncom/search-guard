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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.authz.Action.WellKnownAction;
import com.floragunn.searchguard.privileges.PrivilegesEvaluationResult;
import com.floragunn.searchguard.privileges.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.sgconf.ActionGroups;
import com.floragunn.searchguard.sgconf.ConfigModelV7.IndexPattern;
import com.floragunn.searchguard.sgconf.ConfigModelV7.SgRole;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.support.Pattern;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.StringInterpolationException;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.util.CheckTable;
import com.floragunn.searchsupport.util.ImmutableMap;
import com.floragunn.searchsupport.util.ImmutableSet;

public class RoleBasedActionAuthorization implements ActionAuthorization {
    private static final Logger log = LogManager.getLogger(RoleBasedActionAuthorization.class);

    private final Cluster cluster;

    public RoleBasedActionAuthorization(SgDynamicConfiguration<Role> roles, ActionGroups actionGroups, Actions actions,
            org.elasticsearch.cluster.metadata.Metadata clusterStateMetadata) {
        this.cluster = new Cluster(roles, actionGroups, actions);
    }

    @Override
    public boolean hasClusterPermission(Action action, User user, Set<String> mappedRoles) {
        return cluster.hasPermission(action, mappedRoles);
    }

    @Override
    public boolean hasIndexPermission(Action action, ResolvedIndices resolvedIndices, User user, Set<String> mappedRoles) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean hasTenantPermission(Action action, String requestedTenant, User user, Set<String> mappedRoles) {
        // TODO Auto-generated method stub
        return false;
    }

    static class Cluster {
        private final ImmutableMap<Action, ImmutableSet<String>> actionToRoles;
        private final ImmutableSet<String> rolesWithWildcardPermissions;
        private final ImmutableMap<String, Pattern> rolesToActionPattern;

        // TODO rework exclusions to global

        Cluster(SgDynamicConfiguration<Role> roles, ActionGroups actionGroups, Actions actions) {
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
                            if (!excludedPattern.matches(permission)) {
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

        private boolean hasPermission(Action action, Set<String> roles) {
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

    static class Index {
        private final ImmutableMap<Action, ImmutableMap<String, ImmutableSet<String>>> actionToIndexToRoles;
        private final ImmutableMap<Action, ImmutableSet<String>> actionToRolesWithWildcardIndexPrivileges;

        private final ImmutableSet<String> rolesWithWildcardPermissions;
        private final ImmutableMap<String, ImmutableMap<String, Pattern>> rolesToIndexToActionPattern;
        private final ImmutableMap<String, Pattern> rolesWithWildcardIndexPrivilegesToActionPattern;

        private final ImmutableMap<Action, ImmutableMap<String, ImmutableSet<String>>> excludedActionToIndexToRoles;
        private final ImmutableMap<String, ImmutableMap<String, Pattern>> rolesToIndexToExcludedActionPattern;

        Index(SgDynamicConfiguration<Role> roles, ActionGroups actionGroups, Actions actions,
                org.elasticsearch.cluster.metadata.Metadata clusterStateMetadata) {
            ImmutableMap.Builder<Action, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>> actionToIndexToRoles = //
                    new ImmutableMap.Builder<Action, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<String, ImmutableSet.Builder<String>>()
                                    .defaultValue((k2) -> new ImmutableSet.Builder<String>()));

            ImmutableMap.Builder<Action, ImmutableSet.Builder<String>> actionToRolesWithWildcardIndexPrivileges = //
                    new ImmutableMap.Builder<Action, ImmutableSet.Builder<String>>().defaultValue((k) -> new ImmutableSet.Builder<String>());

            ImmutableSet.Builder<String> rolesWithWildcardPermissions = new ImmutableSet.Builder<>();
            ImmutableMap.Builder<String, ImmutableMap.Builder<String, Pattern>> rolesToIndexToActionPattern = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<String, Pattern>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<String, Pattern>());
            ImmutableMap.Builder<String, Pattern> rolesWithWildcardIndexPrivilegesToActionPattern = //
                    new ImmutableMap.Builder<String, Pattern>();

            ImmutableMap.Builder<Action, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>> excludedActionToIndexToRoles = //
                    new ImmutableMap.Builder<Action, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<String, ImmutableSet.Builder<String>>()
                                    .defaultValue((k2) -> new ImmutableSet.Builder<String>()));
            ImmutableMap.Builder<String, ImmutableMap.Builder<String, Pattern>> rolesToIndexToExcludedActionPattern = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<String, Pattern>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<String, Pattern>());

            Set<String> indexNames = clusterStateMetadata.indices().keySet();

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();

                    for (Role.ExcludeIndex excludedIndexPermissions : role.getExcludeIndexPermissions()) {
                        ImmutableSet<String> permissions = actionGroups.resolve(excludedIndexPermissions.getActions());
                        List<Pattern> patterns = new ArrayList<>();
                        Set<String> indices = new HashSet<>();

                        if (excludedIndexPermissions.getIndexPatterns().contains("*")) {
                            // TODO
                        }

                        for (String permission : permissions) {
                            for (String indexPattern : excludedIndexPermissions.getIndexPatterns()) {
                                boolean containsPlaceholder = indexPattern.contains("${");

                                if (containsPlaceholder) {

                                } else {
                                    if (Pattern.isConstant(permission)) {
                                        for (String index : Pattern.create(indexPattern).iterateMatching(indexNames)) {
                                            excludedActionToIndexToRoles.get(actions.get(permission)).get(index).add(roleName);
                                        }
                                    } else {
                                        Pattern pattern = Pattern.create(permission);

                                        ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.indexActions()
                                                .matching((a) -> pattern.matches(a.name()));

                                        for (String index : Pattern.create(indexPattern).iterateMatching(indexNames)) {
                                            for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                                excludedActionToIndexToRoles.get(action).get(index).add(roleName);
                                            }

                                            patterns.add(pattern);
                                            indices.add(index);
                                        }
                                    }
                                }
                            }
                        }

                        if (!patterns.isEmpty()) {
                            for (String index : indices) {
                                rolesToIndexToExcludedActionPattern.get(roleName).put(index, Pattern.join(patterns));
                            }
                        }
                    }

                    for (Role.Index indexPermissions : role.getIndexPermissions()) {
                        ImmutableSet<String> permissions = actionGroups.resolve(indexPermissions.getAllowedActions());

                        if (permissions.contains("*") && indexPermissions.getIndexPatterns().contains("*")) {
                            rolesWithWildcardPermissions.add(roleName);
                            continue;
                        }

                        if (indexPermissions.getIndexPatterns().contains("*")) {
                            List<Pattern> patterns = new ArrayList<>();

                            for (String permission : permissions) {
                                if (Pattern.isConstant(permission)) {
                                    actionToRolesWithWildcardIndexPrivileges.get(actions.get(permission)).add(roleName);
                                } else {
                                    Pattern pattern = Pattern.create(permission);

                                    ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.indexActions()
                                            .matching((a) -> pattern.matches(a.name()));

                                    for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                        actionToRolesWithWildcardIndexPrivileges.get(action).add(roleName);
                                    }

                                    patterns.add(pattern);
                                }
                            }

                            rolesWithWildcardIndexPrivilegesToActionPattern.put(roleName, Pattern.join(patterns));

                        } else {

                            List<Pattern> patterns = new ArrayList<>();
                            Set<String> indices = new HashSet<>();

                            for (String permission : permissions) {
                                for (String indexPattern : indexPermissions.getIndexPatterns()) {
                                    boolean containsPlaceholder = indexPattern.contains("${");

                                    if (containsPlaceholder) {

                                    } else {
                                        if (Pattern.isConstant(permission)) {
                                            for (String index : Pattern.create(indexPattern).iterateMatching(indexNames)) {
                                                actionToIndexToRoles.get(actions.get(permission)).get(index).add(roleName);
                                            }

                                            // TODO also store index pattern for operations which work on non-existing indices
                                        } else {
                                            Pattern pattern = Pattern.create(permission);

                                            ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.indexActions()
                                                    .matching((a) -> pattern.matches(a.name()));

                                            for (String index : Pattern.create(indexPattern).iterateMatching(indexNames)) {
                                                for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                                    actionToIndexToRoles.get(action).get(index).add(roleName);
                                                }

                                                patterns.add(pattern);
                                                indices.add(index);
                                            }

                                        }

                                    }
                                }
                            }

                            if (!patterns.isEmpty()) {

                                for (String index : indices) {
                                    rolesToIndexToActionPattern.get(roleName).put(index, Pattern.join(patterns));
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
            this.actionToRolesWithWildcardIndexPrivileges = actionToRolesWithWildcardIndexPrivileges.build(ImmutableSet.Builder::build);
            this.rolesWithWildcardPermissions = rolesWithWildcardPermissions.build();
            this.rolesToIndexToActionPattern = rolesToIndexToActionPattern.build(ImmutableMap.Builder::build);
            this.rolesWithWildcardIndexPrivilegesToActionPattern = rolesWithWildcardIndexPrivilegesToActionPattern.build();
            this.excludedActionToIndexToRoles = excludedActionToIndexToRoles.build((b) -> b.build(ImmutableSet.Builder::build));
            this.rolesToIndexToExcludedActionPattern = rolesToIndexToExcludedActionPattern.build(ImmutableMap.Builder::build);
        }

        private PrivilegesEvaluationResult hasPermission(Set<Action> actions, ResolvedIndices resolvedIndices, User user, Set<String> mappedRoles) {
            ImmutableSet<PrivilegesEvaluationResult.Error> errors = ImmutableSet.empty();

            if (resolvedIndices.isLocalAll()) {
                // If we have a query on all indices, first check for roles which give privileges for *. Thus, we avoid costly index resolution

                CheckTable<String, Action> checkTable = CheckTable.create("*", actions);

                top: for (Action action : actions) {
                    ImmutableSet<String> rolesWithWildcardIndex = actionToRolesWithWildcardIndexPrivileges.get(action);

                    if (rolesWithWildcardIndex != null && rolesWithWildcardIndex.containsAny(mappedRoles)) {
                        if (checkTable.check("*", action)) {
                            break;
                        }
                    } else if (!(action instanceof WellKnownAction)) {
                        // Actions which are not "well known" cannot be completely resolved into the actionToRolesWithWildcardIndexPrivileges map.
                        // Thus, we need to check the patterns from the roles

                        for (String mappedRole : mappedRoles) {
                            Pattern actionPattern = rolesWithWildcardIndexPrivilegesToActionPattern.get(mappedRole);

                            if (actionPattern != null && actionPattern.matches(action.name())) {
                                if (checkTable.check("*", action)) {
                                    break top;
                                }
                            }
                        }
                    }
                }

                if (checkTable.isComplete() && !exclusionsPresent(context, resolvedIndices, checkTable)) {
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

            if (resolvedIndices.getLocalIndices().isEmpty()) {
                log.debug("No local indices; grant the request");
                return PrivilegesEvaluationResult.OK;
            }

            CheckTable<String, Action> checkTable = CheckTable.create(resolvedIndices.getLocalIndices(), actions);

            top: for (Action action : actions) {
                ImmutableMap<String, ImmutableSet<String>> indexToRoles = actionToIndexToRoles.get(action);

                if (indexToRoles != null) {
                    for (String index : resolvedIndices.getLocalIndices()) {
                        ImmutableSet<String> rolesWithPrivileges = indexToRoles.get(index);

                        if (rolesWithPrivileges != null && rolesWithPrivileges.containsAny(mappedRoles)) {
                            if (checkTable.check(index, action)) {
                                break top;
                            }
                        }
                    }
                }

                if (!(action instanceof WellKnownAction)) {
                    // Actions which are not "well known" cannot be completely resolved into the actionToRolesWithWildcardIndexPrivileges map.
                    // Thus, we need to check the patterns from the roles

                    for (String mappedRole : mappedRoles) {
                        ImmutableMap<String, Pattern> indexToActionPattern = rolesToIndexToActionPattern.get(mappedRole);

                        if (indexToActionPattern == null) {
                            continue;
                        }

                        for (String index : resolvedIndices.getLocalIndices()) {
                            Pattern actionPattern = indexToActionPattern.get(index);

                            if (actionPattern != null && actionPattern.matches(action.name())) {
                                if (checkTable.check(index, action)) {
                                    break top;
                                }
                            }
                        }
                    }
                }
            }

            uncheckExclusions(context, resolved, checkTable);

            if (checkTable.isComplete()) {
                return PrivilegesEvaluationResult.OK;
            }

            ImmutableSet<String> availableIndices = checkTable.getCompleteRows();

            if (!availableIndices.isEmpty()) {
                return PrivilegesEvaluationResult.PARTIALLY_OK.availableIndices(availableIndices, checkTable);
            }

            return PrivilegesEvaluationResult.INSUFFICIENT.with(checkTable, errors);
        }

    }

}
