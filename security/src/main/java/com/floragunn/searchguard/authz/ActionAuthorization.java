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
import com.floragunn.searchguard.sgconf.ActionGroups;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.support.Pattern;
import com.floragunn.searchsupport.util.ImmutableMap;
import com.floragunn.searchsupport.util.ImmutableSet;

public class ActionAuthorization {
    private static final Logger log = LogManager.getLogger(ActionAuthorization.class);

    static class Cluster {
        private final ImmutableMap<Action, ImmutableSet<String>> actionToRoles;
        private final ImmutableSet<String> rolesWithWildcardPermissions;
        private final ImmutableMap<String, Pattern> rolesToActionPattern;

        // TODO rework exclusions to global

        public Cluster(SgDynamicConfiguration<Role> roles, ActionGroups actionGroups, Actions actions) {
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

        public boolean hasPermission(Action action, Set<String> roles) {
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
        private final ImmutableSet<String> rolesWithWildcardPermissions;
        private final ImmutableMap<String, ImmutableMap<String, Pattern>> rolesToIndexToActionPattern;

        private final ImmutableMap<Action, ImmutableMap<String, ImmutableSet<String>>> excludedActionToIndexToRoles;
        private final ImmutableMap<String, ImmutableMap<String, Pattern>> rolesToIndexToExcludedActionPattern;

        public Index(SgDynamicConfiguration<Role> roles, ActionGroups actionGroups, Actions actions,
                org.elasticsearch.cluster.metadata.Metadata clusterStateMetadata) {
            ImmutableMap.Builder<Action, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>> actionToIndexToRoles = //
                    new ImmutableMap.Builder<Action, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<String, ImmutableSet.Builder<String>>()
                                    .defaultValue((k2) -> new ImmutableSet.Builder<String>()));
            ImmutableSet.Builder<String> rolesWithWildcardPermissions = new ImmutableSet.Builder<>();
            ImmutableMap.Builder<String, ImmutableMap.Builder<String, Pattern>> rolesToIndexToActionPattern = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<String, Pattern>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<String, Pattern>());

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
                        List<Pattern> patterns = new ArrayList<>();
                        Set<String> indices = new HashSet<>();

                        if (permissions.contains("*") && indexPermissions.getIndexPatterns().contains("*")) {
                            rolesWithWildcardPermissions.add(roleName);
                            continue;
                        }

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

                } catch (ConfigValidationException e) {
                    log.error("Invalid pattern in role: " + entry + "\nThis should have been caught before. Ignoring role.", e);
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                }
            }

            this.actionToIndexToRoles = actionToIndexToRoles.build((b) -> b.build(ImmutableSet.Builder::build));
            this.rolesWithWildcardPermissions = rolesWithWildcardPermissions.build();
            this.rolesToIndexToActionPattern = rolesToIndexToActionPattern.build(ImmutableMap.Builder::build);
            this.excludedActionToIndexToRoles = excludedActionToIndexToRoles.build((b) -> b.build(ImmutableSet.Builder::build));
            this.rolesToIndexToExcludedActionPattern = rolesToIndexToExcludedActionPattern.build(ImmutableMap.Builder::build);
        }
    }

}
