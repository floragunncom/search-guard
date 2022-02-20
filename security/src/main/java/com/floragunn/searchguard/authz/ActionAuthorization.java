/*
 * Copyright 2015-2022 floragunn GmbH
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

                    for (String permission : permissions) {
                        if ("*".equals(permission) && excludedPermissions.isEmpty()) {
                            rolesWithWildcardPermissions.add(roleName);
                        } else if (Pattern.isConstant(permission)) {
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
                }
            }

            this.actionToRoles = actionToRoles.build(ImmutableSet.Builder::build);
            this.rolesWithWildcardPermissions = rolesWithWildcardPermissions.build();
            this.rolesToActionPattern = rolesToActionPattern.build();
        }

    }

    static class Index {
        private final ImmutableMap<Action, ImmutableSet<String>> actionToRoles;
        private final ImmutableSet<String> rolesWithWildcardPermissions;
        private final ImmutableMap<String, Pattern> rolesToActionPattern;

        public Index(SgDynamicConfiguration<Role> roles, ActionGroups actionGroups, Actions actions, org.elasticsearch.cluster.metadata.Metadata clusterStateMetadata) {
            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();
                   
                    role.g
                    
                } catch (ConfigValidationException e) {
                    log.error("Invalid pattern in role: " + entry + "\nThis should have been caught before. Ignoring role.", e);
                }
            }

        }
    }

    private ImmutableMap<WellKnownAction<?, ?, ?>, Object> roles;

    public ActionAuthorization(SgDynamicConfiguration<Role> roles, RoleMapping.InvertedIndex roleMapping, ActionGroups actionGroups,
            org.elasticsearch.cluster.metadata.Metadata clusterStateMetadata) {
        for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {

        }
    }

}
