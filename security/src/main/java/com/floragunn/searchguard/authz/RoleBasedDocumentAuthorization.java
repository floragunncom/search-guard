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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.config.templates.Template;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.fluent.collections.ImmutableMap.Builder;
import com.floragunn.searchguard.privileges.PrivilegesEvaluationContext;
import com.floragunn.searchguard.sgconf.ActionGroups;
import com.floragunn.searchguard.sgconf.EvaluatedDlsFlsConfig;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.support.Pattern;
import com.floragunn.searchguard.user.User;

public class RoleBasedDocumentAuthorization implements DocumentAuthorization {
    private static final Logger log = LogManager.getLogger(RoleBasedDocumentAuthorization.class);

    private final SgDynamicConfiguration<Role> roles;
    private final ActionGroups actionGroups;
    private final Actions actions;

    private final IndexPermissions index;
    private volatile StatefulIndexPermssions statefulIndex;

    public RoleBasedDocumentAuthorization(SgDynamicConfiguration<Role> roles, ActionGroups actionGroups, Actions actions, Set<String> indices) {
        this.roles = roles;
        this.actionGroups = actionGroups;
        this.actions = actions;

        this.index = new IndexPermissions(roles);

        if (indices != null) {
            this.statefulIndex = new StatefulIndexPermssions(roles, actionGroups, actions, indices);
        }
    }

    @Override
    public EvaluatedDlsFlsConfig getDlsFlsConfig(User user, ImmutableSet<String> mappedRoles, PrivilegesEvaluationContext context) {
        StatefulIndexPermssions statefulIndex = this.statefulIndex;

        if (statefulIndex == null) {
            return EvaluatedDlsFlsConfig.EMPTY;
        }

        boolean hasDls = statefulIndex.rolesToIndexToDlsQueryTemplates.containsAny(mappedRoles);
        boolean hasFls = statefulIndex.rolesToIndexToFlsFieldSpec.containsAny(mappedRoles);
        boolean hasFieldMasking = statefulIndex.rolesToIndexToMaskedFieldSpec.containsAny(mappedRoles);

        if (!hasDls && !hasFls && !hasFieldMasking) {
            if (log.isDebugEnabled()) {
                log.debug("No fls or dls found for {} in {} sg roles", user, mappedRoles);
            }

            return EvaluatedDlsFlsConfig.EMPTY;
        }

        Pattern indicesWithoutDls = hasDls ? index.getPatternForIndicesWithoutDls(mappedRoles) : null;
        Pattern indicesWithoutFls = hasFls ? index.getPatternForIndicesWithoutFls(mappedRoles) : null;
        Pattern indicesWithoutFieldMasking = hasFieldMasking ? index.getPatternForIndicesWithoutFieldMasking(mappedRoles) : null;

        Map<String, Set<String>> dlsQueriesByIndex = new HashMap<String, Set<String>>();
        Map<String, Set<String>> flsFields = new HashMap<String, Set<String>>();
        Map<String, Set<String>> maskedFieldsMap = new HashMap<String, Set<String>>();

        for (String role : mappedRoles) {
            ImmutableMap<String, ImmutableList<Template<String>>> indexToDlsQueryTemplates = statefulIndex.rolesToIndexToDlsQueryTemplates.get(role);

            if (indexToDlsQueryTemplates != null) {
                indexToDlsQueryTemplates.forEach((index, queryTemplates) -> {
                    if (!indicesWithoutDls.matches(index)) {
                        for (Template<String> queryTemplate : queryTemplates) {
                            String dlsQuery = queryTemplate.toString(); // TODO replace props
                            dlsQueriesByIndex.computeIfAbsent(index, (key) -> new HashSet<String>()).add(dlsQuery);
                        }
                    }
                });
            }

            ImmutableMap<String, ImmutableSet<String>> indexToFlsFieldSpec = statefulIndex.rolesToIndexToFlsFieldSpec.get(role);

            if (indexToFlsFieldSpec != null) {
                indexToFlsFieldSpec.forEach((index, flsFieldSpec) -> {
                    if (!indicesWithoutFls.matches(index)) {
                        flsFields.computeIfAbsent(index, (k) -> new HashSet<String>()).addAll(flsFieldSpec);
                    }
                });
            }

            ImmutableMap<String, ImmutableSet<String>> indexToMaskedFieldSpec = statefulIndex.rolesToIndexToMaskedFieldSpec.get(role);

            if (indexToMaskedFieldSpec != null) {
                indexToMaskedFieldSpec.forEach((index, maskedFieldSpec) -> {
                    if (!indicesWithoutFieldMasking.matches(index)) {
                        maskedFieldsMap.computeIfAbsent(index, (k) -> new HashSet<String>()).addAll(maskedFieldSpec);
                    }
                });
            }
        }

        return new EvaluatedDlsFlsConfig(dlsQueriesByIndex, flsFields, maskedFieldsMap);
    }

    @Override
    public void updateIndices(Set<String> indices) {
        StatefulIndexPermssions statefulIndex = this.statefulIndex;

        if (statefulIndex != null && statefulIndex.indices.equals(indices)) {
            return;
        }

        this.statefulIndex = new StatefulIndexPermssions(roles, actionGroups, actions, indices);
    }

    static class IndexPermissions {
        private final ImmutableMap<String, Pattern> rolesToIndexWithoutDls;
        private final ImmutableMap<String, Pattern> rolesToIndexWithoutFls;
        private final ImmutableMap<String, Pattern> rolesToIndexWithoutFieldMasking;

  //      private final ImmutableMap<String, ImmutableSet<Template<Pattern>>> rolesToIndexTemplatesWithoutDls;
  //      private final ImmutableMap<String, ImmutableSet<Template<Pattern>>> rolesToIndexTemplatesWithoutFls;
  //      private final ImmutableMap<String, ImmutableSet<Template<Pattern>>> rolesToIndexTemplatesWithoutFieldMasking;

        IndexPermissions(SgDynamicConfiguration<Role> roles) {
            ImmutableMap.Builder<String, Pattern> rolesToIndexWithoutDls = new ImmutableMap.Builder<>();
            ImmutableMap.Builder<String, Pattern> rolesToIndexWithoutFls = new ImmutableMap.Builder<>();
            ImmutableMap.Builder<String, Pattern> rolesToIndexWithoutFieldMasking = new ImmutableMap.Builder<>();

            ImmutableMap.Builder<String, ImmutableSet<Template<Pattern>>> rolesToIndexTemplatesWithoutDls = new ImmutableMap.Builder<>();
            ImmutableMap.Builder<String, ImmutableSet<Template<Pattern>>> rolesToIndexTemplatesWithoutFls = new ImmutableMap.Builder<>();
            ImmutableMap.Builder<String, ImmutableSet<Template<Pattern>>> rolesToIndexTemplatesWithoutFieldMasking = new ImmutableMap.Builder<>();

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();
                    List<Pattern> indexPatternsWithoutDls = new ArrayList<>();
                    List<Pattern> indexPatternsWithoutFls = new ArrayList<>();
                    List<Pattern> indexPatternsWithoutFieldMasking = new ArrayList<>();
                    
                    List<Template<Pattern>> indexPatternTemplatesWithoutDls = new ArrayList<>();
                    List<Template<Pattern>> indexPatternTemplatesWithoutFls = new ArrayList<>();
                    List<Template<Pattern>> indexPatternTemplatesWithoutFieldMasking = new ArrayList<>();

                    for (Role.Index indexPermissions : role.getIndexPermissions()) {
                        if (indexPermissions.getDls() == null) {
                            for (Template<Pattern> indexPatternTemplate : indexPermissions.getIndexPatterns()) {
                                if (indexPatternTemplate.isConstant()) {
                                    indexPatternsWithoutDls.add(indexPatternTemplate.getConstantValue());
                                } else {
                                    indexPatternTemplatesWithoutDls.add(indexPatternTemplate);
                                }
                            }
                        }

                        if (indexPermissions.getFls() == null || indexPermissions.getFls().isEmpty()) {
                            for (Template<Pattern> indexPatternTemplate : indexPermissions.getIndexPatterns()) {
                                if (indexPatternTemplate.isConstant()) {
                                    indexPatternsWithoutFls.add(indexPatternTemplate.getConstantValue());
                                } else {
                                    indexPatternTemplatesWithoutFls.add(indexPatternTemplate);
                                }
                            }
                        }

                        if (indexPermissions.getMaskedFields() == null || indexPermissions.getMaskedFields().isEmpty()) {
                            for (Template<Pattern> indexPatternTemplate : indexPermissions.getIndexPatterns()) {
                                if (indexPatternTemplate.isConstant()) {
                                    indexPatternsWithoutFieldMasking.add(indexPatternTemplate.getConstantValue());
                                } else {
                                    indexPatternTemplatesWithoutFieldMasking.add(indexPatternTemplate);
                                }
                            }
                        }
                    }

                    if (!indexPatternsWithoutDls.isEmpty()) {
                        rolesToIndexWithoutDls.put(roleName, Pattern.join(indexPatternsWithoutDls));
                    }

                    if (!indexPatternsWithoutFls.isEmpty()) {
                        rolesToIndexWithoutFls.put(roleName, Pattern.join(indexPatternsWithoutFls));
                    }

                    if (!indexPatternsWithoutFieldMasking.isEmpty()) {
                        rolesToIndexWithoutFieldMasking.put(roleName, Pattern.join(indexPatternsWithoutFieldMasking));
                    }
                    
                    if (!indexPatternTemplatesWithoutDls.isEmpty()) {
                        rolesToIndexTemplatesWithoutDls.put(roleName, ImmutableSet.of(indexPatternTemplatesWithoutDls));
                    }

                    if (!indexPatternTemplatesWithoutFls.isEmpty()) {
                        rolesToIndexTemplatesWithoutFls.put(roleName, ImmutableSet.of(indexPatternTemplatesWithoutFls));
                    }
                    
                    if (!indexPatternTemplatesWithoutFieldMasking.isEmpty()) {
                        rolesToIndexTemplatesWithoutFieldMasking.put(roleName, ImmutableSet.of(indexPatternTemplatesWithoutFieldMasking));
                    }
                    
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                }
            }

            this.rolesToIndexWithoutDls = rolesToIndexWithoutDls.build();
            this.rolesToIndexWithoutFls = rolesToIndexWithoutFls.build();
            this.rolesToIndexWithoutFieldMasking = rolesToIndexWithoutFieldMasking.build();
        }

        Pattern getPatternForIndicesWithoutDls(ImmutableSet<String> mappedRoles) {
            return Pattern.join(rolesToIndexWithoutDls.valuesForKeys(mappedRoles));
        }

        Pattern getPatternForIndicesWithoutFls(ImmutableSet<String> mappedRoles) {
            return Pattern.join(rolesToIndexWithoutFls.valuesForKeys(mappedRoles));
        }

        Pattern getPatternForIndicesWithoutFieldMasking(ImmutableSet<String> mappedRoles) {
            return Pattern.join(rolesToIndexWithoutFieldMasking.valuesForKeys(mappedRoles));
        }
    }

    static class StatefulIndexPermssions {
        private final ImmutableMap<String, ImmutableMap<String, ImmutableList<Template<String>>>> rolesToIndexToDlsQueryTemplates;
        private final ImmutableMap<String, ImmutableMap<String, ImmutableSet<String>>> rolesToIndexToFlsFieldSpec;
        private final ImmutableMap<String, ImmutableMap<String, ImmutableSet<String>>> rolesToIndexToMaskedFieldSpec;

        private final ImmutableSet<String> indices;

        StatefulIndexPermssions(SgDynamicConfiguration<Role> roles, ActionGroups actionGroups, Actions actions, Set<String> indexNames) {
            ImmutableMap.Builder<String, ImmutableMap.Builder<String, ImmutableList.Builder<Template<String>>>> rolesToIndexToDlsQueryTemplates = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<String, ImmutableList.Builder<Template<String>>>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<String, ImmutableList.Builder<Template<String>>>()
                                    .defaultValue((k2) -> new ImmutableList.Builder<Template<String>>()));

            ImmutableMap.Builder<String, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>> rolesToIndexToFlsFieldSpec = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<String, ImmutableSet.Builder<String>>()
                                    .defaultValue((k2) -> new ImmutableSet.Builder<String>()));

            ImmutableMap.Builder<String, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>> rolesToIndexToMaskedFieldSpec = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<String, ImmutableSet.Builder<String>>()
                                    .defaultValue((k2) -> new ImmutableSet.Builder<String>()));

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();

                    for (Role.Index indexPermissions : role.getIndexPermissions()) {
                        if (indexPermissions.getDls() != null || (indexPermissions.getFls() != null && !indexPermissions.getFls().isEmpty())) {
                            for (Template<Pattern> indexPatternTemplate : indexPermissions.getIndexPatterns()) {
                                if (!indexPatternTemplate.isConstant()) {
                                    // TODO
                                    continue;
                                }

                                Pattern indexPattern = indexPatternTemplate.getConstantValue();

                                for (String index : indexPattern.iterateMatching(indexNames)) {
                                    if (indexPermissions.getDls() != null) {
                                        rolesToIndexToDlsQueryTemplates.get(roleName).get(index).with(indexPermissions.getDls());
                                    }

                                    if (indexPermissions.getFls() != null && !indexPermissions.getFls().isEmpty()) {
                                        rolesToIndexToFlsFieldSpec.get(roleName).get(index).with(indexPermissions.getFls());
                                    }

                                    if (indexPermissions.getMaskedFields() != null && !indexPermissions.getMaskedFields().isEmpty()) {
                                        rolesToIndexToMaskedFieldSpec.get(roleName).get(index).with(indexPermissions.getMaskedFields());
                                    }
                                }
                            }
                        }

                    }
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                }
            }

            this.rolesToIndexToDlsQueryTemplates = rolesToIndexToDlsQueryTemplates.build((b) -> b.build(ImmutableList.Builder::build));
            this.rolesToIndexToFlsFieldSpec = rolesToIndexToFlsFieldSpec.build((b) -> b.build(ImmutableSet.Builder::build));
            this.rolesToIndexToMaskedFieldSpec = rolesToIndexToMaskedFieldSpec.build((b) -> b.build(ImmutableSet.Builder::build));

            this.indices = ImmutableSet.of(indexNames);
        }

    }

}