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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;

import com.floragunn.codova.config.templates.ExpressionEvaluationException;
import com.floragunn.codova.config.templates.Template;
import com.floragunn.codova.config.text.Pattern;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.google.common.collect.Sets;

public class LegacyRoleBasedDocumentAuthorization implements DocumentAuthorization, ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(LegacyRoleBasedDocumentAuthorization.class);

    private final SgDynamicConfiguration<Role> roles;
    private final IndexNameExpressionResolver resolver;
    private final ClusterService clusterService;
    private final ComponentState componentState = new ComponentState("role_based_document_authorization");

    public LegacyRoleBasedDocumentAuthorization(SgDynamicConfiguration<Role> roles, IndexNameExpressionResolver resolver,
            ClusterService clusterService) {
        this.roles = roles;
        this.resolver = resolver;
        this.clusterService = clusterService;
        this.componentState.setInitialized();
        this.componentState.setConfigVersion(roles.getDocVersion());
    }

    @Override
    public EvaluatedDlsFlsConfig getDlsFlsConfig(User user, ImmutableSet<String> mappedRoles, PrivilegesEvaluationContext context)
            throws PrivilegesEvaluationException {
        if (!containsDlsFlsConfig(mappedRoles)) {
            if (log.isDebugEnabled()) {
                log.debug("No fls or dls found for {}", user);
            }

            return EvaluatedDlsFlsConfig.EMPTY;
        }

        Map<String, Set<String>> dlsQueriesByIndex = new HashMap<String, Set<String>>();
        Map<String, Set<String>> flsFields = new HashMap<String, Set<String>>();
        Map<String, Set<String>> maskedFieldsMap = new HashMap<String, Set<String>>();

        Set<String> noDlsConcreteIndices = new HashSet<>();
        Set<String> noFlsConcreteIndices = new HashSet<>();
        Set<String> noMaskedFieldConcreteIndices = new HashSet<>();

        for (Map.Entry<String, Role> entry : this.roles.getCEntries().entrySet()) {
            if (!mappedRoles.contains(entry.getKey())) {
                continue;
            }

            Role role = entry.getValue();

            for (Role.Index index : role.getIndexPermissions()) {

                for (Template<Pattern> indexPattern : index.getIndexPatterns()) {

                    String[] concreteIndices;

                    try {
                        concreteIndices = getResolvedIndexPatterns(user, indexPattern);
                    } catch (ExpressionEvaluationException e) {
                        componentState.addLastException("get_dls_fls_config", e);
                        throw new PrivilegesEvaluationException("Error while evaluating index pattern template of role " + entry.getKey() + ":\nPattern: " + indexPattern + "\nUser: " + user.toStringWithAttributes(), e);
                    }

                    if (index.getDls() != null) {
                        try {
                            String dls = index.getDls().render(user);

                            if (dls != null && dls.length() > 0) {
                                for (int i = 0; i < concreteIndices.length; i++) {
                                    dlsQueriesByIndex.computeIfAbsent(concreteIndices[i], (key) -> new HashSet<String>()).add(dls);
                                }
                            } else {
                                noDlsConcreteIndices.addAll(Arrays.asList(concreteIndices));
                            }

                        } catch (ExpressionEvaluationException e) {
                            componentState.addLastException("get_dls_fls_config", e);
                            throw new PrivilegesEvaluationException("Error while evaluating DLS query template of role " + entry.getKey()
                                    + ":\nQuery template: " + index.getDls() + "\nUser: " + user.toStringWithAttributes(), e);
                        }
                    } else {
                        noDlsConcreteIndices.addAll(Arrays.asList(concreteIndices));
                    }

                    ImmutableList<String> fls = index.getFls();

                    if (fls != null && fls.size() > 0) {

                        for (int i = 0; i < concreteIndices.length; i++) {
                            final String ci = concreteIndices[i];
                            if (flsFields.containsKey(ci)) {
                                flsFields.get(ci).addAll(Sets.newHashSet(fls));
                            } else {
                                flsFields.put(ci, new HashSet<String>());
                                flsFields.get(ci).addAll(Sets.newHashSet(fls));
                            }
                        }
                    } else {
                        noFlsConcreteIndices.addAll(Arrays.asList(concreteIndices));
                    }

                    ImmutableList<String> maskedFields = index.getMaskedFields();

                    if (maskedFields != null && maskedFields.size() > 0) {

                        for (int i = 0; i < concreteIndices.length; i++) {
                            final String ci = concreteIndices[i];
                            if (maskedFieldsMap.containsKey(ci)) {
                                maskedFieldsMap.get(ci).addAll(Sets.newHashSet(maskedFields));
                            } else {
                                maskedFieldsMap.put(ci, new HashSet<String>());
                                maskedFieldsMap.get(ci).addAll(Sets.newHashSet(maskedFields));
                            }
                        }
                    } else {
                        noMaskedFieldConcreteIndices.addAll(Arrays.asList(concreteIndices));
                    }
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Index patterns with no dls queries attached: {} - They will be removed from {}", noDlsConcreteIndices,
                    dlsQueriesByIndex.keySet());
            log.debug("Index patterns with no fls fields attached: {} - They will be removed from {}", noFlsConcreteIndices, flsFields.keySet());
            log.debug("Index patterns with no masked fields attached: {} - They will be removed from {}", noMaskedFieldConcreteIndices,
                    maskedFieldsMap.keySet());
        }

        WildcardMatcher.wildcardRemoveFromSet(dlsQueriesByIndex.keySet(), noDlsConcreteIndices);
        WildcardMatcher.wildcardRemoveFromSet(flsFields.keySet(), noFlsConcreteIndices);
        WildcardMatcher.wildcardRemoveFromSet(maskedFieldsMap.keySet(), noMaskedFieldConcreteIndices);

        return new EvaluatedDlsFlsConfig(dlsQueriesByIndex, flsFields, maskedFieldsMap);
    }

    private boolean containsDlsFlsConfig(ImmutableSet<String> mappedRoles) {
        for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
            String roleName = entry.getKey();

            if (!mappedRoles.contains(roleName)) {
                continue;
            }

            Role role = entry.getValue();

            for (Role.Index index : role.getIndexPermissions()) {
                if (containsDlsFlsConfig(index)) {
                    return true;
                }
            }
        }

        return false;
    }

    private boolean containsDlsFlsConfig(Role.Index index) {
        if (index.getDls() != null || (index.getFls() != null && index.getFls().size() != 0)
                || (index.getMaskedFields() != null && index.getMaskedFields().size() != 0)) {
            return true;
        } else {
            return false;
        }
    }

    private String[] getResolvedIndexPatterns(User user, Template<Pattern> indexPattern) throws ExpressionEvaluationException {
        String unresolved = indexPattern.renderToString(user);

        String[] resolved = resolver.concreteIndexNames(clusterService.state(), IndicesOptions.lenientExpandOpen(), unresolved);

        if (resolved == null || resolved.length == 0) {
            return new String[] { unresolved };
        } else {
            return resolved;
        }
    }

    @Override
    public void updateIndices(Set<String> indices) {

    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

}
