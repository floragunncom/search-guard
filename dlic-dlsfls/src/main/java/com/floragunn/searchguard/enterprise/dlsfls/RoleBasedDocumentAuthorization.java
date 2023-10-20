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

package com.floragunn.searchguard.enterprise.dlsfls;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.config.templates.ExpressionEvaluationException;
import com.floragunn.codova.config.templates.Template;
import com.floragunn.codova.config.text.Pattern;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.cstate.metrics.TimeAggregation;
import com.floragunn.searchsupport.queries.Query;

public class RoleBasedDocumentAuthorization implements ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(RoleBasedDocumentAuthorization.class);

    private final SgDynamicConfiguration<Role> roles;
    private final StaticIndexQueries staticIndexQueries;
    private volatile StatefulIndexQueries statefulIndexQueries;
    private final ComponentState componentState = new ComponentState("role_based_document_authorization");
    private final MetricsLevel metricsLevel;
    private final TimeAggregation statefulIndexRebuild = new TimeAggregation.Milliseconds();

    private final boolean dfmEmptyOverridesAll;

    public RoleBasedDocumentAuthorization(SgDynamicConfiguration<Role> roles, Set<String> indices, MetricsLevel metricsLevel,
                                          boolean dfmEmptyOverridesAll) {
        this.roles = roles;
        this.metricsLevel = metricsLevel;
        this.dfmEmptyOverridesAll = dfmEmptyOverridesAll;
        this.staticIndexQueries = new StaticIndexQueries(roles, dfmEmptyOverridesAll);
        try (Meter meter = Meter.basic(metricsLevel, statefulIndexRebuild)) {
            this.statefulIndexQueries = new StatefulIndexQueries(roles, indices, dfmEmptyOverridesAll);
        }
        this.componentState.addPart(this.staticIndexQueries.getComponentState());
        this.componentState.addPart(this.statefulIndexQueries.getComponentState());
        this.componentState.setConfigVersion(roles.getDocVersion());
        this.componentState.updateStateFromParts();

        if (metricsLevel.basicEnabled()) {
            this.componentState.addMetrics("stateful_index_rebuilds", statefulIndexRebuild);
        }
    }

    boolean hasDlsRestrictions(PrivilegesEvaluationContext context, Collection<String> indices, Meter meter) throws PrivilegesEvaluationException {
        try (Meter subMeter = meter.detail("has_dls_restriction")) {
            if (this.staticIndexQueries.rolesWithIndexWildcardWithoutQuery.containsAny(context.getMappedRoles())) {
                return false;
            }

            StatefulIndexQueries statefulIndexQueries = this.statefulIndexQueries;

            if (!statefulIndexQueries.indices.containsAll(indices)) {
                // We get a request for an index unknown to this instance. Usually, this is the case because the index simply does not exist.
                // For non-existing indices, it is safe to assume that no documents can be accessed.

                if (log.isDebugEnabled()) {
                    log.debug("Indices {} do not exist. Assuming full document restriction.", indices);
                }

                return true;
            }

            for (String index : indices) {
                ImmutableSet<String> roleWithoutQuery = statefulIndexQueries.indexToRoleWithoutQuery.get(index);

                if (roleWithoutQuery != null && roleWithoutQuery.containsAny(context.getMappedRoles())) {
                    continue;
                }

                ImmutableMap<String, DlsQuery> roleToQuery = this.statefulIndexQueries.indexToRoleToQuery.get(index);

                for (String role : context.getMappedRoles()) {
                    {
                        DlsQuery query = this.staticIndexQueries.roleWithIndexWildcardToQuery.get(role);

                        if (query != null) {
                            return true;
                        }
                    }

                    if (roleToQuery != null) {
                        DlsQuery query = roleToQuery.get(role);

                        if (query != null) {
                            return true;
                        }
                    }

                    ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, DlsQuery> indexPatternTemplateToQuery = this.staticIndexQueries.rolesToIndexPatternTemplateToQuery
                            .get(role);

                    if (indexPatternTemplateToQuery != null) {
                        for (Map.Entry<Role.IndexPatterns.IndexPatternTemplate, DlsQuery> entry : indexPatternTemplateToQuery.entrySet()) {
                            try {
                                Pattern pattern = context.getRenderedPattern(entry.getKey().getTemplate());

                                if (pattern.matches(index) && !entry.getKey().getExclusions().matches(index)) {
                                    return true;
                                }
                            } catch (ExpressionEvaluationException e) {
                                throw new PrivilegesEvaluationException("Error while rendering index pattern of role " + role, e);
                            }
                        }
                    }
                }
            }

            return false;
        } catch (PrivilegesEvaluationException e) {
            componentState.addLastException("has_dls_restriction", e);
            throw e;
        } catch (RuntimeException e) {
            componentState.addLastException("has_dls_restriction_u", e);
            throw e;
        }
    }

    public DlsRestriction getDlsRestriction(PrivilegesEvaluationContext context, String index, Meter meter) throws PrivilegesEvaluationException {
        try (Meter subMeter = meter.detail("evaluate_dls")) {
            return getDlsRestrictionInternal(context, index);
        } catch (PrivilegesEvaluationException e) {
            componentState.addLastException("get_dls_restriction", e);
            throw e;
        } catch (RuntimeException e) {
            componentState.addLastException("get_dls_restriction_u", e);
            throw e;
        }
    }

    public DlsRestriction.IndexMap getDlsRestriction(PrivilegesEvaluationContext context, Collection<String> indices, Meter meter)
            throws PrivilegesEvaluationException {
        try (Meter subMeter = meter.detail("evaluate_dls")) {
            if (this.staticIndexQueries.rolesWithIndexWildcardWithoutQuery.containsAny(context.getMappedRoles())) {
                return DlsRestriction.IndexMap.NONE;
            }

            ImmutableMap.Builder<String, DlsRestriction> result = new ImmutableMap.Builder<>(indices.size());

            int restrictedIndices = 0;

            for (String index : indices) {
                DlsRestriction restriction = getDlsRestrictionInternal(context, index);

                if (!restriction.isUnrestricted()) {
                    restrictedIndices++;
                }

                result.put(index, restriction);
            }

            if (restrictedIndices == 0) {
                return DlsRestriction.IndexMap.NONE;
            }

            return new DlsRestriction.IndexMap(result.build());
        } catch (PrivilegesEvaluationException e) {
            componentState.addLastException("get_dls_restriction", e);
            throw e;
        } catch (RuntimeException e) {
            componentState.addLastException("get_dls_restriction_u", e);
            throw e;
        }
    }

    private DlsRestriction getDlsRestrictionInternal(PrivilegesEvaluationContext context, String index) throws PrivilegesEvaluationException {
        if (this.staticIndexQueries.rolesWithIndexWildcardWithoutQuery.containsAny(context.getMappedRoles())) {
            return DlsRestriction.NONE;
        }

        StatefulIndexQueries statefulIndexQueries = this.statefulIndexQueries;

        if (!statefulIndexQueries.indices.contains(index)) {
            // We get a request for an index unknown to this instance. Usually, this is the case because the index simply does not exist.
            // For non-existing indices, it is safe to assume that no documents can be accessed.

            if (log.isDebugEnabled()) {
                log.debug("Index {} does not exist. Assuming full document restriction.", index);
            }

            return DlsRestriction.FULL;
        }

        ImmutableSet<String> roleWithoutQuery = statefulIndexQueries.indexToRoleWithoutQuery.get(index);

        if (roleWithoutQuery != null && roleWithoutQuery.containsAny(context.getMappedRoles())) {
            return DlsRestriction.NONE;
        }

        ImmutableMap<String, DlsQuery> roleToQuery = this.statefulIndexQueries.indexToRoleToQuery.get(index);

        Set<DlsQuery> queries = new HashSet<>();

        for (String role : context.getMappedRoles()) {
            {
                DlsQuery query = this.staticIndexQueries.roleWithIndexWildcardToQuery.get(role);

                if (query != null) {
                    queries.add(query);
                }
            }

            if (roleToQuery != null) {
                DlsQuery query = roleToQuery.get(role);

                if (query != null) {
                    queries.add(query);
                }
            }

            ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, DlsQuery> indexPatternTemplateToQuery = this.staticIndexQueries.rolesToIndexPatternTemplateToQuery
                    .get(role);

            if (indexPatternTemplateToQuery != null) {
                for (Map.Entry<Role.IndexPatterns.IndexPatternTemplate, DlsQuery> entry : indexPatternTemplateToQuery.entrySet()) {
                    try {
                        Pattern pattern = context.getRenderedPattern(entry.getKey().getTemplate());

                        if (pattern.matches(index) && !entry.getKey().getExclusions().matches(index)) {
                            queries.add(entry.getValue());
                        }
                    } catch (ExpressionEvaluationException e) {
                        throw new PrivilegesEvaluationException("Error while rendering index pattern of role " + role, e);
                    }
                }
            }
        }

        if (queries.isEmpty()) {
            return DlsRestriction.NONE;
        }

        List<com.floragunn.searchsupport.queries.Query> renderedQueries = new ArrayList<>(queries.size());

        for (DlsQuery query : queries) {
            try {
                renderedQueries.add(query.queryTemplate.render(context.getUser()));
            } catch (ExpressionEvaluationException e) {
                throw new PrivilegesEvaluationException("Error while rendering query " + query, e);
            }
        }

        return new DlsRestriction(ImmutableList.of(renderedQueries));

    }

    static class StaticIndexQueries implements ComponentStateProvider {
        private final ComponentState componentState;

        private final ImmutableSet<String> rolesWithIndexWildcardWithoutQuery;
        private final ImmutableMap<String, DlsQuery> roleWithIndexWildcardToQuery;
        private final ImmutableMap<String, ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, DlsQuery>> rolesToIndexPatternTemplateToQuery;
        private final ImmutableMap<String, ImmutableList<Exception>> rolesToInitializationErrors;

        StaticIndexQueries(SgDynamicConfiguration<Role> roles, boolean dfmEmptyOverridesAll) {
            this.componentState = new ComponentState("static_index_queries");

            ImmutableSet.Builder<String> rolesWithIndexWildcardWithoutQuery = new ImmutableSet.Builder<>();
            ImmutableMap.Builder<String, DlsQuery> roleWithIndexWildcardToQuery = new ImmutableMap.Builder<String, DlsQuery>();
            ImmutableMap.Builder<String, ImmutableMap.Builder<Role.IndexPatterns.IndexPatternTemplate, DlsQuery>> rolesToIndexPatternTemplateToQuery = new ImmutableMap.Builder<String, ImmutableMap.Builder<Role.IndexPatterns.IndexPatternTemplate, DlsQuery>>()
                    .defaultValue((k) -> new ImmutableMap.Builder<>());

            ImmutableMap.Builder<String, ImmutableList.Builder<Exception>> rolesToInitializationErrors = new ImmutableMap.Builder<String, ImmutableList.Builder<Exception>>()
                    .defaultValue((k) -> new ImmutableList.Builder<Exception>());

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();

                    for (Role.Index indexPermissions : role.getIndexPermissions()) {
                        if (indexPermissions.getIndexPatterns().getPattern().isWildcard()) {
                            Template<Query> dlsQueryTemplate = indexPermissions.getDls();

                            if (dlsQueryTemplate == null) {
                                rolesWithIndexWildcardWithoutQuery.add(roleName);
                            } else {
                                DlsQuery dlsConfig = new DlsQuery(dlsQueryTemplate);
                                roleWithIndexWildcardToQuery.put(roleName, dlsConfig);
                            }

                            continue;
                        }

                        for (Role.IndexPatterns.IndexPatternTemplate indexPatternTemplate : indexPermissions.getIndexPatterns().getPatternTemplates()) {                           
                            Template<Query> dlsQueryTemplate = indexPermissions.getDls();

                            if (dlsQueryTemplate == null) {
                                continue;
                            }

                            DlsQuery dlsConfig = new DlsQuery(dlsQueryTemplate);

                            rolesToIndexPatternTemplateToQuery.get(roleName).put(indexPatternTemplate, dlsConfig);
                        }
                        
                        // Note: Date math expressions are not supported any more in this implementation
                    }
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                    rolesToInitializationErrors.get(entry.getKey()).with(e);
                }
            }

            this.rolesWithIndexWildcardWithoutQuery = dfmEmptyOverridesAll? rolesWithIndexWildcardWithoutQuery.build() : ImmutableSet.empty();
            this.roleWithIndexWildcardToQuery = roleWithIndexWildcardToQuery.build();
            this.rolesToIndexPatternTemplateToQuery = rolesToIndexPatternTemplateToQuery.build(ImmutableMap.Builder::build);
            this.rolesToInitializationErrors = rolesToInitializationErrors.build(ImmutableList.Builder::build);

            if (this.rolesToInitializationErrors.isEmpty()) {
                this.componentState.initialized();
            } else {
                this.componentState.setState(State.PARTIALLY_INITIALIZED, "roles_with_errors");
                this.componentState.addDetail(rolesToInitializationErrors);
            }

        }

        @Override
        public ComponentState getComponentState() {
            return componentState;
        }
    }

    static class StatefulIndexQueries implements ComponentStateProvider {
        private final ImmutableMap<String, ImmutableMap<String, DlsQuery>> indexToRoleToQuery;
        private final ImmutableMap<String, ImmutableSet<String>> indexToRoleWithoutQuery;

        private final ImmutableSet<String> indices;

        private final ImmutableMap<String, ImmutableList<Exception>> rolesToInitializationErrors;
        private final ComponentState componentState;

        StatefulIndexQueries(SgDynamicConfiguration<Role> roles, Set<String> indices, boolean dfmEmptyOverridesAll) {
            this.indices = ImmutableSet.of(indices);
            this.componentState = new ComponentState("stateful_index_queries");

            ImmutableMap.Builder<String, ImmutableMap.Builder<String, DlsQuery>> indexToRoleToQuery = new ImmutableMap.Builder<String, ImmutableMap.Builder<String, DlsQuery>>()
                    .defaultValue((k) -> new ImmutableMap.Builder<String, DlsQuery>());

            ImmutableMap.Builder<String, ImmutableSet.Builder<String>> indexToRoleWithoutQuery = new ImmutableMap.Builder<String, ImmutableSet.Builder<String>>()
                    .defaultValue((k) -> new ImmutableSet.Builder<String>());

            ImmutableMap.Builder<String, ImmutableList.Builder<Exception>> rolesToInitializationErrors = new ImmutableMap.Builder<String, ImmutableList.Builder<Exception>>()
                    .defaultValue((k) -> new ImmutableList.Builder<Exception>());

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();

                    for (Role.Index indexPermissions : role.getIndexPermissions()) {
                        if (indexPermissions.getIndexPatterns().getPattern().isWildcard()) {
                            // This is handled in the static IndexPermissions object.
                            continue;
                        }

                        if (indexPermissions.getIndexPatterns().getPattern().isBlank()) {
                            // The pattern is likely blank because there are only templated patterns. Index patterns with templates are not handled here, but in the static IndexPermissions object
                            continue;
                        }

                        Pattern indexPattern = indexPermissions.getIndexPatterns().getPattern();
                        Template<Query> dlsQueryTemplate = indexPermissions.getDls();

                        if (dlsQueryTemplate != null) {
                            DlsQuery dlsConfig = new DlsQuery(dlsQueryTemplate);

                            for (String index : indexPattern.iterateMatching(indices)) {
                                indexToRoleToQuery.get(index).put(roleName, dlsConfig);
                            }
                        } else if (dfmEmptyOverridesAll) {
                            for (String index : indexPattern.iterateMatching(indices)) {
                                indexToRoleWithoutQuery.get(index).add(roleName);
                            }
                        }

                    }
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                    rolesToInitializationErrors.get(entry.getKey()).with(e);
                }
            }

            this.indexToRoleToQuery = indexToRoleToQuery.build((b) -> b.build());
            this.indexToRoleWithoutQuery = indexToRoleWithoutQuery.build((b) -> b.build());
            this.rolesToInitializationErrors = rolesToInitializationErrors.build((b) -> b.build());

            if (this.rolesToInitializationErrors.isEmpty()) {
                this.componentState.initialized();
            } else {
                this.componentState.setState(State.PARTIALLY_INITIALIZED, "roles_with_errors");
                this.componentState.addDetail(rolesToInitializationErrors);
            }
        }

        @Override
        public ComponentState getComponentState() {
            return componentState;
        }

    }

    static class DlsQuery {
        final Template<com.floragunn.searchsupport.queries.Query> queryTemplate;

        DlsQuery(Template<com.floragunn.searchsupport.queries.Query> queryTemplate) {
            this.queryTemplate = queryTemplate;
        }

        @Override
        public int hashCode() {
            return queryTemplate.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof DlsQuery)) {
                return false;
            }
            DlsQuery other = (DlsQuery) obj;
            if (queryTemplate == null) {
                if (other.queryTemplate != null) {
                    return false;
                }
            } else if (!queryTemplate.equals(other.queryTemplate)) {
                return false;
            }
            return true;
        }
    }

    public synchronized void updateIndices(Set<String> indices) {
        StatefulIndexQueries statefulIndexQueries = this.statefulIndexQueries;

        if (!statefulIndexQueries.indices.equals(indices)) {
            try (Meter meter = Meter.basic(metricsLevel, statefulIndexRebuild)) {
                this.statefulIndexQueries = new StatefulIndexQueries(roles, indices, dfmEmptyOverridesAll);
                this.componentState.replacePart(this.statefulIndexQueries.getComponentState());
            }
        }
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

}
