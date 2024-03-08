/*
 * Copyright 2015-2024 floragunn GmbH
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

package com.floragunn.searchguard.enterprise.dlsfls;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.config.templates.ExpressionEvaluationException;
import com.floragunn.codova.config.text.Pattern;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.cstate.metrics.TimeAggregation;
import com.floragunn.searchsupport.meta.Meta;

public abstract class RoleBasedAuthorizationBase<SingleRule, JoinedRule> implements ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(RoleBasedDocumentAuthorization.class);

    protected final SgDynamicConfiguration<Role> roles;
    protected final StaticRules.Index<SingleRule> staticIndexQueries;
    protected final StaticRules.Alias<SingleRule> staticAliasQueries;
    protected final StaticRules.DataStream<SingleRule> staticDataStreamQueries;

    private volatile StatefulRules<SingleRule> statefulRules;

    private final Function<Role.Index, SingleRule> roleToRuleFunction;

    protected final ComponentState componentState = new ComponentState(this.componentName());
    protected final MetricsLevel metricsLevel;
    private final TimeAggregation statefulIndexRebuild = new TimeAggregation.Milliseconds();

    public RoleBasedAuthorizationBase(SgDynamicConfiguration<Role> roles, Meta indexMetadata, MetricsLevel metricsLevel,
            Function<Role.Index, SingleRule> roleToRuleFunction) {
        this.roles = roles;
        this.metricsLevel = metricsLevel;
        this.roleToRuleFunction = roleToRuleFunction;
        this.staticIndexQueries = new StaticRules.Index<>(roles, roleToRuleFunction);
        this.staticAliasQueries = new StaticRules.Alias<>(roles, roleToRuleFunction);
        this.staticDataStreamQueries = new StaticRules.DataStream<>(roles, roleToRuleFunction);

        try (Meter meter = Meter.basic(metricsLevel, statefulIndexRebuild)) {
            this.statefulRules = new StatefulRules<>(roles, indexMetadata, roleToRuleFunction);
        }

        this.componentState.addPart(this.staticIndexQueries.getComponentState());
        // this.componentState.addPart(this.statefulIndexQueries.getComponentState()); TODO
        this.componentState.setConfigVersion(roles.getDocVersion());
        this.componentState.updateStateFromParts();

        if (metricsLevel.basicEnabled()) {
            this.componentState.addMetrics("stateful_index_rebuilds", statefulIndexRebuild);
        }
    }

    boolean hasRestrictions(PrivilegesEvaluationContext context, ResolvedIndices resolved, Meter meter) throws PrivilegesEvaluationException {
        try (Meter subMeter = meter.detail(hasRestrictionsMetricName())) {
            if (this.staticIndexQueries.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
                return false;
            }

            StatefulRules<SingleRule> statefulRules = this.statefulRules;

            if (!resolved.getLocal().hasNonExistingObjects()) {
                // We get a request for an index unknown to this instance. Usually, this is the case because the index simply does not exist.
                // For non-existing indices, it is safe to assume that no documents can be accessed.

                if (log.isDebugEnabled()) {
                    log.debug("ResolvedIndices {} contain non-existing indices. Assuming full document restriction.", resolved);
                }

                return true;
            }

            if (this.staticIndexQueries.roleWithIndexWildcardToRule.keySet().containsAny(context.getMappedRoles())) {
                return true;
            }

            if (this.staticIndexQueries.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
                return false;
            }

            for (Meta.Index index : resolved.getLocal().getPureIndices()) {
                ImmutableSet<String> roleWithoutQuery = statefulRules.index.indexToRoleWithoutRule.get(index);

                if (roleWithoutQuery != null && roleWithoutQuery.containsAny(context.getMappedRoles())) {
                    continue;
                }

                ImmutableMap<String, SingleRule> roleToQuery = statefulRules.index.indexToRoleToRule.get(index);

                for (String role : context.getMappedRoles()) {

                    if (roleToQuery != null) {
                        SingleRule query = roleToQuery.get(role);

                        if (query != null) {
                            return true;
                        }
                    }

                    ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, SingleRule> indexPatternTemplateToQuery = this.staticIndexQueries.rolesToIndexPatternTemplateToRule
                            .get(role);

                    if (indexPatternTemplateToQuery != null) {
                        for (Map.Entry<Role.IndexPatterns.IndexPatternTemplate, SingleRule> entry : indexPatternTemplateToQuery.entrySet()) {
                            try {
                                Pattern pattern = context.getRenderedPattern(entry.getKey().getTemplate());

                                if (pattern.matches(index.name()) && !entry.getKey().getExclusions().matches(index.name())) {
                                    return true;
                                }
                            } catch (ExpressionEvaluationException e) {
                                throw new PrivilegesEvaluationException("Error while rendering index pattern of role " + role, e);
                            }
                        }
                    }
                }

                // TODO do i also have to check containing aliases or data streams?
            }

            for (Meta.Alias alias : resolved.getLocal().getAliases()) {
                ImmutableSet<String> roleWithoutQuery = statefulRules.alias.aliasToRoleWithoutRule.get(alias);

                if (roleWithoutQuery != null && roleWithoutQuery.containsAny(context.getMappedRoles())) {
                    continue;
                }

                ImmutableMap<String, SingleRule> roleToRule = statefulRules.alias.aliasToRoleToRule.get(alias);

                for (String role : context.getMappedRoles()) {

                    if (roleToRule != null) {
                        SingleRule rule = roleToRule.get(role);

                        if (rule != null) {
                            return true;
                        }
                    }

                    ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, SingleRule> aliasPatternTemplateToRule = this.staticAliasQueries.rolesToIndexPatternTemplateToRule
                            .get(role);

                    if (aliasPatternTemplateToRule != null) {
                        for (Map.Entry<Role.IndexPatterns.IndexPatternTemplate, SingleRule> entry : aliasPatternTemplateToRule.entrySet()) {
                            try {
                                Pattern pattern = context.getRenderedPattern(entry.getKey().getTemplate());

                                if (pattern.matches(alias.name()) && !entry.getKey().getExclusions().matches(alias.name())) {
                                    return true;
                                }
                            } catch (ExpressionEvaluationException e) {
                                throw new PrivilegesEvaluationException("Error while rendering index pattern of role " + role, e);
                            }
                        }
                    }
                }
            }

            for (Meta.DataStream dataStream : resolved.getLocal().getDataStreams()) {
                ImmutableSet<String> roleWithoutQuery = statefulRules.dataStream.dataStreamToRoleWithoutRule.get(dataStream);

                if (roleWithoutQuery != null && roleWithoutQuery.containsAny(context.getMappedRoles())) {
                    continue;
                }

                ImmutableMap<String, SingleRule> roleToRule = statefulRules.dataStream.dataStreamToRoleToRule.get(dataStream);

                for (String role : context.getMappedRoles()) {

                    if (roleToRule != null) {
                        SingleRule query = roleToRule.get(role);

                        if (query != null) {
                            return true;
                        }
                    }

                    ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, SingleRule> dataStreamPatternTemplateToQuery = this.staticDataStreamQueries.rolesToIndexPatternTemplateToRule
                            .get(role);

                    if (dataStreamPatternTemplateToQuery != null) {
                        for (Map.Entry<Role.IndexPatterns.IndexPatternTemplate, SingleRule> entry : dataStreamPatternTemplateToQuery.entrySet()) {
                            try {
                                Pattern pattern = context.getRenderedPattern(entry.getKey().getTemplate());

                                if (pattern.matches(dataStream.name()) && !entry.getKey().getExclusions().matches(dataStream.name())) {
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

    boolean hasRestrictions(PrivilegesEvaluationContext context, Meta.Index index, Meter meter) throws PrivilegesEvaluationException {
        try (Meter subMeter = meter.detail(hasRestrictionsMetricName())) {
            if (this.staticIndexQueries.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
                return false;
            }

            StatefulRules<SingleRule> statefulRules = this.statefulRules;

            if (!index.exists()) {
                // We get a request for an index unknown to this instance. Usually, this is the case because the index simply does not exist.
                // For non-existing indices, it is safe to assume that no documents can be accessed.

                if (log.isDebugEnabled()) {
                    log.debug("Index {} does not exist. Assuming full document restriction.", index);
                }

                return true;
            }

            if (this.staticIndexQueries.roleWithIndexWildcardToRule.keySet().containsAny(context.getMappedRoles())) {
                return true;
            }

            if (this.staticIndexQueries.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
                return false;
            }

            ImmutableSet<String> roleWithoutQuery = statefulRules.index.indexToRoleWithoutRule.get(index);

            if (roleWithoutQuery != null && roleWithoutQuery.containsAny(context.getMappedRoles())) {
                return false;
            }

            ImmutableMap<String, SingleRule> roleToQuery = statefulRules.index.indexToRoleToRule.get(index);

            for (String role : context.getMappedRoles()) {

                if (roleToQuery != null) {
                    SingleRule query = roleToQuery.get(role);

                    if (query != null) {
                        return true;
                    }
                }

                ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, SingleRule> indexPatternTemplateToQuery = this.staticIndexQueries.rolesToIndexPatternTemplateToRule
                        .get(role);

                if (indexPatternTemplateToQuery != null) {
                    for (Map.Entry<Role.IndexPatterns.IndexPatternTemplate, SingleRule> entry : indexPatternTemplateToQuery.entrySet()) {
                        try {
                            Pattern pattern = context.getRenderedPattern(entry.getKey().getTemplate());

                            if (pattern.matches(index.name()) && !entry.getKey().getExclusions().matches(index.name())) {
                                return true;
                            }
                        } catch (ExpressionEvaluationException e) {
                            throw new PrivilegesEvaluationException("Error while rendering index pattern of role " + role, e);
                        }
                    }
                }

                // TODO do i also have to check containing aliases or data streams?

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

    public JoinedRule getRestriction(PrivilegesEvaluationContext context, Meta.Index index, Meter meter) throws PrivilegesEvaluationException {
        try (Meter subMeter = meter.detail(evaluateRestrictionsMetricName())) {
            return getRestrictionImpl(context, index);
        } catch (PrivilegesEvaluationException e) {
            componentState.addLastException("get_restriction", e);
            throw e;
        } catch (RuntimeException e) {
            componentState.addLastException("get_restriction_u", e);
            throw e;
        }
    }

    protected JoinedRule getRestrictionImpl(PrivilegesEvaluationContext context, Meta.Index index) throws PrivilegesEvaluationException {
        Meta.DataStream parentDataStream = index.parentDataStream();

        if (this.staticIndexQueries.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
            return unrestricted();
        }

        if (parentDataStream != null) {
            if (this.staticDataStreamQueries.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
                return unrestricted();
            }
        }
        
        StatefulRules<SingleRule> statefulRules = this.statefulRules;

        {
            ImmutableSet<String> roleWithoutQuery = statefulRules.index.indexToRoleWithoutRule.get(index);

            if (roleWithoutQuery != null && roleWithoutQuery.containsAny(context.getMappedRoles())) {
                return unrestricted();
            }
        }

        if (parentDataStream != null) {
            ImmutableSet<String> roleWithoutQuery = statefulRules.dataStream.dataStreamToRoleWithoutRule.get(parentDataStream);

            if (roleWithoutQuery != null && roleWithoutQuery.containsAny(context.getMappedRoles())) {
                return unrestricted();
            }
        }

        ImmutableMap<String, SingleRule> roleToQueryForIndex = statefulRules.index.indexToRoleToRule.get(index);
        ImmutableMap<String, SingleRule> roleToQueryForDataStream = parentDataStream != null
                ? statefulRules.dataStream.dataStreamToRoleToRule.get(parentDataStream)
                : null;

        Set<SingleRule> rules = new HashSet<>();

        for (String role : context.getMappedRoles()) {
            {
                SingleRule rule = this.staticIndexQueries.roleWithIndexWildcardToRule.get(role);

                if (rule != null) {
                    rules.add(rule);
                }
            }
            
            if (parentDataStream != null) {
                SingleRule rule = this.staticDataStreamQueries.roleWithIndexWildcardToRule.get(role);
                
                if (rule != null) {
                    rules.add(rule);
                }
            }

            if (roleToQueryForIndex != null) {
                SingleRule rule = roleToQueryForIndex.get(role);

                if (rule != null) {
                    rules.add(rule);
                }
            }

            if (roleToQueryForDataStream != null) {
                SingleRule rule = roleToQueryForDataStream.get(role);

                if (rule != null) {
                    rules.add(rule);
                }

            }

            ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, SingleRule> indexPatternTemplateToQuery = this.staticIndexQueries.rolesToIndexPatternTemplateToRule
                    .get(role);

            if (indexPatternTemplateToQuery != null) {
                for (Map.Entry<Role.IndexPatterns.IndexPatternTemplate, SingleRule> entry : indexPatternTemplateToQuery.entrySet()) {
                    try {
                        Pattern pattern = context.getRenderedPattern(entry.getKey().getTemplate());

                        if (pattern.matches(index.name()) && !entry.getKey().getExclusions().matches(index.name())) {
                            rules.add(entry.getValue());
                        }
                    } catch (ExpressionEvaluationException e) {
                        throw new PrivilegesEvaluationException("Error while rendering index pattern of role " + role, e);
                    }
                }
            }

            if (!index.parentAliases().isEmpty()) {
                ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, SingleRule> aliasPatternTemplateToQuery = this.staticAliasQueries.rolesToIndexPatternTemplateToRule
                        .get(role);

                if (aliasPatternTemplateToQuery != null) {
                    for (Meta.Alias alias : index.parentAliases()) {
                        for (Map.Entry<Role.IndexPatterns.IndexPatternTemplate, SingleRule> entry : aliasPatternTemplateToQuery.entrySet()) {
                            try {
                                Pattern pattern = context.getRenderedPattern(entry.getKey().getTemplate());

                                if (pattern.matches(alias.name()) && !entry.getKey().getExclusions().matches(alias.name())) {
                                    rules.add(entry.getValue());
                                }
                            } catch (ExpressionEvaluationException e) {
                                throw new PrivilegesEvaluationException("Error while rendering index pattern of role " + role, e);
                            }
                        }
                    }
                }
            }

            if (parentDataStream != null) {
                ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, SingleRule> dataStreamPatternTemplateToQuery = this.staticDataStreamQueries.rolesToIndexPatternTemplateToRule
                        .get(role);

                if (dataStreamPatternTemplateToQuery != null) {
                    for (Map.Entry<Role.IndexPatterns.IndexPatternTemplate, SingleRule> entry : dataStreamPatternTemplateToQuery.entrySet()) {
                        try {
                            Pattern pattern = context.getRenderedPattern(entry.getKey().getTemplate());

                            if (pattern.matches(parentDataStream.name()) && !entry.getKey().getExclusions().matches(parentDataStream.name())) {
                                rules.add(entry.getValue());
                            }
                        } catch (ExpressionEvaluationException e) {
                            throw new PrivilegesEvaluationException("Error while rendering index pattern of role " + role, e);
                        }
                    }
                }
            }

        }

        if (rules.isEmpty()) {
            return unrestricted();
        } else {
            return compile(context, rules);
        }

        /*
        List<com.floragunn.searchsupport.queries.Query> renderedQueries = new ArrayList<>(rules.size());
        
        for (DlsQuery query : rules) {
            try {
                renderedQueries.add(query.queryTemplate.render(context.getUser()));
            } catch (ExpressionEvaluationException e) {
                throw new PrivilegesEvaluationException("Error while rendering query " + query, e);
            }
        }
        
        return new DlsRestriction(ImmutableList.of(renderedQueries));
        */

    }

    protected abstract JoinedRule unrestricted();

    protected abstract JoinedRule compile(PrivilegesEvaluationContext context, Collection<SingleRule> rules) throws PrivilegesEvaluationException;

    protected abstract String hasRestrictionsMetricName();

    protected abstract String evaluateRestrictionsMetricName();

    protected abstract String componentName();

    public synchronized void updateIndices(Meta indexMetadata) {
        StatefulRules<SingleRule> statefulRules = this.statefulRules;

        if (statefulRules == null || !statefulRules.indexMetadata.equals(indexMetadata)) {
            try (Meter meter = Meter.basic(metricsLevel, statefulIndexRebuild)) {
                this.statefulRules = new StatefulRules<>(roles, indexMetadata, this.roleToRuleFunction);
                this.componentState.replacePart(this.statefulRules.getComponentState());
            }
        }
    }

    @Override
    public ComponentState getComponentState() {
        return this.componentState;
    }

    static class StaticRules<MetaDataObject extends Meta.IndexLikeObject, RolePermissions extends Role.Index, SingleRule>
            implements ComponentStateProvider {

        static class Index<SingleRule> extends StaticRules<Meta.Index, Role.Index, SingleRule> {
            Index(SgDynamicConfiguration<Role> roles, Function<Role.Index, SingleRule> roleToRuleFunction) {
                super(roles, "index", Role::getIndexPermissions, roleToRuleFunction);
            }
        }

        static class Alias<SingleRule> extends StaticRules<Meta.Alias, Role.Alias, SingleRule> {
            Alias(SgDynamicConfiguration<Role> roles, Function<Role.Index, SingleRule> roleToRuleFunction) {
                super(roles, "alias", Role::getAliasPermissions, roleToRuleFunction);
            }
        }

        static class DataStream<SingleRule> extends StaticRules<Meta.Alias, Role.DataStream, SingleRule> {
            DataStream(SgDynamicConfiguration<Role> roles, Function<Role.Index, SingleRule> roleToRuleFunction) {
                super(roles, "data_stream", Role::getDataStreamPermissions, roleToRuleFunction);
            }
        }

        private final ComponentState componentState;

        protected final ImmutableSet<String> rolesWithIndexWildcardWithoutRule;
        protected final ImmutableMap<String, SingleRule> roleWithIndexWildcardToRule;
        protected final ImmutableMap<String, ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, SingleRule>> rolesToIndexPatternTemplateToRule;
        protected final ImmutableMap<String, ImmutableList<Exception>> rolesToInitializationErrors;
        protected final Function<Role.Index, SingleRule> roleToRuleFunction;

        StaticRules(SgDynamicConfiguration<Role> roles, String objectName, Function<Role, Collection<RolePermissions>> getPermissionsFunction,
                Function<Role.Index, SingleRule> roleToRuleFunction) {
            this.componentState = new ComponentState("static_rules_" + objectName);
            this.roleToRuleFunction = roleToRuleFunction;

            ImmutableSet.Builder<String> rolesWithIndexWildcardWithoutRule = new ImmutableSet.Builder<>();
            ImmutableMap.Builder<String, SingleRule> roleWithIndexWildcardToRule = new ImmutableMap.Builder<String, SingleRule>();
            ImmutableMap.Builder<String, ImmutableMap.Builder<Role.IndexPatterns.IndexPatternTemplate, SingleRule>> rolesToIndexPatternTemplateToRule = new ImmutableMap.Builder<String, ImmutableMap.Builder<Role.IndexPatterns.IndexPatternTemplate, SingleRule>>()
                    .defaultValue((k) -> new ImmutableMap.Builder<>());

            ImmutableMap.Builder<String, ImmutableList.Builder<Exception>> rolesToInitializationErrors = new ImmutableMap.Builder<String, ImmutableList.Builder<Exception>>()
                    .defaultValue((k) -> new ImmutableList.Builder<Exception>());

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();

                    for (RolePermissions rolePermissions : getPermissionsFunction.apply(role)) {
                        if (rolePermissions.getIndexPatterns().getPattern().isWildcard()) {
                            SingleRule singleRule = this.roleToRule(rolePermissions);

                            if (singleRule == null) {
                                rolesWithIndexWildcardWithoutRule.add(roleName);
                            } else {
                                roleWithIndexWildcardToRule.put(roleName, singleRule);
                            }

                            /*
                            ImmutableList<Role.Index.FlsPattern> flsPatterns = rolePermissions.getFls();
                            
                            if (flsPatterns == null || flsPatterns.isEmpty()) {
                                rolesWithIndexWildcardWithoutRule.add(roleName);
                            } else {
                                FlsRule flsRule = new FlsRule.SingleRole(role, rolePermissions);
                                roleWithIndexWildcardToRule.put(roleName, flsRule);
                            }
                            */

                        } else {
                            for (Role.IndexPatterns.IndexPatternTemplate indexPatternTemplate : rolePermissions.getIndexPatterns()
                                    .getPatternTemplates()) {
                                SingleRule singleRule = this.roleToRule(rolePermissions);

                                if (singleRule != null) {
                                    rolesToIndexPatternTemplateToRule.get(roleName).put(indexPatternTemplate, singleRule);
                                }

                                /*
                                ImmutableList<Role.Index.FlsPattern> flsPatterns = rolePermissions.getFls();
                                
                                if (flsPatterns == null || flsPatterns.isEmpty()) {
                                    continue;
                                }
                                
                                FlsRule flsRule = new FlsRule.SingleRole(role, rolePermissions);
                                
                                rolesToIndexPatternTemplateToRule.get(roleName).put(indexPatternTemplate, flsRule);
                                */
                            }

                        }

                    }
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                    rolesToInitializationErrors.get(entry.getKey()).with(e);
                }
            }

            this.rolesWithIndexWildcardWithoutRule = rolesWithIndexWildcardWithoutRule.build();
            this.roleWithIndexWildcardToRule = roleWithIndexWildcardToRule.build();
            this.rolesToIndexPatternTemplateToRule = rolesToIndexPatternTemplateToRule.build((b) -> b.build());
            this.rolesToInitializationErrors = rolesToInitializationErrors.build((b) -> b.build());

            if (this.rolesToInitializationErrors.isEmpty()) {
                this.componentState.initialized();
            } else {
                this.componentState.setState(State.PARTIALLY_INITIALIZED, "roles_with_errors");
                this.componentState.addDetail(rolesToInitializationErrors);
            }
        }

        protected SingleRule roleToRule(Role.Index rolePermissions) {
            return this.roleToRuleFunction.apply(rolePermissions);
        }

        @Override
        public ComponentState getComponentState() {
            return componentState;
        }

    }

    static class StatefulRules<SingleRule> implements ComponentStateProvider {
        final Meta indexMetadata;
        final Index<SingleRule> index;
        final Alias<SingleRule> alias;
        final DataStream<SingleRule> dataStream;
        final ComponentState componentState;

        StatefulRules(SgDynamicConfiguration<Role> roles, Meta indexMetadata, Function<Role.Index, SingleRule> roleToRuleFunction) {
            this.index = new Index<>(roles, indexMetadata, roleToRuleFunction);
            this.alias = new Alias<>(roles, indexMetadata, roleToRuleFunction);
            this.dataStream = new DataStream<>(roles, indexMetadata, roleToRuleFunction);
            this.indexMetadata = indexMetadata;
            this.componentState = new ComponentState("stateful_rules");
            this.componentState.addParts(this.index.getComponentState(), this.alias.getComponentState(), this.dataStream.getComponentState());
        }

        boolean covers(String index) {
            return this.indexMetadata.getIndexOrLike(index) != null;
        }

        static class Index<SingleRule> implements ComponentStateProvider {
            final ImmutableMap<Meta.Index, ImmutableMap<String, SingleRule>> indexToRoleToRule;
            final ImmutableMap<Meta.Index, ImmutableSet<String>> indexToRoleWithoutRule;

            final ImmutableMap<String, ImmutableList<Exception>> rolesToInitializationErrors;

            private final Function<Role.Index, SingleRule> roleToRuleFunction;
            private final ComponentState componentState;

            Index(SgDynamicConfiguration<Role> roles, Meta indexMetadata, Function<Role.Index, SingleRule> roleToRuleFunction) {
                this.componentState = new ComponentState("index");
                this.roleToRuleFunction = roleToRuleFunction;

                ImmutableMap.Builder<Meta.Index, ImmutableMap.Builder<String, SingleRule>> indexToRoleToQuery = new ImmutableMap.Builder<Meta.Index, ImmutableMap.Builder<String, SingleRule>>()
                        .defaultValue((k) -> new ImmutableMap.Builder<String, SingleRule>());

                ImmutableMap.Builder<Meta.Index, ImmutableSet.Builder<String>> indexToRoleWithoutQuery = new ImmutableMap.Builder<Meta.Index, ImmutableSet.Builder<String>>()
                        .defaultValue((k) -> new ImmutableSet.Builder<String>());

                ImmutableMap.Builder<String, ImmutableList.Builder<Exception>> rolesToInitializationErrors = new ImmutableMap.Builder<String, ImmutableList.Builder<Exception>>()
                        .defaultValue((k) -> new ImmutableList.Builder<Exception>());

                for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                    try {
                        String roleName = entry.getKey();
                        Role role = entry.getValue();

                        for (Role.Index indexPermissions : role.getIndexPermissions()) {
                            Pattern indexPattern = indexPermissions.getIndexPatterns().getPattern();

                            if (indexPattern.isWildcard()) {
                                // This is handled in the static IndexPermissions object.
                                continue;
                            }

                            if (indexPattern.isBlank()) {
                                // The pattern is likely blank because there are only templated patterns. Index patterns with templates are not handled here, but in the static IndexPermissions object
                                continue;
                            }

                            SingleRule rule = this.roleToRule(indexPermissions);

                            if (rule != null) {
                                for (Meta.Index index : indexPattern.iterateMatching(indexMetadata.indices(), Meta.Index::name)) {
                                    indexToRoleToQuery.get(index).put(roleName, rule);
                                }
                            } else {
                                for (Meta.Index index : indexPattern.iterateMatching(indexMetadata.indices(), Meta.Index::name)) {
                                    indexToRoleWithoutQuery.get(index).add(roleName);
                                }
                            }

                            /*
                            Template<Query> dlsQueryTemplate = indexPermissions.getDls();
                            
                            if (dlsQueryTemplate != null) {
                                DlsQuery dlsConfig = new DlsQuery(dlsQueryTemplate);
                            
                            } else {
                            
                            }*/
                        }

                        for (Role.Index aliasPermissions : role.getAliasPermissions()) {
                            Pattern aliasPattern = aliasPermissions.getIndexPatterns().getPattern();

                            if (aliasPattern.isWildcard()) {
                                // Wildcard index patterns are handled in the static IndexPermissions object.
                                continue;
                            }

                            if (aliasPattern.isBlank()) {
                                // The pattern is likely blank because there are only templated patterns. Index patterns with templates are not handled here, but in the static IndexPermissions object
                                continue;
                            }

                            SingleRule rule = this.roleToRule(aliasPermissions);

                            if (rule != null) {
                                for (Meta.Alias alias : aliasPattern.iterateMatching(indexMetadata.aliases(), Meta.Alias::name)) {
                                    alias.members().forEach((member) -> {
                                        if (member instanceof Meta.Index) {
                                            indexToRoleToQuery.get((Meta.Index) member).put(roleName, rule);
                                        }
                                        // DataStreams are handled in the DataStream object
                                    });
                                }
                            } else {
                                for (Meta.Alias alias : aliasPattern.iterateMatching(indexMetadata.aliases(), Meta.Alias::name)) {
                                    alias.members().forEach((member) -> {
                                        if (member instanceof Meta.Index) {
                                            indexToRoleWithoutQuery.get((Meta.Index) member).add(roleName);
                                        }
                                        // DataStreams are handled in the DataStream object
                                    });
                                }
                            }

                            /*
                            Template<Query> dlsQueryTemplate = aliasPermissions.getDls();
                            
                            if (dlsQueryTemplate != null) {
                                DlsQuery dlsConfig = new DlsQuery(dlsQueryTemplate);
                            
                                for (Meta.Alias alias : aliasPattern.iterateMatching(indexMetadata.aliases(), Meta.Alias::name)) {
                                    alias.resolveDeepAsIndex(Meta.Alias.ResolutionMode.NORMAL).forEach((index) -> {
                                        // TODO this also puts data stream backing indices here in case we have a alias to a data stream
                                        // CHECK
                                        indexToRoleToQuery.get(index).put(roleName, dlsConfig);
                                    });
                                }
                            } else {
                                for (Meta.Alias alias : aliasPattern.iterateMatching(indexMetadata.aliases(), Meta.Alias::name)) {
                                    alias.resolveDeepAsIndex(Meta.Alias.ResolutionMode.NORMAL).forEach((index) -> {
                                        indexToRoleWithoutQuery.get(index).add(roleName);
                                    });
                                }
                            }*/
                        }

                    } catch (Exception e) {
                        log.error("Unexpected exception while processing role: {}\nIgnoring role.", entry, e);
                        rolesToInitializationErrors.get(entry.getKey()).with(e);
                    }
                }

                this.indexToRoleToRule = indexToRoleToQuery.build((b) -> b.build());
                this.indexToRoleWithoutRule = indexToRoleWithoutQuery.build((b) -> b.build());
                this.rolesToInitializationErrors = rolesToInitializationErrors.build((b) -> b.build());

                if (this.rolesToInitializationErrors.isEmpty()) {
                    this.componentState.initialized();
                } else {
                    this.componentState.setState(State.PARTIALLY_INITIALIZED, "roles_with_errors");
                    this.componentState.addDetail(rolesToInitializationErrors);
                }
            }

            protected SingleRule roleToRule(Role.Index rolePermissions) {
                return this.roleToRuleFunction.apply(rolePermissions);
            }

            @Override
            public ComponentState getComponentState() {
                return componentState;
            }

        }

        static class Alias<SingleRule> implements ComponentStateProvider {
            final ImmutableMap<Meta.Alias, ImmutableMap<String, SingleRule>> aliasToRoleToRule;
            final ImmutableMap<Meta.Alias, ImmutableSet<String>> aliasToRoleWithoutRule;

            final ImmutableMap<String, ImmutableList<Exception>> rolesToInitializationErrors;

            private final Function<Role.Index, SingleRule> roleToRuleFunction;
            private final ComponentState componentState;

            Alias(SgDynamicConfiguration<Role> roles, Meta indexMetadata, Function<Role.Index, SingleRule> roleToRuleFunction) {
                this.componentState = new ComponentState("alias");
                this.roleToRuleFunction = roleToRuleFunction;

                ImmutableMap.Builder<Meta.Alias, ImmutableMap.Builder<String, SingleRule>> indexToRoleToQuery = new ImmutableMap.Builder<Meta.Alias, ImmutableMap.Builder<String, SingleRule>>()
                        .defaultValue((k) -> new ImmutableMap.Builder<String, SingleRule>());

                ImmutableMap.Builder<Meta.Alias, ImmutableSet.Builder<String>> indexToRoleWithoutQuery = new ImmutableMap.Builder<Meta.Alias, ImmutableSet.Builder<String>>()
                        .defaultValue((k) -> new ImmutableSet.Builder<String>());

                ImmutableMap.Builder<String, ImmutableList.Builder<Exception>> rolesToInitializationErrors = new ImmutableMap.Builder<String, ImmutableList.Builder<Exception>>()
                        .defaultValue((k) -> new ImmutableList.Builder<Exception>());

                for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                    try {
                        String roleName = entry.getKey();
                        Role role = entry.getValue();

                        for (Role.Alias aliasPermissions : role.getAliasPermissions()) {
                            Pattern aliasPattern = aliasPermissions.getIndexPatterns().getPattern();

                            if (aliasPattern.isWildcard()) {
                                // This is handled in the static IndexPermissions object.
                                continue;
                            }

                            if (aliasPattern.isBlank()) {
                                // The pattern is likely blank because there are only templated patterns. Index patterns with templates are not handled here, but in the static IndexPermissions object
                                continue;
                            }

                            SingleRule rule = this.roleToRule(aliasPermissions);

                            if (rule != null) {
                                for (Meta.Alias alias : aliasPattern.iterateMatching(indexMetadata.aliases(), Meta.Alias::name)) {
                                    indexToRoleToQuery.get(alias).put(roleName, rule);
                                }
                            } else {
                                for (Meta.Alias alias : aliasPattern.iterateMatching(indexMetadata.aliases(), Meta.Alias::name)) {
                                    indexToRoleWithoutQuery.get(alias).add(roleName);
                                }
                            }
                        }

                    } catch (Exception e) {
                        log.error("Unexpected exception while processing role: {}\nIgnoring role.", entry, e);
                        rolesToInitializationErrors.get(entry.getKey()).with(e);
                    }
                }

                this.aliasToRoleToRule = indexToRoleToQuery.build((b) -> b.build());
                this.aliasToRoleWithoutRule = indexToRoleWithoutQuery.build((b) -> b.build());
                this.rolesToInitializationErrors = rolesToInitializationErrors.build((b) -> b.build());

                if (this.rolesToInitializationErrors.isEmpty()) {
                    this.componentState.initialized();
                } else {
                    this.componentState.setState(State.PARTIALLY_INITIALIZED, "roles_with_errors");
                    this.componentState.addDetail(rolesToInitializationErrors);
                }
            }

            protected SingleRule roleToRule(Role.Index rolePermissions) {
                return this.roleToRuleFunction.apply(rolePermissions);
            }

            @Override
            public ComponentState getComponentState() {
                return componentState;
            }

        }

        static class DataStream<SingleRule> implements ComponentStateProvider {
            final ImmutableMap<Meta.DataStream, ImmutableMap<String, SingleRule>> dataStreamToRoleToRule;
            final ImmutableMap<Meta.DataStream, ImmutableSet<String>> dataStreamToRoleWithoutRule;

            final ImmutableMap<String, ImmutableList<Exception>> rolesToInitializationErrors;

            private final Function<Role.Index, SingleRule> roleToRuleFunction;
            private final ComponentState componentState;

            DataStream(SgDynamicConfiguration<Role> roles, Meta indexMetadata, Function<Role.Index, SingleRule> roleToRuleFunction) {
                this.componentState = new ComponentState("data_stream");
                this.roleToRuleFunction = roleToRuleFunction;

                ImmutableMap.Builder<Meta.DataStream, ImmutableMap.Builder<String, SingleRule>> indexToRoleToQuery = new ImmutableMap.Builder<Meta.DataStream, ImmutableMap.Builder<String, SingleRule>>()
                        .defaultValue((k) -> new ImmutableMap.Builder<String, SingleRule>());

                ImmutableMap.Builder<Meta.DataStream, ImmutableSet.Builder<String>> indexToRoleWithoutQuery = new ImmutableMap.Builder<Meta.DataStream, ImmutableSet.Builder<String>>()
                        .defaultValue((k) -> new ImmutableSet.Builder<String>());

                ImmutableMap.Builder<String, ImmutableList.Builder<Exception>> rolesToInitializationErrors = new ImmutableMap.Builder<String, ImmutableList.Builder<Exception>>()
                        .defaultValue((k) -> new ImmutableList.Builder<Exception>());

                for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                    try {
                        String roleName = entry.getKey();
                        Role role = entry.getValue();

                        for (Role.DataStream dataStreamPermissions : role.getDataStreamPermissions()) {
                            Pattern dataStreamPattern = dataStreamPermissions.getIndexPatterns().getPattern();

                            if (dataStreamPattern.isWildcard()) {
                                // This is handled in the static IndexPermissions object.
                                continue;
                            }

                            if (dataStreamPattern.isBlank()) {
                                // The pattern is likely blank because there are only templated patterns. Index patterns with templates are not handled here, but in the static IndexPermissions object
                                continue;
                            }

                            SingleRule rule = this.roleToRule(dataStreamPermissions);

                            if (rule != null) {
                                for (Meta.DataStream dataStream : dataStreamPattern.iterateMatching(indexMetadata.dataStreams(),
                                        Meta.DataStream::name)) {
                                    indexToRoleToQuery.get(dataStream).put(roleName, rule);
                                }
                            } else {
                                for (Meta.DataStream dataStream : dataStreamPattern.iterateMatching(indexMetadata.dataStreams(),
                                        Meta.DataStream::name)) {
                                    indexToRoleWithoutQuery.get(dataStream).add(roleName);
                                }
                            }
                        }

                        for (Role.Index aliasPermissions : role.getAliasPermissions()) {
                            Pattern aliasPattern = aliasPermissions.getIndexPatterns().getPattern();

                            if (aliasPattern.isWildcard()) {
                                // Wildcard index patterns are handled in the static IndexPermissions object.
                                continue;
                            }

                            if (aliasPattern.isBlank()) {
                                // The pattern is likely blank because there are only templated patterns. Index patterns with templates are not handled here, but in the static IndexPermissions object
                                continue;
                            }

                            SingleRule rule = this.roleToRule(aliasPermissions);

                            if (rule != null) {
                                for (Meta.Alias alias : aliasPattern.iterateMatching(indexMetadata.aliases(), Meta.Alias::name)) {
                                    alias.members().forEach((member) -> {
                                        if (member instanceof Meta.DataStream) {
                                            indexToRoleToQuery.get((Meta.DataStream) member).put(roleName, rule);
                                        }
                                        // DataStreams are handled in the DataStream object
                                    });
                                }
                            } else {
                                for (Meta.Alias alias : aliasPattern.iterateMatching(indexMetadata.aliases(), Meta.Alias::name)) {
                                    alias.members().forEach((member) -> {
                                        if (member instanceof Meta.DataStream) {
                                            indexToRoleWithoutQuery.get((Meta.DataStream) member).add(roleName);
                                        }
                                        // DataStreams are handled in the DataStream object
                                    });
                                }
                            }

                        }

                    } catch (Exception e) {
                        log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
                        rolesToInitializationErrors.get(entry.getKey()).with(e);
                    }
                }

                this.dataStreamToRoleToRule = indexToRoleToQuery.build((b) -> b.build());
                this.dataStreamToRoleWithoutRule = indexToRoleWithoutQuery.build((b) -> b.build());
                this.rolesToInitializationErrors = rolesToInitializationErrors.build((b) -> b.build());

                if (this.rolesToInitializationErrors.isEmpty()) {
                    this.componentState.initialized();
                } else {
                    this.componentState.setState(State.PARTIALLY_INITIALIZED, "roles_with_errors");
                    this.componentState.addDetail(rolesToInitializationErrors);
                }
            }

            protected SingleRule roleToRule(Role.Index rolePermissions) {
                return this.roleToRuleFunction.apply(rolePermissions);
            }

            @Override
            public ComponentState getComponentState() {
                return componentState;
            }

        }

        @Override
        public ComponentState getComponentState() {
            return componentState;
        }

    }

}
