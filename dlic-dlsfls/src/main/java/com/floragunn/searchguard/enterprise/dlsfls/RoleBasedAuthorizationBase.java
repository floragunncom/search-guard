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
import com.floragunn.searchguard.authz.actions.ResolvedIndices;
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
    protected final StaticRules.Index<SingleRule> staticIndexRules;
    protected final StaticRules.Alias<SingleRule> staticAliasRules;
    protected final StaticRules.DataStream<SingleRule> staticDataStreamRules;

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
        this.staticIndexRules = new StaticRules.Index<>(roles, roleToRuleFunction);
        this.staticAliasRules = new StaticRules.Alias<>(roles, roleToRuleFunction);
        this.staticDataStreamRules = new StaticRules.DataStream<>(roles, roleToRuleFunction);

        try (Meter meter = Meter.basic(metricsLevel, statefulIndexRebuild)) {
            this.statefulRules = new StatefulRules<>(roles, indexMetadata, roleToRuleFunction);
        }

        this.componentState.addPart(this.staticIndexRules.getComponentState());
        // this.componentState.addPart(this.statefulIndexQueries.getComponentState()); TODO
        this.componentState.setConfigVersion(roles.getDocVersion());
        this.componentState.updateStateFromParts();

        if (metricsLevel.basicEnabled()) {
            this.componentState.addMetrics("stateful_index_rebuilds", statefulIndexRebuild);
        }
    }

    boolean hasRestrictions(PrivilegesEvaluationContext context, ResolvedIndices resolved, Meter meter) throws PrivilegesEvaluationException {
        try (Meter subMeter = meter.detail(hasRestrictionsMetricName())) {
            if (context.getMappedRoles().isEmpty()) {
                return true;
            }

            if (this.staticIndexRules.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
                return false;
            }

            StatefulRules<SingleRule> statefulRules = this.statefulRules;

            if (resolved.getLocal().hasNonExistingObjects()) {
                // We get a request for an index unknown to this instance. Usually, this is the case because the index simply does not exist.
                // For non-existing indices, it is safe to assume that no documents can be accessed.

                if (log.isDebugEnabled()) {
                    log.debug("ResolvedIndices {} contain non-existing indices. Assuming full document restriction.", resolved);
                }

                return true;
            }

            if (this.staticIndexRules.roleWithIndexWildcardToRule.keySet().containsAny(context.getMappedRoles())) {
                return true;
            }

            // The logic is here a bit tricky: For each index/alias/data stream we assume restrictions until we found an unrestricted role.
            // If we found an unrestricted role, we continue with the next index/alias/data stream. If we found a restricted role, we abort 
            // early and return true.

            for (Meta.Index index : resolved.getLocal().getPureIndices()) {
                if (hasRestrictions(context, statefulRules, index)) {
                    return true;
                }
            }

            for (Meta.Alias alias : resolved.getLocal().getAliases()) {
                ImmutableSet<String> roleWithoutRules = statefulRules.alias.aliasToRoleWithoutRule.get(alias);

                if (roleWithoutRules != null && roleWithoutRules.containsAny(context.getMappedRoles())) {
                    // Unrestricted => check next alias/data stream
                    continue;
                }

                if (this.staticAliasRules.hasUnrestrictedPatternTemplates(context, alias)) {
                    // Unrestricted => check next alias/data stream
                    continue;
                }

                // There are restrictions => abort early
                return true;
            }

            for (Meta.DataStream dataStream : resolved.getLocal().getDataStreams()) {
                if (hasRestrictions(context, statefulRules, dataStream)) {
                    // There are restrictions => abort early
                    return true;
                }
            }

            return false;
        } catch (PrivilegesEvaluationException e) {
            componentState.addLastException("has_restriction", e);
            throw e;
        } catch (RuntimeException e) {
            componentState.addLastException("has_restriction_u", e);
            throw e;
        }
    }

    boolean hasRestrictions(PrivilegesEvaluationContext context, Meta.Index index, Meter meter) throws PrivilegesEvaluationException {
        try (Meter subMeter = meter.detail(hasRestrictionsMetricName())) {
            if (context.getMappedRoles().isEmpty()) {
                return true;
            }

            if (this.staticIndexRules.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
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

            if (this.staticIndexRules.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
                return false;
            }

            return hasRestrictions(context, statefulRules, index);
        } catch (PrivilegesEvaluationException e) {
            componentState.addLastException("has_dls_restriction", e);
            throw e;
        } catch (RuntimeException e) {
            componentState.addLastException("has_dls_restriction_u", e);
            throw e;
        }
    }

    private boolean hasRestrictions(PrivilegesEvaluationContext context, StatefulRules<SingleRule> statefulRules, Meta.Index index)
            throws PrivilegesEvaluationException {

        {
            ImmutableSet<String> roleWithoutRule = statefulRules.index.indexToRoleWithoutRule.get(index);

            if (roleWithoutRule != null && roleWithoutRule.containsAny(context.getMappedRoles())) {
                return false;
            }

            // We assume that we have a restriction unless there are roles without restriction. This, we only have to check the roles without restriction.
            if (this.staticIndexRules.hasUnrestrictedPatternTemplates(context, index)) {
                return false;
            }
        }

        if (!index.parentAliases().isEmpty()) {
            if (!hasRestrictions(context, statefulRules, index.parentAliases())) {
                return false;
            }
        }

        if (index.parentDataStream() != null) {
            if (!hasRestrictions(context, statefulRules, index.parentDataStream())) {
                return false;
            }
        }

        // If we found no roles without restriction, we assume a restriction
        return true;
    }

    private boolean hasRestrictions(PrivilegesEvaluationContext context, StatefulRules<SingleRule> statefulRules, Collection<Meta.Alias> aliases)
            throws PrivilegesEvaluationException {
        if (aliases.isEmpty()) {
            return true;
        }

        if (this.staticAliasRules.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
            return false;
        }

        for (Meta.Alias alias : aliases) {
            ImmutableSet<String> roleWithoutRule = statefulRules.alias.aliasToRoleWithoutRule.get(alias);

            if (roleWithoutRule != null && roleWithoutRule.containsAny(context.getMappedRoles())) {
                return false;
            }

            if (this.staticAliasRules.hasUnrestrictedPatternTemplates(context, alias)) {
                return false;
            }
        }

        // If we found no roles without restriction, we assume a restriction
        return true;
    }

    private boolean hasRestrictions(PrivilegesEvaluationContext context, StatefulRules<SingleRule> statefulRules, Meta.DataStream dataStream)
            throws PrivilegesEvaluationException {
        if (this.staticDataStreamRules.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
            return false;
        }

        {
            ImmutableSet<String> roleWithoutRule = statefulRules.dataStream.dataStreamToRoleWithoutRule.get(dataStream);

            if (roleWithoutRule != null && roleWithoutRule.containsAny(context.getMappedRoles())) {
                return false;
            }

            // We assume that we have a restriction unless there are roles without restriction. This, we only have to check the roles without restriction.
            if (this.staticDataStreamRules.hasUnrestrictedPatternTemplates(context, dataStream)) {
                return false;
            }
        }

        if (!dataStream.parentAliases().isEmpty()) {
            if (!hasRestrictions(context, statefulRules, dataStream.parentAliases())) {
                return false;
            }
        }

        // If we found no roles without restriction, we assume a restriction
        return true;
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
        if (context.getMappedRoles().isEmpty()) {
            return fullyRestricted();
        }

        Meta.DataStream parentDataStream = index.parentDataStream();

        if (this.staticIndexRules.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
            return unrestricted();
        }

        if (parentDataStream != null) {
            if (this.staticDataStreamRules.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
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
                SingleRule rule = this.staticIndexRules.roleWithIndexWildcardToRule.get(role);

                if (rule != null) {
                    rules.add(rule);
                }
            }

            if (parentDataStream != null) {
                SingleRule rule = this.staticDataStreamRules.roleWithIndexWildcardToRule.get(role);

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

            ImmutableSet<Role.IndexPatterns.IndexPatternTemplate> indexPatternTemplatesWithoutRole = this.staticIndexRules.rolesToIndexPatternTemplateWithoutRule
                    .get(role);

            if (indexPatternTemplatesWithoutRole != null) {
                for (Role.IndexPatterns.IndexPatternTemplate indexPatternTemplate : indexPatternTemplatesWithoutRole) {
                    try {
                        Pattern pattern = context.getRenderedPattern(indexPatternTemplate.getTemplate());

                        if (pattern.matches(index.name()) && !indexPatternTemplate.getExclusions().matches(index.name())) {
                            return unrestricted();
                        }
                    } catch (ExpressionEvaluationException e) {
                        throw new PrivilegesEvaluationException("Error while rendering index pattern of role " + role, e);
                    }
                }
            }

            ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, SingleRule> indexPatternTemplateToQuery = this.staticIndexRules.rolesToIndexPatternTemplateToRule
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
                if (this.staticAliasRules.isUnrestrictedViaParentAlias(context, index, role)) {
                    return unrestricted();
                }

                // As a side effect, this adds rules to the rules HashSet
                this.staticAliasRules.collectRulesViaParentAlias(context, index, role, rules);
            }

            if (parentDataStream != null) {
                ImmutableSet<Role.IndexPatterns.IndexPatternTemplate> dataStreamPatternTemplateWithoutRule = this.staticDataStreamRules.rolesToIndexPatternTemplateWithoutRule
                        .get(role);

                if (dataStreamPatternTemplateWithoutRule != null) {
                    for (Role.IndexPatterns.IndexPatternTemplate indexPatternTemplate : dataStreamPatternTemplateWithoutRule) {
                        try {
                            Pattern pattern = context.getRenderedPattern(indexPatternTemplate.getTemplate());

                            if (pattern.matches(parentDataStream.name()) && !indexPatternTemplate.getExclusions().matches(parentDataStream.name())) {
                                return unrestricted();
                            }
                        } catch (ExpressionEvaluationException e) {
                            throw new PrivilegesEvaluationException("Error while rendering index pattern of role " + role, e);
                        }
                    }
                }

                ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, SingleRule> dataStreamPatternTemplateToRule = this.staticDataStreamRules.rolesToIndexPatternTemplateToRule
                        .get(role);

                if (dataStreamPatternTemplateToRule != null) {
                    for (Map.Entry<Role.IndexPatterns.IndexPatternTemplate, SingleRule> entry : dataStreamPatternTemplateToRule.entrySet()) {
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

                if (!parentDataStream.parentAliases().isEmpty()) {
                    if (this.staticAliasRules.isUnrestrictedViaParentAlias(context, parentDataStream, role)) {
                        return unrestricted();
                    }

                    // As a side effect, this adds rules to the rules HashSet
                    this.staticAliasRules.collectRulesViaParentAlias(context, parentDataStream, role, rules);
                }
            }
        }

        if (rules.isEmpty()) {
            return fullyRestricted();
        } else {
            return compile(context, rules);
        }
    }

    protected abstract JoinedRule unrestricted();

    protected abstract JoinedRule fullyRestricted();

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

            boolean isUnrestrictedViaParentAlias(PrivilegesEvaluationContext context, Meta.IndexLikeObject indexLike, String role)
                    throws PrivilegesEvaluationException {
                ImmutableSet<Role.IndexPatterns.IndexPatternTemplate> aliasPatternTemplatesWithoutRole = this.rolesToIndexPatternTemplateWithoutRule
                        .get(role);

                if (aliasPatternTemplatesWithoutRole != null) {
                    for (Meta.Alias alias : indexLike.parentAliases()) {
                        for (Role.IndexPatterns.IndexPatternTemplate indexPatternTemplate : aliasPatternTemplatesWithoutRole) {
                            try {
                                Pattern pattern = context.getRenderedPattern(indexPatternTemplate.getTemplate());

                                if (pattern.matches(alias.name()) && !indexPatternTemplate.getExclusions().matches(alias.name())) {
                                    return true;
                                }
                            } catch (ExpressionEvaluationException e) {
                                throw new PrivilegesEvaluationException("Error while rendering index pattern of role " + role, e);
                            }
                        }
                    }
                }

                return false;
            }

            void collectRulesViaParentAlias(PrivilegesEvaluationContext context, Meta.IndexLikeObject indexLike, String role,
                    Set<SingleRule> rulesSink) throws PrivilegesEvaluationException {
                ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, SingleRule> aliasPatternTemplateToRule = this.rolesToIndexPatternTemplateToRule
                        .get(role);

                if (aliasPatternTemplateToRule != null) {
                    for (Meta.Alias alias : indexLike.parentAliases()) {
                        for (Map.Entry<Role.IndexPatterns.IndexPatternTemplate, SingleRule> entry : aliasPatternTemplateToRule.entrySet()) {
                            try {
                                Pattern pattern = context.getRenderedPattern(entry.getKey().getTemplate());

                                if (pattern.matches(alias.name()) && !entry.getKey().getExclusions().matches(alias.name())) {
                                    rulesSink.add(entry.getValue());
                                }
                            } catch (ExpressionEvaluationException e) {
                                throw new PrivilegesEvaluationException("Error while rendering index pattern of role " + role, e);
                            }
                        }
                    }
                }
            }
        }

        static class DataStream<SingleRule> extends StaticRules<Meta.DataStream, Role.DataStream, SingleRule> {
            DataStream(SgDynamicConfiguration<Role> roles, Function<Role.Index, SingleRule> roleToRuleFunction) {
                super(roles, "data_stream", Role::getDataStreamPermissions, roleToRuleFunction);
            }
        }

        private final ComponentState componentState;

        protected final ImmutableSet<String> rolesWithIndexWildcardWithoutRule;
        protected final ImmutableMap<String, SingleRule> roleWithIndexWildcardToRule;
        protected final ImmutableMap<String, ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, SingleRule>> rolesToIndexPatternTemplateToRule;
        protected final ImmutableMap<String, ImmutableSet<Role.IndexPatterns.IndexPatternTemplate>> rolesToIndexPatternTemplateWithoutRule;

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
            ImmutableMap.Builder<String, ImmutableSet.Builder<Role.IndexPatterns.IndexPatternTemplate>> rolesToIndexPatternTemplateWithoutRule = new ImmutableMap.Builder<String, ImmutableSet.Builder<Role.IndexPatterns.IndexPatternTemplate>>()
                    .defaultValue((k) -> new ImmutableSet.Builder<>());

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
                        } else {
                            for (Role.IndexPatterns.IndexPatternTemplate indexPatternTemplate : rolePermissions.getIndexPatterns()
                                    .getPatternTemplates()) {
                                SingleRule singleRule = this.roleToRule(rolePermissions);

                                if (singleRule == null) {
                                    rolesToIndexPatternTemplateWithoutRule.get(roleName).add(indexPatternTemplate);
                                } else {
                                    rolesToIndexPatternTemplateToRule.get(roleName).put(indexPatternTemplate, singleRule);
                                }
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
            this.rolesToIndexPatternTemplateWithoutRule = rolesToIndexPatternTemplateWithoutRule.build((b) -> b.build());
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

        boolean hasUnrestrictedPatternTemplates(PrivilegesEvaluationContext context, MetaDataObject indexLike) throws PrivilegesEvaluationException {
            // We assume that we have a restriction unless there are roles without restriction. This, we only have to check the roles without restriction.
            for (String role : context.getMappedRoles()) {
                ImmutableSet<Role.IndexPatterns.IndexPatternTemplate> indexPatternTemplatesWithoutRole = this.rolesToIndexPatternTemplateWithoutRule
                        .get(role);

                if (indexPatternTemplatesWithoutRole != null) {
                    for (Role.IndexPatterns.IndexPatternTemplate indexPatternTemplate : indexPatternTemplatesWithoutRole) {
                        try {
                            Pattern pattern = context.getRenderedPattern(indexPatternTemplate.getTemplate());

                            if (pattern.matches(indexLike.name()) && !indexPatternTemplate.getExclusions().matches(indexLike.name())) {
                                return true;
                            }
                        } catch (ExpressionEvaluationException e) {
                            log.error("Error while rendering index pattern of role " + role, e);
                        }
                    }
                }
            }

            // If we found no roles without restriction, we assume a restriction
            return false;
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

                ImmutableMap.Builder<Meta.Index, ImmutableMap.Builder<String, SingleRule>> indexToRoleToRule = new ImmutableMap.Builder<Meta.Index, ImmutableMap.Builder<String, SingleRule>>()
                        .defaultValue((k) -> new ImmutableMap.Builder<String, SingleRule>());

                ImmutableMap.Builder<Meta.Index, ImmutableSet.Builder<String>> indexToRoleWithoutRule = new ImmutableMap.Builder<Meta.Index, ImmutableSet.Builder<String>>()
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
                                    indexToRoleToRule.get(index).put(roleName, rule);
                                }
                            } else {
                                for (Meta.Index index : indexPattern.iterateMatching(indexMetadata.indices(), Meta.Index::name)) {
                                    indexToRoleWithoutRule.get(index).add(roleName);
                                }
                            }
                        }

                        for (Role.Index aliasPermissions : role.getAliasPermissions()) {
                            Pattern aliasPattern = aliasPermissions.getIndexPatterns().getPattern();

                            if (aliasPattern.isBlank()) {
                                // The pattern is likely blank because there are only templated patterns. Index patterns with templates are not handled here, but in the static IndexPermissions object
                                continue;
                            }

                            SingleRule rule = this.roleToRule(aliasPermissions);

                            if (rule != null) {
                                for (Meta.Alias alias : aliasPattern.iterateMatching(indexMetadata.aliases(), Meta.Alias::name)) {
                                    alias.members().forEach((member) -> {
                                        if (member instanceof Meta.Index) {
                                            indexToRoleToRule.get((Meta.Index) member).put(roleName, rule);
                                        }
                                        // DataStreams are handled in the DataStream object
                                    });
                                }
                            } else {
                                for (Meta.Alias alias : aliasPattern.iterateMatching(indexMetadata.aliases(), Meta.Alias::name)) {
                                    alias.members().forEach((member) -> {
                                        if (member instanceof Meta.Index) {
                                            indexToRoleWithoutRule.get((Meta.Index) member).add(roleName);
                                        }
                                        // DataStreams are handled in the DataStream object
                                    });
                                }
                            }
                        }

                    } catch (Exception e) {
                        log.error("Unexpected exception while processing role: {}\nIgnoring role.", entry, e);
                        rolesToInitializationErrors.get(entry.getKey()).with(e);
                    }
                }

                this.indexToRoleToRule = indexToRoleToRule.build((b) -> b.build());
                this.indexToRoleWithoutRule = indexToRoleWithoutRule.build((b) -> b.build());
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
                                    });
                                }
                            } else {
                                for (Meta.Alias alias : aliasPattern.iterateMatching(indexMetadata.aliases(), Meta.Alias::name)) {
                                    alias.members().forEach((member) -> {
                                        if (member instanceof Meta.DataStream) {
                                            indexToRoleWithoutQuery.get((Meta.DataStream) member).add(roleName);
                                        }
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
