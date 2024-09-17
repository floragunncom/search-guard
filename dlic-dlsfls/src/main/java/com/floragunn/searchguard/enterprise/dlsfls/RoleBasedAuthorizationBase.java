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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
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
import com.selectivem.collections.CompactMapGroupBuilder;
import com.selectivem.collections.DeduplicatingCompactSubSetBuilder;
import com.selectivem.collections.ImmutableCompactSubSet;

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

        if (indexMetadata != null) {
            try (Meter meter = Meter.basic(metricsLevel, statefulIndexRebuild)) {
                this.statefulRules = new StatefulRules<>(roles, indexMetadata, roleToRuleFunction);
            }
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

            if (resolved == null) {
                return true;
            }

            if (resolved.getLocal().hasNonExistingObjects()) {
                // We get a request for an index unknown to this instance. Usually, this is the case because the index simply does not exist.
                // For non-existing indices, it is safe to assume that no documents can be accessed.


                log.debug("ResolvedIndices {} contain non-existing indices. Assuming full document restriction.", resolved);

                return true;
            }

            if (this.staticIndexRules.roleWithIndexWildcardToRule.keySet().containsAny(context.getMappedRoles())) {
                return true;
            }

            StatefulRules<SingleRule> statefulRules = this.statefulRules;

            // The logic is here a bit tricky: For each index/alias/data stream we assume restrictions until we found an unrestricted role.
            // If we found an unrestricted role, we continue with the next index/alias/data stream. If we found a restricted role, we abort 
            // early and return true.

            for (Meta.Index index : resolved.getLocal().getPureIndices()) {
                if (hasRestrictions(context, statefulRules, index)) {
                    return true;
                }
            }

            if (!this.staticAliasRules.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
                // Only go through the aliases if we do not have a global role which marks all indices as unrestricted

                for (Meta.Alias alias : resolved.getLocal().getAliases()) {
                    if (hasRestrictions(context, statefulRules, alias)) {
                        return true;
                    }
                }
            }

            if (!this.staticDataStreamRules.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
                // Only go through the data streams if we do not have a global role which marks all indices as unrestricted

                for (Meta.DataStream dataStream : resolved.getLocal().getDataStreams()) {
                    if (hasRestrictions(context, statefulRules, dataStream)) {
                        // There are restrictions => abort early
                        return true;
                    }
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

            if (!index.exists()) {
                // We get a request for an index unknown to this instance. Usually, this is the case because the index simply does not exist.
                // For non-existing indices, it is safe to assume that no documents can be accessed.

                log.debug("Index {} does not exist. Assuming full document restriction.", index);

                return true;
            }

            if (this.staticIndexRules.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
                return false;
            }

            return hasRestrictions(context, this.statefulRules, index);

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

        if (statefulRules != null) {
            ImmutableCompactSubSet<String> roleWithoutRule = statefulRules.index.indexToRoleWithoutRule.get(index);

            if (roleWithoutRule != null && roleWithoutRule.containsAny(context.getMappedRoles())) {
                return false;
            }
        } else {
            if (this.staticIndexRules.hasUnrestrictedPatterns(context, index)) {
                return false;
            }
        }

        // We assume that we have a restriction unless there are roles without restriction. This, we only have to check the roles without restriction.
        if (this.staticIndexRules.hasUnrestrictedPatternTemplates(context, index)) {
            return false;
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

    private boolean hasRestrictions(PrivilegesEvaluationContext context, StatefulRules<SingleRule> statefulRules, Meta.Alias alias)
            throws PrivilegesEvaluationException {

        if (statefulRules != null) {
            ImmutableCompactSubSet<String> roleWithoutRule = statefulRules.alias.aliasToRoleWithoutRule.get(alias);

            if (roleWithoutRule != null && roleWithoutRule.containsAny(context.getMappedRoles())) {
                return false;
            }
        } else {
            if (this.staticAliasRules.hasUnrestrictedPatterns(context, alias)) {
                return false;
            }

        }

        // We assume that we have a restriction unless there are roles without restriction. This, we only have to check the roles without restriction.
        if (this.staticAliasRules.hasUnrestrictedPatternTemplates(context, alias)) {
            return false;
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
            if (statefulRules != null) {
                ImmutableCompactSubSet<String> roleWithoutRule = statefulRules.alias.aliasToRoleWithoutRule.get(alias);

                if (roleWithoutRule != null && roleWithoutRule.containsAny(context.getMappedRoles())) {
                    return false;
                }
            } else {
                if (this.staticAliasRules.hasUnrestrictedPatterns(context, alias)) {
                    return false;
                }
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
        if (statefulRules != null) {
            ImmutableCompactSubSet<String> roleWithoutRule = statefulRules.dataStream.dataStreamToRoleWithoutRule.get(dataStream);

            if (roleWithoutRule != null && roleWithoutRule.containsAny(context.getMappedRoles())) {
                return false;
            }
        } else {
            if (this.staticDataStreamRules.hasUnrestrictedPatterns(context, dataStream)) {
                return false;
            }
        }
        // We assume that we have a restriction unless there are roles without restriction. This, we only have to check the roles without restriction.
        if (this.staticDataStreamRules.hasUnrestrictedPatternTemplates(context, dataStream)) {
            return false;
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

        if (!index.parentAliases().isEmpty()) {
            if (this.staticAliasRules.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
                return unrestricted();
            }
        }

        StatefulRules<SingleRule> statefulRules = this.statefulRules;
        Map<String, SingleRule> roleToQueryForIndex = null;
        Map<String, SingleRule> roleToQueryForDataStream = null;

        if (statefulRules != null) {
            ImmutableCompactSubSet<String> roleWithoutQuery = statefulRules.index.indexToRoleWithoutRule.get(index);

            if (roleWithoutQuery != null && roleWithoutQuery.containsAny(context.getMappedRoles())) {
                return unrestricted();
            }

            roleToQueryForIndex = statefulRules.index.indexToRoleToRule.get(index);
        }

        if (statefulRules != null && parentDataStream != null) {
            ImmutableCompactSubSet<String> roleWithoutQuery = statefulRules.dataStream.dataStreamToRoleWithoutRule.get(parentDataStream);

            if (roleWithoutQuery != null && roleWithoutQuery.containsAny(context.getMappedRoles())) {
                return unrestricted();
            }

            roleToQueryForDataStream = statefulRules.dataStream.dataStreamToRoleToRule.get(parentDataStream);
        }

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

            if (statefulRules == null) {
                // Only when we have no stateful information, we also check the simple index patterns

                Pattern indexPatternWithoutRule = this.staticIndexRules.rolesToIndexPatternWithoutRule.get(role);
                if (indexPatternWithoutRule != null && indexPatternWithoutRule.matches(index.name())) {
                    return unrestricted();
                }

                ImmutableMap<Pattern, SingleRule> indexPatternToRule = this.staticIndexRules.rolesToIndexPatternToRule.get(role);
                if (indexPatternToRule != null) {
                    for (Map.Entry<Pattern, SingleRule> entry : indexPatternToRule.entrySet()) {
                        Pattern pattern = entry.getKey();

                        if (pattern.matches(index.name())) {
                            rules.add(entry.getValue());
                        }
                    }
                }
            }

            ImmutableSet<Role.IndexPatterns.IndexPatternTemplate> indexPatternTemplatesWithoutRule = this.staticIndexRules.rolesToIndexPatternTemplateWithoutRule
                    .get(role);

            if (indexPatternTemplatesWithoutRule != null) {
                for (Role.IndexPatterns.IndexPatternTemplate indexPatternTemplate : indexPatternTemplatesWithoutRule) {
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

            ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, SingleRule> indexPatternTemplateToRule = this.staticIndexRules.rolesToIndexPatternTemplateToRule
                    .get(role);

            if (indexPatternTemplateToRule != null) {
                for (Map.Entry<Role.IndexPatterns.IndexPatternTemplate, SingleRule> entry : indexPatternTemplateToRule.entrySet()) {
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
                if (statefulRules == null && this.staticAliasRules.isUnrestrictedViaParentAlias(context, index, role)) {
                    return unrestricted();
                }

                if (this.staticAliasRules.isUnrestrictedViaTemplatesOnParentAlias(context, index, role)) {
                    return unrestricted();
                }

                // As a side effect, this adds rules to the rules HashSet
                this.staticAliasRules.collectRulesViaTemplatesOnParentAlias(context, index, role, rules);

                if (statefulRules == null) {
                    this.staticAliasRules.collectRulesViaParentAlias(context, index, role, rules);
                }
            }

            if (parentDataStream != null) {
                if (statefulRules == null) {
                    // If there are no stateful rules, we also need to check the static data stream rules.
                    // There is no direct equivalent for the case when there are stateful rules, as the privileges
                    // then come via the backing indices

                    Pattern dataStreamPatternWithoutRule = this.staticDataStreamRules.rolesToIndexPatternWithoutRule.get(role);
                    if (dataStreamPatternWithoutRule != null && dataStreamPatternWithoutRule.matches(parentDataStream.name())) {
                        return unrestricted();
                    }

                    ImmutableMap<Pattern, SingleRule> dataStreamPatternToRule = this.staticDataStreamRules.rolesToIndexPatternToRule.get(role);
                    if (dataStreamPatternToRule != null) {
                        for (Map.Entry<Pattern, SingleRule> entry : dataStreamPatternToRule.entrySet()) {
                            Pattern pattern = entry.getKey();

                            if (pattern.matches(parentDataStream.name())) {
                                rules.add(entry.getValue());
                            }
                        }
                    }
                }

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
                    if (statefulRules == null && this.staticAliasRules.isUnrestrictedViaParentAlias(context, parentDataStream, role)) {
                        return unrestricted();
                    }

                    if (this.staticAliasRules.isUnrestrictedViaTemplatesOnParentAlias(context, parentDataStream, role)) {
                        return unrestricted();
                    }

                    // As a side effect, this adds rules to the rules HashSet
                    this.staticAliasRules.collectRulesViaTemplatesOnParentAlias(context, parentDataStream, role, rules);

                    if (statefulRules == null) {
                        this.staticAliasRules.collectRulesViaParentAlias(context, parentDataStream, role, rules);
                    }
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

            /**
             * Only necessary when no stateful index information is present.
             */
            boolean isUnrestrictedViaParentAlias(PrivilegesEvaluationContext context, Meta.IndexLikeObject indexLike, String role)
                    throws PrivilegesEvaluationException {
                Pattern aliasPatternWithoutRule = this.rolesToIndexPatternWithoutRule.get(role);

                if (aliasPatternWithoutRule != null) {
                    for (Meta.Alias alias : indexLike.parentAliases()) {
                        if (aliasPatternWithoutRule.matches(alias.name())) {
                            return true;
                        }
                    }
                }

                return false;
            }

            boolean isUnrestrictedViaTemplatesOnParentAlias(PrivilegesEvaluationContext context, Meta.IndexLikeObject indexLike, String role)
                    throws PrivilegesEvaluationException {
                ImmutableSet<Role.IndexPatterns.IndexPatternTemplate> aliasPatternTemplatesWithoutRule = this.rolesToIndexPatternTemplateWithoutRule
                        .get(role);

                if (aliasPatternTemplatesWithoutRule != null) {
                    for (Meta.Alias alias : indexLike.parentAliases()) {
                        for (Role.IndexPatterns.IndexPatternTemplate indexPatternTemplate : aliasPatternTemplatesWithoutRule) {
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
                ImmutableMap<Pattern, SingleRule> aliasPatternToRule = this.rolesToIndexPatternToRule.get(role);

                if (aliasPatternToRule != null) {
                    for (Meta.Alias alias : indexLike.parentAliases()) {
                        for (Map.Entry<Pattern, SingleRule> entry : aliasPatternToRule.entrySet()) {
                            if (entry.getKey().matches(alias.name())) {
                                rulesSink.add(entry.getValue());
                            }
                        }
                    }
                }
            }

            void collectRulesViaTemplatesOnParentAlias(PrivilegesEvaluationContext context, Meta.IndexLikeObject indexLike, String role,
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

        /**
         * Only used when no index metadata is available upon construction
         */
        protected final ImmutableMap<String, ImmutableMap<Pattern, SingleRule>> rolesToIndexPatternToRule;

        /**
         * Only used when no index metadata is available upon construction
         */
        protected final ImmutableMap<String, Pattern> rolesToIndexPatternWithoutRule;

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
            ImmutableMap.Builder<String, ImmutableMap.Builder<Pattern, SingleRule>> rolesToIndexPatternToRule = new ImmutableMap.Builder<String, ImmutableMap.Builder<Pattern, SingleRule>>()
                    .defaultValue((k) -> new ImmutableMap.Builder<>());
            ImmutableMap.Builder<String, List<Pattern>> rolesToIndexPatternWithoutRule = new ImmutableMap.Builder<String, List<Pattern>>()
                    .defaultValue((k) -> new ArrayList<>());

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
                            SingleRule singleRule = this.roleToRule(rolePermissions);

                            for (Role.IndexPatterns.IndexPatternTemplate indexPatternTemplate : rolePermissions.getIndexPatterns()
                                    .getPatternTemplates()) {
                                if (singleRule == null) {
                                    rolesToIndexPatternTemplateWithoutRule.get(roleName).add(indexPatternTemplate);
                                } else {
                                    rolesToIndexPatternTemplateToRule.get(roleName).put(indexPatternTemplate, singleRule);
                                }
                            }

                            if (!rolePermissions.getIndexPatterns().getPattern().isBlank()) {
                                if (singleRule == null) {
                                    rolesToIndexPatternWithoutRule.get(roleName).add(rolePermissions.getIndexPatterns().getPattern());
                                } else {
                                    rolesToIndexPatternToRule.get(roleName).put(rolePermissions.getIndexPatterns().getPattern(), singleRule);
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

            this.rolesToIndexPatternToRule = rolesToIndexPatternToRule.build((b) -> b.build());
            this.rolesToIndexPatternWithoutRule = rolesToIndexPatternWithoutRule.build((b) -> Pattern.join(b));

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

        /**
         * Only to be used if there is no stateful index information
         */
        boolean hasUnrestrictedPatterns(PrivilegesEvaluationContext context, MetaDataObject indexLike) throws PrivilegesEvaluationException {
            // We assume that we have a restriction unless there are roles without restriction. This, we only have to check the roles without restriction.
            for (String role : context.getMappedRoles()) {
                Pattern pattern = this.rolesToIndexPatternWithoutRule.get(role);

                if (pattern != null && pattern.matches(indexLike.name())) {
                    return true;
                }
            }

            // If we found no roles without restriction, we assume a restriction
            return false;
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
            final ImmutableMap<Meta.Index, Map<String, SingleRule>> indexToRoleToRule;
            final ImmutableMap<Meta.Index, ImmutableCompactSubSet<String>> indexToRoleWithoutRule;

            final ImmutableMap<String, ImmutableList<Exception>> rolesToInitializationErrors;

            private final Function<Role.Index, SingleRule> roleToRuleFunction;
            private final ComponentState componentState;

            Index(SgDynamicConfiguration<Role> roles, Meta indexMetadata, Function<Role.Index, SingleRule> roleToRuleFunction) {
                this.componentState = new ComponentState("index");
                this.roleToRuleFunction = roleToRuleFunction;

                DeduplicatingCompactSubSetBuilder<String> roleSetBuilder = new DeduplicatingCompactSubSetBuilder<>(roles.getCEntries().keySet());
                CompactMapGroupBuilder<String, SingleRule> roleMapBuilder = new CompactMapGroupBuilder<>(roles.getCEntries().keySet());

                ImmutableMap.Builder<Meta.Index, CompactMapGroupBuilder.MapBuilder<String, SingleRule>> indexToRoleToRule = new ImmutableMap.Builder<Meta.Index, CompactMapGroupBuilder.MapBuilder<String, SingleRule>>()
                        .defaultValue((k) -> roleMapBuilder.createMapBuilder());

                ImmutableMap.Builder<Meta.Index, DeduplicatingCompactSubSetBuilder.SubSetBuilder<String>> indexToRoleWithoutRule = new ImmutableMap.Builder<Meta.Index, DeduplicatingCompactSubSetBuilder.SubSetBuilder<String>>()
                        .defaultValue((k) -> roleSetBuilder.createSubSetBuilder());

                ImmutableMap.Builder<String, ImmutableList.Builder<Exception>> rolesToInitializationErrors = new ImmutableMap.Builder<String, ImmutableList.Builder<Exception>>()
                        .defaultValue((k) -> new ImmutableList.Builder<Exception>());

                for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                    try {
                        String roleName = entry.getKey();
                        Role role = entry.getValue();
                        roleSetBuilder.next(roleName);

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

                DeduplicatingCompactSubSetBuilder.Completed<String> completed = roleSetBuilder.build();

                this.indexToRoleToRule = indexToRoleToRule.build((b) -> b.build());
                this.indexToRoleWithoutRule = indexToRoleWithoutRule.build((b) -> b.build(completed));
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
            final ImmutableMap<Meta.Alias, Map<String, SingleRule>> aliasToRoleToRule;
            final ImmutableMap<Meta.Alias, ImmutableCompactSubSet<String>> aliasToRoleWithoutRule;

            final ImmutableMap<String, ImmutableList<Exception>> rolesToInitializationErrors;

            private final Function<Role.Index, SingleRule> roleToRuleFunction;
            private final ComponentState componentState;

            Alias(SgDynamicConfiguration<Role> roles, Meta indexMetadata, Function<Role.Index, SingleRule> roleToRuleFunction) {
                this.componentState = new ComponentState("alias");
                this.roleToRuleFunction = roleToRuleFunction;

                DeduplicatingCompactSubSetBuilder<String> roleSetBuilder = new DeduplicatingCompactSubSetBuilder<>(roles.getCEntries().keySet());
                CompactMapGroupBuilder<String, SingleRule> roleMapBuilder = new CompactMapGroupBuilder<>(roles.getCEntries().keySet());
                
                ImmutableMap.Builder<Meta.Alias, CompactMapGroupBuilder.MapBuilder<String, SingleRule>> indexToRoleToQuery = new ImmutableMap.Builder<Meta.Alias, CompactMapGroupBuilder.MapBuilder<String, SingleRule>>()
                        .defaultValue((k) -> roleMapBuilder.createMapBuilder());

                ImmutableMap.Builder<Meta.Alias, DeduplicatingCompactSubSetBuilder.SubSetBuilder<String>> indexToRoleWithoutQuery = new ImmutableMap.Builder<Meta.Alias, DeduplicatingCompactSubSetBuilder.SubSetBuilder<String>>()
                        .defaultValue((k) ->  roleSetBuilder.createSubSetBuilder());

                ImmutableMap.Builder<String, ImmutableList.Builder<Exception>> rolesToInitializationErrors = new ImmutableMap.Builder<String, ImmutableList.Builder<Exception>>()
                        .defaultValue((k) -> new ImmutableList.Builder<Exception>());

                for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                    try {
                        String roleName = entry.getKey();
                        Role role = entry.getValue();
                        roleSetBuilder.next(roleName);

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

                DeduplicatingCompactSubSetBuilder.Completed<String> completed = roleSetBuilder.build();
                
                this.aliasToRoleToRule = indexToRoleToQuery.build((b) -> b.build());
                this.aliasToRoleWithoutRule = indexToRoleWithoutQuery.build((b) -> b.build(completed));
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
            final ImmutableMap<Meta.DataStream, Map<String, SingleRule>> dataStreamToRoleToRule;
            final ImmutableMap<Meta.DataStream, ImmutableCompactSubSet<String>> dataStreamToRoleWithoutRule;

            final ImmutableMap<String, ImmutableList<Exception>> rolesToInitializationErrors;

            private final Function<Role.Index, SingleRule> roleToRuleFunction;
            private final ComponentState componentState;

            DataStream(SgDynamicConfiguration<Role> roles, Meta indexMetadata, Function<Role.Index, SingleRule> roleToRuleFunction) {
                this.componentState = new ComponentState("data_stream");
                this.roleToRuleFunction = roleToRuleFunction;
                
                DeduplicatingCompactSubSetBuilder<String> roleSetBuilder = new DeduplicatingCompactSubSetBuilder<>(roles.getCEntries().keySet());
                CompactMapGroupBuilder<String, SingleRule> roleMapBuilder = new CompactMapGroupBuilder<>(roles.getCEntries().keySet());

                ImmutableMap.Builder<Meta.DataStream, CompactMapGroupBuilder.MapBuilder<String, SingleRule>> indexToRoleToQuery = new ImmutableMap.Builder<Meta.DataStream, CompactMapGroupBuilder.MapBuilder<String, SingleRule>>()
                        .defaultValue((k) -> roleMapBuilder.createMapBuilder());

                ImmutableMap.Builder<Meta.DataStream, DeduplicatingCompactSubSetBuilder.SubSetBuilder<String>> indexToRoleWithoutQuery = new ImmutableMap.Builder<Meta.DataStream, DeduplicatingCompactSubSetBuilder.SubSetBuilder<String>>()
                        .defaultValue((k) -> roleSetBuilder.createSubSetBuilder());

                ImmutableMap.Builder<String, ImmutableList.Builder<Exception>> rolesToInitializationErrors = new ImmutableMap.Builder<String, ImmutableList.Builder<Exception>>()
                        .defaultValue((k) -> new ImmutableList.Builder<Exception>());

                for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                    try {
                        String roleName = entry.getKey();
                        Role role = entry.getValue();
                        roleSetBuilder.next(roleName);
                        
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

                DeduplicatingCompactSubSetBuilder.Completed<String> completed = roleSetBuilder.build();
                
                this.dataStreamToRoleToRule = indexToRoleToQuery.build((b) -> b.build());
                this.dataStreamToRoleWithoutRule = indexToRoleWithoutQuery.build((b) -> b.build(completed));
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
