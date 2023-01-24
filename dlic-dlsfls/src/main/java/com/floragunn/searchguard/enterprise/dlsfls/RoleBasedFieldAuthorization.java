/*
  * Copyright 2015-2022 by floragunn GmbH - All rights reserved
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

import com.floragunn.codova.config.templates.ExpressionEvaluationException;
import com.floragunn.codova.config.templates.Template;
import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.validation.ConfigValidationException;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RoleBasedFieldAuthorization implements ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(RoleBasedFieldAuthorization.class);

    private final SgDynamicConfiguration<Role> roles;
    private final StaticIndexRules staticIndexQueries;
    private volatile StatefulIndexRules statefulIndexQueries;
    private final ComponentState componentState = new ComponentState("role_based_field_authorization");

    public RoleBasedFieldAuthorization(SgDynamicConfiguration<Role> roles, Set<String> indices, MetricsLevel metricsLevel) {
        this.roles = roles;
        this.staticIndexQueries = new StaticIndexRules(roles);
        this.statefulIndexQueries = new StatefulIndexRules(roles, indices);
        this.componentState.setInitialized();
        this.componentState.setConfigVersion(roles.getDocVersion());
        this.componentState.addPart(staticIndexQueries.getComponentState());
        this.componentState.addPart(statefulIndexQueries.getComponentState());
    }

    public FlsRule getFlsRule(PrivilegesEvaluationContext context, String index, Meter meter) throws PrivilegesEvaluationException {
        try (Meter subMeter = meter.detail("evaluate_fls")) {

            if (this.staticIndexQueries.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
                return FlsRule.ALLOW_ALL;
            }

            StatefulIndexRules statefulIndexQueries = this.statefulIndexQueries;

            if (!statefulIndexQueries.indices.contains(index)) {
                // We get a request for an index unknown to this instance. Usually, this is the case because the index simply does not exist.
                // For non-existing indices, it is safe to assume that no documents can be accessed.

                if (log.isDebugEnabled()) {
                    log.debug("Index {} do not exist. Assuming full field restriction.", index);
                }

                return FlsRule.DENY_ALL;
            }

            ImmutableSet<String> rolesWithoutRule = statefulIndexQueries.indexToRoleWithoutRule.get(index);

            if (rolesWithoutRule != null && rolesWithoutRule.containsAny(context.getMappedRoles())) {
                return FlsRule.ALLOW_ALL;
            }

            ImmutableMap<String, FlsRule> roleToRule = this.statefulIndexQueries.indexToRoleToRule.get(index);
            List<FlsRule> rules = new ArrayList<>();

            for (String role : context.getMappedRoles()) {
                {
                    FlsRule rule = this.staticIndexQueries.roleWithIndexWildcardToRule.get(role);

                    if (rule != null) {
                        rules.add(rule);
                    }
                }

                if (roleToRule != null) {
                    FlsRule rule = roleToRule.get(role);

                    if (rule != null) {
                        rules.add(rule);
                    }
                }

                ImmutableMap<Template<Pattern>, FlsRule> indexPatternTemplateToQuery = this.staticIndexQueries.rolesToIndexPatternTemplateToRule
                        .get(role);

                if (indexPatternTemplateToQuery != null) {
                    for (Map.Entry<Template<Pattern>, FlsRule> entry : indexPatternTemplateToQuery.entrySet()) {
                        try {
                            Pattern pattern = context.getRenderedPattern(entry.getKey());

                            if (pattern.matches(index)) {
                                rules.add(entry.getValue());
                            }
                        } catch (ExpressionEvaluationException e) {
                            throw new PrivilegesEvaluationException("Error while rendering index pattern of role " + role, e);
                        }
                    }
                }
            }

            if (rules.isEmpty()) {
                return FlsRule.ALLOW_ALL;
            } else {
                return FlsRule.merge(rules);
            }
        } catch (PrivilegesEvaluationException e) {
            componentState.addLastException("evaluate", e);
            throw e;
        } catch (RuntimeException e) {
            componentState.addLastException("evaluate_u", e);
            throw e;
        }
    }

    boolean hasFlsRestrictions(PrivilegesEvaluationContext context, Collection<String> indices, Meter meter) throws PrivilegesEvaluationException {
        try (Meter subMeter = meter.detail("has_fls_restriction")) {
            if (this.staticIndexQueries.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
                return false;
            }

            StatefulIndexRules statefulIndexQueries = this.statefulIndexQueries;

            if (!statefulIndexQueries.indices.containsAll(indices)) {
                // We get a request for an index unknown to this instance. Usually, this is the case because the index simply does not exist.
                // For non-existing indices, it is safe to assume that no documents can be accessed.

                if (log.isDebugEnabled()) {
                    log.debug("Indices {} do not exist. Assuming full field restriction.", indices);
                }

                return true;
            }

            for (String index : indices) {
                if (hasFlsRestrictions(context, index, statefulIndexQueries)) {
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

    boolean hasFlsRestrictions(PrivilegesEvaluationContext context, String index, Meter meter) throws PrivilegesEvaluationException {
        try (Meter subMeter = meter.detail("has_fls_restriction")) {
            if (this.staticIndexQueries.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
                return false;
            }

            StatefulIndexRules statefulIndexQueries = this.statefulIndexQueries;

            if (!statefulIndexQueries.indices.contains(index)) {
                // We get a request for an index unknown to this instance. Usually, this is the case because the index simply does not exist.
                // For non-existing indices, it is safe to assume that no documents can be accessed.

                if (log.isDebugEnabled()) {
                    log.debug("Index {} do not exist. Assuming full field restriction.", index);
                }

                return true;
            }

            return hasFlsRestrictions(context, index, statefulIndexQueries);
        } catch (PrivilegesEvaluationException e) {
            componentState.addLastException("has_restriction", e);
            throw e;
        } catch (RuntimeException e) {
            componentState.addLastException("has_restriction_u", e);
            throw e;
        }
    }

    private boolean hasFlsRestrictions(PrivilegesEvaluationContext context, String index, StatefulIndexRules statefulIndexQueries)
            throws PrivilegesEvaluationException {

        ImmutableSet<String> roleWithoutRule = statefulIndexQueries.indexToRoleWithoutRule.get(index);

        if (roleWithoutRule != null && roleWithoutRule.containsAny(context.getMappedRoles())) {
            return false;
        }

        ImmutableMap<String, FlsRule> roleToRule = statefulIndexQueries.indexToRoleToRule.get(index);

        for (String role : context.getMappedRoles()) {
            {
                FlsRule rule = this.staticIndexQueries.roleWithIndexWildcardToRule.get(role);

                if (rule != null) {
                    return true;
                }
            }

            if (roleToRule != null) {
                FlsRule rule = roleToRule.get(role);

                if (rule != null) {
                    return true;
                }
            }

            ImmutableMap<Template<Pattern>, FlsRule> indexPatternTemplateToRule = this.staticIndexQueries.rolesToIndexPatternTemplateToRule.get(role);

            if (indexPatternTemplateToRule != null) {
                for (Map.Entry<Template<Pattern>, FlsRule> entry : indexPatternTemplateToRule.entrySet()) {
                    try {
                        Pattern pattern = context.getRenderedPattern(entry.getKey());

                        if (pattern.matches(index)) {
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

    static class StaticIndexRules implements ComponentStateProvider {
        private final ComponentState componentState;

        private final ImmutableSet<String> rolesWithIndexWildcardWithoutRule;
        private final ImmutableMap<String, FlsRule> roleWithIndexWildcardToRule;
        private final ImmutableMap<String, ImmutableMap<Template<Pattern>, FlsRule>> rolesToIndexPatternTemplateToRule;
        private final ImmutableMap<String, ImmutableList<Exception>> rolesToInitializationErrors;

        StaticIndexRules(SgDynamicConfiguration<Role> roles) {
            this.componentState = new ComponentState("static_index_rules");

            ImmutableSet.Builder<String> rolesWithIndexWildcardWithoutRule = new ImmutableSet.Builder<>();
            ImmutableMap.Builder<String, FlsRule> roleWithIndexWildcardToRule = new ImmutableMap.Builder<String, FlsRule>();
            ImmutableMap.Builder<String, ImmutableMap.Builder<Template<Pattern>, FlsRule>> rolesToIndexPatternTemplateToRule = new ImmutableMap.Builder<String, ImmutableMap.Builder<Template<Pattern>, FlsRule>>()
                    .defaultValue((k) -> new ImmutableMap.Builder<>());

            ImmutableMap.Builder<String, ImmutableList.Builder<Exception>> rolesToInitializationErrors = new ImmutableMap.Builder<String, ImmutableList.Builder<Exception>>()
                    .defaultValue((k) -> new ImmutableList.Builder<Exception>());

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();

                    for (Role.Index indexPermissions : role.getIndexPermissions()) {
                        if (indexPermissions.getIndexPatterns().forAnyApplies((p) -> p.isConstant() && p.getConstantValue().isWildcard())) {
                            ImmutableList<Role.Index.FlsPattern> flsPatterns = indexPermissions.getFls();

                            if (flsPatterns == null || flsPatterns.isEmpty()) {
                                rolesWithIndexWildcardWithoutRule.add(roleName);
                            } else {
                                FlsRule flsRule = new FlsRule.SingleRole(role, indexPermissions);
                                roleWithIndexWildcardToRule.put(roleName, flsRule);
                            }

                            continue;
                        }

                        for (Template<Pattern> indexPatternTemplate : indexPermissions.getIndexPatterns()) {
                            if (indexPatternTemplate.isConstant()) {
                                continue;
                            }

                            ImmutableList<Role.Index.FlsPattern> flsPatterns = indexPermissions.getFls();

                            if (flsPatterns == null || flsPatterns.isEmpty()) {
                                continue;
                            }

                            FlsRule flsRule = new FlsRule.SingleRole(role, indexPermissions);

                            rolesToIndexPatternTemplateToRule.get(roleName).put(indexPatternTemplate, flsRule);
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

        @Override
        public ComponentState getComponentState() {
            return componentState;
        }
    }

    static class StatefulIndexRules implements ComponentStateProvider {
        private final ImmutableMap<String, ImmutableMap<String, FlsRule>> indexToRoleToRule;
        private final ImmutableMap<String, ImmutableSet<String>> indexToRoleWithoutRule;

        private final ImmutableSet<String> indices;

        private final ImmutableMap<String, ImmutableList<Exception>> rolesToInitializationErrors;
        private final ComponentState componentState;

        StatefulIndexRules(SgDynamicConfiguration<Role> roles, Set<String> indices) {
            this.indices = ImmutableSet.of(indices);
            this.componentState = new ComponentState("stateful_index_queries");

            ImmutableMap.Builder<String, ImmutableMap.Builder<String, FlsRule>> indexToRoleToRule = new ImmutableMap.Builder<String, ImmutableMap.Builder<String, FlsRule>>()
                    .defaultValue((k) -> new ImmutableMap.Builder<String, FlsRule>());

            ImmutableMap.Builder<String, ImmutableSet.Builder<String>> indexToRoleWithoutRule = new ImmutableMap.Builder<String, ImmutableSet.Builder<String>>()
                    .defaultValue((k) -> new ImmutableSet.Builder<String>());

            ImmutableMap.Builder<String, ImmutableList.Builder<Exception>> rolesToInitializationErrors = new ImmutableMap.Builder<String, ImmutableList.Builder<Exception>>()
                    .defaultValue((k) -> new ImmutableList.Builder<Exception>());

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();

                    for (Role.Index indexPermissions : role.getIndexPermissions()) {
                        if (indexPermissions.getIndexPatterns().forAnyApplies((p) -> p.isConstant() && p.getConstantValue().isWildcard())) {
                            // This is handled in the static IndexPermissions object.
                            continue;
                        }

                        for (Template<Pattern> indexPatternTemplate : indexPermissions.getIndexPatterns()) {
                            if (!indexPatternTemplate.isConstant()) {
                                continue;
                            }

                            Pattern indexPattern = indexPatternTemplate.getConstantValue();
                            ImmutableList<Role.Index.FlsPattern> flsPatterns = indexPermissions.getFls();

                            if (flsPatterns != null && !flsPatterns.isEmpty()) {
                                FlsRule flsRule = new FlsRule.SingleRole(role, indexPermissions);

                                for (String index : indexPattern.iterateMatching(indices)) {
                                    indexToRoleToRule.get(index).put(roleName, flsRule);
                                }
                            } else {
                                for (String index : indexPattern.iterateMatching(indices)) {
                                    indexToRoleWithoutRule.get(index).add(roleName);
                                }
                            }

                        }
                    }
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: " + entry + "\nIgnoring role.", e);
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

        @Override
        public ComponentState getComponentState() {
            return componentState;
        }

    }

    public static abstract class FlsRule {
        public static FlsRule of(String... rules) throws ConfigValidationException {
            ImmutableList.Builder<Role.Index.FlsPattern> patterns = new ImmutableList.Builder<>();

            for (String rule : rules) {
                patterns.add(new Role.Index.FlsPattern(rule));
            }

            return new SingleRole(patterns.build());
        }

        static FlsRule merge(List<FlsRule> rules) {
            if (rules.size() == 1) {
                return rules.get(0);
            }

            ImmutableList.Builder<SingleRole> entries = new ImmutableList.Builder<>(rules.size());

            for (FlsRule rule : rules) {
                if (rule instanceof SingleRole) {
                    entries.add((SingleRole) rule);
                } else if (rule instanceof MultiRole) {
                    for (SingleRole subRule : ((MultiRole) rule).entries) {
                        entries.add(subRule);
                    }
                }
            }

            return new FlsRule.MultiRole(entries.build());
        }

        public static final FlsRule ALLOW_ALL = new FlsRule.SingleRole(ImmutableList.empty());
        public static final FlsRule DENY_ALL = new FlsRule.SingleRole(ImmutableList.of(Role.Index.FlsPattern.EXCLUDE_ALL));

        public abstract boolean isAllowed(String field);

        public abstract boolean isAllowAll();

        static class SingleRole extends FlsRule {

            final Role sourceRole;
            final Role.Index sourceIndex;
            final ImmutableList<Role.Index.FlsPattern> patterns;
            final Map<String, Boolean> cache;
            final boolean allowAll;

            SingleRole(Role sourceRole, Role.Index sourceIndex) {
                this.sourceRole = sourceRole;
                this.sourceIndex = sourceIndex;

                int exclusions = 0;
                int inclusions = 0;

                for (Role.Index.FlsPattern pattern : sourceIndex.getFls()) {
                    if (pattern.isExcluded()) {
                        exclusions++;
                    } else {
                        inclusions++;
                    }
                }

                if (exclusions == 0 && inclusions == 0) {
                    // Empty
                    this.patterns = ImmutableList.of(Role.Index.FlsPattern.INCLUDE_ALL);
                } else if (exclusions != 0 && inclusions == 0) {
                    // Only exclusions
                    this.patterns = ImmutableList.of(Role.Index.FlsPattern.INCLUDE_ALL).with(sourceIndex.getFls());
                } else if (exclusions == 0 && inclusions != 0) {
                    // Only inclusions
                    this.patterns = ImmutableList.of(Role.Index.FlsPattern.EXCLUDE_ALL).with(sourceIndex.getFls());
                } else {
                    // Mixed
                    this.patterns = sourceIndex.getFls();
                }

                this.allowAll = patterns.isEmpty()
                        || (patterns.size() == 1 && patterns.get(0).getPattern().isWildcard() && !patterns.get(0).isExcluded());

                if (this.allowAll) {
                    this.cache = null;
                } else {
                    this.cache = new ConcurrentHashMap<String, Boolean>();
                }
            }

            public SingleRole(ImmutableList<Role.Index.FlsPattern> patterns) {
                this.patterns = patterns;
                this.sourceIndex = null;
                this.sourceRole = null;
                this.allowAll = patterns.isEmpty()
                        || (patterns.size() == 1 && patterns.get(0).getPattern().isWildcard() && !patterns.get(0).isExcluded());
                this.cache = null;
            }

            public boolean isAllowed(String field) {
                if (cache == null) {
                    return internalIsAllowed(field);
                } else {
                    Boolean allowed = this.cache.get(field);

                    if (allowed != null) {
                        return allowed;
                    } else {
                        allowed = internalIsAllowed(field);
                        this.cache.put(field, allowed);
                        return allowed;
                    }
                }
            }

            private boolean internalIsAllowed(String field) {
                field = stripKeywordSuffix(field);

                boolean allowed = true;

                for (Role.Index.FlsPattern pattern : this.patterns) {
                    if (pattern.getPattern().matches(field)) {
                        if (pattern.isExcluded()) {
                            allowed = false;
                        } else {
                            allowed = true;
                        }
                    }
                }

                return allowed;
            }

            public boolean isAllowAll() {
                return allowAll;
            }

            @Override
            public String toString() {
                if (isAllowAll()) {
                    return "FLS:*";
                } else {
                    return "FLS:" + patterns;
                }
            }
        }

        static class MultiRole extends FlsRule {
            final ImmutableList<SingleRole> entries;
            final Map<String, Boolean> cache;
            final boolean allowAll;

            MultiRole(ImmutableList<SingleRole> entries) {
                this.entries = entries;
                this.allowAll = entries.forAnyApplies((e) -> e.isAllowAll());

                if (this.allowAll) {
                    this.cache = null;
                } else {
                    this.cache = new ConcurrentHashMap<String, Boolean>();

                }
            }

            public boolean isAllowed(String field) {
                if (allowAll) {
                    return true;
                } else if (cache == null) {
                    return internalIsAllowed(field);
                } else {
                    Boolean allowed = this.cache.get(field);

                    if (allowed != null) {
                        return allowed;
                    } else {
                        allowed = internalIsAllowed(field);
                        this.cache.put(field, allowed);
                        return allowed;
                    }
                }
            }

            private boolean internalIsAllowed(String field) {
                field = stripKeywordSuffix(field);

                for (SingleRole entry : this.entries) {
                    if (entry.isAllowed(field)) {
                        return true;
                    }
                }

                return false;
            }

            public boolean isAllowAll() {
                return allowAll;
            }

            @Override
            public String toString() {
                if (isAllowAll()) {
                    return "FLS:*";
                } else {
                    return "FLS:" + entries.map((e) -> e.patterns);
                }
            }

        }

        static String stripKeywordSuffix(String field) {
            if (field.endsWith(".keyword")) {
                return field.substring(0, field.length() - ".keyword".length());
            } else {
                return field;
            }
        }
    }

    public synchronized void updateIndices(Set<String> indices) {
        StatefulIndexRules statefulIndexQueries = this.statefulIndexQueries;

        if (!statefulIndexQueries.indices.equals(indices)) {
            this.statefulIndexQueries = new StatefulIndexRules(roles, indices);
            this.componentState.replacePart(this.statefulIndexQueries.getComponentState());
        }
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

}
