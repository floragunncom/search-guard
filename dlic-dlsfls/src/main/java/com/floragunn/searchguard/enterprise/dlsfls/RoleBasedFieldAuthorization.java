/*
 * Copyright 2015-2024 floragunn GmbH
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
/*
 * Includes code from https://github.com/opensearch-project/security/blob/70591197c705ca6f42f765186a05837813f80ff3/src/main/java/org/opensearch/security/privileges/dlsfls/FieldPrivileges.java
 * which is Copyright OpenSearch Contributors
 */
package com.floragunn.searchguard.enterprise.dlsfls;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.meta.Meta;

public class RoleBasedFieldAuthorization
        extends RoleBasedAuthorizationBase<RoleBasedFieldAuthorization.FlsRule, RoleBasedFieldAuthorization.FlsRule> {

    public RoleBasedFieldAuthorization(SgDynamicConfiguration<Role> roles, Meta indexMetadata, MetricsLevel metricsLevel) {
        super(roles, indexMetadata, metricsLevel, RoleBasedFieldAuthorization::roleToRule);
    }

    static FlsRule roleToRule(Role.Index rolePermissions) {
        ImmutableList<Role.Index.FlsPattern> flsPatterns = rolePermissions.getFls();

        if (flsPatterns != null && !flsPatterns.isEmpty()) {
            return new FlsRule.SingleRole(rolePermissions);
        } else {
            return null;
        }
    }

    @Override
    protected FlsRule unrestricted() {
        return FlsRule.ALLOW_ALL;
    }

    @Override
    protected FlsRule fullyRestricted() {
        return FlsRule.DENY_ALL;
    }

    @Override
    protected FlsRule compile(PrivilegesEvaluationContext context, Collection<FlsRule> rules) throws PrivilegesEvaluationException {
        if (rules.isEmpty()) {
            return FlsRule.DENY_ALL;
        } else {
            return FlsRule.merge(rules);
        }
    }

    @Override
    protected String hasRestrictionsMetricName() {
        return "has_fls_restrictions";
    }

    @Override
    protected String evaluateRestrictionsMetricName() {
        return "evaluate_fls_restrictions";
    }

    @Override
    protected String componentName() {
        return "role_based_field_authorization";
    }

    public static abstract class FlsRule {
        public static FlsRule of(String... rules) throws ConfigValidationException {
            ImmutableList.Builder<Role.Index.FlsPattern> patterns = new ImmutableList.Builder<>();

            for (String rule : rules) {
                patterns.add(new Role.Index.FlsPattern(rule));
            }

            return new SingleRole(patterns.build());
        }

        static FlsRule merge(Collection<FlsRule> rules) {
            if (rules.size() == 1) {
                return rules.iterator().next();
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

        public static final FlsRule ALLOW_ALL = new FlsRule.SingleRole(ImmutableList.of(Role.Index.FlsPattern.INCLUDE_ALL));
        public static final FlsRule DENY_ALL = new FlsRule.SingleRole(ImmutableList.of(Role.Index.FlsPattern.EXCLUDE_ALL));

        /**
         * This method checks if the method is allowed in terms of FLS. To determine if access to a field is allowed for a user, we also need to 
         * consider field masking. Method {@link com.floragunn.searchguard.enterprise.dlsfls.lucene.DlsFlsActionContext#isAllowed(String)} does this.
         * Therefore, the method {@link com.floragunn.searchguard.enterprise.dlsfls.lucene.DlsFlsActionContext#isAllowed(String)} should be preferred
         * over this method.
         */
        public abstract boolean isAllowed(String field);

        public abstract boolean isAllowAll();

        static class SingleRole extends FlsRule {
            final Role.Index sourceIndex;
            final ImmutableList<Role.Index.FlsPattern> patterns;
            final Map<String, Boolean> cache;
            final boolean allowAll;

            SingleRole(Role.Index sourceIndex) {
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
                this.allowAll = patterns.isEmpty()
                        || (patterns.size() == 1 && patterns.get(0).getPattern().isWildcard() && !patterns.get(0).isExcluded());
                this.cache = null;
            }

            public boolean isAllowed(String field) {
                if (allowAll) {
                    return true;
                }
                
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

                boolean allowed = false;

                for (Role.Index.FlsPattern pattern : this.patterns) {
                    for (String fieldOrItsParent : fieldItselfWithAllItsParent(field)) {
                        Boolean directlyAllowed = isDirectlyAllowed(pattern, fieldOrItsParent);
                        if(directlyAllowed != null) {
                            allowed = directlyAllowed;
                            break;
                        }
                    }
                }

                return allowed;
            }

            private static Boolean isDirectlyAllowed(Role.Index.FlsPattern pattern, String field) {
                if (pattern.getPattern().matches(field)) {
                    return !pattern.isExcluded();
                }
                return null;
            }

            static List<String> fieldItselfWithAllItsParent(String fieldName) {
                List<String> result = new ArrayList<>();
                result.add(fieldName);
                for(int lastDot = fieldName.lastIndexOf("."); fieldName.contains(".");  lastDot = fieldName.lastIndexOf(".")) {
                    fieldName = fieldName.substring(0, lastDot);
                    result.add(fieldName);
                }
                return result;
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

}
