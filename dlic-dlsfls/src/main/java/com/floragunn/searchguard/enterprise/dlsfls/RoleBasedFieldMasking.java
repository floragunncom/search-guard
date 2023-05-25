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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.bouncycastle.crypto.digests.Blake2bDigest;
import org.bouncycastle.util.encoders.Hex;

import com.floragunn.codova.config.templates.ExpressionEvaluationException;
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
import com.google.common.primitives.Bytes;

public class RoleBasedFieldMasking implements ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(RoleBasedFieldMasking.class);

    private final SgDynamicConfiguration<Role> roles;
    private final StaticIndexRules staticIndexQueries;
    private volatile StatefulIndexRules statefulIndexQueries;
    private final ComponentState componentState = new ComponentState("role_based_field_masking");
    private final DlsFlsConfig.FieldMasking fieldMaskingConfig;

    public RoleBasedFieldMasking(SgDynamicConfiguration<Role> roles, DlsFlsConfig.FieldMasking fieldMaskingConfig, Set<String> indices,
            MetricsLevel metricsLevel) {
        this.roles = roles;
        this.fieldMaskingConfig = fieldMaskingConfig;
        this.staticIndexQueries = new StaticIndexRules(roles, fieldMaskingConfig);
        this.statefulIndexQueries = new StatefulIndexRules(roles, fieldMaskingConfig, indices);
        this.componentState.setInitialized();
        this.componentState.setConfigVersion(roles.getDocVersion());
        this.componentState.addPart(this.statefulIndexQueries.getComponentState());
        this.componentState.addPart(this.staticIndexQueries.getComponentState());
    }

    public FieldMaskingRule getFieldMaskingRule(PrivilegesEvaluationContext context, String index, Meter meter) throws PrivilegesEvaluationException {
        try (Meter subMeter = meter.detail("evaluate_fm")) {

            if (this.staticIndexQueries.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
                return FieldMaskingRule.ALLOW_ALL;
            }

            StatefulIndexRules statefulIndexQueries = this.statefulIndexQueries;

            if (!statefulIndexQueries.indices.contains(index)) {
                return FieldMaskingRule.MASK_ALL;
            }

            ImmutableSet<String> rolesWithoutRule = statefulIndexQueries.indexToRoleWithoutRule.get(index);

            if (rolesWithoutRule != null && rolesWithoutRule.containsAny(context.getMappedRoles())) {
                return FieldMaskingRule.ALLOW_ALL;
            }

            ImmutableMap<String, FieldMaskingRule.SingleRole> roleToRule = this.statefulIndexQueries.indexToRoleToRule.get(index);
            List<FieldMaskingRule.SingleRole> rules = new ArrayList<>();

            for (String role : context.getMappedRoles()) {
                {
                    FieldMaskingRule.SingleRole rule = this.staticIndexQueries.roleWithIndexWildcardToRule.get(role);

                    if (rule != null) {
                        rules.add(rule);
                    }
                }

                if (roleToRule != null) {
                    FieldMaskingRule.SingleRole rule = roleToRule.get(role);

                    if (rule != null) {
                        rules.add(rule);
                    }
                }

                ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, FieldMaskingRule.SingleRole> indexPatternTemplateToQuery = this.staticIndexQueries.rolesToIndexPatternTemplateToRule
                        .get(role);

                if (indexPatternTemplateToQuery != null) {
                    for (Map.Entry<Role.IndexPatterns.IndexPatternTemplate, FieldMaskingRule.SingleRole> entry : indexPatternTemplateToQuery
                            .entrySet()) {
                        try {
                            Pattern pattern = context.getRenderedPattern(entry.getKey().getTemplate());

                            if (pattern.matches(index) && !entry.getKey().getExclusions().matches(index)) {
                                rules.add(entry.getValue());
                            }
                        } catch (ExpressionEvaluationException e) {
                            throw new PrivilegesEvaluationException("Error while rendering index pattern of role " + role, e);
                        }
                    }
                }
            }

            if (rules.isEmpty()) {
                return FieldMaskingRule.ALLOW_ALL;
            } else {
                return new FieldMaskingRule.MultiRole(rules);
            }
        } catch (PrivilegesEvaluationException e) {
            componentState.addLastException("evaluate", e);
            throw e;
        } catch (RuntimeException e) {
            componentState.addLastException("evaluate_u", e);
            throw e;
        }
    }

    boolean hasFieldMaskingRestrictions(PrivilegesEvaluationContext context, Collection<String> indices, Meter meter)
            throws PrivilegesEvaluationException {
        try (Meter subMeter = meter.detail("has_fm_restriction")) {
            if (this.staticIndexQueries.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
                return false;
            }

            StatefulIndexRules statefulIndexQueries = this.statefulIndexQueries;

            if (!statefulIndexQueries.indices.containsAll(indices)) {
                return true;
            }

            for (String index : indices) {
                if (hasFieldMaskingRestrictions(context, index, statefulIndexQueries)) {
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

    boolean hasFieldMaskingRestrictions(PrivilegesEvaluationContext context, String index, Meter meter) throws PrivilegesEvaluationException {
        try (Meter subMeter = meter.detail("has_fm_restriction")) {
            if (this.staticIndexQueries.rolesWithIndexWildcardWithoutRule.containsAny(context.getMappedRoles())) {
                return false;
            }

            StatefulIndexRules statefulIndexQueries = this.statefulIndexQueries;

            if (!statefulIndexQueries.indices.contains(index)) {
                return true;
            }

            return hasFieldMaskingRestrictions(context, index, statefulIndexQueries);
        } catch (PrivilegesEvaluationException e) {
            componentState.addLastException("has_restriction", e);
            throw e;
        } catch (RuntimeException e) {
            componentState.addLastException("has_restriction_u", e);
            throw e;
        }
    }

    private boolean hasFieldMaskingRestrictions(PrivilegesEvaluationContext context, String index, StatefulIndexRules statefulIndexQueries)
            throws PrivilegesEvaluationException {

        ImmutableSet<String> roleWithoutRule = statefulIndexQueries.indexToRoleWithoutRule.get(index);

        if (roleWithoutRule != null && roleWithoutRule.containsAny(context.getMappedRoles())) {
            return false;
        }

        ImmutableMap<String, FieldMaskingRule.SingleRole> roleToRule = statefulIndexQueries.indexToRoleToRule.get(index);

        for (String role : context.getMappedRoles()) {
            {
                FieldMaskingRule rule = this.staticIndexQueries.roleWithIndexWildcardToRule.get(role);

                if (rule != null) {
                    return true;
                }
            }

            if (roleToRule != null) {
                FieldMaskingRule rule = roleToRule.get(role);

                if (rule != null) {
                    return true;
                }
            }

            ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, FieldMaskingRule.SingleRole> indexPatternTemplateToRule = this.staticIndexQueries.rolesToIndexPatternTemplateToRule
                    .get(role);

            if (indexPatternTemplateToRule != null) {
                for (Map.Entry<Role.IndexPatterns.IndexPatternTemplate, FieldMaskingRule.SingleRole> entry : indexPatternTemplateToRule.entrySet()) {
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

        return false;
    }

    static class StaticIndexRules implements ComponentStateProvider {
        private final ComponentState componentState;

        private final ImmutableSet<String> rolesWithIndexWildcardWithoutRule;
        private final ImmutableMap<String, FieldMaskingRule.SingleRole> roleWithIndexWildcardToRule;
        private final ImmutableMap<String, ImmutableMap<Role.IndexPatterns.IndexPatternTemplate, FieldMaskingRule.SingleRole>> rolesToIndexPatternTemplateToRule;
        private final ImmutableMap<String, ImmutableList<Exception>> rolesToInitializationErrors;

        StaticIndexRules(SgDynamicConfiguration<Role> roles, DlsFlsConfig.FieldMasking fieldMaskingConfig) {
            this.componentState = new ComponentState("static_index_rules");

            ImmutableSet.Builder<String> rolesWithIndexWildcardWithoutRule = new ImmutableSet.Builder<>();
            ImmutableMap.Builder<String, FieldMaskingRule.SingleRole> roleWithIndexWildcardToRule = new ImmutableMap.Builder<String, FieldMaskingRule.SingleRole>();
            ImmutableMap.Builder<String, ImmutableMap.Builder<Role.IndexPatterns.IndexPatternTemplate, FieldMaskingRule.SingleRole>> rolesToIndexPatternTemplateToRule = new ImmutableMap.Builder<String, ImmutableMap.Builder<Role.IndexPatterns.IndexPatternTemplate, FieldMaskingRule.SingleRole>>()
                    .defaultValue((k) -> new ImmutableMap.Builder<>());

            ImmutableMap.Builder<String, ImmutableList.Builder<Exception>> rolesToInitializationErrors = new ImmutableMap.Builder<String, ImmutableList.Builder<Exception>>()
                    .defaultValue((k) -> new ImmutableList.Builder<Exception>());

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();

                    for (Role.Index indexPermissions : role.getIndexPermissions()) {
                        if (indexPermissions.getIndexPatterns().getPattern().isWildcard()) {
                            ImmutableList<Role.Index.FieldMaskingExpression> fmExpression = indexPermissions.getMaskedFields();

                            if (fmExpression == null || fmExpression.isEmpty()) {
                                rolesWithIndexWildcardWithoutRule.add(roleName);
                            } else {
                                FieldMaskingRule.SingleRole fmRule = new FieldMaskingRule.SingleRole(role, indexPermissions, fieldMaskingConfig);
                                roleWithIndexWildcardToRule.put(roleName, fmRule);
                            }

                            continue;
                        }

                        for (Role.IndexPatterns.IndexPatternTemplate indexPatternTemplate : indexPermissions.getIndexPatterns().getPatternTemplates()) {
                            ImmutableList<Role.Index.FieldMaskingExpression> fmExpression = indexPermissions.getMaskedFields();

                            if (fmExpression == null || fmExpression.isEmpty()) {
                                continue;
                            }

                            FieldMaskingRule.SingleRole fmRule = new FieldMaskingRule.SingleRole(role, indexPermissions, fieldMaskingConfig);

                            rolesToIndexPatternTemplateToRule.get(roleName).put(indexPatternTemplate, fmRule);
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
        private final ImmutableMap<String, ImmutableMap<String, FieldMaskingRule.SingleRole>> indexToRoleToRule;
        private final ImmutableMap<String, ImmutableSet<String>> indexToRoleWithoutRule;

        private final ImmutableSet<String> indices;

        private final ImmutableMap<String, ImmutableList<Exception>> rolesToInitializationErrors;
        private final ComponentState componentState;

        StatefulIndexRules(SgDynamicConfiguration<Role> roles, DlsFlsConfig.FieldMasking fieldMaskingConfig, Set<String> indices) {
            this.indices = ImmutableSet.of(indices);
            this.componentState = new ComponentState("stateful_index_queries");

            ImmutableMap.Builder<String, ImmutableMap.Builder<String, FieldMaskingRule.SingleRole>> indexToRoleToRule = new ImmutableMap.Builder<String, ImmutableMap.Builder<String, FieldMaskingRule.SingleRole>>()
                    .defaultValue((k) -> new ImmutableMap.Builder<String, FieldMaskingRule.SingleRole>());

            ImmutableMap.Builder<String, ImmutableSet.Builder<String>> indexToRoleWithoutRule = new ImmutableMap.Builder<String, ImmutableSet.Builder<String>>()
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
                            continue;
                        }

                        ImmutableList<Role.Index.FieldMaskingExpression> fmExpressions = indexPermissions.getMaskedFields();

                        if (fmExpressions != null && !fmExpressions.isEmpty()) {
                            FieldMaskingRule.SingleRole fmRule = new FieldMaskingRule.SingleRole(role, indexPermissions, fieldMaskingConfig);

                            for (String index : indexPattern.iterateMatching(indices)) {
                                indexToRoleToRule.get(index).put(roleName, fmRule);
                            }
                        } else {
                            for (String index : indexPattern.iterateMatching(indices)) {
                                indexToRoleWithoutRule.get(index).add(roleName);
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

    public static abstract class FieldMaskingRule {
        public static final FieldMaskingRule ALLOW_ALL = new FieldMaskingRule.SingleRole(ImmutableList.empty());
        public static final FieldMaskingRule MASK_ALL = new FieldMaskingRule.SingleRole(ImmutableList.of(new Field(Role.Index.FieldMaskingExpression.MASK_ALL, DlsFlsConfig.FieldMasking.DEFAULT)));

        public static FieldMaskingRule of(DlsFlsConfig.FieldMasking fieldMaskingConfig, String... rules) throws ConfigValidationException {
            ImmutableList.Builder<Role.Index.FieldMaskingExpression> patterns = new ImmutableList.Builder<>();

            for (String rule : rules) {
                patterns.add(new Role.Index.FieldMaskingExpression(rule));
            }

            return new SingleRole(patterns.build().map((e) -> new Field(e, fieldMaskingConfig)));
        }

        public abstract Field get(String field);

        public abstract boolean isAllowAll();

        public static class SingleRole extends FieldMaskingRule {

            final Role sourceRole;
            final Role.Index sourceIndex;
            final ImmutableList<FieldMaskingRule.Field> expressions;

            SingleRole(Role sourceRole, Role.Index sourceIndex, DlsFlsConfig.FieldMasking fieldMaskingConfig) {
                this.sourceRole = sourceRole;
                this.sourceIndex = sourceIndex;
                this.expressions = ImmutableList
                        .of(sourceIndex.getMaskedFields().stream().map((e) -> new Field(e, fieldMaskingConfig)).collect(Collectors.toList()));
            }

            SingleRole(ImmutableList<Field> expressions) {
                this.sourceIndex = null;
                this.sourceRole = null;
                this.expressions = expressions;
            }

            public Field get(String field) {
                return internalGet(stripKeywordSuffix(field));
            }

            private Field internalGet(String field) {
                for (Field expression : this.expressions) {
                    if (expression.getPattern().matches(field)) {
                        return expression;
                    }
                }

                return null;
            }

            public boolean isAllowAll() {
                return expressions.isEmpty();
            }

            @Override
            public String toString() {
                if (isAllowAll()) {
                    return "FM:*";
                } else {
                    return "FM:" + expressions;
                }
            }
        }

        public static class MultiRole extends FieldMaskingRule {
            final ImmutableList<FieldMaskingRule.SingleRole> parts;
            final boolean allowAll;

            MultiRole(Collection<FieldMaskingRule.SingleRole> parts) {
                this.parts = ImmutableList.of(parts);
                this.allowAll = this.parts.forAnyApplies((p) -> p.isAllowAll());
            }

            public Field get(String field) {
                field = stripKeywordSuffix(field);
                
                Field masking = null;

                for (FieldMaskingRule.SingleRole part : parts) {
                    masking = part.get(field);

                    if (masking == null) {
                        return null;
                    }
                }

                return masking;
            }

            public boolean isAllowAll() {
                return allowAll;
            }

            @Override
            public String toString() {
                if (isAllowAll()) {
                    return "FM:*";
                } else {
                    return "FM:" + parts.map((p) -> p.expressions);
                }
            }
        }

        public static class Field {
            private final Role.Index.FieldMaskingExpression expression;

            private final byte[] salt;
            private final byte[] personalization;
            private final byte[] prefix;

            Field(Role.Index.FieldMaskingExpression expression, DlsFlsConfig.FieldMasking fieldMaskingConfig) {
                this.expression = expression;
                this.salt = fieldMaskingConfig.getSalt();
                this.personalization = fieldMaskingConfig.getPersonalization();
                this.prefix = fieldMaskingConfig.getPrefix() != null ? fieldMaskingConfig.getPrefix().getBytes() : null;
            }

            public Pattern getPattern() {
                return expression.getPattern();
            }

            public byte[] apply(byte[] value) {
                if (isDefault()) {
                    return blake2bHash(value);
                } else {
                    return customHash(value);
                }
            }

            public String apply(String value) {
                if (isDefault()) {
                    return blake2bHash(value);
                } else {
                    return customHash(value);
                }
            }

            public BytesRef apply(BytesRef value) {
                if (value == null) {
                    return null;
                }

                if (isDefault()) {
                    return blake2bHash(value);
                } else {
                    return customHash(value);
                }
            }

            @Override
            public String toString() {
                return expression.toString();
            }
            
            private boolean isDefault() {
                return expression.getAlgo() == null && expression.getRegexReplacements() == null;
            }

            private byte[] customHash(byte[] in) {
                MessageDigest algo = expression.getAlgo();

                if (algo != null) {
                    if (prefix != null) {
                        return Bytes.concat(prefix, Hex.encode(algo.digest(in)));
                    }

                    return Hex.encode(algo.digest(in));
                } else if (expression.getRegexReplacements() != null) {
                    String string = new String(in, StandardCharsets.UTF_8);
                    for (Role.Index.FieldMaskingExpression.RegexReplacement rr : expression.getRegexReplacements()) {
                        string = rr.getRegex().matcher(string).replaceAll(rr.getReplacement());
                    }

                    if (prefix != null) {
                        return Bytes.concat(prefix, string.getBytes(StandardCharsets.UTF_8));
                    } else {
                        return string.getBytes(StandardCharsets.UTF_8);
                    }
                } else {
                    throw new IllegalArgumentException();
                }
            }

            private BytesRef customHash(BytesRef in) {
                final BytesRef copy = BytesRef.deepCopyOf(in);
                return new BytesRef(customHash(copy.bytes));
            }

            private String customHash(String in) {
                return new String(customHash(in.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
            }

            private byte[] blake2bHash(byte[] in) {
                final Blake2bDigest hash = new Blake2bDigest(null, 32, salt, personalization);
                hash.update(in, 0, in.length);
                final byte[] out = new byte[hash.getDigestSize()];
                hash.doFinal(out, 0);

                if (prefix != null) {
                    return Bytes.concat(prefix, Hex.encode(out));
                }

                return Hex.encode(out);
            }

            private BytesRef blake2bHash(BytesRef in) {
                final BytesRef copy = BytesRef.deepCopyOf(in);
                return new BytesRef(blake2bHash(copy.bytes));
            }

            private String blake2bHash(String in) {
                return new String(blake2bHash(in.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
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
            this.statefulIndexQueries = new StatefulIndexRules(roles, fieldMaskingConfig, indices);
            this.componentState.replacePart(this.statefulIndexQueries.getComponentState());
        }
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    public DlsFlsConfig.FieldMasking getFieldMaskingConfig() {
        return fieldMaskingConfig;
    }

}
