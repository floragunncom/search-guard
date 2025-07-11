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

package com.floragunn.searchguard.enterprise.dlsfls;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.lucene.util.BytesRef;

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.meta.Meta;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Bytes;

public class RoleBasedFieldMasking
        extends RoleBasedAuthorizationBase<RoleBasedFieldMasking.FieldMaskingRule.SingleRole, RoleBasedFieldMasking.FieldMaskingRule> {

    private final DlsFlsConfig.FieldMasking fieldMaskingConfig;
    private final static BaseEncoding hex = BaseEncoding.base16().lowerCase();

    public RoleBasedFieldMasking(SgDynamicConfiguration<Role> roles, DlsFlsConfig.FieldMasking fieldMaskingConfig, Meta indexMetadata,
            MetricsLevel metricsLevel) {
        super(roles, indexMetadata, metricsLevel, (rolePermissions) -> roleToRule(rolePermissions, fieldMaskingConfig));
        this.fieldMaskingConfig = fieldMaskingConfig;
    }

    static FieldMaskingRule.SingleRole roleToRule(Role.Index rolePermissions, DlsFlsConfig.FieldMasking fieldMaskingConfig) {
        ImmutableList<Role.Index.FieldMaskingExpression> fmExpressions = rolePermissions.getMaskedFields();

        if (fmExpressions != null && !fmExpressions.isEmpty()) {
            return new FieldMaskingRule.SingleRole(rolePermissions, fieldMaskingConfig);
        } else {
            return null;
        }
    }

    @Override
    protected FieldMaskingRule unrestricted() {
        return FieldMaskingRule.ALLOW_ALL;
    }

    @Override
    protected FieldMaskingRule fullyRestricted() {
        return FieldMaskingRule.MASK_ALL;
    }

    @Override
    protected FieldMaskingRule compile(PrivilegesEvaluationContext context, Collection<FieldMaskingRule.SingleRole> rules)
            throws PrivilegesEvaluationException {
        return new FieldMaskingRule.MultiRole(rules);
    }

    @Override
    protected String hasRestrictionsMetricName() {
        return "has_fm_restriction";
    }

    @Override
    protected String evaluateRestrictionsMetricName() {
        return "evaluate_fm_restriction";
    }

    @Override
    protected String componentName() {
        return "role_based_field_masking";
    }

    public DlsFlsConfig.FieldMasking getFieldMaskingConfig() {
        return fieldMaskingConfig;
    }

    public static abstract class FieldMaskingRule {
        public static final FieldMaskingRule ALLOW_ALL = new FieldMaskingRule.SingleRole(ImmutableList.empty());
        public static final FieldMaskingRule MASK_ALL = new FieldMaskingRule.SingleRole(
                ImmutableList.of(new Field(Role.Index.FieldMaskingExpression.MASK_ALL, DlsFlsConfig.FieldMasking.DEFAULT)));

        public static FieldMaskingRule of(DlsFlsConfig.FieldMasking fieldMaskingConfig, String... rules) throws ConfigValidationException {
            ImmutableList.Builder<Role.Index.FieldMaskingExpression> patterns = new ImmutableList.Builder<>();

            for (String rule : rules) {
                patterns.add(new Role.Index.FieldMaskingExpression(rule));
            }

            return new SingleRole(patterns.build().map((e) -> new Field(e, fieldMaskingConfig)));
        }

        public abstract Field get(String field);

        public boolean isNotMasked(String field) {
            return get(field) == null;
        }

        public abstract boolean isAllowAll();

        public static class SingleRole extends FieldMaskingRule {

            final Role.Index sourceIndex;
            final ImmutableList<FieldMaskingRule.Field> expressions;

            SingleRole(Role.Index sourceIndex, DlsFlsConfig.FieldMasking fieldMaskingConfig) {
                this.sourceIndex = sourceIndex;
                this.expressions = ImmutableList
                        .of(sourceIndex.getMaskedFields().stream().map((e) -> new Field(e, fieldMaskingConfig)).collect(Collectors.toList()));
            }

            SingleRole(ImmutableList<Field> expressions) {
                this.sourceIndex = null;
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
                        return Bytes.concat(prefix, hex.encode(algo.digest(in)).getBytes());
                    }

                    return hex.encode(algo.digest(in)).getBytes();
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
                    return Bytes.concat(prefix, hex.encode(out).getBytes());
                }

                return hex.encode(out).getBytes();
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

}
