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
/*
 * Includes code from https://github.com/opensearch-project/security/blob/70591197c705ca6f42f765186a05837813f80ff3/src/main/java/org/opensearch/security/privileges/dlsfls/FieldPrivileges.java
 * which is Copyright OpenSearch Contributors
 */

package com.floragunn.searchguard.authz.config;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.PatternSyntaxException;

import com.floragunn.searchguard.authz.actions.Actions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.config.templates.Template;
import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.documents.Parser.Context;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.Hideable;
import com.floragunn.searchguard.configuration.StaticDefinable;
import com.floragunn.searchsupport.queries.Query;
import com.floragunn.searchsupport.xcontent.XContentParserContext;
import com.google.common.base.Splitter;

public class Role implements Document<Role>, Hideable, StaticDefinable {
    private static final Logger log = LogManager.getLogger(Role.class);

    public static ValidationResult<Role> parse(DocNode docNode, ConfigurationRepository.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        boolean reserved = vNode.get("reserved").withDefault(false).asBoolean();
        boolean hidden = vNode.get("hidden").withDefault(false).asBoolean();
        boolean isStatic = vNode.get("static").withDefault(false).asBoolean();

        // Just for validation:
        vNode.get("cluster_permissions").by(Pattern::parse);
        vNode.get("exclude_cluster_permissions").by(Pattern::parse);
        ImmutableList<String> clusterPermissions = vNode.get("cluster_permissions").asList().withEmptyListAsDefault().ofStrings();
        ImmutableList<String> excludeClusterPermissions = vNode.get("exclude_cluster_permissions").asList().withEmptyListAsDefault().ofStrings();

        ImmutableList<Index> indexPermissions = vNode.get("index_permissions").asList().withEmptyListAsDefault()
                .ofObjectsParsedBy((Parser<Index, Parser.Context>) Index::new);
        ImmutableList<Alias> aliasPermissions = vNode.get("alias_permissions").asList().withEmptyListAsDefault()
                .ofObjectsParsedBy((Parser<Alias, Parser.Context>) Alias::new);
        ImmutableList<DataStream> dataStreamPermissions = vNode.get("data_stream_permissions").asList().withEmptyListAsDefault()
                .ofObjectsParsedBy((Parser<DataStream, Parser.Context>) DataStream::new);
        ImmutableList<Tenant> tenantPermissions = vNode.get("tenant_permissions").asList().withEmptyListAsDefault()
                .ofObjectsParsedBy((Parser<Tenant, Parser.Context>) Tenant::new);

        List<String> excludeIndexPermissions = vNode.get("exclude_index_permissions").asList().withEmptyListAsDefault().ofStrings();
        if (excludeIndexPermissions != null && !excludeIndexPermissions.isEmpty()) {
            if (context != null && context.isLenientValidationRequested()) {
                log.error("exclude_index_permissions in sg_roles is no longer supported");
            } else {
                validationErrors.add(new ValidationError("exclude_index_permissions", "This attribute is no longer supported"));
            }
        }

        String description = vNode.get("description").asString();

        if (context != null && context.getActions() != null) {
            Actions actions = context.getActions();

            warnWhenIndexPermsAreAssignedToClusterPerms(clusterPermissions, actions);
            warnWhenClusterPermsAreAssignedToIndexLikePerms(indexPermissions, actions, "index");
            warnWhenClusterPermsAreAssignedToIndexLikePerms(aliasPermissions, actions, "alias");
            warnWhenClusterPermsAreAssignedToIndexLikePerms(dataStreamPermissions, actions, "data stream");
        }

        warnWhenDlsOrFlsRuleIsAssignedToWildcardPattern(indexPermissions);
        warnWhenDlsOrFlsRuleIsAssignedToWildcardPattern(aliasPermissions);
        warnWhenDlsOrFlsRuleIsAssignedToWildcardPattern(dataStreamPermissions);

        vNode.checkForUnusedAttributes();

        return new ValidationResult<Role>(new Role(docNode, reserved, hidden, isStatic, description, clusterPermissions, indexPermissions,
                aliasPermissions, dataStreamPermissions, tenantPermissions, excludeClusterPermissions), validationErrors);
    }

    private static <T extends Index> void warnWhenClusterPermsAreAssignedToIndexLikePerms(ImmutableList<T> permissions, Actions actions, String expectedPermissionType) {
        List<String> clusterPermissionUsedAsIndexLikePermissions = permissions.stream()
                .flatMap(indexLikePermission  -> findClusterPermissions(indexLikePermission.getAllowedActions(), actions)
                        .stream()
                ).toList();

        if (! clusterPermissionUsedAsIndexLikePermissions.isEmpty()) {
            log.warn("The following cluster permissions are assigned as {} permissions: {}", expectedPermissionType, clusterPermissionUsedAsIndexLikePermissions);
        }
    }

    private static void warnWhenIndexPermsAreAssignedToClusterPerms(ImmutableList<String> clusterPermissions, Actions actions) {
        List<String> indexPermissionsUsedAsClusterPermissions = findIndexPermissions(clusterPermissions, actions);

        if (! indexPermissionsUsedAsClusterPermissions.isEmpty()) {
            log.warn("The following index permissions are assigned as cluster permissions: {}", indexPermissionsUsedAsClusterPermissions);
        }
    }

    private static List<String> findIndexPermissions(ImmutableList<String> permissions, Actions actions) {
        return permissions.stream().filter(permission -> ! "*".equals(permission) && actions.get(permission).isIndexLikePrivilege()).toList();
    }

    private static List<String> findClusterPermissions(ImmutableList<String> permissions, Actions actions) {
        return permissions.stream().filter(permission -> ! "*".equals(permission) && actions.get(permission).isClusterPrivilege()).toList();
    }

    private static <T extends Index> void warnWhenDlsOrFlsRuleIsAssignedToWildcardPattern(ImmutableList<T> permissions) {
        permissions.forEach(permission -> {
            if (permission.getIndexPatterns().getPattern().isWildcard()) {
                if (permission.getDls() != null) {
                    log.warn("Role assigns a DLS rule '{}' to wildcard (*) {}",
                            permission.getDls().getSource(), permission.getPatternAttributeName()
                    );
                }
                if (! permission.getFls().isEmpty()) {
                    log.warn("Role assigns a FLS rule '{}' to wildcard (*) {}",
                            permission.getFls().stream().map(Index.FlsPattern::getSource).toList(), permission.getPatternAttributeName()
                    );
                }
            }
        });
    }

    private final DocNode source;
    private final boolean reserved;
    private final boolean hidden;
    private final boolean isStatic;

    private final String description;
    private final ImmutableList<String> clusterPermissions;
    private final ImmutableList<Index> indexPermissions;
    private final ImmutableList<Alias> aliasPermissions;
    private final ImmutableList<DataStream> dataStreamPermissions;
    private final ImmutableList<Tenant> tenantPermissions;
    private final ImmutableList<String> excludeClusterPermissions;

    public Role(DocNode source, boolean reserved, boolean hidden, boolean isStatic, String description, ImmutableList<String> clusterPermissions,
            ImmutableList<Index> indexPermissions, ImmutableList<Alias> aliasPermissions, ImmutableList<DataStream> dataStreamPermissions,
            ImmutableList<Tenant> tenantPermissions, ImmutableList<String> excludeClusterPermissions) {
        this.source = source;
        this.reserved = reserved;
        this.isStatic = isStatic;
        this.hidden = hidden;
        this.description = description;
        this.clusterPermissions = clusterPermissions;
        this.indexPermissions = indexPermissions;
        this.aliasPermissions = aliasPermissions;
        this.dataStreamPermissions = dataStreamPermissions;
        this.tenantPermissions = tenantPermissions;
        this.excludeClusterPermissions = excludeClusterPermissions;
    }

    @Override
    public Object toBasicObject() {
        return source;
    }

    public boolean isReserved() {
        return reserved;
    }

    public boolean isHidden() {
        return hidden;
    }

    public static class Index {

        private final IndexPatterns indexPatterns;

        private final Template<Query> dls;
        private final ImmutableList<FlsPattern> fls;
        private final ImmutableList<FieldMaskingExpression> maskedFields;
        private final ImmutableList<String> allowedActions;

        /**
         * @deprecated these do not handle negations in pattern correctly. Only used in LegacyRoleBasedDocumentAuthorization.
         */
        private final ImmutableList<Template<Pattern>> legacyIndexPatterns;

        Index(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

            this.dls = vNode.get("dls").asTemplate((s) -> new Query(s, (XContentParserContext) context));
            this.fls = vNode.get("fls").asList().ofObjectsParsedByString(FlsPattern::new);
            this.maskedFields = vNode.get("masked_fields").asList().withEmptyListAsDefault().ofObjectsParsedByString(FieldMaskingExpression::new);

            // Just for validation: 
            vNode.get("allowed_actions").by(Pattern::parse);
            this.legacyIndexPatterns = vNode.get(getPatternAttributeName()).asList().ofTemplates(Pattern::create);

            this.allowedActions = ImmutableList.of(vNode.get("allowed_actions").asList().withEmptyListAsDefault().ofStrings());
            ImmutableList<Template<String>> indexPatterns = ImmutableList
                    .of(vNode.get(getPatternAttributeName()).asList().withEmptyListAsDefault().ofTemplates());

            this.indexPatterns = new IndexPatterns.Builder(indexPatterns).build();

            vNode.checkForUnusedAttributes();
            validationErrors.throwExceptionForPresentErrors();
        }

        public Index(ImmutableList<Template<Pattern>> indexPatterns, Template<Query> dls, ImmutableList<FlsPattern> fls,
                ImmutableList<FieldMaskingExpression> maskedFields, ImmutableList<String> allowedActions) {
            try {
                this.indexPatterns = new IndexPatterns.Builder(indexPatterns.map(t -> t.toStringTemplate())).build();
            } catch (ConfigValidationException e) {
                // This should not happen
                throw new RuntimeException(e);
            }
            this.legacyIndexPatterns = indexPatterns;
            this.dls = dls;
            this.fls = fls;
            this.maskedFields = maskedFields;
            this.allowedActions = allowedActions;
        }

        public IndexPatterns getIndexPatterns() {
            return indexPatterns;
        }

        /**
         * @deprecated these do not handle negations in pattern correctly. Only used in LegacyRoleBasedDocumentAuthorization.
         */
        public ImmutableList<Template<Pattern>> getLegacyIndexPatterns() {
            return legacyIndexPatterns;
        }

        public Template<Query> getDls() {
            return dls;
        }

        public ImmutableList<FlsPattern> getFls() {
            return fls;
        }

        public ImmutableList<FieldMaskingExpression> getMaskedFields() {
            return maskedFields;
        }

        public ImmutableList<String> getAllowedActions() {
            return allowedActions;
        }

        protected String getPatternAttributeName() {
            return "index_patterns";
        }

        public boolean usesTemplates() {
            return !this.indexPatterns.getPatternTemplates().isEmpty() || (this.dls != null && !this.dls.isConstant());
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexPatterns, dls, fls, maskedFields, allowedActions, legacyIndexPatterns);
        }

        public static class FlsPattern {
            public static final FlsPattern INCLUDE_ALL = new FlsPattern(Pattern.wildcard(), false, "*");
            public static final FlsPattern EXCLUDE_ALL = new FlsPattern(Pattern.wildcard(), true, "~*");

            private final boolean excluded;
            private final Pattern pattern;
            private final String source;

            public FlsPattern(String string) throws ConfigValidationException {
                if (string.startsWith("~") || string.startsWith("!")) {
                    excluded = true;
                    pattern = Pattern.create(string.substring(1));
                } else {
                    pattern = Pattern.create(string);
                    excluded = false;
                }

                this.source = string;
            }

            FlsPattern(Pattern pattern, boolean excluded, String source) {
                this.pattern = pattern;
                this.excluded = excluded;
                this.source = source;
            }

            public String getSource() {
                return source;
            }

            public Pattern getPattern() {
                return pattern;
            }

            public boolean isExcluded() {
                return excluded;
            }

            public List<FlsPattern> getParentObjectPatterns() {
                if (excluded || source.indexOf('.') == -1) {
                    return Collections.emptyList();
                }

                List<FlsPattern> subPatterns = new ArrayList<>();

                for (int pos = source.indexOf('.'); pos != -1; pos = source.indexOf('.', pos + 1)) {
                    String subString = source.substring(0, pos);

                    subPatterns.add(new FlsPattern(Pattern.createUnchecked(subString), false, subString));
                }

                return subPatterns;
            }

            @Override
            public boolean equals(Object o) {
                if (o instanceof FlsPattern that) {
                    return this.source.equals(that.source);
                } else {
                    return false;
                }
            }

            @Override
            public int hashCode() {
                return source.hashCode();
            }

            @Override
            public String toString() {
                return source;
            }
        }

        public static class FieldMaskingExpression {
            public static final FieldMaskingExpression MASK_ALL = new FieldMaskingExpression(Pattern.wildcard(), "*");

            private final Pattern pattern;
            private final String algo;
            private final List<RegexReplacement> regexReplacements;
            private final String source;

            public FieldMaskingExpression(String value) throws ConfigValidationException {
                this.source = value;

                List<String> tokens = Splitter.on("::").splitToList(value);
                pattern = Pattern.create(tokens.get(0));

                if (tokens.size() == 1) {
                    algo = null;
                    regexReplacements = null;
                } else if (tokens.size() == 2) {
                    regexReplacements = null;

                    try {
                        // Check if the algorithm is available
                        String algorithm = tokens.get(1);
                        MessageDigest.getInstance(algorithm);
                        this.algo = algorithm;
                    } catch (NoSuchAlgorithmException e) {
                        throw new ConfigValidationException(new ValidationError(null, "Invalid algorithm " + tokens.get(1)));
                    }

                } else {
                    algo = null;
                    regexReplacements = new ArrayList<>((tokens.size() - 1) / 2);
                    for (int i = 1; i < tokens.size() - 1; i = i + 2) {
                        regexReplacements.add(new RegexReplacement(tokens.get(i), tokens.get(i + 1)));
                    }
                }
            }

            private FieldMaskingExpression(Pattern pattern, String source) {
                this.pattern = pattern;
                this.source = source;
                this.algo = null;
                this.regexReplacements = null;
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(source);
            }

            public static class RegexReplacement {
                private final java.util.regex.Pattern regex;
                private final String replacement;

                public RegexReplacement(String regex, String replacement) throws ConfigValidationException {
                    if (!regex.startsWith("/") || !regex.endsWith("/")) {
                        throw new ConfigValidationException(new ValidationError(null, "A regular expression needs to be wrapped in /.../"));
                    }

                    try {
                        this.regex = java.util.regex.Pattern.compile(regex.substring(1).substring(0, regex.length() - 2));
                    } catch (PatternSyntaxException e) {
                        throw new ConfigValidationException(new ValidationError(null, e.getMessage()).cause(e));
                    }

                    this.replacement = replacement;
                }

                public java.util.regex.Pattern getRegex() {
                    return regex;
                }

                public String getReplacement() {
                    return replacement;
                }

                @Override
                public String toString() {
                    return "RegexReplacement [regex=" + regex + ", replacement=" + replacement + "]";
                }

            }

            @Override
            public String toString() {
                return source;
            }

            public MessageDigest getAlgo() {
                try {
                    // Check if the algorithm is available
                    if(algo == null) {
                        return null;
                    }
                    return MessageDigest.getInstance(this.algo);
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException("Algorithm " + this.algo + " has been already validated", e);
                }
            }

            public List<RegexReplacement> getRegexReplacements() {
                return regexReplacements;
            }

            public Pattern getPattern() {
                return pattern;
            }

            public String getSource() {
                return source;
            }
        }
    }

    public static class Alias extends Index {

        Alias(DocNode docNode, Context context) throws ConfigValidationException {
            super(docNode, context);
        }

        public Alias(ImmutableList<Template<Pattern>> aliasPatterns, Template<Query> dls, ImmutableList<FlsPattern> fls,
                ImmutableList<FieldMaskingExpression> maskedFields, ImmutableList<String> allowedActions) {
            super(aliasPatterns, dls, fls, maskedFields, allowedActions);
        }

        @Override
        protected String getPatternAttributeName() {
            return "alias_patterns";
        }
    }

    public static class DataStream extends Index {

        DataStream(DocNode docNode, Context context) throws ConfigValidationException {
            super(docNode, context);
        }

        public DataStream(ImmutableList<Template<Pattern>> dataStreamPatterns, Template<Query> dls, ImmutableList<FlsPattern> fls,
                ImmutableList<FieldMaskingExpression> maskedFields, ImmutableList<String> allowedActions) {
            super(dataStreamPatterns, dls, fls, maskedFields, allowedActions);
        }

        @Override
        protected String getPatternAttributeName() {
            return "data_stream_patterns";
        }
    }

    public static class Tenant {

        private final ImmutableList<Template<Pattern>> tenantPatterns;
        private final ImmutableList<String> allowedActions;

        public Tenant(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);
            vNode.get("allowed_actions").by(Pattern::parse);

            this.tenantPatterns = ImmutableList.of(vNode.get("tenant_patterns").asList().withEmptyListAsDefault().ofTemplates(Pattern::create));
            this.allowedActions = ImmutableList.of(vNode.get("allowed_actions").asList().withEmptyListAsDefault().ofStrings());

            vNode.checkForUnusedAttributes();
            validationErrors.throwExceptionForPresentErrors();
        }

        public Tenant(ImmutableList<Template<Pattern>> tenantPatterns, ImmutableList<String> allowedActions) {
            this.tenantPatterns = tenantPatterns;
            this.allowedActions = allowedActions;
        }

        public ImmutableList<Template<Pattern>> getTenantPatterns() {
            return tenantPatterns;
        }

        public ImmutableList<String> getAllowedActions() {
            return allowedActions;
        }

    }

    public static class IndexPatterns {

        private final Pattern pattern;
        private final ImmutableList<IndexPatternTemplate> patternTemplates;
        private final ImmutableList<DateMathExpression> dateMathExpressions;
        private final String asString;
        private final ImmutableList<Template<String>> source;

        IndexPatterns(Pattern pattern, ImmutableList<IndexPatternTemplate> patternTemplates, ImmutableList<DateMathExpression> dateMathExpressions, ImmutableList<Template<String>> source) {
            this.pattern = pattern;
            this.patternTemplates = patternTemplates;
            this.dateMathExpressions = dateMathExpressions;

            StringBuilder asString = new StringBuilder();

            if (pattern != null && !pattern.isBlank()) {
                asString.append(pattern);
            }

            if (patternTemplates != null && !patternTemplates.isEmpty()) {
                if (asString.length() != 0) {
                    asString.append(" ");
                }
                asString.append(patternTemplates);
            }

            if (dateMathExpressions != null && !dateMathExpressions.isEmpty()) {
                if (asString.length() != 0) {
                    asString.append(" ");
                }
                asString.append(dateMathExpressions);
            }

            if (asString.length() == 0) {
                asString.append("-/-");
            }

            this.asString = asString.toString();
            this.source = source;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(asString);
        }

        public Pattern getPattern() {
            return pattern;
        }

        public ImmutableList<IndexPatternTemplate> getPatternTemplates() {
            return patternTemplates;
        }

        public ImmutableList<DateMathExpression> getDateMathExpressions() {
            return dateMathExpressions;
        }

        public ImmutableList<Template<String>> getSource() {
            return this.source;
        }

        @Override
        public String toString() {
            return this.asString;
        }

        static class Builder {
            private final ImmutableList<Template<String>> source;
            private List<String> constantPatterns = new ArrayList<>();
            private List<IndexPatternTemplate> patternTemplates = new ArrayList<>();
            private List<DateMathExpression> dateMathExpressions = new ArrayList<>();
            private Map<Integer, Pattern> negations = new HashMap<>();

            Builder(ImmutableList<Template<String>> source) {
                this.source = source;
            }

            IndexPatterns build() throws ConfigValidationException {
                ValidationErrors validationErrors = new ValidationErrors();

                for (int i = 0; i < source.size(); i++) {
                    try {
                        Template<String> indexPattern = source.get(i);

                        if (indexPattern.getSource().startsWith("<") && indexPattern.getSource().endsWith(">")) {
                            dateMathExpressions.add(new DateMathExpression(indexPattern.getSource(), getExclusions(i + 1)));
                        } else if (indexPattern.isConstant()) {
                            constantPatterns.add(indexPattern.getConstantValue());
                        } else {
                            patternTemplates.add(new IndexPatternTemplate(indexPattern.parser(Pattern::create), getExclusions(i + 1)));
                        }
                    } catch (ConfigValidationException e) {
                        validationErrors.add(String.valueOf(i), e);
                    }
                }

                validationErrors.throwExceptionForPresentErrors();

                return new IndexPatterns(Pattern.create(removeLeadingNegations(constantPatterns)), ImmutableList.of(patternTemplates),
                        ImmutableList.of(dateMathExpressions), source);

            }

            private List<String> removeLeadingNegations(List<String> patterns) {
                // This handles the case templated_pattern_${x}, -negation, constant_pattern
                // In this case, the constant pattern would be filtered to -negation, constant_pattern which would be invalid
                // We now remove the leading negations and just return constant_pattern

                if (patterns.isEmpty()) {
                    return patterns;
                }

                if (!patterns.get(0).startsWith("-")) {
                    return patterns;
                }

                int firstNonNegated = -1;

                for (int i = 1; i < patterns.size(); i++) {
                    if (!patterns.get(i).startsWith("-")) {
                        firstNonNegated = i;
                        break;
                    }
                }

                if (firstNonNegated != -1) {
                    return patterns.subList(firstNonNegated, patterns.size());
                } else {
                    return ImmutableList.empty();
                }
            }

            private Pattern getExclusions(int start) {
                if (start >= source.size()) {
                    return Pattern.blank();
                }

                int firstNegation = findFirstNegation(start);
                if (firstNegation == -1) {
                    return Pattern.blank();
                }

                Pattern negation = this.negations.get(firstNegation);
                if (negation != null) {
                    return negation;
                }

                List<String> negations = new ArrayList<>(source.size());

                for (int i = firstNegation; i < source.size(); i++) {
                    Template<String> pattern = source.get(i);

                    if (pattern.isConstant() && pattern.getConstantValue().startsWith("-")) {
                        negations.add(pattern.getConstantValue().substring(1));
                    }
                }

                negation = Pattern.createUnchecked(negations);
                this.negations.put(firstNegation, negation);
                return negation;
            }

            private int findFirstNegation(int start) {
                for (int i = start; i < source.size(); i++) {
                    Template<String> pattern = source.get(i);

                    if (pattern.isConstant() && pattern.getConstantValue().startsWith("-")) {
                        return i;
                    }
                }

                return -1;

            }

        }

        public static class IndexPatternTemplate {
            private final Template<Pattern> template;
            private final Pattern exclusions;

            IndexPatternTemplate(Template<Pattern> template, Pattern exclusions) {
                this.template = template;
                this.exclusions = exclusions;
            }

            public Template<Pattern> getTemplate() {
                return template;
            }

            public Pattern getExclusions() {
                return exclusions;
            }

            @Override
            public String toString() {
                if (this.exclusions.isBlank()) {
                    return this.template.toString();
                } else {
                    return this.template + " -" + this.exclusions;
                }
            }
        }

        public static class DateMathExpression {
            private final String dateMathExpression;
            private final Pattern exclusions;

            DateMathExpression(String dateMathExpression, Pattern exclusions) {
                this.dateMathExpression = dateMathExpression;
                this.exclusions = exclusions;
            }

            public String getDateMathExpression() {
                return dateMathExpression;
            }

            public Pattern getExclusions() {
                return exclusions;
            }

            @Override
            public String toString() {
                if (this.exclusions.isBlank()) {
                    return this.dateMathExpression.toString();
                } else {
                    return this.dateMathExpression + " -" + this.exclusions;
                }
            }
        }

    }

    public ImmutableList<String> getClusterPermissions() {
        return clusterPermissions;
    }

    public ImmutableList<String> getExcludeClusterPermissions() {
        return excludeClusterPermissions;
    }

    public ImmutableList<Index> getIndexPermissions() {
        return indexPermissions;
    }

    public ImmutableList<Tenant> getTenantPermissions() {
        return tenantPermissions;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean isStatic() {
        return isStatic;
    }

    public ImmutableList<Alias> getAliasPermissions() {
        return aliasPermissions;
    }

    public ImmutableList<DataStream> getDataStreamPermissions() {
        return dataStreamPermissions;
    }
}
