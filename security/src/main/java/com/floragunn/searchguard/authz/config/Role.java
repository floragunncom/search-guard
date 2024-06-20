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

package com.floragunn.searchguard.authz.config;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

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
import com.floragunn.searchguard.configuration.Hideable;
import com.floragunn.searchguard.configuration.StaticDefinable;
import com.floragunn.searchsupport.queries.Query;
import com.floragunn.searchsupport.xcontent.XContentParserContext;
import com.google.common.base.Splitter;

public class Role implements Document<Role>, Hideable, StaticDefinable {

    public static ValidationResult<Role> parse(DocNode docNode, Parser.Context context) {
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
        ImmutableList<ExcludeIndex> excludeIndexPermissions = vNode.get("exclude_index_permissions").asList().withEmptyListAsDefault()
                .ofObjectsParsedBy((Parser<ExcludeIndex, Parser.Context>) ExcludeIndex::new);
        String description = vNode.get("description").asString();

        vNode.checkForUnusedAttributes();

        return new ValidationResult<Role>(new Role(docNode, reserved, hidden, isStatic, description, clusterPermissions, indexPermissions,
                aliasPermissions, dataStreamPermissions, tenantPermissions, excludeClusterPermissions, excludeIndexPermissions), validationErrors);
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
    private final ImmutableList<ExcludeIndex> excludeIndexPermissions;

    public Role(DocNode source, boolean reserved, boolean hidden, boolean isStatic, String description, ImmutableList<String> clusterPermissions,
            ImmutableList<Index> indexPermissions, ImmutableList<Alias> aliasPermissions, ImmutableList<DataStream> dataStreamPermissions,
            ImmutableList<Tenant> tenantPermissions, ImmutableList<String> excludeClusterPermissions,
            ImmutableList<ExcludeIndex> excludeIndexPermissions) {
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
        this.excludeIndexPermissions = excludeIndexPermissions;
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

            @Override
            public String toString() {
                return source;
            }
        }

        public static class FieldMaskingExpression {
            public static final FieldMaskingExpression MASK_ALL = new FieldMaskingExpression(Pattern.wildcard(), "*");

            private final Pattern pattern;
            private final MessageDigest algo;
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
                        algo = MessageDigest.getInstance(tokens.get(1));
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
                return algo;
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

        Alias(ImmutableList<Template<Pattern>> indexPatterns, Template<Query> dls, ImmutableList<FlsPattern> fls,
                ImmutableList<FieldMaskingExpression> maskedFields, ImmutableList<String> allowedActions) {
            super(indexPatterns, dls, fls, maskedFields, allowedActions);
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

        DataStream(ImmutableList<Template<Pattern>> indexPatterns, Template<Query> dls, ImmutableList<FlsPattern> fls,
                ImmutableList<FieldMaskingExpression> maskedFields, ImmutableList<String> allowedActions) {
            super(indexPatterns, dls, fls, maskedFields, allowedActions);
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

    public static class ExcludeIndex {

        private final IndexPatterns indexPatterns;
        private final ImmutableList<String> actions;

        public ExcludeIndex(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

            // Just for validation: 
            vNode.get("actions").by(Pattern::parse);
            vNode.get("index_patterns").asList().ofTemplates(Pattern::create);

            this.actions = ImmutableList.of(vNode.get("actions").asList().withEmptyListAsDefault().ofStrings());
            ImmutableList<Template<String>> indexPatterns = ImmutableList
                    .of(vNode.get("index_patterns").asList().withEmptyListAsDefault().ofTemplates());

            this.indexPatterns = new IndexPatterns.Builder(indexPatterns).build();

            vNode.checkForUnusedAttributes();
            validationErrors.throwExceptionForPresentErrors();
        }

        public ExcludeIndex(ImmutableList<Template<Pattern>> indexPatterns, ImmutableList<String> actions) {
            try {
                this.indexPatterns = new IndexPatterns.Builder(indexPatterns.map(t -> t.toStringTemplate())).build();
            } catch (ConfigValidationException e) {
                // This should not happen
                throw new RuntimeException(e);
            }
            this.actions = actions;
        }

        public IndexPatterns getIndexPatterns() {
            return indexPatterns;
        }

        public ImmutableList<String> getActions() {
            return actions;
        }
    }

    public static class IndexPatterns {

        private final Pattern pattern;
        private final ImmutableList<IndexPatternTemplate> patternTemplates;
        private final ImmutableList<DateMathExpression> dateMathExpressions;
        private final String asString;

        IndexPatterns(Pattern pattern, ImmutableList<IndexPatternTemplate> patternTemplates, ImmutableList<DateMathExpression> dateMathExpressions) {
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
                        ImmutableList.of(dateMathExpressions));

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

    public ImmutableList<ExcludeIndex> getExcludeIndexPermissions() {
        return excludeIndexPermissions;
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
