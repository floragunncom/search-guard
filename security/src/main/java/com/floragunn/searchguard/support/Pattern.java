/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.searchguard.support;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.PatternSyntaxException;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;

public interface Pattern extends Document<Pattern>, Predicate<String> {

    public static Pattern create(String pattern) throws ConfigValidationException {
        if ("*".equals(pattern)) {
            return WILDCARD;
        } else if (pattern.startsWith("/") && pattern.endsWith("/")) {
            return new JavaPattern(pattern.substring(1, pattern.length() - 1));
        } else if (pattern.endsWith("*") && pattern.indexOf('*') == pattern.length() - 1 && !pattern.contains("?")) {
            return new PrefixPattern(pattern.substring(0, pattern.length() - 1));
        } else if (pattern.contains("?") || pattern.contains("*")) {
            return new SimplePattern(pattern);
        } else {
            return new Constant(pattern);
        }
    }

    public static Pattern create(Collection<String> patterns) throws ConfigValidationException {
        if (patterns.size() == 0) {
            return BLANK;
        } else if (patterns.size() == 1) {
            return create(patterns.iterator().next());
        } else {
            return CompoundPattern.create(patterns);
        }
    }

    public static Pattern join(Collection<Pattern> patterns) {
        if (patterns.size() == 0) {
            return BLANK;
        } else if (patterns.size() == 1) {
            return patterns.iterator().next();
        } else {
            return CompoundPattern.join(patterns);
        }
    }

    public static Pattern parse(DocNode docNode, Parser.Context context) throws ConfigValidationException {
        if (docNode.isList()) {
            return create(docNode.toListOfStrings());
        } else {
            return create(docNode.toString());
        }
    }

    public static boolean isConstant(String pattern) {
        if ("*".equals(pattern)) {
            return false;
        } else if (pattern.startsWith("/") && pattern.endsWith("/")) {
            return false;
        } else if (pattern.contains("?") || pattern.contains("*")) {
            return false;
        } else {
            return true;
        }
    }
    
    public static Pattern wildcard() {
        return WILDCARD;
    }

    boolean matches(String string);

    boolean matches(Iterable<String> string);

    ImmutableSet<String> getMatching(ImmutableSet<String> strings);

    Iterable<String> iterateMatching(Iterable<String> strings);

    ImmutableSet<String> getConstants();

    ImmutableSet<String> getPatterns();

    <E> ImmutableSet<E> getMatching(ImmutableSet<E> set, Function<E, String> stringMappingFunction);

    Pattern excluding(Pattern exludingPattern);

    boolean isWildcard();
    
    static class Constant extends AbstractPattern {
        private final String value;

        Constant(String value) {
            this.value = value;
        }

        @Override
        public boolean matches(String string) {
            return value.equals(string);
        }

        @Override
        public Iterable<String> iterateMatching(Iterable<String> strings) {
            if (strings instanceof ImmutableSet) {
                ImmutableSet<String> set = (ImmutableSet<String>) strings;

                if (set.size() == 0) {
                    return set;
                } else if (set.size() == 1) {
                    String value = set.only();

                    if (matches(value)) {
                        return set;
                    } else {
                        return ImmutableSet.empty();
                    }
                }
            }
            return super.iterateMatching(strings);
        }

        @Override
        public ImmutableSet<String> getConstants() {
            return ImmutableSet.of(value);
        }

        @Override
        public ImmutableSet<String> getPatterns() {
            return ImmutableSet.empty();
        }

        String getValue() {
            return value;
        }

        @Override
        public Object toBasicObject() {
            return value;
        }
        
        @Override
        public String toString() {
            return value;
        }

    }

    static class PrefixPattern extends AbstractPattern {
        private final String prefix;
        private final String source;

        PrefixPattern(String prefix) {
            this.prefix = prefix;
            this.source = prefix + "*";
        }

        @Override
        public boolean matches(String string) {
            return string.startsWith(this.prefix);
        }

        @Override
        public int hashCode() {
            return prefix.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof PrefixPattern)) {
                return false;
            }

            return ((PrefixPattern) obj).prefix.equals(this.prefix);
        }

        @Override
        public String toString() {
            return source;
        }

        @Override
        public ImmutableSet<String> getConstants() {
            return ImmutableSet.empty();
        }

        @Override
        public ImmutableSet<String> getPatterns() {
            return ImmutableSet.of(source);
        }

        @Override
        public Object toBasicObject() {
            return ImmutableList.of(source);
        }
    }

    static class SimplePattern extends AbstractPattern {
        private final String pattern;

        SimplePattern(String pattern) {
            this.pattern = pattern;
        }

        @Override
        public boolean matches(String string) {
            return WildcardMatcher.simpleWildcardMatch(this.pattern, string);
        }

        @Override
        public int hashCode() {
            return pattern.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof SimplePattern)) {
                return false;
            }

            return ((SimplePattern) obj).pattern.equals(this.pattern);
        }

        @Override
        public String toString() {
            return pattern;
        }

        @Override
        public ImmutableSet<String> getConstants() {
            return ImmutableSet.empty();
        }

        @Override
        public ImmutableSet<String> getPatterns() {
            return ImmutableSet.of(pattern);
        }

        @Override
        public Object toBasicObject() {
            return ImmutableList.of(pattern);
        }
    }

    static class JavaPattern extends AbstractPattern {
        private final java.util.regex.Pattern javaPattern;
        private final String patternString;

        JavaPattern(String pattern) throws ConfigValidationException {
            try {
                this.javaPattern = java.util.regex.Pattern.compile(pattern);
                this.patternString = pattern;
            } catch (PatternSyntaxException e) {
                throw new ConfigValidationException(new InvalidAttributeValue(null, pattern, "A regular expression pattern"));
            }
        }

        @Override
        public boolean matches(String string) {
            return javaPattern.matcher(string).matches();
        }

        @Override
        public int hashCode() {
            return patternString.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof JavaPattern)) {
                return false;
            }

            return ((JavaPattern) obj).patternString.equals(this.patternString);
        }

        @Override
        public String toString() {
            return patternString;
        }

        @Override
        public ImmutableSet<String> getConstants() {
            return ImmutableSet.empty();
        }

        @Override
        public ImmutableSet<String> getPatterns() {
            return ImmutableSet.of(patternString);
        }

        @Override
        public Object toBasicObject() {
            return ImmutableList.of(patternString);
        }
    }

    static class CompoundPattern extends AbstractPattern {

        private final String asString;
        private final ImmutableList<String> source;

        static Pattern create(Collection<String> patterns) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();

            ImmutableSet.Builder<Pattern> patternSet = new ImmutableSet.Builder<>();
            ImmutableSet.Builder<String> constantSet = new ImmutableSet.Builder<>();

            int i = 0;

            for (String patternString : patterns) {
                try {
                    Pattern pattern = Pattern.create(patternString);

                    if (pattern == WILDCARD) {
                        return pattern;
                    } else if (pattern instanceof Constant) {
                        constantSet.with(patternString);
                    } else {
                        patternSet.with(pattern);
                    }

                    i++;
                } catch (ConfigValidationException e) {
                    validationErrors.add(String.valueOf(i), e);

                }
            }

            validationErrors.throwExceptionForPresentErrors();

            int totalCount = patternSet.size() + constantSet.size();

            if (totalCount == 0) {
                return BLANK;
            } else if (totalCount == 1) {
                if (constantSet.size() == 1) {
                    return new Constant(constantSet.any());
                } else if (patternSet.size() == 1) {
                    return patternSet.any();
                }
            }

            return new CompoundPattern(patternSet.build(), constantSet.build(), patterns.toString(), ImmutableList.of(patterns));
        }

        static Pattern join(Collection<Pattern> patterns) {

            ImmutableSet.Builder<Pattern> patternSet = new ImmutableSet.Builder<>();
            ImmutableSet.Builder<String> constantSet = new ImmutableSet.Builder<>();

            for (Pattern pattern : patterns) {

                if (pattern == WILDCARD) {
                    return pattern;
                } else if (pattern instanceof Constant) {
                    constantSet.with(((Constant) pattern).getValue());
                } else {
                    patternSet.with(pattern);
                }

            }

            int totalCount = patternSet.size() + constantSet.size();

            if (totalCount == 0) {
                return BLANK;
            } else if (totalCount == 1) {
                if (constantSet.size() == 1) {
                    return new Constant(constantSet.any());
                } else if (patternSet.size() == 1) {
                    return patternSet.any();
                }
            }

            return new CompoundPattern(patternSet.build(), constantSet.build(), patterns.toString(), ImmutableList.empty());
        }

        private final ImmutableSet<Pattern> patterns;
        private final ImmutableSet<String> constants;

        CompoundPattern(ImmutableSet<Pattern> patterns, ImmutableSet<String> constants, String asString, ImmutableList<String> source) {
            this.constants = constants;
            this.patterns = patterns;
            this.asString = asString;
            this.source = source;
        }

        @Override
        public boolean matches(String string) {
            if (constants.contains(string)) {
                return true;
            }

            for (Pattern pattern : this.patterns) {
                if (pattern.matches(string)) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public int hashCode() {
            return patterns.hashCode() + constants.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof CompoundPattern)) {
                return false;
            }

            return ((CompoundPattern) obj).patterns.equals(this.patterns) && ((CompoundPattern) obj).constants.equals(this.constants);
        }

        @Override
        public String toString() {
            return asString;
        }

        @Override
        public ImmutableSet<String> getConstants() {
            return constants;
        }

        @Override
        public ImmutableSet<String> getPatterns() {
            return patterns.mapFlat(p -> p.getPatterns());
        }

        ImmutableSet<Pattern> getPatternObjects() {
            return patterns;
        }

        @Override
        public Object toBasicObject() {
            return source;
        }
    }

    static class ExcludingPattern extends AbstractPattern {
        private final Pattern exclusions;
        private final Pattern base;

        ExcludingPattern(Pattern exclusions, Pattern base) {
            this.exclusions = exclusions;
            this.base = base;
        }

        @Override
        public boolean matches(String string) {
            return !exclusions.matches(string) && base.matches(string);
        }

        @Override
        public ImmutableSet<String> getConstants() {
            return ImmutableSet.empty();
        }

        @Override
        public ImmutableSet<String> getPatterns() {
            return ImmutableSet.empty();
        }

        @Override
        public Object toBasicObject() {
            return base.toBasicObject();
        }
    }

    static abstract class AbstractPattern implements Pattern {
        @Override
        public ImmutableSet<String> getMatching(ImmutableSet<String> strings) {
            return strings.matching((s) -> matches(s));
        }

        @Override
        public <E> ImmutableSet<E> getMatching(ImmutableSet<E> set, Function<E, String> stringMappingFunction) {
            return set.matching((e) -> matches(stringMappingFunction.apply(e)));
        }

        @Override
        public boolean matches(Iterable<String> strings) {
            for (String string : strings) {
                if (matches(string)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean test(String string) {
            return matches(string);
        }

        @Override
        public Iterable<String> iterateMatching(Iterable<String> strings) {
            return new Iterable<String>() {

                @Override
                public Iterator<String> iterator() {
                    Iterator<String> delegate = strings.iterator();

                    return new Iterator<String>() {

                        private String next;

                        @Override
                        public boolean hasNext() {
                            if (next == null) {
                                init();
                            }

                            return next != null;
                        }

                        @Override
                        public String next() {
                            String result = next;
                            next = null;
                            return result;
                        }

                        private void init() {
                            while (delegate.hasNext()) {
                                String candidate = delegate.next();

                                if (matches(candidate)) {
                                    next = candidate;
                                    break;
                                }
                            }
                        }
                    };
                }

            };
        }
        
        @Override
        public boolean isWildcard() {
            return false;
        }

        @Override
        public Pattern excluding(Pattern exludingPattern) {
            if (exludingPattern == BLANK) {
                return this;
            }

            return new ExcludingPattern(exludingPattern, this);
        }
    }

    static Pattern WILDCARD = new Pattern() {

        @Override
        public boolean matches(String string) {
            return true;
        }

        @Override
        public Iterable<String> iterateMatching(Iterable<String> strings) {
            return strings;
        }

        @Override
        public ImmutableSet<String> getMatching(ImmutableSet<String> strings) {
            return strings;
        }

        @Override
        public boolean matches(Iterable<String> string) {
            if (string.iterator().hasNext()) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return "*";
        }

        @Override
        public ImmutableSet<String> getConstants() {
            return ImmutableSet.empty();
        }

        @Override
        public ImmutableSet<String> getPatterns() {
            return ImmutableSet.of("*");
        }

        @Override
        public Object toBasicObject() {
            return ImmutableList.of("*");
        }

        @Override
        public <E> ImmutableSet<E> getMatching(ImmutableSet<E> set, Function<E, String> stringMappingFunction) {
            return set;
        }

        @Override
        public boolean test(String t) {
            return true;
        }

        @Override
        public Pattern excluding(Pattern exludingPattern) {
            return new ExcludingPattern(exludingPattern, this);
        }

        @Override
        public boolean isWildcard() {
            return true;
        }
    };

    static Pattern BLANK = new Pattern() {

        @Override
        public boolean matches(String string) {
            return false;
        }

        @Override
        public Iterable<String> iterateMatching(Iterable<String> strings) {
            return ImmutableSet.empty();
        }

        @Override
        public ImmutableSet<String> getMatching(ImmutableSet<String> strings) {
            return ImmutableSet.empty();
        }

        @Override
        public boolean matches(Iterable<String> string) {
            return false;
        }

        @Override
        public String toString() {
            return "-/-";
        }

        @Override
        public ImmutableSet<String> getConstants() {
            return ImmutableSet.empty();
        }

        @Override
        public ImmutableSet<String> getPatterns() {
            return ImmutableSet.empty();
        }

        @Override
        public Object toBasicObject() {
            return ImmutableList.empty();
        }

        @Override
        public <E> ImmutableSet<E> getMatching(ImmutableSet<E> set, Function<E, String> stringMappingFunction) {
            return ImmutableSet.empty();
        }

        @Override
        public boolean test(String t) {
            return false;
        }

        @Override
        public Pattern excluding(Pattern exludingPattern) {
            return this;
        }

        @Override
        public boolean isWildcard() {
            return false;
        }
    };

}
