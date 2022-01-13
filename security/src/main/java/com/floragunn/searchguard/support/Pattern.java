/*
 * Copyright 2021 floragunn GmbH
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

import com.floragunn.searchsupport.util.ImmutableSet;

public interface Pattern {

    public static Pattern create(String pattern) {
        if ("*".equals(pattern)) {
            return WILDCARD;
        } else if (pattern.startsWith("/") && pattern.endsWith("/")) {
            return new JavaPattern(pattern.substring(1, pattern.length() - 1));
        } else if (pattern.contains("?") || pattern.contains("*")) {
            return new SimplePattern(pattern);
        } else {
            return new Constant(pattern);
        }
    }

    public static Pattern create(Collection<String> patterns) {
        if (patterns.size() == 0) {
            return BLANK;
        } else if (patterns.size() == 1) {
            return create(patterns.iterator().next());
        } else {
            return CompoundPattern.create(patterns);
        }
    }

    boolean matches(String string);

    boolean matches(Iterable<String> string);

    ImmutableSet<String> getMatching(ImmutableSet<String> strings);

    Iterable<String> iterateMatching(Iterable<String> strings);

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
    }

    static class JavaPattern extends AbstractPattern {
        private final java.util.regex.Pattern javaPattern;
        private final String patternString;

        JavaPattern(String pattern) {
            this.javaPattern = java.util.regex.Pattern.compile(pattern);
            this.patternString = pattern;
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
    }

    static class CompoundPattern extends AbstractPattern {

        static Pattern create(Collection<String> patterns) {
            ImmutableSet.Builder<Pattern> patternSet = new ImmutableSet.Builder<>();
            ImmutableSet.Builder<String> constantSet = new ImmutableSet.Builder<>();

            for (String patternString : patterns) {
                Pattern pattern = Pattern.create(patternString);

                if (pattern == WILDCARD) {
                    return pattern;
                } else if (pattern instanceof Constant) {
                    constantSet.with(patternString);
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

            return new CompoundPattern(patternSet.build(), constantSet.build());
        }

        private final ImmutableSet<Pattern> patterns;
        private final ImmutableSet<String> constants;

        CompoundPattern(ImmutableSet<Pattern> patterns, ImmutableSet<String> constants) {
            this.constants = constants;
            this.patterns = patterns;
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
            return patterns.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof CompoundPattern)) {
                return false;
            }

            return ((CompoundPattern) obj).patterns.equals(this.patterns);
        }

    }

    static abstract class AbstractPattern implements Pattern {
        @Override
        public ImmutableSet<String> getMatching(ImmutableSet<String> strings) {
            return strings.matching((s) -> matches(s));
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

    };

}
