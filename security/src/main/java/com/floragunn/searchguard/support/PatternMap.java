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
import java.util.Map;

import com.floragunn.searchsupport.util.ImmutableMap;
import com.floragunn.searchsupport.util.ImmutableSet;
import com.google.common.collect.HashMultimap;

public class PatternMap<V> {
    private final ImmutableMap<String, ImmutableSet<V>> constants;
    private final ImmutableMap<Pattern, ImmutableSet<V>> patterns;

    private PatternMap(ImmutableMap<String, ImmutableSet<V>> constants, ImmutableMap<Pattern, ImmutableSet<V>> patterns) {
        this.constants = constants;
        this.patterns = patterns;
    }

    public ImmutableSet<V> get(String value) {
        ImmutableSet<V> result = constants.get(value);

        if (result == null) {
            result = ImmutableSet.empty();
        }

        if (patterns.isEmpty()) {
            return result;
        }

        ImmutableSet.Builder<V> resultBuilder = null;

        for (Map.Entry<Pattern, ImmutableSet<V>> entry : patterns.entrySet()) {
            Pattern pattern = entry.getKey();

            if (pattern.matches(value)) {
                if (resultBuilder == null) {
                    resultBuilder = new ImmutableSet.Builder<V>(result);
                }

                resultBuilder.addAll(entry.getValue());
            }
        }

        if (resultBuilder != null) {
            return resultBuilder.build();
        } else {
            return result;
        }
    }
    
    public ImmutableSet<V> get(Collection<String> keys) {
        ImmutableSet.Builder<V> resultBuilder = new ImmutableSet.Builder<V>();
        
        for (String key : keys) {
            ImmutableSet<V> value = constants.get(key);

            if (value != null) {
                resultBuilder.addAll(value);
            }            
            
            for (Map.Entry<Pattern, ImmutableSet<V>> entry : patterns.entrySet()) {
                Pattern pattern = entry.getKey();

                if (pattern.matches(key)) {
                    resultBuilder.addAll(entry.getValue());
                }
            }

        }
        
        return resultBuilder.build();
    }
    
    public boolean isEmpty() {
        return constants.isEmpty() && patterns.isEmpty();
    }

    public static class Builder<V> {
        private HashMultimap<String, V> constants = HashMultimap.create();
        private HashMultimap<Pattern, V> patterns = HashMultimap.create();

        public void add(Pattern pattern, V value) {
            if (pattern instanceof Pattern.Constant) {
                constants.put(((Pattern.Constant) pattern).getValue(), value);
            } else if (pattern instanceof Pattern.CompoundPattern) {
                for (String constant : ((Pattern.CompoundPattern) pattern).getConstants()) {
                    constants.put(constant, value);
                }

                for (Pattern subPattern : ((Pattern.CompoundPattern) pattern).getPatternObjects()) {
                    this.add(subPattern, value);
                }
            } else {
                patterns.put(pattern, value);
            }
        }

        public PatternMap<V> build() {
            return new PatternMap<>(ImmutableMap.map(constants.asMap(), (k) -> k, (v) -> ImmutableSet.of(v)),
                    ImmutableMap.map(patterns.asMap(), (k) -> k, (v) -> ImmutableSet.of(v)));
        }
    }
}
