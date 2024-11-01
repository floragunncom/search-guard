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

package com.floragunn.searchguard.enterprise.dlsfls.legacy;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.support.SgUtils;
import com.floragunn.searchguard.support.WildcardMatcher;

public class FlsFieldFilter implements Function<String, Predicate<String>> {
    private static final String KEYWORD = ".keyword";
    private final ThreadPool threadPool;
    private final AtomicReference<DlsFlsProcessedConfig> config;

    FlsFieldFilter(ThreadPool threadPool, AtomicReference<DlsFlsProcessedConfig> config) {
        this.threadPool = threadPool;
        this.config = config;
    }

    @Override
    public Predicate<String> apply(String index) {
        if (threadPool == null) {
            return field -> true;
        }

        if (!config.get().isEnabled()) {
            return field -> true;
        }
        
        final Map<String, Set<String>> allowedFlsFields = (Map<String, Set<String>>) HeaderHelper
                .deserializeSafeFromHeader(threadPool.getThreadContext(), ConfigConstants.SG_FLS_FIELDS_HEADER);

        final String eval = SgUtils.evalMap(allowedFlsFields, index);

        if (eval == null) {
            return field -> true;
        } else {

            final Set<String> includesExcludes = allowedFlsFields.get(eval);

            final Set<String> includesSet = new HashSet<>(includesExcludes.size());
            final Set<String> excludesSet = new HashSet<>(includesExcludes.size());

            for (final String incExc : includesExcludes) {
                final char firstChar = incExc.charAt(0);

                if (firstChar == '!' || firstChar == '~') {
                    excludesSet.add(incExc.substring(1));
                } else {
                    includesSet.add(incExc);
                }
            }

            if (!excludesSet.isEmpty()) {
                return field -> !WildcardMatcher.matchAny(excludesSet, handleKeyword(field));
            } else {
                return field -> WildcardMatcher.matchAny(includesSet, handleKeyword(field));
            }
        }
    };
    
    private static String handleKeyword(final String field) {
        if (field != null && field.endsWith(KEYWORD)) {
            return field.substring(0, field.length() - KEYWORD.length());
        }
        return field;
    }
}
