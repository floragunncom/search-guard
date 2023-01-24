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
package com.floragunn.searchguard.enterprise.dlsfls.legacy;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.support.SgUtils;
import com.floragunn.searchguard.support.WildcardMatcher;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import org.elasticsearch.threadpool.ThreadPool;

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
