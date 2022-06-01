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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Weight;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.support.SgUtils;

public class FlsQueryCacheWeightProvider implements SearchGuardModule.QueryCacheWeightProvider {

    private final ThreadPool threadPool;
    private final AtomicReference<DlsFlsProcessedConfig> config;

    FlsQueryCacheWeightProvider(ThreadPool threadPool, AtomicReference<DlsFlsProcessedConfig> config) {
        this.threadPool = threadPool;
        this.config = config;
    }

    @Override
    public Weight apply(Index index, Weight weight, QueryCachingPolicy policy) {
        if (!config.get().isEnabled()) {
            return null;
        }

        final Map<String, Set<String>> allowedFlsFields = (Map<String, Set<String>>) HeaderHelper
                .deserializeSafeFromHeader(threadPool.getThreadContext(), ConfigConstants.SG_FLS_FIELDS_HEADER);

        if (SgUtils.evalMap(allowedFlsFields, index.getName()) != null) {
            return weight;
        } else {

            final Map<String, Set<String>> maskedFieldsMap = (Map<String, Set<String>>) HeaderHelper
                    .deserializeSafeFromHeader(threadPool.getThreadContext(), ConfigConstants.SG_MASKED_FIELD_HEADER);

            if (SgUtils.evalMap(maskedFieldsMap, index.getName()) != null) {
                return weight;
            } else {
                return null;
            }
        }
    }

}
