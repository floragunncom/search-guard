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

import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.support.SgUtils;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Weight;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;

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
