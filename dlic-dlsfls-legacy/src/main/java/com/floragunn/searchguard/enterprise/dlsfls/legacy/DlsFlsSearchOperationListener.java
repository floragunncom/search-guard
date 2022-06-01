package com.floragunn.searchguard.enterprise.dlsfls.legacy;
/*
 * Copyright 2016-2022 by floragunn GmbH - All rights reserved
 * 
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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.support.SgUtils;

public class DlsFlsSearchOperationListener implements SearchOperationListener {

    private final ThreadPool threadPool;
    private final DlsQueryParser dlsQueryParser;
    private final AtomicReference<DlsFlsProcessedConfig> config;

    DlsFlsSearchOperationListener(ThreadPool threadPool, DlsQueryParser dlsQueryParser, AtomicReference<DlsFlsProcessedConfig> config) {
        this.threadPool = threadPool;
        this.dlsQueryParser = dlsQueryParser;
        this.config = config;
    }

    @Override
    public void onPreQueryPhase(SearchContext context) {
        try {
            if (!config.get().isEnabled()) {
                return;
            }

            @SuppressWarnings("unchecked")
            final Map<String, Set<String>> queries = (Map<String, Set<String>>) HeaderHelper.deserializeSafeFromHeader(threadPool.getThreadContext(),
                    ConfigConstants.SG_DLS_QUERY_HEADER);

            final String dlsEval = SgUtils.evalMap(queries, context.indexShard().indexSettings().getIndex().getName());

            if (dlsEval != null) {

                if (context.suggest() != null) {
                    return;
                }

                assert context.parsedQuery() != null;

                final Set<String> unparsedDlsQueries = queries.get(dlsEval);

                if (unparsedDlsQueries != null && !unparsedDlsQueries.isEmpty()) {

                    BooleanQuery.Builder queryBuilder = dlsQueryParser.parse(unparsedDlsQueries, context.getQueryShardContext(),
                            (q) -> new ConstantScoreQuery(q));

                    queryBuilder.add(context.parsedQuery().query(), Occur.MUST);

                    ParsedQuery dlsQuery = new ParsedQuery(queryBuilder.build());

                    if (dlsQuery != null) {
                        context.parsedQuery(dlsQuery);
                        context.preProcess(true);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error evaluating dls for a search query: " + e, e);
        }
    }

}
