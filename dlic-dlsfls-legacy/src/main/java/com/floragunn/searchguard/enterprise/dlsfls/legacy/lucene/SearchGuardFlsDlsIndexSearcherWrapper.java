/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.dlsfls.legacy.lucene;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.enterprise.dlsfls.legacy.DlsFlsBaseContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.enterprise.dlsfls.legacy.DlsFlsComplianceConfig;
import com.floragunn.searchguard.enterprise.dlsfls.legacy.DlsFlsProcessedConfig;
import com.floragunn.searchguard.enterprise.dlsfls.legacy.DlsQueryParser;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.support.SgUtils;
import com.google.common.collect.Sets;

public class SearchGuardFlsDlsIndexSearcherWrapper implements CheckedFunction<DirectoryReader, DirectoryReader, IOException> {

    private static final Set<String> metaFields = Sets.union(Sets.newHashSet("_primary_term"),
            Sets.newHashSet(IndicesModule.getBuiltInMetadataFields()));
    private final ClusterService clusterService;
    private final IndexService indexService;
    private final DlsFlsComplianceConfig complianceConfig;
    private final AuditLog auditlog;
    private final LongSupplier nowInMillis;
    private final DlsQueryParser dlsQueryParser;
    private final ThreadContext threadContext;
    private final Index index;
    private final AtomicReference<DlsFlsProcessedConfig> config;
    private final DlsFlsBaseContext dlsFlsBaseContext;

    public SearchGuardFlsDlsIndexSearcherWrapper(final IndexService indexService, final Settings settings, final ClusterService clusterService,
                                                 final AuditLog auditlog, final DlsFlsComplianceConfig complianceConfig, AtomicReference<DlsFlsProcessedConfig> config, NamedXContentRegistry xContentRegistry,
                                                 DlsFlsBaseContext dlsFlsBaseContext) {
        this.clusterService = clusterService;
        this.indexService = indexService;
        this.complianceConfig = complianceConfig;
        this.auditlog = auditlog;
        this.dlsQueryParser = new DlsQueryParser(xContentRegistry);
        this.threadContext = indexService.getThreadPool().getThreadContext();
        this.index = indexService.index();
        this.config = config;
        final boolean allowNowinDlsQueries = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_UNSUPPORTED_ALLOW_NOW_IN_DLS, false);
        if (allowNowinDlsQueries) {
            nowInMillis = () -> System.currentTimeMillis();
        } else {
            nowInMillis = () -> {
                throw new IllegalArgumentException("'now' is not allowed in DLS queries");
            };
        }
        this.dlsFlsBaseContext = dlsFlsBaseContext;
    }

    @Override
    public final DirectoryReader apply(DirectoryReader reader) throws IOException {       
        if (!config.get().isEnabled()) {
            return reader;
        }

        PrivilegesEvaluationContext privilegesEvaluationContext = dlsFlsBaseContext.getPrivilegesEvaluationContext();

        if (privilegesEvaluationContext == null) {
            return reader;
        }

        final ShardId shardId = ShardUtils.extractShardId(reader);

        Set<String> flsFields = null;
        Set<String> maskedFields = null;
        Query dlsQuery = null;

        final Map<String, Set<String>> allowedFlsFields = (Map<String, Set<String>>) HeaderHelper.deserializeSafeFromHeader(threadContext,
                ConfigConstants.SG_FLS_FIELDS_HEADER);
        final Map<String, Set<String>> queries = (Map<String, Set<String>>) HeaderHelper.deserializeSafeFromHeader(threadContext,
                ConfigConstants.SG_DLS_QUERY_HEADER);
        final Map<String, Set<String>> maskedFieldsMap = (Map<String, Set<String>>) HeaderHelper.deserializeSafeFromHeader(threadContext,
                ConfigConstants.SG_MASKED_FIELD_HEADER);

        final String flsEval = SgUtils.evalMap(allowedFlsFields, index.getName());
        final String dlsEval = SgUtils.evalMap(queries, index.getName());
        final String maskedEval = SgUtils.evalMap(maskedFieldsMap, index.getName());

        if (flsEval != null) {
            flsFields = new HashSet<>(metaFields);
            flsFields.addAll(allowedFlsFields.get(flsEval));
        }

        if (dlsEval != null) {
            Set<String> unparsedDlsQueries = queries.get(dlsEval);

            if (unparsedDlsQueries != null && !unparsedDlsQueries.isEmpty()) {
                SearchExecutionContext queryShardContext = this.indexService.newSearchExecutionContext(shardId.getId(), 0, null, nowInMillis, null,
                        Collections.emptyMap());
                // no need for scoring here, so its possible to wrap this in a
                // ConstantScoreQuery
                dlsQuery = new ConstantScoreQuery(dlsQueryParser.parse(unparsedDlsQueries, queryShardContext).build());
            }
        }

        if (maskedEval != null) {
            maskedFields = new HashSet<>();
            maskedFields.addAll(maskedFieldsMap.get(maskedEval));
        }

        return new DlsFlsFilterLeafReader.DlsFlsDirectoryReader(reader, flsFields, dlsQuery, indexService, threadContext, clusterService,
                complianceConfig, auditlog, maskedFields, shardId);
    }
}
