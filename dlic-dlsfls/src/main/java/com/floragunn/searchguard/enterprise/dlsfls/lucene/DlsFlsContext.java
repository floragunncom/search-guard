/*
 * Copyright 2022 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.enterprise.dlsfls.lucene;

import org.apache.lucene.search.Query;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.ShardId;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.enterprise.dlsfls.DlsFlsLicenseInfo;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldAuthorization.FlsRule;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldMasking.FieldMaskingRule;

public class DlsFlsContext {
    private final FlsRule flsRule;
    private final Query dlsQuery;
    private final IndexService indexService;
    private final ThreadContext threadContext;
    private final DlsFlsLicenseInfo licenseInfo;
    private final AuditLog auditlog;
    private final FieldMaskingRule fieldMaskingRule;
    private final ShardId shardId;

    public DlsFlsContext(Query dlsQuery, FlsRule flsRule, FieldMaskingRule fieldMaskingRule, IndexService indexService, ThreadContext threadContext,
            DlsFlsLicenseInfo licenseInfo, AuditLog auditlog, ShardId shardId) {
        this.dlsQuery = dlsQuery;
        this.flsRule = flsRule;
        this.indexService = indexService;
        this.threadContext = threadContext;
        this.licenseInfo = licenseInfo;
        this.auditlog = auditlog;
        this.fieldMaskingRule = fieldMaskingRule;
        this.shardId = shardId;
    }

    public Index index() {
        return indexService.index();
    }

    public boolean hasFlsRestriction() {
        return !flsRule.isAllowAll();
    }

    public boolean hasFieldMasking() {
        return licenseInfo.isLicenseForFieldMaskingAvailable() && !fieldMaskingRule.isAllowAll();
    }

    public FlsRule getFlsRule() {
        return flsRule;
    }

    public Query getDlsQuery() {
        return dlsQuery;
    }

    public IndexService getIndexService() {
        return indexService;
    }

    public ThreadContext getThreadContext() {
        return threadContext;
    }

    public AuditLog getAuditlog() {
        return auditlog;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public FieldMaskingRule getFieldMaskingRule() {
        return fieldMaskingRule;
    }

    @Override
    public String toString() {
        return indexService.index() + " [" + dlsQuery + "; " + flsRule + "; " + fieldMaskingRule + "]";
    }
}
