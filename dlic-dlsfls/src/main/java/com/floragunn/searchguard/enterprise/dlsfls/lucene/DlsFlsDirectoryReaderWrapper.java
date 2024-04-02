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

package com.floragunn.searchguard.enterprise.dlsfls.lucene;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.enterprise.dlsfls.DlsFlsBaseContext;
import com.floragunn.searchguard.enterprise.dlsfls.DlsFlsLicenseInfo;
import com.floragunn.searchguard.enterprise.dlsfls.DlsFlsProcessedConfig;
import com.floragunn.searchguard.enterprise.dlsfls.DlsRestriction;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedDocumentAuthorization;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldAuthorization;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldAuthorization.FlsRule;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldMasking;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldMasking.FieldMaskingRule;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.cstate.metrics.TimeAggregation;

public class DlsFlsDirectoryReaderWrapper implements CheckedFunction<DirectoryReader, DirectoryReader, IOException> {
    private static final Logger log = LogManager.getLogger(DlsFlsDirectoryReaderWrapper.class);

    private final IndexService indexService;
    private final AuditLog auditlog;
    private final Index index;
    private final ThreadContext threadContext;
    private final DlsFlsBaseContext dlsFlsBaseContext;
    private final AtomicReference<DlsFlsProcessedConfig> config;
    private final AtomicReference<DlsFlsLicenseInfo> licenseInfo;
    private final ComponentState componentState;
    private final TimeAggregation directoryReaderWrapperApplyAggregation;

    public DlsFlsDirectoryReaderWrapper(IndexService indexService, AuditLog auditlog, DlsFlsBaseContext dlsFlsBaseContext,
            AtomicReference<DlsFlsProcessedConfig> config, AtomicReference<DlsFlsLicenseInfo> licenseInfo,
            ComponentState directoryReaderWrapperComponentState, TimeAggregation directoryReaderWrapperApplyAggregation) {
        this.componentState = directoryReaderWrapperComponentState;
        this.directoryReaderWrapperApplyAggregation = directoryReaderWrapperApplyAggregation;
        this.indexService = indexService;
        this.index = indexService.index();
        this.auditlog = auditlog;
        this.threadContext = indexService.getThreadPool().getThreadContext();
        this.config = config;
        this.licenseInfo = licenseInfo;
        this.dlsFlsBaseContext = dlsFlsBaseContext;
    }

    @Override
    public final DirectoryReader apply(DirectoryReader reader) throws IOException {
        DlsFlsProcessedConfig config = this.config.get();

        if (!config.isEnabled()) {
            return reader;
        }

        PrivilegesEvaluationContext privilegesEvaluationContext = this.dlsFlsBaseContext.getPrivilegesEvaluationContext();

        if (privilegesEvaluationContext == null) {
            log.trace("DlsFlsDirectoryReaderWrapper.apply(): No PrivilegesEvaluationContext");           
            return reader;
        }

        try (Meter meter = Meter.detail(config.getMetricsLevel(), directoryReaderWrapperApplyAggregation)) {

            DlsFlsLicenseInfo licenseInfo = this.licenseInfo.get();

            ShardId shardId = ShardUtils.extractShardId(reader);

            RoleBasedDocumentAuthorization documentAuthorization = config.getDocumentAuthorization();
            RoleBasedFieldAuthorization fieldAuthorization = config.getFieldAuthorization();
            RoleBasedFieldMasking fieldMasking = config.getFieldMasking();

            if (privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext() != null
                    && privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext().getRolesConfig() != null) {
                SgDynamicConfiguration<Role> roles = privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext().getRolesConfig();
                Set<String> indices = ImmutableSet.of(index.getName());
                documentAuthorization = new RoleBasedDocumentAuthorization(roles, indices, MetricsLevel.NONE);
                fieldAuthorization = new RoleBasedFieldAuthorization(roles, indices, MetricsLevel.NONE);
                fieldMasking = new RoleBasedFieldMasking(roles, fieldMasking.getFieldMaskingConfig(), indices, MetricsLevel.NONE);
            }

            DlsRestriction dlsRestriction;

            if (!this.dlsFlsBaseContext.isDlsDoneOnFilterLevel()) {
                dlsRestriction = documentAuthorization.getDlsRestriction(privilegesEvaluationContext, index.getName(), meter);
            } else {
                dlsRestriction = DlsRestriction.NONE;
            }

            FlsRule flsRule = fieldAuthorization.getFlsRule(privilegesEvaluationContext, index.getName(), meter);
            FieldMaskingRule fieldMaskingRule = fieldMasking.getFieldMaskingRule(privilegesEvaluationContext, index.getName(), meter);
            Query dlsQuery;

            if (dlsRestriction.isUnrestricted()) {
                dlsQuery = null;
            } else {
                SearchExecutionContext queryShardContext = this.indexService.newSearchExecutionContext(shardId.getId(), 0, null, nowSupplier(config),
                        null, Collections.emptyMap());

                // no need for scoring here, so its possible to wrap this in a ConstantScoreQuery
                dlsQuery = new ConstantScoreQuery(dlsRestriction.toQuery(queryShardContext, null));
            }

            if (log.isDebugEnabled()) {
                log.debug("Applying DLS/FLS:\nIndex: {}\ndlsRestriction: {}\ndlsQuery: {}\nfls: {}\nfieldMasking: {}", indexService.index().getName(),
                        dlsRestriction, dlsQuery, flsRule, fieldMaskingRule);
            }

            DlsFlsActionContext dlsFlsContext = new DlsFlsActionContext(dlsQuery, flsRule, fieldMaskingRule, indexService, threadContext, licenseInfo, auditlog,
                    shardId);

            return new DlsFlsDirectoryReader(reader, dlsFlsContext);
        } catch (PrivilegesEvaluationException e) {
            log.error("Error while evaluating privileges in " + this, e);
            componentState.addLastException("wrap_reader", e);
            throw new RuntimeException(e);
        }
    }

    private LongSupplier nowSupplier(DlsFlsProcessedConfig config) {
        if (config.getDlsFlsConfig().isNowAllowedInQueries()) {
            return () -> System.currentTimeMillis();
        } else {
            return () -> {
                throw new IllegalArgumentException("'now' is not allowed in DLS queries");
            };
        }
    }
}
