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
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.AuthInfoService;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.enterprise.dlsfls.DlsFlsLicenseInfo;
import com.floragunn.searchguard.enterprise.dlsfls.DlsFlsProcessedConfig;
import com.floragunn.searchguard.enterprise.dlsfls.DlsRestriction;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedDocumentAuthorization;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldAuthorization;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldAuthorization.FlsRule;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldMasking;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldMasking.FieldMaskingRule;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.user.User;

public class DlsFlsIndexSearcherWrapper implements CheckedFunction<DirectoryReader, DirectoryReader, IOException> {
    private static final Logger log = LogManager.getLogger(DlsFlsIndexSearcherWrapper.class);

    private final IndexService indexService;
    private final AuditLog auditlog;
    private final Index index;
    private final ThreadContext threadContext;
    private final AuthInfoService authInfoService;
    private final AuthorizationService authorizationService;
    private final AtomicReference<DlsFlsProcessedConfig> config;
    private final AtomicReference<DlsFlsLicenseInfo> licenseInfo;

    public DlsFlsIndexSearcherWrapper(IndexService indexService, AuditLog auditlog, AuthInfoService authInfoService,
            AuthorizationService authorizationService, AtomicReference<DlsFlsProcessedConfig> config,
            AtomicReference<DlsFlsLicenseInfo> licenseInfo) {
        this.indexService = indexService;
        this.index = indexService.index();
        this.auditlog = auditlog;
        this.threadContext = indexService.getThreadPool().getThreadContext();
        this.config = config;
        this.licenseInfo = licenseInfo;
        this.authInfoService = authInfoService;
        this.authorizationService = authorizationService;
    }

    @Override
    public final DirectoryReader apply(DirectoryReader reader) throws IOException {
        try {
            DlsFlsProcessedConfig config = this.config.get();

            if (!config.isEnabled()) {
                return reader;
            }

            PrivilegesEvaluationContext privilegesEvaluationContext = getPrivilegesEvaluationContext();

            if (privilegesEvaluationContext == null) {
                return reader;
            }
            
            DlsFlsLicenseInfo licenseInfo = this.licenseInfo.get();

            ShardId shardId = ShardUtils.extractShardId(reader);

            RoleBasedDocumentAuthorization documentAuthorization = config.getDocumentAuthorization();
            RoleBasedFieldAuthorization fieldAuthorization = config.getFieldAuthorization();
            RoleBasedFieldMasking fieldMasking = config.getFieldMasking();
            
            if (privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext() != null
                    && privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext().getRolesConfig() != null) {
                SgDynamicConfiguration<Role> roles = privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext().getRolesConfig();
                Set<String> indices = ImmutableSet.of(index.getName());
                documentAuthorization = new RoleBasedDocumentAuthorization(roles, indices);
                fieldAuthorization = new RoleBasedFieldAuthorization(roles, indices);
                fieldMasking = new RoleBasedFieldMasking(roles, fieldMasking.getFieldMaskingConfig(), indices);
            }

            DlsRestriction dlsRestriction = documentAuthorization.getDlsRestriction(privilegesEvaluationContext, index.getName());
            FlsRule flsRule = fieldAuthorization.getFlsRule(privilegesEvaluationContext, index.getName());
            FieldMaskingRule fieldMaskingRule = fieldMasking.getFieldMaskingRule(privilegesEvaluationContext, index.getName());
            Query dlsQuery;

            if (dlsRestriction.isUnrestricted()) {
                dlsQuery = null;
            } else {
                QueryShardContext queryShardContext = this.indexService.newQueryShardContext(shardId.getId(), null, nowSupplier(config), null);

                // no need for scoring here, so its possible to wrap this in a ConstantScoreQuery
                dlsQuery = new ConstantScoreQuery(dlsRestriction.toQueryBuilder(queryShardContext, null).build());
            }

            if (log.isDebugEnabled()) {
                log.debug("Applying DLS/FLS:\nIndex: " + indexService.index().getName() + "\ndlsQuery: " + dlsQuery + "\nfls: " + flsRule
                        + "\nfieldMasking: " + fieldMaskingRule);
            }

            DlsFlsContext dlsFlsContext = new DlsFlsContext(dlsQuery, flsRule, fieldMaskingRule, indexService, threadContext, licenseInfo, auditlog,
                    shardId);

            return new DlsFlsDirectoryReader(reader, dlsFlsContext);
        } catch (PrivilegesEvaluationException e) {
            log.error("Error while evaluating privileges in " + this, e);
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

    private PrivilegesEvaluationContext getPrivilegesEvaluationContext() {
        User user = authInfoService.peekCurrentUser();
        
        if (user == null) {
            return null;
        }
        
        SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext = authInfoService.getSpecialPrivilegesEvaluationContext();
        ImmutableSet<String> mappedRoles = this.authorizationService.getMappedRoles(user, specialPrivilegesEvaluationContext);

        return new PrivilegesEvaluationContext(user, mappedRoles, null, null, false, null, specialPrivilegesEvaluationContext, null);
    }

}
