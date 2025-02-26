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

package com.floragunn.searchguard.enterprise.auditlog;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.floragunn.searchguard.enterprise.auditlog.access_log.write.ComplianceIndexActionFilter;
import com.floragunn.searchguard.enterprise.auditlog.access_log.write.ComplianceIndexTemplateActionFilter;
import com.floragunn.searchguard.support.ConfigConstants;
import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexingOperationListener;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.enterprise.auditlog.access_log.read.ReadLogDirectoryReaderWrapper;
import com.floragunn.searchguard.enterprise.auditlog.access_log.write.ComplianceIndexingOperationListenerImpl;
import com.floragunn.searchguard.enterprise.auditlog.impl.AuditLogImpl;

public class AuditLogModule implements SearchGuardModule {

    private AuditLogImpl auditLog;
    private ComplianceIndexingOperationListenerImpl indexingOperationListener;
    private ComplianceIndexTemplateActionFilter complianceIndexTemplateActionFilter;
    private ComplianceIndexActionFilter complianceIndexActionFilter;
    private AuditLogConfig auditLogConfig;
    private boolean externalConfigLogged = false;

    @Override
    public Collection<Object> createComponents(BaseDependencies baseDependencies) {
        //only when audit logging is enabled
        if (baseDependencies.getSettings().get(ConfigConstants.SEARCHGUARD_AUDIT_TYPE_DEFAULT) != null) {
            this.auditLogConfig = new AuditLogConfig(baseDependencies.getEnvironment(), baseDependencies.getConfigurationRepository());
            this.auditLog = new AuditLogImpl(baseDependencies.getSettings(), baseDependencies.getEnvironment().configDir(),
                    baseDependencies.getLocalClient(), baseDependencies.getThreadPool(), baseDependencies.getIndexNameExpressionResolver(),
                    baseDependencies.getClusterService(), baseDependencies.getConfigurationRepository());
            this.auditLog.setComplianceConfig(auditLogConfig);

            baseDependencies.getLicenseRepository().subscribeOnLicenseChange((searchGuardLicense) -> {
                AuditLogModule.this.auditLogConfig.onChange(searchGuardLicense);
                logExternalConfig(baseDependencies.getSettings(), baseDependencies.getEnvironment());
            });

            this.indexingOperationListener = new ComplianceIndexingOperationListenerImpl(this.auditLogConfig, auditLog,
                    baseDependencies.getGuiceDependencies());
            this.complianceIndexTemplateActionFilter = new ComplianceIndexTemplateActionFilter(this.auditLogConfig, this.auditLog, baseDependencies.getClusterService());
            this.complianceIndexActionFilter = new ComplianceIndexActionFilter(this.auditLogConfig, auditLog);
        }

        return ImmutableList.empty();
    }

    @Override
    public ImmutableList<Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>>> getDirectoryReaderWrappersForAllOperations() {
        return auditLogConfig != null? ImmutableList.of((indexService) -> new ReadLogDirectoryReaderWrapper(indexService, auditLog, auditLogConfig)) :
                ImmutableList.empty();
    }

    @Override
    public ImmutableList<IndexingOperationListener> getIndexOperationListeners() {
        return indexingOperationListener != null? ImmutableList.of(indexingOperationListener) : ImmutableList.empty();
    }

    @Override
    public ImmutableList<ActionFilter> getActionFilters() {
        return ImmutableList.of(
                Stream.of(complianceIndexActionFilter, complianceIndexTemplateActionFilter)
                        .filter(Objects::nonNull).collect(Collectors.toList())
        );
    }

    @Override
    public AuditLog getAuditLog() {
        return auditLog;
    }

    /**
     * Logs external configs like elasticsearch.yml, env vars etc.
     * 
     * Usually only done once per startup, after the license has become available.
     */
    private void logExternalConfig(Settings settings, Environment environment) {
        if (this.auditLogConfig.isEnabled() && !externalConfigLogged && auditLogConfig.isLogExternalConfig()) {
            auditLog.logExternalConfig(settings, environment);
            externalConfigLogged = true;
        }
    }
}
