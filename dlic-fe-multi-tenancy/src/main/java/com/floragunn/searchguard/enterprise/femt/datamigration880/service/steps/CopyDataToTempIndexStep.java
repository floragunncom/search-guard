/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.authz.TenantManager;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.TenantIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.search.SearchHit;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.BACKUP_FROM_PREVIOUS_MIGRATION_NOT_AVAILABLE_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.BACKUP_INDICES_CONTAIN_MIGRATION_MARKER;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.DOCUMENT_ALREADY_MIGRATED_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.DOCUMENT_ALREADY_EXISTS_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.INCORRECT_INDEX_NAME_PREFIX_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.UNKNOWN_USER_PRIVATE_TENANT_NAME_ERROR;
import static java.util.Objects.requireNonNull;

class CopyDataToTempIndexStep implements MigrationStep {

    private static final Logger log = LogManager.getLogger(CopyDataToTempIndexStep.class);

    private final StepRepository repository;
    private final FeMultiTenancyConfigurationProvider configurationProvider;

    private final IndexSettingsManager indexSettingsManager;

    CopyDataToTempIndexStep(StepRepository repository, FeMultiTenancyConfigurationProvider configurationProvider,
        IndexSettingsManager indexSettingsManager) {
        this.repository = requireNonNull(repository, "Step repository is required");
        this.configurationProvider = requireNonNull(configurationProvider, "Multi tenancy configuration provider is required");
        this.indexSettingsManager = requireNonNull(indexSettingsManager, "Index settings manager is required");
    }

    @Override
    public StepResult execute(DataMigrationContext context) throws StepException {
        AtomicLong documentCounter = new AtomicLong(0);
        String indexNamePrefix = configurationProvider.getKibanaIndex();
        ImmutableList<TenantIndex> sourceIndices = chooseSourceIndices(context);
        for(TenantIndex tenantIndex : sourceIndices) {
            log.info("Start moving documents to temp index for tenant '{}'.", tenantIndex);
            repository.forEachDocumentInIndex(tenantIndex.indexName(), 100, searchHits -> {
                Map<String, String> map = new HashMap<>();
                for(SearchHit hit : searchHits) {
                    String scopedId = scopeId(hit, tenantIndex, indexNamePrefix);
                    log.debug("Scoped id '{}' assigned to document '{}' from index '{}'", scopedId, hit.getId(), hit.getIndex());
                    if(map.containsKey(scopedId)) {
                        String details = "Document with id '" + hit.getId() + "' already exists in index '" + tenantIndex.indexName() + "'.";
                        throw new StepException("Document already exists", DOCUMENT_ALREADY_EXISTS_ERROR, details);
                    }
                    String source = hit.getSourceAsString();
                    map.put(scopedId, source);
                }
                if(!map.isEmpty()) {
                    log.debug("Creating '{}' documents in temp index.", map.size());
                    repository.bulkCreate(context.getTempIndexName(), map);
                    documentCounter.addAndGet(map.size());
                }
            });
        }
        repository.refreshIndex(context.getTempIndexName());
        repository.flushIndex(context.getTempIndexName());
        long count = documentCounter.get();
        String message = "Stored '" + count + "' documents in temp index '" + context.getTempIndexName() + "'.";
        String details = sourceIndices.stream() //
            .map(TenantIndex::indexName) //
            .map(indexName -> "'" + indexName + "'") //
            .collect(Collectors.joining(", "));
        return new StepResult(OK, message, "Source indices: " + details);
    }

    private ImmutableList<TenantIndex> chooseSourceIndices(DataMigrationContext context) {
        if(indexSettingsManager.isMigrationMarkerPresent(context.getGlobalTenantIndexName())) {
            String backupIndexName = getNewestBackupIndexWithoutMigrationMarker(context);
            return ImmutableList.of(new TenantIndex(backupIndexName, Tenant.GLOBAL_TENANT_ID)) //
                .with(context.getTenantIndicesWithoutGlobalTenant());
        } else {
            return context.getTenantIndices();
        }
    }

    private String getNewestBackupIndexWithoutMigrationMarker(DataMigrationContext context) {
        ImmutableList<String> backupIndices = context.getBackupIndices();
        if(backupIndices.isEmpty()) {
            String message = "Global tenant index contains migration marker and backup index does not exist";
            throw new StepException(message, BACKUP_FROM_PREVIOUS_MIGRATION_NOT_AVAILABLE_ERROR, null);
        }
        for(String backupIndex : backupIndices){
            if(indexSettingsManager.isMigrationMarkerPresent(backupIndex)){
                continue;
            }
            return backupIndex;
        }
        String message = "Backup index without migration marker not found";
        String details = "Existing backup indices: " + backupIndices.stream() //
            .map(index -> "'" + index + "'") //
            .collect(Collectors.joining(", "));
        throw new StepException(message, BACKUP_INDICES_CONTAIN_MIGRATION_MARKER, details);
    }

    private static String scopeId(SearchHit hit, TenantIndex tenant, String indexNamePrefix) {
        String id = hit.getId();
        if(RequestResponseTenantData.isScopedId(id)) {
            String message = "Index '" + hit.getIndex() + "' contains already migrated document '" + hit.getId() + "'.";
            throw new StepException(message, DOCUMENT_ALREADY_MIGRATED_ERROR, null);
        }
        if(tenant.belongsToUserPrivateTenant()) {
            if(! tenant.indexName().startsWith(indexNamePrefix)) {
                String message = "Incorrect index name prefix for user private tenant";
                String details = "Invalid index name '" + tenant.indexName() + "'.";
                throw new StepException(message, INCORRECT_INDEX_NAME_PREFIX_ERROR, details);
            }
            return RequestResponseTenantData.scopedIdForPrivateTenantIndexName(id, tenant.indexName(), indexNamePrefix) //
                .orElseThrow(() -> new StepException("Cannot extract user private tenant name from index name '" + tenant.indexName() + "'",
                    UNKNOWN_USER_PRIVATE_TENANT_NAME_ERROR, null));
        }
        String internalTenantName = TenantManager.toInternalTenantName(tenant.tenantName());
        return RequestResponseTenantData.scopedId(id, internalTenantName);
    }

    @Override
    public String name() {
        return "copy data to temp index";
    }
}
