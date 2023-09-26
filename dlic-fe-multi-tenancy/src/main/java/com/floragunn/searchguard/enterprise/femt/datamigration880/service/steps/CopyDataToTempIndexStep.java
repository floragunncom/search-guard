package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.searchguard.authz.TenantManager;
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

    CopyDataToTempIndexStep(StepRepository repository, FeMultiTenancyConfigurationProvider configurationProvider) {
        this.repository = requireNonNull(repository, "Step repository is required");
        this.configurationProvider = requireNonNull(configurationProvider, "Multi tenancy configuration provider is required");
    }

    @Override
    public StepResult execute(DataMigrationContext context) throws StepException {
        AtomicLong documentCounter = new AtomicLong(0);
        String indexNamePrefix = configurationProvider.getKibanaIndex();
        //TODO this steps must recognize if backup index or global index should be used. This should be implemented when step related
        // to backup index creation is ready
        for(TenantIndex tenantIndex : context.getTenantIndices()) {
            log.info("Start moving documents to temp index for tenant '{}'.", tenantIndex);
            repository.forEachDocumentInIndex(tenantIndex.indexName(), 100, searchHits -> {
                Map<String, Map<String, Object>> map = new HashMap<>();
                for(SearchHit hit : searchHits) {
                    String scopedId = scopeId(hit, tenantIndex, indexNamePrefix);
                    log.debug("Scoped id '{}' assigned to document '{}' from index '{}'", scopedId, hit.getId(), hit.getIndex());
                    if(map.containsKey(scopedId)) {
                        String details = "Document with id '" + hit.getId() + "' already exists in index '" + tenantIndex.indexName() + "'.";
                        throw new StepException("Document already exists", DOCUMENT_ALREADY_EXISTS_ERROR, details);
                    }
                    Map<String, Object> source = hit.getSourceAsMap();
                    map.put(scopedId, source);
                }
                if(!map.isEmpty()) {
                    log.debug("Creating '{}' documents in temp index.", map.size());
                    repository.bulkCreate(context.getTempIndexName(), map);
                    documentCounter.addAndGet(map.size());
                }
            });
        }
        repository.flushIndex(context.getTempIndexName());
        long count = documentCounter.get();
        String details = "Stored '" + count + "' documents in temp index '" + context.getTempIndexName() + "'.";
        return new StepResult(OK, "Documents moved to temp index", details);
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
