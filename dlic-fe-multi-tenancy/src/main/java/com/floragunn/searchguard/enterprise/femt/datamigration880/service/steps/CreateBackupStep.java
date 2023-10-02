package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

import java.util.Objects;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.BACKUP_UNEXPECTED_OPERATION_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.MISSING_DOCUMENTS_IN_BACKUP_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;

class CreateBackupStep implements MigrationStep {

    private final StepRepository repository;
    private final IndexSettingsDuplicator duplicator;

    public CreateBackupStep(StepRepository repository, IndexSettingsDuplicator duplicator) {
        this.repository = Objects.requireNonNull(repository, "Step repository is required");
        this.duplicator = Objects.requireNonNull(duplicator, "Index setting duplicator is required");
    }

    @Override
    public StepResult execute(DataMigrationContext context) throws StepException {
        String backupSource = context.getGlobalTenantIndexName();
        context.setBackupCreated(false);
        if(duplicator.isDuplicate(backupSource)) {
            return new StepResult(OK, "Backup creation omitted", "Index '" + backupSource + "' contains already migrated data");
        }
        String backupDestination = context.getBackupIndexName();
        duplicator.createIndexWithDuplicatedSettings(backupSource, backupDestination, false);
        BulkByScrollResponse response = repository.reindexData(backupSource, backupDestination);
        StringBuilder details = new StringBuilder("Backup of ").append(response.getTotal()).append(" documents created, ") //
            .append("it took ").append(response.getTook()).append(", ") //
            .append("created documents ").append(response.getCreated()).append(", ") //
            .append("updated documents ").append(response.getUpdated()).append(", ") //
            .append("deleted documents ").append(response.getDeleted()).append(", ") //
            .append("version conflicts ").append(response.getVersionConflicts()).append(", ") //
            .append(" used ").append(response.getBatches()).append(" batches, ") //
            .append("batch retries ").append(response.getBulkRetries()).append(", ") //
            .append("search retries ").append(response.getSearchRetries()).append(".");
        if(!isZero(response.getUpdated(), response.getDeleted(), response.getVersionConflicts())) {
            String message = "Documents should be not updated or deleted, version conflicts are not allowed";
            return new StepResult(BACKUP_UNEXPECTED_OPERATION_ERROR, message, details.toString());
        }
        long documentInSource = repository.countDocuments(backupSource);
        long documentsInDestination = repository.countDocuments(backupDestination);
        if(documentInSource != documentsInDestination) {
            String message = "Backup does not contains all required documents.";
            String description = "Expected number of documents " + documentInSource + " in index '" + backupSource
                + "', actual number of documents " + documentsInDestination + " in index '" + backupDestination + "'.";
            return new StepResult(MISSING_DOCUMENTS_IN_BACKUP_ERROR, message, description);
        }
        repository.writeBlockIndices(ImmutableList.of(backupDestination));
        context.setBackupCreated(true);
        return new StepResult(OK, "Backup created", details.toString());
    }

    private boolean isZero(long...numbers) {
        for(long current : numbers) {
            if(current != 0L) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String name() {
        return "create backup";
    }
}
