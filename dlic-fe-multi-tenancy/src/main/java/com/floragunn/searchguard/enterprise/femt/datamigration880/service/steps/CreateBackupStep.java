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
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import com.google.common.base.Throwables;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask.StatusOrException;

import java.util.List;
import java.util.Objects;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.BACKUP_UNEXPECTED_OPERATION_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.MISSING_DOCUMENTS_IN_BACKUP_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.SLICE_PARTIAL_ERROR;

class CreateBackupStep implements MigrationStep {

    private final StepRepository repository;
    private final IndexSettingsManager indexSettingsManager;

    public CreateBackupStep(StepRepository repository, IndexSettingsManager indexSettingsManager) {
        this.repository = Objects.requireNonNull(repository, "Step repository is required");
        this.indexSettingsManager = Objects.requireNonNull(indexSettingsManager, "Index setting duplicator is required");
    }

    @Override
    public StepResult execute(DataMigrationContext context) throws StepException {
        String backupSource = context.getGlobalTenantIndexName();
        context.setBackupCreated(false);
        if(indexSettingsManager.isMigrationMarkerPresent(backupSource)) {
            return new StepResult(OK, "Backup creation omitted", "Index '" + backupSource + "' contains already migrated data");
        }
        String backupDestination = context.getBackupIndexName();
        indexSettingsManager.createIndexWithClonedSettings(backupSource, backupDestination, false);
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
        List<StatusOrException> sliceStatuses = response.getStatus().getSliceStatuses();
        boolean errorDetected = false;
        for(int i = 0; i < sliceStatuses.size(); ++i) {
            StatusOrException currentStatus = sliceStatuses.get(i);
            Exception exception = currentStatus.getException();
            if(exception != null) {
                errorDetected = true;
                String stackTraceAsString = Throwables.getStackTraceAsString(exception);
                details.append(" Slice no. ").append(i).append(" error detected ").append(stackTraceAsString).append(".");
            }
        }
        if(errorDetected) {
            return new StepResult(SLICE_PARTIAL_ERROR, "Cannot copy data to backup index", details.toString());
        }
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

    @Override
    public String name() {
        return "create backup";
    }
}
