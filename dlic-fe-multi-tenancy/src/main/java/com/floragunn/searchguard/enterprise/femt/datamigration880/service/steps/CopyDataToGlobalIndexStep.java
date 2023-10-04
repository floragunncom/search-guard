package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

import java.util.Objects;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.MISSING_DOCUMENTS_IN_GLOBAL_TENANT_INDEX_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.REINDEX_DATA_INTO_GLOBAL_TENANT_ERROR;

public class CopyDataToGlobalIndexStep implements MigrationStep {

    private final StepRepository stepRepository;

    public CopyDataToGlobalIndexStep(StepRepository stepRepository) {
        this.stepRepository = Objects.requireNonNull(stepRepository, "Step repository is required");
    }

    @Override
    public StepResult execute(DataMigrationContext context) throws StepException {
        String source = context.getTempIndexName();
        long numberOfDocumentsInSourceIndex = stepRepository.countDocuments(source);
        String destination = context.getGlobalTenantIndexName();
        BulkByScrollResponse response = stepRepository.reindexData(source, destination);
        StringBuilder details = new StringBuilder("Reindexing ").append(response.getTotal()).append(" documents, ") //
            .append("took ").append(response.getTook()).append(", ") //
            .append("created documents ").append(response.getCreated()).append(", ") //
            .append("updated documents ").append(response.getUpdated()).append(", ") //
            .append("deleted documents ").append(response.getDeleted()).append(", ") //
            .append("version conflicts ").append(response.getVersionConflicts()).append(", ") //
            .append(" used ").append(response.getBatches()).append(" batches, ") //
            .append("batch retries ").append(response.getBulkRetries()).append(", ") //
            .append("search retries ").append(response.getSearchRetries()).append(".");
        String message = "Documents copied from '" + source + "' to '" + destination + "' index";
        if(!isZero(response.getUpdated(), response.getDeleted(), response.getVersionConflicts())) {
            String errorMessage = "Documents should be not updated or deleted, version conflicts are not allowed";
            return new StepResult(REINDEX_DATA_INTO_GLOBAL_TENANT_ERROR, errorMessage, details.toString());
        }
        long numberOfDocumentsInDestinationIndex = stepRepository.countDocuments(destination);
        if(numberOfDocumentsInDestinationIndex != numberOfDocumentsInSourceIndex) {
            details.append(" Number of documents in source index '").append(source).append("' is equal to ") //
                .append(numberOfDocumentsInSourceIndex) //
                .append(" whereas destination index '").append(destination).append("' contains ").append(numberOfDocumentsInDestinationIndex) //
                .append(" documents.");
            String errorMessage = "Insufficient document count in global tenant index";
            return new StepResult(MISSING_DOCUMENTS_IN_GLOBAL_TENANT_INDEX_ERROR, errorMessage, details.toString());
        }
        return new StepResult(OK, message, details.toString());
    }

    @Override
    public String name() {
        return "copy data from temp to global index";
    }
}
