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
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

import java.util.Objects;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.NOT_EMPTY_INDEX;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;

class DeleteGlobalIndexContentStep implements MigrationStep {

    private final StepRepository stepRepository;

    public DeleteGlobalIndexContentStep(StepRepository stepRepository) {
        this.stepRepository = Objects.requireNonNull(stepRepository, "Step repository is required");
    }

    @Override
    public StepResult execute(DataMigrationContext context) throws StepException {
        String globalTenantIndexName = context.getGlobalTenantIndexName();
        stepRepository.releaseWriteLock(ImmutableList.of(globalTenantIndexName));
        BulkByScrollResponse response = stepRepository.deleteAllDocuments(globalTenantIndexName);
        StringBuilder details = new StringBuilder("Deleted total ").append(response.getTotal()).append(" documents, ") //
            .append("it took ").append(response.getTook()).append(", ") //
            .append("created documents ").append(response.getCreated()).append(", ") //
            .append("updated documents ").append(response.getUpdated()).append(", ") //
            .append("deleted documents ").append(response.getDeleted()).append(", ") //
            .append("version conflicts ").append(response.getVersionConflicts()).append(", ") //
            .append(" used ").append(response.getBatches()).append(" batches, ") //
            .append("batch retries ").append(response.getBulkRetries()).append(", ") //
            .append("search retries ").append(response.getSearchRetries()).append(".");
        long numberOfDocuments = stepRepository.countDocuments(globalTenantIndexName);
        if(numberOfDocuments != 0) {
            String message = "Cannot remove all documents from index '" + globalTenantIndexName + "'";
            String errorDetails = "Index '" + globalTenantIndexName + "' contains " + numberOfDocuments + " documents";
            return new StepResult(NOT_EMPTY_INDEX, message, errorDetails);
        }
        return new StepResult(OK, "Content of index '" + globalTenantIndexName + "' deleted", details.toString());
    }

    @Override
    public String name() {
        return "Delete global index content";
    }
}
