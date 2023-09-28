package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;

import java.util.Objects;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;

class DeleteTempIndexStep implements MigrationStep {

    private final StepRepository repository;

    public DeleteTempIndexStep(StepRepository repository) {
        this.repository = Objects.requireNonNull(repository, "Step repository is required");
    }

    @Override
    public StepResult execute(DataMigrationContext context) throws StepException {
        String tempIndexName = context.getTempIndexName();
        repository.deleteIndices(tempIndexName);
        return new StepResult(OK, "Temp index deleted", "Index '" + tempIndexName + "' deleted");
    }

    @Override
    public String name() {
        return "delete temp index";
    }
}
