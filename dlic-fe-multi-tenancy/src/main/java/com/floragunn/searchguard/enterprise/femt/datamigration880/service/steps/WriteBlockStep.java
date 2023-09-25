package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.ROLLBACK;

class WriteBlockStep implements MigrationStep {

    private final StepRepository repository;

    public WriteBlockStep(StepRepository repository) {
        this.repository = Objects.requireNonNull(repository, "Step repository is required");
    }

    @Override
    public StepResult execute(DataMigrationContext context) throws StepException {
        ImmutableList<String> existingBackupIndices = context.getBackupIndices();
        List<String> blockedIndices = new ArrayList<>(existingBackupIndices);
        blockedIndices.addAll(context.getDataIndicesNames());
        repository.writeBlockIndices(ImmutableList.of(blockedIndices));
        String details = "Writes blocked indices " + blockedIndices.stream() //
            .map(name -> "'" + name + "'") //
            .collect(Collectors.joining(", "));
        return new StepResult(OK, "Indices write blocked", details);
    }

    @Override
    public String name() {
        return "write lock step";
    }

    @Override
    public StepResult rollback(DataMigrationContext context) throws StepException {
        ImmutableList<String> dataIndicesNames = context.getDataIndicesNames();
        if(! dataIndicesNames.isEmpty()) {
            repository.releaseWriteLock(dataIndicesNames);
        }
        String details = "Indices unlocked: " + dataIndicesNames.stream().map(name -> "'" + name + "'").collect(Collectors.joining(", "));
        return new StepResult(ROLLBACK, "Rollback indices write lock", details);
    }
}
