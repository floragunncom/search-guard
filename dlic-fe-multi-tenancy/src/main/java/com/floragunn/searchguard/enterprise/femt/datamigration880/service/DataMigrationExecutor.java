package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.fluent.collections.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus.FAILURE;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus.IN_PROGRESS;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus.SUCCESS;

class DataMigrationExecutor {

    private static final Logger log = LogManager.getLogger(DataMigrationExecutor.class);
    static final String MIGRATION_ID = "migration_8_8_0";

    private final MigrationStateRepository migrationStateRepository;

    private final Clock clock;
    private final ImmutableList<MigrationStep> steps;

    DataMigrationExecutor(MigrationStateRepository migrationStateRepository, Clock clock, ImmutableList<MigrationStep> steps) {
        this.migrationStateRepository = Objects.requireNonNull(migrationStateRepository, "Migration state repository is required");
        this.clock = Objects.requireNonNull(clock, "Clock is required");
        this.steps = Objects.requireNonNull(steps, "Steps list is required");
        if(steps.isEmpty()) {
            throw new IllegalStateException("Step list cannot be empty");
        }
        if(steps.stream().anyMatch(Objects::isNull)) {
            throw new IllegalStateException("Step list contain null element " + steps);
        }
    }

    public MigrationExecutionSummary execute() {
        DataMigrationContext dataMigrationContext = new DataMigrationContext(clock);
        List<StepExecutionSummary> accomplishedSteps = new ArrayList<>();
        for(int i = 0; i < steps.size(); ++i) {
            MigrationStep step = steps.get(i);
            LocalDateTime stepStartTime = LocalDateTime.now(clock);
            log.info("Starting execution of migration step '{}' at '{}'", step.name(), stepStartTime);
            try {
                StepResult result = step.execute(dataMigrationContext);
                StepExecutionSummary stepSummary = new StepExecutionSummary(i, stepStartTime, step.name(), result);
                accomplishedSteps.add(stepSummary);
                boolean lastStep = i == steps.size() - 1;
                ExecutionStatus status = lastStep ? (result.isSuccess() ? SUCCESS : FAILURE) : (result.isSuccess() ? IN_PROGRESS : FAILURE);
                var migrationSummary = persistState(dataMigrationContext, accomplishedSteps, status);
                log.info("Step '{}' executed with result '{}", step.name(), result);
                if(lastStep || (!SUCCESS.equals(result.status()))) {
                    return migrationSummary;
                }
            } catch (Exception ex) {
                String stepName = step.name();
                String message = "Unexpected error: " + ex.getMessage();
                accomplishedSteps.add(new StepExecutionSummary(i, stepStartTime, stepName, FAILURE, message));
                log.error("Unexpected error occured during execution of data migration step '{}' which is '{}'.", i, stepName, ex);
                return persistState(dataMigrationContext, accomplishedSteps, FAILURE);
            }
        }
        throw new IllegalStateException("Migration already finished, no more steps to execute!");
    }

    private MigrationExecutionSummary persistState(
        DataMigrationContext dataMigrationContext, List<StepExecutionSummary> accomplishedSteps,
        ExecutionStatus status) {
        MigrationExecutionSummary migrationExecutionSummary = createMigrationSummary(dataMigrationContext, accomplishedSteps, status);
        migrationStateRepository.upsert(MIGRATION_ID, migrationExecutionSummary);
        return migrationExecutionSummary;
    }

    private static MigrationExecutionSummary createMigrationSummary(
        DataMigrationContext dataMigrationContext,
        List<StepExecutionSummary> accomplishedSteps,
        ExecutionStatus status) {
        return new MigrationExecutionSummary(
            dataMigrationContext.getStartTime(), status,
                dataMigrationContext.getTempIndexName(),
                dataMigrationContext.getBackupIndexName(), ImmutableList.of(accomplishedSteps));
    }

}
