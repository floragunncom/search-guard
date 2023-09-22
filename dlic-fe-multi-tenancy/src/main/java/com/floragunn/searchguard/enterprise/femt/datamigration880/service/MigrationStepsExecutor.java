package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.StepException;
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
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.UNEXPECTED_ERROR;

class MigrationStepsExecutor {

    private static final Logger log = LogManager.getLogger(MigrationStepsExecutor.class);
    static final String MIGRATION_ID = "migration_8_8_0";

    private final MigrationStateRepository migrationStateRepository;
    private final Clock clock;
    private final ImmutableList<MigrationStep> steps;

    private final MigrationConfig config;

    MigrationStepsExecutor(MigrationConfig config, MigrationStateRepository migrationStateRepository, Clock clock,
        ImmutableList<MigrationStep> steps) {
        this.migrationStateRepository = Objects.requireNonNull(migrationStateRepository, "Migration state repository is required");
        this.clock = Objects.requireNonNull(clock, "Clock is required");
        this.config = Objects.requireNonNull(config, "Migration config is required");
        if(steps.isEmpty()) {
            throw new IllegalStateException("Step list cannot be empty");
        }
        if(steps.stream().anyMatch(Objects::isNull)) {
            throw new IllegalStateException("Step list contain null element " + steps);
        }
        this.steps = Objects.requireNonNull(steps, "Steps list is required");
    }

    public MigrationExecutionSummary execute() {
        DataMigrationContext context = new DataMigrationContext(config, clock);
        List<StepExecutionSummary> accomplishedSteps = new ArrayList<>();
        for(int i = 0; i < steps.size(); ++i) {
            MigrationStep step = steps.get(i);
            LocalDateTime stepStartTime = LocalDateTime.now(clock);
            log.info("Starting execution of migration step '{}' at '{}'", step.name(), stepStartTime);
            try {
                StepResult result = step.execute(context);
                StepExecutionSummary stepSummary = new StepExecutionSummary(i, stepStartTime, step.name(), result);
                accomplishedSteps.add(stepSummary);
                boolean lastStep = i == steps.size() - 1;
                ExecutionStatus status = lastStep ? (result.isSuccess() ? SUCCESS : FAILURE) : (result.isSuccess() ? IN_PROGRESS : FAILURE);
                log.info("Step '{}' executed with result '{}", step.name(), result);
                if(result.isFailure()) {
                    return rollbackMigration(i, context, accomplishedSteps);
                }
                var migrationSummary = persistState(context, accomplishedSteps, status);
                if(lastStep) {
                    return migrationSummary;
                }
            } catch (StepException ex) {
                String stepName = step.name();
                var stepSummary = new StepExecutionSummary(i, stepStartTime, stepName, ex.getStatus(), ex.getMessage(), ex.getDetails());
                accomplishedSteps.add(stepSummary);
                return rollbackMigration(i, context, accomplishedSteps);
            } catch (Exception ex) {
                String stepName = step.name();
                String message = "Unexpected error: " + ex.getMessage();
                accomplishedSteps.add(new StepExecutionSummary(i, stepStartTime, stepName, UNEXPECTED_ERROR, message, ex));
                log.error("Unexpected error occurred during execution of data migration step '{}' which is '{}'.", i, stepName, ex);
                return rollbackMigration(i, context, accomplishedSteps);
            }
        }
        throw new IllegalStateException("Migration already finished, no more steps to execute!");
    }

    private MigrationExecutionSummary rollbackMigration(final int currentStep,
        DataMigrationContext context,
        List<StepExecutionSummary> accomplishedSteps) {
        MigrationExecutionSummary migrationExecutionSummary = persistState(context, accomplishedSteps, FAILURE);// persist failure first
        List<MigrationStep> stepsToRollBack = steps.subList(0, currentStep).map(SafeStep::new);
        for(int i = stepsToRollBack.size() -1; i >= 0; --i) {
            LocalDateTime stepStartTime = LocalDateTime.now(clock);
            MigrationStep step = stepsToRollBack.get(i);
            log.info("Step '{}' needs to be rollback.", step.name());
            StepResult result = step.rollback(context);
            StepExecutionSummary stepSummary = new StepExecutionSummary(i, stepStartTime, step.name(), result);
            accomplishedSteps.add(stepSummary);
            migrationExecutionSummary = persistState(context, accomplishedSteps, FAILURE);
        }
        return migrationExecutionSummary;
    }

    private final static class SafeStep implements MigrationStep {
        private final MigrationStep step;

        public SafeStep(MigrationStep step) {
            this.step = Objects.requireNonNull(step, "Migration step is required");
        }

        @Override
        public StepResult execute(DataMigrationContext dataMigrationContext) throws StepException {
            return step.execute(dataMigrationContext);
        }

        @Override
        public String name() {
            return "rollback - " + step.name();
        }

        @Override
        public StepResult rollback(DataMigrationContext dataMigrationContext) throws StepException {
            try {
                return step.rollback(dataMigrationContext);
            } catch (Exception ex) {
                log.error("Cannot rollback step '{}'.", name(), ex);
                return new StepResult(UNEXPECTED_ERROR, "Unexpected error during step rollback", ex.getMessage());
            }
        }
    }


    private MigrationExecutionSummary persistState(DataMigrationContext dataMigrationContext,
        List<StepExecutionSummary> accomplishedSteps,
        ExecutionStatus status) {
        MigrationExecutionSummary migrationExecutionSummary = createExecutionSummary(dataMigrationContext, accomplishedSteps, status);
        migrationStateRepository.upsert(MIGRATION_ID, migrationExecutionSummary);
        return migrationExecutionSummary;
    }

    private static MigrationExecutionSummary createExecutionSummary(DataMigrationContext dataMigrationContext,
        List<StepExecutionSummary> accomplishedSteps,
        ExecutionStatus status) {
        return new MigrationExecutionSummary(
            dataMigrationContext.getStartTime(),
            status,
            dataMigrationContext.getTempIndexName(),
            dataMigrationContext.getBackupIndexName(),
            ImmutableList.of(accomplishedSteps));
    }

}
