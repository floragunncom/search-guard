package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.StepsFactory;
import com.floragunn.searchsupport.action.StandardResponse;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Objects;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus.FAILURE;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus.SUCCESS;
import static org.apache.http.HttpStatus.SC_BAD_REQUEST;
import static org.apache.http.HttpStatus.SC_CONFLICT;
import static org.apache.http.HttpStatus.SC_GONE;
import static org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_PRECONDITION_FAILED;

public class DataMigrationService {
    public static final String STAGE_NAME_PRECONDITIONS = "preconditions check";

    private final MigrationStateRepository migrationStateRepository;

    private final StepsFactory stepsFactory;
    private final Clock clock;

    public DataMigrationService(MigrationStateRepository migrationStateRepository, StepsFactory stepsFactory, Clock clock) {
        this.migrationStateRepository = Objects.requireNonNull(migrationStateRepository, "Migration state repository is required");
        this.stepsFactory = Objects.requireNonNull(stepsFactory, "Step factory is required");
        this.clock = Objects.requireNonNull(clock, "Clock is required");
    }

    public StandardResponse migrateData() {
        try {
            if (!migrationStateRepository.isIndexCreated()) {
                migrationStateRepository.createIndex();
            }
            return migrationStateRepository.findById(DataMigrationExecutor.MIGRATION_ID) //
                .map(this::restartMigration) //
                .orElseGet(this::performFirstMigrationStart);
        } catch (IndexAlreadyExistsException e) {
            String message = """
                    Cannot create index to store migration related data./
                    Possibly another data migration process was started in parallel.
                    """.trim();
            return errorResponse(SC_CONFLICT, message, e);
        }
    }

    private StandardResponse executeMigrationSteps() {
        DataMigrationExecutor executor = new DataMigrationExecutor(migrationStateRepository, clock, stepsFactory.createSteps());
        MigrationExecutionSummary summary = executor.execute();
        int httpStatus = summary.isSuccessful() ? SC_OK : SC_INTERNAL_SERVER_ERROR;
        return new StandardResponse(httpStatus).data(summary);
    }

    private StandardResponse performFirstMigrationStart() {
        LocalDateTime now = LocalDateTime.now(clock);
        MigrationExecutionSummary summary = migrationStartedSummary(now, "The first start of data migration process");
        try {
            migrationStateRepository.create(DataMigrationExecutor.MIGRATION_ID, summary);
            return executeMigrationSteps();
        } catch (OptimisticLockException e) {
            String message = "Another migration process has just been started.";
            return errorResponse(SC_PRECONDITION_FAILED, message, e);
        }
    }

    private StandardResponse restartMigration(MigrationExecutionSummary migration) {
        LocalDateTime now = LocalDateTime.now(clock);
        if(migration.isMigrationInProgress(now)) {
            String message = "Data migration started previously at " + migration.startTime() +
                " is already in progress. Cannot run more than one migration process at the time.";
            return errorResponse(SC_BAD_REQUEST, message, null);
        }
        MigrationExecutionSummary restartedMigration = migrationStartedSummary(now, "Migration restarted");
        try {
            migrationStateRepository.updateWithLock(DataMigrationExecutor.MIGRATION_ID, restartedMigration, migration.lockData());
            return executeMigrationSteps();
        } catch (OptimisticLockException ex) {
            String errorMessage = "Another instance of data migration process is just starting, aborting.";
            return errorResponse(SC_CONFLICT, errorMessage, ex);
        }
    }

    private MigrationExecutionSummary migrationStartedSummary(LocalDateTime now, String message) {
        StepExecutionSummary preconditionStage = new StepExecutionSummary(0, now, STAGE_NAME_PRECONDITIONS, SUCCESS, message);
        ImmutableList<StepExecutionSummary> stages = ImmutableList.of(preconditionStage);
        return new MigrationExecutionSummary(now, ExecutionStatus.IN_PROGRESS, null, null, stages);
    }

    private StandardResponse errorResponse(int httpStatus, String message, Throwable ex) {
        LocalDateTime now = LocalDateTime.now(clock);
        var stages = ImmutableList.of(new StepExecutionSummary(0, now, STAGE_NAME_PRECONDITIONS, FAILURE, message, ex));
        MigrationExecutionSummary summary = new MigrationExecutionSummary(now, FAILURE, null, null, stages);
        return new StandardResponse(httpStatus).data(summary);
    }
}
