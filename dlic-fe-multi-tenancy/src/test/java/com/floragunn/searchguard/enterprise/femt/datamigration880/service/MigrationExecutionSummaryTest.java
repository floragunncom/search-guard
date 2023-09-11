package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.fluent.collections.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.time.LocalDateTime;

import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containSubstring;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class MigrationExecutionSummaryTest {

    private static final Logger log = LogManager.getLogger(MigrationExecutionSummaryTest.class);

    private final static LocalDateTime NOW = LocalDateTime.of(2023, 5, 25, 12, 7);
    public static final int STEP_NO = 7;
    public static final String STEP_NAME = "the only step name";
    public static final String STEP_MESSAGE = "step message";
    public static final String TEMP_INDEX_NAME = "temp index name";
    public static final String BACKUP_INDEX_NAME = "backup index name";
    public static final String STEP_DETAILS = "step details";

    /**
     * The main purpose of the tests is to verify that during serialization of classes <code>StepExecutionSummary</code> and
     * <code>DataMigrationSummary</code> to JSON errors do not occur.
     */
    @Test
    public void shouldSerializeToJson() throws DocumentParseException {
        MigrationExecutionSummary summary = createSummary(NOW, ExecutionStatus.IN_PROGRESS);

        String jsonString = DocWriter.json().writeAsString(summary);

        log.debug("Migration execution summary serialized to JSON: '{}'", jsonString);
        DocNode docNode = DocNode.parse(Format.JSON).from(jsonString);
        assertThat(docNode, containsValue("$.start_time", "2023-05-25T12:07:00.000Z"));
        assertThat(docNode, containsValue("$.status", "in_progress"));
        assertThat(docNode, containsValue("$.temp_index_name", TEMP_INDEX_NAME));
        assertThat(docNode, containsValue("$.backup_index_name", BACKUP_INDEX_NAME));
        assertThat(docNode, containsValue("$.stages[0].start_time", "2023-05-25T12:07:00.000Z"));
        assertThat(docNode, containsValue("$.stages[0].name", STEP_NAME));
        assertThat(docNode, containsValue("$.stages[0].status", "success"));
        assertThat(docNode, containsValue("$.stages[0].message", STEP_MESSAGE));
        assertThat(docNode, containsValue("$.stages[0].details", STEP_DETAILS));
    }

    @Test
    public void shouldIncorporateStackTraceInMessageDetails() throws DocumentParseException {
        String message = "The stack trace should be incorporated into step summary";
        RuntimeException exception = new RuntimeException(message);
        var stepSummary = new StepExecutionSummary(STEP_NO, NOW, STEP_NAME, ExecutionStatus.FAILURE, STEP_MESSAGE, exception);
        ImmutableList<StepExecutionSummary> stages = ImmutableList.of(stepSummary);
        MigrationExecutionSummary summary = new MigrationExecutionSummary(NOW, ExecutionStatus.FAILURE, null, null, stages, null);

        String jsonString = DocWriter.json().writeAsString(summary);

        log.debug("Migration execution summary serialized to JSON: '{}'", jsonString);
        DocNode docNode = DocNode.parse(Format.JSON).from(jsonString);
        assertThat(docNode, containSubstring("$.stages[0].details", message));
        assertThat(docNode, containSubstring("$.stages[0].details", """
            com.floragunn.searchguard.enterprise.femt\
            .datamigration880.service.MigrationExecutionSummaryTest.shouldIncorporateStackTraceInMessageDetails\
            (MigrationExecutionSummaryTest.java:"""
            .trim()));
    }


    private static MigrationExecutionSummary createSummary(LocalDateTime startTime, ExecutionStatus status) {
        StepExecutionSummary step = new StepExecutionSummary(STEP_NO, NOW, STEP_NAME, ExecutionStatus.SUCCESS, STEP_MESSAGE, STEP_DETAILS);
        return new MigrationExecutionSummary(startTime, status, TEMP_INDEX_NAME, BACKUP_INDEX_NAME, ImmutableList.of(step));
    }

    @Test
    public void shouldNotBeInProgressWhenMigrationExecutionFailed() {
        LocalDateTime nearPast = NOW.minusSeconds(1);
        MigrationExecutionSummary summary = createSummary(nearPast, ExecutionStatus.FAILURE);

        boolean migrationInProgress = summary.isMigrationInProgress(NOW);

        assertThat(migrationInProgress, equalTo(false));
    }

    @Test
    public void shouldNotBeInProgressWhenMigrationIsAccomplishedSuccessfully() {
        LocalDateTime nearPast = NOW.minusSeconds(1);
        MigrationExecutionSummary summary = createSummary(nearPast, ExecutionStatus.SUCCESS);

        boolean migrationInProgress = summary.isMigrationInProgress(NOW);

        assertThat(migrationInProgress, equalTo(false));
    }

    @Test
    public void shouldBeInProgressWhenMigrationIsNotAccomplishedAndMigrationStartedInNearPast() {
        LocalDateTime nearPast = NOW.minusSeconds(1);
        MigrationExecutionSummary summary = createSummary(nearPast, ExecutionStatus.IN_PROGRESS);

        boolean migrationInProgress = summary.isMigrationInProgress(NOW);

        assertThat(migrationInProgress, equalTo(true));
    }

    @Test
    public void shouldNotBeInProgressWhenMigrationIsNotAccomplishedAndMigrationStartedInDistantPast() {
        LocalDateTime distantPast = NOW.minusDays(1);
        MigrationExecutionSummary summary = createSummary(distantPast, ExecutionStatus.IN_PROGRESS);

        boolean migrationInProgress = summary.isMigrationInProgress(NOW);

        assertThat(migrationInProgress, equalTo(false));
    }

}