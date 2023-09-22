package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationConfig;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Optional;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.INVALID_BACKUP_INDEX_NAME;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.INVALID_DATE_IN_BACKUP_INDEX_NAME_ERROR;
import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static java.time.ZoneOffset.UTC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PopulateBackupIndicesStepTest {

    private static final Logger log = LogManager.getLogger(PopulateBackupIndicesStepTest.class);

    private static final ZonedDateTime NOW = ZonedDateTime.of(LocalDateTime.of(1990, 1, 1, 1, 1), UTC);
    private static final Clock CLOCK = Clock.fixed(NOW.toInstant(), UTC);
    public static final String BACKUP_INDEX_1 = "backup_fe_migration_to_8_8_0_1980_01_01_01_06_00";
    public static final String BACKUP_INDEX_2 = "backup_fe_migration_to_8_8_0_1990_01_01_01_02_00";
    public static final String BACKUP_INDEX_3 = "backup_fe_migration_to_8_8_0_1990_01_01_01_03_00";
    public static final String BACKUP_INDEX_4 = "backup_fe_migration_to_8_8_0_1990_01_01_01_04_00";
    public static final String BACKUP_INDEX_5 = "backup_fe_migration_to_8_8_0_2000_01_01_01_01_00";

    @Mock
    private StepRepository repository;
    @Mock
    private GetIndexResponse response;
    private DataMigrationContext context;
    private PopulateBackupIndicesStep step;

    @Before
    public void before() {
        this.step = new PopulateBackupIndicesStep(repository);
        this.context = new DataMigrationContext(new MigrationConfig(false), CLOCK);
    }

    @Test
    public void shouldSetEmptyBackupListInContext() {
        when(repository.findIndexByNameOrAlias(anyString())).thenReturn(Optional.empty());

        StepResult result = step.execute(context);

        log.info("Step result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupIndices(), notNullValue());
        assertThat(context.getBackupIndices(), empty());
    }

    @Test
    public void shouldFindOneBackupIndex() {
        when(response.getIndices()).thenReturn(new String[]{ BACKUP_INDEX_1 });
        when(repository.findIndexByNameOrAlias(anyString())).thenReturn(Optional.of(response));

        StepResult result = step.execute(context);

        log.info("Step result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupIndices(), hasSize(1));
        assertThat(context.getBackupIndices(), containsInAnyOrder(BACKUP_INDEX_1));
    }

    @Test
    public void shouldFindManyBackupIndicesAndSortThemByDate() {
        final String[] indices = { BACKUP_INDEX_3, BACKUP_INDEX_2, BACKUP_INDEX_4, BACKUP_INDEX_5, BACKUP_INDEX_1 };
        when(response.getIndices()).thenReturn(indices);
        when(repository.findIndexByNameOrAlias(anyString())).thenReturn(Optional.of(response));

        StepResult result = step.execute(context);

        log.info("Step result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupIndices(), hasSize(indices.length));
        assertThat(context.getBackupIndices(), contains(BACKUP_INDEX_5, BACKUP_INDEX_4, BACKUP_INDEX_3, BACKUP_INDEX_2, BACKUP_INDEX_1));
        assertThat(context.getNewestExistingBackupIndex().orElseThrow(), equalTo(BACKUP_INDEX_5));
    }

    @Test
    public void shouldSortAnotherBackupIndicesNames() {
        final String[] indices = { BACKUP_INDEX_1, BACKUP_INDEX_2, BACKUP_INDEX_3, BACKUP_INDEX_5, BACKUP_INDEX_4 };
        when(response.getIndices()).thenReturn(indices);
        when(repository.findIndexByNameOrAlias(anyString())).thenReturn(Optional.of(response));

        StepResult result = step.execute(context);

        log.info("Step result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupIndices(), hasSize(indices.length));
        assertThat(context.getBackupIndices(), contains(BACKUP_INDEX_5, BACKUP_INDEX_4, BACKUP_INDEX_3, BACKUP_INDEX_2, BACKUP_INDEX_1));
        assertThat(context.getNewestExistingBackupIndex().orElseThrow(), equalTo(BACKUP_INDEX_5));
    }

    @Test
    public void shouldGetLatestBackupIndexName() {
        final String[] indices = { BACKUP_INDEX_3, BACKUP_INDEX_4, BACKUP_INDEX_2 };
        when(response.getIndices()).thenReturn(indices);
        when(repository.findIndexByNameOrAlias(anyString())).thenReturn(Optional.of(response));

        StepResult result = step.execute(context);

        log.info("Step result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getNewestExistingBackupIndex().orElseThrow(), equalTo(BACKUP_INDEX_4));
    }

    @Test
    public void shouldDetectIncorrectBackupIndexNamePrefix() {
        final String[] indices = { BACKUP_INDEX_1, BACKUP_INDEX_2, BACKUP_INDEX_3, BACKUP_INDEX_5, BACKUP_INDEX_4, "not_backup_index" };
        when(response.getIndices()).thenReturn(indices);
        when(repository.findIndexByNameOrAlias(anyString())).thenReturn(Optional.of(response));

        StepException stepException = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));

        assertThat(stepException.getStatus(), equalTo(INVALID_BACKUP_INDEX_NAME));
    }

    @Test
    public void shouldDetectIncorrectBackupIndexNameDatePart() {
        final String[] indices = { BACKUP_INDEX_1, BACKUP_INDEX_2, BACKUP_INDEX_3,  "backup_fe_migration_to_8_8_0_1980_01_no_td_at_e0"};
        when(response.getIndices()).thenReturn(indices);
        when(repository.findIndexByNameOrAlias(anyString())).thenReturn(Optional.of(response));

        StepException stepException = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));

        assertThat(stepException.getStatus(), equalTo(INVALID_DATE_IN_BACKUP_INDEX_NAME_ERROR));
    }

    @Test
    public void shouldDetectIncorrectBackupIndexNameWithNotExistingDate() {
        final String[] indices = { BACKUP_INDEX_1, BACKUP_INDEX_2, BACKUP_INDEX_3,  "backup_fe_migration_to_8_8_0_2000_02_35_01_01_00"};
        when(response.getIndices()).thenReturn(indices);
        when(repository.findIndexByNameOrAlias(anyString())).thenReturn(Optional.of(response));

        StepException stepException = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));

        assertThat(stepException.getStatus(), equalTo(INVALID_DATE_IN_BACKUP_INDEX_NAME_ERROR));
    }

}