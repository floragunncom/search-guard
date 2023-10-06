package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationConfig;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.TenantIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.BulkByScrollTask.StatusOrException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.IntStream;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.BACKUP_UNEXPECTED_OPERATION_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.MISSING_DOCUMENTS_IN_BACKUP_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.SLICE_PARTIAL_ERROR;
import static java.time.ZoneOffset.UTC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CreateBackupStepTest {

    private static final Logger log = LogManager.getLogger(CreateBackupStepTest.class);

    private static final ZonedDateTime NOW = ZonedDateTime.of(LocalDateTime.of(2033, 1, 1, 1, 7), UTC);
    private static final Clock CLOCK = Clock.fixed(NOW.toInstant(), UTC);
    public static final String INDEX_NAME_GLOBAL_TENANT = "global_tenant_index";
    public static final String BACKUP_DESTINATION = "backup_fe_migration_to_8_8_0_2033_01_01_01_07_00";
    public static final long NUMBER_OF_DOCUMENTS = 100L;
    public static final String GLOBAL_TENANT_NAME = "SGS_GLOBAL_TENANT";

    @Mock
    private StepRepository repository;
    @Mock
    private IndexSettingsManager indexSettingsManager;

    @Mock
    private BulkByScrollResponse response;

    @Mock
    private BulkByScrollTask.Status responseStatus;

    private DataMigrationContext context;

    //under tests
    private CreateBackupStep step;

    @Before
    public void before() {
        this.context = new DataMigrationContext(new MigrationConfig(false), CLOCK);
        this.step = new CreateBackupStep(repository, indexSettingsManager);
    }

    @Test
    public void shouldMarkBackupIndexAsCreated() {
        context.setTenantIndices(ImmutableList.of(new TenantIndex(INDEX_NAME_GLOBAL_TENANT, GLOBAL_TENANT_NAME)));
        when(repository.reindexData(INDEX_NAME_GLOBAL_TENANT, BACKUP_DESTINATION)).thenReturn(response);
        when(response.getStatus()).thenReturn(responseStatus);
        when(responseStatus.getSliceStatuses()).thenReturn(ImmutableList.empty());

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupCreated(), equalTo(true));
        verify(indexSettingsManager).createIndexWithClonedSettings(INDEX_NAME_GLOBAL_TENANT, BACKUP_DESTINATION, false);
    }

    @Test
    public void shouldReportErrorWhenBackupIndexContainsInsufficientNumberOfDocuments() {
        context.setTenantIndices(ImmutableList.of(new TenantIndex(INDEX_NAME_GLOBAL_TENANT, GLOBAL_TENANT_NAME)));
        when(repository.reindexData(INDEX_NAME_GLOBAL_TENANT, BACKUP_DESTINATION)).thenReturn(response);
        when(response.getStatus()).thenReturn(responseStatus);
        when(responseStatus.getSliceStatuses()).thenReturn(ImmutableList.empty());
        when(repository.countDocuments(INDEX_NAME_GLOBAL_TENANT)).thenReturn(NUMBER_OF_DOCUMENTS);
        when(repository.countDocuments(BACKUP_DESTINATION)).thenReturn(NUMBER_OF_DOCUMENTS - 1);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(context.getBackupCreated(), equalTo(false));
        assertThat(result.status(), equalTo(MISSING_DOCUMENTS_IN_BACKUP_ERROR));
    }

    @Test
    public void shouldReportErrorIfDocumentWasUpdatedDuringBackup() {
        context.setTenantIndices(ImmutableList.of(new TenantIndex(INDEX_NAME_GLOBAL_TENANT, GLOBAL_TENANT_NAME)));
        when(repository.reindexData(INDEX_NAME_GLOBAL_TENANT, BACKUP_DESTINATION)).thenReturn(response);
        when(response.getStatus()).thenReturn(responseStatus);
        when(responseStatus.getSliceStatuses()).thenReturn(ImmutableList.empty());
        when(response.getUpdated()).thenReturn(1L);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(BACKUP_UNEXPECTED_OPERATION_ERROR));
        assertThat(context.getBackupCreated(), equalTo(false));
        assertThat(result.status(), equalTo(BACKUP_UNEXPECTED_OPERATION_ERROR));
    }

    @Test
    public void shouldReportErrorIfDocumentWasDeletedDuringBackup() {
        context.setTenantIndices(ImmutableList.of(new TenantIndex(INDEX_NAME_GLOBAL_TENANT, GLOBAL_TENANT_NAME)));
        when(repository.reindexData(INDEX_NAME_GLOBAL_TENANT, BACKUP_DESTINATION)).thenReturn(response);
        when(response.getStatus()).thenReturn(responseStatus);
        when(responseStatus.getSliceStatuses()).thenReturn(ImmutableList.empty());
        when(response.getDeleted()).thenReturn(2L);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(BACKUP_UNEXPECTED_OPERATION_ERROR));
        assertThat(context.getBackupCreated(), equalTo(false));
    }

    @Test
    public void shouldReportErrorIfVersionConflictOccurredDuringBackup() {
        context.setTenantIndices(ImmutableList.of(new TenantIndex(INDEX_NAME_GLOBAL_TENANT, GLOBAL_TENANT_NAME)));
        when(repository.reindexData(INDEX_NAME_GLOBAL_TENANT, BACKUP_DESTINATION)).thenReturn(response);
        when(response.getStatus()).thenReturn(responseStatus);
        when(response.getVersionConflicts()).thenReturn(3L);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(BACKUP_UNEXPECTED_OPERATION_ERROR));
        assertThat(context.getBackupCreated(), equalTo(false));
    }

    @Test
    public void shouldDetectReindexRequestError_1() {
        context.setTenantIndices(ImmutableList.of(new TenantIndex(INDEX_NAME_GLOBAL_TENANT, GLOBAL_TENANT_NAME)));
        when(repository.reindexData(INDEX_NAME_GLOBAL_TENANT, BACKUP_DESTINATION)).thenReturn(response);
        List<StatusOrException> sliceStatuses = IntStream.range(0, 5).mapToObj(i -> mock(StatusOrException.class)).toList();
        when(sliceStatuses.get(3).getException()).thenReturn(new IllegalStateException("Slice three failure"));
        when(responseStatus.getSliceStatuses()).thenReturn(sliceStatuses);
        when(response.getStatus()).thenReturn(responseStatus);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(SLICE_PARTIAL_ERROR));
        assertThat(context.getBackupCreated(), equalTo(false));
    }

    @Test
    public void shouldDetectReindexRequestError_2() {
        context.setTenantIndices(ImmutableList.of(new TenantIndex(INDEX_NAME_GLOBAL_TENANT, GLOBAL_TENANT_NAME)));
        when(repository.reindexData(INDEX_NAME_GLOBAL_TENANT, BACKUP_DESTINATION)).thenReturn(response);
        List<StatusOrException> sliceStatuses = IntStream.range(0, 5).mapToObj(i -> mock(StatusOrException.class)).toList();
        when(sliceStatuses.get(4).getException()).thenReturn(new IllegalStateException("Slice four failure"));
        when(responseStatus.getSliceStatuses()).thenReturn(sliceStatuses);
        when(response.getStatus()).thenReturn(responseStatus);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(SLICE_PARTIAL_ERROR));
        assertThat(context.getBackupCreated(), equalTo(false));
    }

    @Test
    public void shouldDetectReindexRequestError_3() {
        context.setTenantIndices(ImmutableList.of(new TenantIndex(INDEX_NAME_GLOBAL_TENANT, GLOBAL_TENANT_NAME)));
        when(repository.reindexData(INDEX_NAME_GLOBAL_TENANT, BACKUP_DESTINATION)).thenReturn(response);
        List<StatusOrException> sliceStatuses = IntStream.range(0, 5).mapToObj(i -> mock(StatusOrException.class)).toList();
        when(sliceStatuses.get(0).getException()).thenReturn(new IllegalStateException("Slice zero failure"));
        when(sliceStatuses.get(1).getException()).thenReturn(new IllegalStateException("Slice one failure"));
        when(responseStatus.getSliceStatuses()).thenReturn(sliceStatuses);
        when(response.getStatus()).thenReturn(responseStatus);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(SLICE_PARTIAL_ERROR));
        assertThat(context.getBackupCreated(), equalTo(false));
    }

    @Test
    public void shouldDetectReindexRequestError_4() {
        context.setTenantIndices(ImmutableList.of(new TenantIndex(INDEX_NAME_GLOBAL_TENANT, GLOBAL_TENANT_NAME)));
        when(repository.reindexData(INDEX_NAME_GLOBAL_TENANT, BACKUP_DESTINATION)).thenReturn(response);
        List<StatusOrException> sliceStatuses = IntStream.range(0, 15).mapToObj(i -> mock(StatusOrException.class)).toList();
        sliceStatuses.forEach(statusOrException ->
            when(statusOrException.getException()).thenReturn(new IllegalStateException("Slice four failure")));
        when(responseStatus.getSliceStatuses()).thenReturn(sliceStatuses);
        when(response.getStatus()).thenReturn(responseStatus);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(SLICE_PARTIAL_ERROR));
        assertThat(context.getBackupCreated(), equalTo(false));
    }

    @Test
    public void shouldDetectReindexRequestError_5() {
        context.setTenantIndices(ImmutableList.of(new TenantIndex(INDEX_NAME_GLOBAL_TENANT, GLOBAL_TENANT_NAME)));
        when(repository.reindexData(INDEX_NAME_GLOBAL_TENANT, BACKUP_DESTINATION)).thenReturn(response);
        List<StatusOrException> sliceStatuses = IntStream.range(0, 12).mapToObj(i -> mock(StatusOrException.class)).toList();
        sliceStatuses.forEach(statusOrException ->
            when(statusOrException.getException()).thenReturn(null)); // no error detected
        when(responseStatus.getSliceStatuses()).thenReturn(sliceStatuses);
        when(response.getStatus()).thenReturn(responseStatus);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupCreated(), equalTo(true));
    }
}