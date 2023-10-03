package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationConfig;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.TenantIndex;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.BACKUP_UNEXPECTED_OPERATION_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.MISSING_DOCUMENTS_IN_BACKUP_ERROR;
import static java.time.ZoneOffset.UTC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CreateBackupStepTest {

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

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupCreated(), equalTo(true));
        verify(indexSettingsManager).createIndexWithClonedSettings(INDEX_NAME_GLOBAL_TENANT, BACKUP_DESTINATION, false);
    }

    @Test
    public void shouldReportErrorWhenBackupIndexContainsInsufficientNumberOfDocuments() {
        context.setTenantIndices(ImmutableList.of(new TenantIndex(INDEX_NAME_GLOBAL_TENANT, GLOBAL_TENANT_NAME)));
        when(repository.reindexData(INDEX_NAME_GLOBAL_TENANT, BACKUP_DESTINATION)).thenReturn(response);
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
        when(response.getVersionConflicts()).thenReturn(3L);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(BACKUP_UNEXPECTED_OPERATION_ERROR));
        assertThat(context.getBackupCreated(), equalTo(false));
    }
}