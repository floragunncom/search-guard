package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import com.floragunn.searchsupport.junit.ThrowableAssert;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.MISSING_DOCUMENTS_IN_GLOBAL_TENANT_INDEX_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.REINDEX_BULK_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.REINDEX_DATA_INTO_GLOBAL_TENANT_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.REINDEX_SEARCH_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.REINDEX_TIMEOUT_ERROR;
import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CopyDataToGlobalIndexStepTest {

    private static final Logger log = LogManager.getLogger(CopyDataToGlobalIndexStepTest.class);

    public static final String SOURCE_INDEX_NAME_1 = "source_index_name_1";
    public static final String DESTINATION_INDEX_NAME_1 = "destination_index_name_1";

    public static final String SOURCE_INDEX_NAME_2 = "source__index__name__2";
    public static final String DESTINATION_INDEX_NAME_2 = "DestinationIndexName_2";

    public static final String SOURCE_INDEX_NAME_3 = "sourceindexname_3";
    public static final String DESTINATION_INDEX_NAME_3 = "destinationindexname_3";

    @Mock
    private StepRepository stepRepository;

    @Mock
    private DataMigrationContext context;

    @Mock
    private BulkByScrollResponse reindexResponse;

    private CopyDataToGlobalIndexStep step;


    @Before
    public void before() {
        this.step = new CopyDataToGlobalIndexStep(stepRepository);
    }

    @Test
    public void shouldReindexData_1() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_1);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_1);
        when(stepRepository.reindexData(anyString(), anyString())).thenReturn(reindexResponse);
        when(reindexResponse.getCreated()).thenReturn(1023L);
        when(reindexResponse.getTotal()).thenReturn(1023L);
        when(reindexResponse.getBatches()).thenReturn(11);
        when(stepRepository.countDocuments(anyString())).thenReturn(1023L);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        verify(stepRepository).reindexData(SOURCE_INDEX_NAME_1, DESTINATION_INDEX_NAME_1);
        verify(stepRepository).countDocuments(SOURCE_INDEX_NAME_1);
        verify(stepRepository).countDocuments(DESTINATION_INDEX_NAME_1);
        verifyNoMoreInteractions(stepRepository);
    }

    @Test
    public void shouldReindexData_2() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_2);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_2);
        when(stepRepository.reindexData(anyString(), anyString())).thenReturn(reindexResponse);
        when(reindexResponse.getCreated()).thenReturn(999L);
        when(reindexResponse.getTotal()).thenReturn(999L);
        when(reindexResponse.getBatches()).thenReturn(10);
        when(stepRepository.countDocuments(anyString())).thenReturn(999L);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        verify(stepRepository).reindexData(SOURCE_INDEX_NAME_2, DESTINATION_INDEX_NAME_2);
        verify(stepRepository).countDocuments(SOURCE_INDEX_NAME_2);
        verify(stepRepository).countDocuments(DESTINATION_INDEX_NAME_2);
        verifyNoMoreInteractions(stepRepository);
    }

    @Test
    public void shouldReindexData_3() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_3);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_3);
        when(stepRepository.reindexData(anyString(), anyString())).thenReturn(reindexResponse);
        when(reindexResponse.getCreated()).thenReturn(999L);
        when(reindexResponse.getTotal()).thenReturn(999L);
        when(reindexResponse.getBatches()).thenReturn(10);
        when(stepRepository.countDocuments(anyString())).thenReturn(999L);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        verify(stepRepository).reindexData(SOURCE_INDEX_NAME_3, DESTINATION_INDEX_NAME_3);
        verify(stepRepository).countDocuments(SOURCE_INDEX_NAME_3);
        verify(stepRepository).countDocuments(DESTINATION_INDEX_NAME_3);
        verifyNoMoreInteractions(stepRepository);
    }

    @Test
    public void shouldReportErrorWhenDocumentsInGlobalTenantIndexAreMissing() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_1);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_1);
        when(stepRepository.reindexData(anyString(), anyString())).thenReturn(reindexResponse);
        when(stepRepository.countDocuments(SOURCE_INDEX_NAME_1)).thenReturn(1050L);
        when(stepRepository.countDocuments(DESTINATION_INDEX_NAME_1)).thenReturn(1001L);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(MISSING_DOCUMENTS_IN_GLOBAL_TENANT_INDEX_ERROR));
    }

    @Test
    public void shouldReportErrorWhenDocumentsWareUpdatedInDestinationIndex() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_1);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_1);
        when(stepRepository.reindexData(anyString(), anyString())).thenReturn(reindexResponse);
        when(reindexResponse.getUpdated()).thenReturn(1L);
        when(stepRepository.countDocuments(anyString())).thenReturn(1066L);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(REINDEX_DATA_INTO_GLOBAL_TENANT_ERROR));
    }

    @Test
    public void shouldReportErrorWhenDocumentsWhenDeletedDocumentsFromDestinationIndex() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_1);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_1);
        when(stepRepository.reindexData(anyString(), anyString())).thenReturn(reindexResponse);
        when(reindexResponse.getDeleted()).thenReturn(10L);
        when(stepRepository.countDocuments(anyString())).thenReturn(1066L);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(REINDEX_DATA_INTO_GLOBAL_TENANT_ERROR));
    }

    @Test
    public void shouldReportErrorWhenDocumentsWhenVersionConflictsOccurredInDestinationIndex() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_1);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_1);
        when(stepRepository.reindexData(anyString(), anyString())).thenReturn(reindexResponse);
        when(reindexResponse.getVersionConflicts()).thenReturn(13L);
        when(stepRepository.countDocuments(anyString())).thenReturn(1066L);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(REINDEX_DATA_INTO_GLOBAL_TENANT_ERROR));
    }

    @Test
    public void shouldReportErrorInCaseOfDeletionUpdatesAndVersionConflictsInDestinationIndex() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_1);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_1);
        when(stepRepository.reindexData(anyString(), anyString())).thenReturn(reindexResponse);
        when(reindexResponse.getVersionConflicts()).thenReturn(33L);
        when(reindexResponse.getDeleted()).thenReturn(1051L);
        when(reindexResponse.getUpdated()).thenReturn(206L);
        when(stepRepository.countDocuments(anyString())).thenReturn(27_654L);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(REINDEX_DATA_INTO_GLOBAL_TENANT_ERROR));
    }

    @Test
    public void shouldInterruptStepExecutionInCaseOfExceptionThrowsFromRepository_1() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_3);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_3);
        when(stepRepository.reindexData(anyString(), anyString())).thenThrow(new StepException("Test exception", REINDEX_BULK_ERROR, null));

        StepException stepException = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));
        assertThat(stepException.getStatus(), equalTo(REINDEX_BULK_ERROR));
    }

    @Test
    public void shouldInterruptStepExecutionInCaseOfExceptionThrowsFromRepository_2() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_3);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_3);
        when(stepRepository.reindexData(anyString(), anyString())).thenThrow(new StepException("test ex", REINDEX_SEARCH_ERROR, null));

        StepException stepException = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));
        assertThat(stepException.getStatus(), equalTo(REINDEX_SEARCH_ERROR));
    }

    @Test
    public void shouldInterruptStepExecutionInCaseOfExceptionThrowsFromRepository_3() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_3);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_3);
        when(stepRepository.reindexData(anyString(), anyString())).thenThrow(new StepException("test timeout", REINDEX_TIMEOUT_ERROR, null));

        StepException stepException = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));
        assertThat(stepException.getStatus(), equalTo(REINDEX_TIMEOUT_ERROR));
    }
}