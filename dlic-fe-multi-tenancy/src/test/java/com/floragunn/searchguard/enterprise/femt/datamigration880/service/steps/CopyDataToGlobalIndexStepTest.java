/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.BulkByScrollTask.StatusOrException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.stream.IntStream;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.MISSING_DOCUMENTS_IN_GLOBAL_TENANT_INDEX_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.REINDEX_BULK_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.REINDEX_DATA_INTO_GLOBAL_TENANT_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.REINDEX_SEARCH_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.REINDEX_TIMEOUT_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.SLICE_PARTIAL_ERROR;
import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
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

    @Mock
    private BulkByScrollTask.Status status;

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
        when(reindexResponse.getStatus()).thenReturn(status);
        List<StatusOrException> statuses = IntStream.range(0, 7).mapToObj(i -> mock(StatusOrException.class)).toList();
        statuses.forEach(status -> when(status.getException()).thenReturn(null));
        when(status.getSliceStatuses()).thenReturn(statuses);
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
        when(reindexResponse.getStatus()).thenReturn(status);
        when(status.getSliceStatuses()).thenReturn(ImmutableList.empty());
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
        when(reindexResponse.getStatus()).thenReturn(status);
        when(status.getSliceStatuses()).thenReturn(ImmutableList.empty());
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
        when(reindexResponse.getStatus()).thenReturn(status);
        when(status.getSliceStatuses()).thenReturn(ImmutableList.empty());
        when(stepRepository.countDocuments(SOURCE_INDEX_NAME_1)).thenReturn(1050L);
        when(stepRepository.countDocuments(DESTINATION_INDEX_NAME_1)).thenReturn(1001L);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(MISSING_DOCUMENTS_IN_GLOBAL_TENANT_INDEX_ERROR));
    }

    @Test
    public void shouldReportErrorWhenDocumentsWereUpdatedInDestinationIndex() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_1);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_1);
        when(stepRepository.reindexData(anyString(), anyString())).thenReturn(reindexResponse);
        when(reindexResponse.getUpdated()).thenReturn(1L);
        when(stepRepository.countDocuments(anyString())).thenReturn(1066L);
        when(reindexResponse.getStatus()).thenReturn(status);
        when(status.getSliceStatuses()).thenReturn(ImmutableList.empty());

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(REINDEX_DATA_INTO_GLOBAL_TENANT_ERROR));
    }

    @Test
    public void shouldReportErrorWhenDocumentsWereDeletedFromDestinationIndex() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_1);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_1);
        when(stepRepository.reindexData(anyString(), anyString())).thenReturn(reindexResponse);
        when(reindexResponse.getStatus()).thenReturn(status);
        when(status.getSliceStatuses()).thenReturn(ImmutableList.empty());
        when(reindexResponse.getDeleted()).thenReturn(10L);
        when(stepRepository.countDocuments(anyString())).thenReturn(1066L);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(REINDEX_DATA_INTO_GLOBAL_TENANT_ERROR));
    }

    @Test
    public void shouldReportErrorWhenVersionConflictsOccurredInDestinationIndex() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_1);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_1);
        when(stepRepository.reindexData(anyString(), anyString())).thenReturn(reindexResponse);
        when(reindexResponse.getStatus()).thenReturn(status);
        when(status.getSliceStatuses()).thenReturn(ImmutableList.empty());
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
        when(reindexResponse.getStatus()).thenReturn(status);
        when(status.getSliceStatuses()).thenReturn(ImmutableList.empty());
        when(stepRepository.countDocuments(anyString())).thenReturn(27_654L);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(REINDEX_DATA_INTO_GLOBAL_TENANT_ERROR));
    }

    @Test
    public void shouldInterruptStepExecutionInCaseOfExceptionThrownFromRepository_1() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_3);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_3);
        when(stepRepository.reindexData(anyString(), anyString())).thenThrow(new StepException("Test exception", REINDEX_BULK_ERROR, null));

        StepException stepException = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));
        assertThat(stepException.getStatus(), equalTo(REINDEX_BULK_ERROR));
    }

    @Test
    public void shouldInterruptStepExecutionInCaseOfExceptionThrownFromRepository_2() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_3);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_3);
        when(stepRepository.reindexData(anyString(), anyString())).thenThrow(new StepException("test ex", REINDEX_SEARCH_ERROR, null));

        StepException stepException = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));
        assertThat(stepException.getStatus(), equalTo(REINDEX_SEARCH_ERROR));
    }

    @Test
    public void shouldInterruptStepExecutionInCaseOfExceptionThrownFromRepository_3() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_3);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_3);
        when(stepRepository.reindexData(anyString(), anyString())).thenThrow(new StepException("test timeout", REINDEX_TIMEOUT_ERROR, null));

        StepException stepException = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));
        assertThat(stepException.getStatus(), equalTo(REINDEX_TIMEOUT_ERROR));
    }

    @Test
    public void shouldDetectErrorDuringReindexingData_1() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_1);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_1);
        when(stepRepository.reindexData(anyString(), anyString())).thenReturn(reindexResponse);
        when(reindexResponse.getStatus()).thenReturn(status);
        List<StatusOrException> statuses = IntStream.range(0, 7).mapToObj(i -> mock(StatusOrException.class)).toList();
        when(statuses.get(0).getException()).thenReturn(new IllegalStateException("Slice 0 error"));
        when(status.getSliceStatuses()).thenReturn(statuses);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(SLICE_PARTIAL_ERROR));
    }

    @Test
    public void shouldDetectErrorDuringReindexingData_2() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_1);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_1);
        when(stepRepository.reindexData(anyString(), anyString())).thenReturn(reindexResponse);
        when(reindexResponse.getStatus()).thenReturn(status);
        List<StatusOrException> statuses = IntStream.range(0, 7).mapToObj(i -> mock(StatusOrException.class)).toList();
        when(statuses.get(6).getException()).thenReturn(new IllegalStateException("Slice 7 error"));
        when(status.getSliceStatuses()).thenReturn(statuses);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(SLICE_PARTIAL_ERROR));
    }
    @Test
    public void shouldDetectErrorDuringReindexingData_3() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_1);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_1);
        when(stepRepository.reindexData(anyString(), anyString())).thenReturn(reindexResponse);
        when(reindexResponse.getStatus()).thenReturn(status);
        List<StatusOrException> statuses = IntStream.range(0, 7).mapToObj(i -> mock(StatusOrException.class)).toList();
        when(statuses.get(3).getException()).thenReturn(new IllegalStateException("Slice 3 error"));
        when(status.getSliceStatuses()).thenReturn(statuses);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(SLICE_PARTIAL_ERROR));
    }

    @Test
    public void shouldDetectErrorDuringReindexingData_4() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_1);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_1);
        when(stepRepository.reindexData(anyString(), anyString())).thenReturn(reindexResponse);
        when(reindexResponse.getStatus()).thenReturn(status);
        List<StatusOrException> statuses = IntStream.range(0, 7).mapToObj(i -> mock(StatusOrException.class)).toList();
        when(statuses.get(2).getException()).thenReturn(new IllegalStateException("Slice 2 error"));
        when(statuses.get(4).getException()).thenReturn(new IllegalStateException("Slice 4 error"));
        when(status.getSliceStatuses()).thenReturn(statuses);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(SLICE_PARTIAL_ERROR));
    }

    @Test
    public void shouldDetectErrorDuringReindexingData_5() {
        when(context.getTempIndexName()).thenReturn(SOURCE_INDEX_NAME_1);
        when(context.getGlobalTenantIndexName()).thenReturn(DESTINATION_INDEX_NAME_1);
        when(stepRepository.reindexData(anyString(), anyString())).thenReturn(reindexResponse);
        when(reindexResponse.getStatus()).thenReturn(status);
        List<StatusOrException> statuses = IntStream.range(0, 7).mapToObj(i -> mock(StatusOrException.class)).toList();
        statuses.forEach(statusOrException -> when(statusOrException.getException()).thenReturn(new IllegalArgumentException("Oops!")));
        when(status.getSliceStatuses()).thenReturn(statuses);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(SLICE_PARTIAL_ERROR));
    }

}