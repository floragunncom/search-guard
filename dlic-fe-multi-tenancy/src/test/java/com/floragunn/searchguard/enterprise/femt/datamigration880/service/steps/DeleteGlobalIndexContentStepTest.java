package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.NOT_EMPTY_INDEX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class DeleteGlobalIndexContentStepTest {

    private StepRepository stepRepository;
    private DataMigrationContext context;

    private BulkByScrollResponse deletionResponse;

    // under tests
    private DeleteGlobalIndexContentStep step;

    private final String indexName;
    private final long numberOfDocuments;

    public DeleteGlobalIndexContentStepTest(String indexName, long numberOfDocuments) {
        Strings.requireNonEmpty(indexName, "Index name is required");
        this.indexName = indexName;
        this.numberOfDocuments = numberOfDocuments;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { "data_migration_temp_fe_1990_01_01_01_01_00", 1L },
            { "openfind", 2L },
            { "my_index", 3L },
            { "please_do_not_delete_me", 7L },
            { "log_index", 8L },
            { ".very_big_index", 13L },
            { ".one_hundred", 100L },
            { ".three_thousand", 3000L },
            { "another_index_name", 1492L },
            { "index_for_deletion", Long.MAX_VALUE }
        });
    }

    @Before
    public void before() {
        this.stepRepository = mock(StepRepository.class);
        this.context = mock(DataMigrationContext.class);
        this.deletionResponse = mock(BulkByScrollResponse.class);
        this.step = new DeleteGlobalIndexContentStep(stepRepository);
    }

    @Test
    public void shouldDeleteOnlyGlobalTenantIndexContent() {
        when(context.getGlobalTenantIndexName()).thenReturn(indexName);
        when(stepRepository.deleteAllDocuments(anyString())).thenReturn(deletionResponse);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        verify(stepRepository).deleteAllDocuments(indexName);
        verify(stepRepository).releaseWriteLock(ImmutableList.of(indexName));
        verify(stepRepository).countDocuments(indexName);
        verifyNoMoreInteractions(stepRepository);
        verify(context).getGlobalTenantIndexName();
        verifyNoMoreInteractions(context); // verify that other indices are not read from the context therefore are not deleted
    }

    @Test
    public void shouldReportErrorWhenItIsNotPossibleToDeleteAllDocumentsFromTheIndex() {
        when(context.getGlobalTenantIndexName()).thenReturn(indexName);
        when(stepRepository.deleteAllDocuments(anyString())).thenReturn(deletionResponse);
        when(stepRepository.countDocuments(indexName)).thenReturn(numberOfDocuments);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(NOT_EMPTY_INDEX));
    }

}