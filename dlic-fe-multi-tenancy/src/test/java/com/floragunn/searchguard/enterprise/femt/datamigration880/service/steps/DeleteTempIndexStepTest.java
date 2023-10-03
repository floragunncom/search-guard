package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import org.elasticsearch.common.Strings;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class DeleteTempIndexStepTest {

    private StepRepository repository;

    private DataMigrationContext context;

    // under tests
    private DeleteTempIndexStep step;

    private final String indexNameParameter;

    public DeleteTempIndexStepTest(String indexNameParameter) {
        Strings.requireNonEmpty(indexNameParameter, "Index name is required");
        this.indexNameParameter = indexNameParameter;
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { "data_migration_temp_fe_1990_01_01_01_01_00" },
            { "data_migration_temp_fe_1993_01_01_01_01_00" },
            { "data_migration_temp_fe_1993_03_02_09_08_07" },
            { "data_migration_temp_fe_1995_12_11_10_15_59" },
            { "data_migration_temp_fe_1999_09_09_09_09_09" },
            { "data_migration_temp_fe_2001_05_03_07_16_23" },
            { "data_migration_temp_fe_2000_01_01_01_01_00" },
            { "another_index_name" },
            { "index_for_deletion" }
        });
    }

    @Before
    public void before() {
        this.repository = mock(StepRepository.class);
        this.context = mock(DataMigrationContext.class);
        this.step = new DeleteTempIndexStep(repository);
    }

    @Test
    public void shouldDeleteOnlyTempIndex() {
        when(context.getTempIndexName()).thenReturn(indexNameParameter);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        verify(repository).deleteIndices(indexNameParameter);
        verifyNoMoreInteractions(repository);
        verify(context).getTempIndexName();
        verifyNoMoreInteractions(context);
    }
}