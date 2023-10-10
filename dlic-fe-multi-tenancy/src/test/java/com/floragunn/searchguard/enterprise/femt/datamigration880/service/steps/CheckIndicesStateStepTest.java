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
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationConfig;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.TenantIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.UNHEALTHY_INDICES_ERROR;
import static java.time.ZoneOffset.UTC;
import static org.elasticsearch.cluster.health.ClusterHealthStatus.GREEN;
import static org.elasticsearch.cluster.health.ClusterHealthStatus.RED;
import static org.elasticsearch.cluster.health.ClusterHealthStatus.YELLOW;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CheckIndicesStateStepTest {

    private static final Logger log = LogManager.getLogger(CheckIndicesStateStepTest.class);

    private static final ZonedDateTime NOW = ZonedDateTime.of(LocalDateTime.of(1990, 1, 1, 1, 1), UTC);
    private static final Clock CLOCK = Clock.fixed(NOW.toInstant(), UTC);
    public static final String TENANT_INDEX_1 = "tenant_index_1";
    public static final String TENANT_INDEX_2 = "tenant_index_2";
    public static final String TENANT_INDEX_3 = "tenant_index_3";
    public static final String TENANT_NAME_1 = "tenant name 1";
    public static final String TENANT_NAME_2 = "tenant name 2";
    public static final String TENANT_NAME_3 = "tenant name 3";
    public static final MigrationConfig STRICT_CONFIGURATION = new MigrationConfig(false);
    public static final MigrationConfig LENIENT_CONFIG = new MigrationConfig(true);
    public static final String BACKUP_INDEX_1 = "backup_index_0001";

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private StepRepository repository;

    //under test
    private CheckIndicesStateStep step;

    @Before
    public void before() {
        this.step = new CheckIndicesStateStep(repository);
    }

    @Test
    public void shouldReportErrorWhenIndexStateIsUnavailable() {
        DataMigrationContext context = new DataMigrationContext(STRICT_CONFIGURATION, CLOCK);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(TENANT_INDEX_1, TENANT_NAME_1)));
        context.setBackupIndices(ImmutableList.empty());
        IndicesStatsResponse responseMock = repository.findIndexState(TENANT_INDEX_1);
        when(responseMock.getIndex(TENANT_INDEX_1)).thenReturn(null);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.details(), containsString("Cannot retrieve index"));
        assertThat(result.details(), containsString(TENANT_INDEX_1));
        assertThat(result.status(), equalTo(UNHEALTHY_INDICES_ERROR));
    }

    @Test
    public void shouldReturnSuccessResponseWhenIndexIsInGreenState() {
        DataMigrationContext context = new DataMigrationContext(STRICT_CONFIGURATION, CLOCK);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(TENANT_INDEX_1, TENANT_NAME_1)));
        context.setBackupIndices(ImmutableList.empty());
        IndexStats indexStateMock = repository.findIndexState(TENANT_INDEX_1).getIndex(TENANT_INDEX_1);
        when(indexStateMock.getHealth()).thenReturn(GREEN);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
    }

    @Test
    public void shouldSearchForDataAndBackupIndexState() {
        DataMigrationContext context = new DataMigrationContext(STRICT_CONFIGURATION, CLOCK);
        context.setTenantIndices(ImmutableList.empty());
        context.setBackupIndices(ImmutableList.of(BACKUP_INDEX_1));
        IndexStats indexStateMock = repository.findIndexState(BACKUP_INDEX_1).getIndex(BACKUP_INDEX_1);
        when(indexStateMock.getHealth()).thenReturn(GREEN);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
    }

    @Test
    public void shouldReturnFailureResponseWhenBackupIndexIsInYellowState() {
        DataMigrationContext context = new DataMigrationContext(STRICT_CONFIGURATION, CLOCK);
        context.setTenantIndices(ImmutableList.empty());
        context.setBackupIndices(ImmutableList.of(BACKUP_INDEX_1));
        IndexStats indexStateMock = repository.findIndexState(BACKUP_INDEX_1).getIndex(BACKUP_INDEX_1);
        when(indexStateMock.getHealth()).thenReturn(YELLOW);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
    }

    @Test
    public void shouldReturnFailureResponseWhenBackupIndexIsInYellowStateAndDataIndexIsInGreenState() {
        DataMigrationContext context = new DataMigrationContext(STRICT_CONFIGURATION, CLOCK);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(TENANT_INDEX_1, TENANT_NAME_1)));
        context.setBackupIndices(ImmutableList.of(BACKUP_INDEX_1));
        IndexStats indexStateTenantMock = Mockito.mock(IndexStats.class);
        IndexStats indexStateBackupMock = Mockito.mock(IndexStats.class);
        IndicesStatsResponse response = Mockito.mock(IndicesStatsResponse.class);
        when(repository.findIndexState(Mockito.any())).thenReturn(response);
        when(response.getIndex(TENANT_INDEX_1)).thenReturn(indexStateTenantMock);
        when(response.getIndex(BACKUP_INDEX_1)).thenReturn(indexStateBackupMock);
        when(indexStateTenantMock.getHealth()).thenReturn(GREEN);
        when(indexStateBackupMock.getHealth()).thenReturn(YELLOW);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.details(), containsString("Index 'backup_index_0001' status is 'YELLOW' but GREEN status is required"));
    }

    @Test
    public void shouldReturnFailureResponseWhenBackupIndexIsInGreenStateAndDataIndexIsInYellowState() {
        DataMigrationContext context = new DataMigrationContext(STRICT_CONFIGURATION, CLOCK);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(TENANT_INDEX_1, TENANT_NAME_1)));
        context.setBackupIndices(ImmutableList.of(BACKUP_INDEX_1));
        IndexStats indexStateTenantMock = Mockito.mock(IndexStats.class);
        IndexStats indexStateBackupMock = Mockito.mock(IndexStats.class);
        IndicesStatsResponse response = Mockito.mock(IndicesStatsResponse.class);
        when(repository.findIndexState(Mockito.any())).thenReturn(response);
        when(response.getIndex(TENANT_INDEX_1)).thenReturn(indexStateTenantMock);
        when(response.getIndex(BACKUP_INDEX_1)).thenReturn(indexStateBackupMock);
        when(indexStateTenantMock.getHealth()).thenReturn(YELLOW);
        when(indexStateBackupMock.getHealth()).thenReturn(GREEN);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.details(), containsString("Index 'tenant_index_1' status is 'YELLOW' but GREEN status is required"));
    }

    @Test
    public void shouldReturnFailureResponseWhenIndexIsInYellowStateInStrictConfiguration() {
        DataMigrationContext context = new DataMigrationContext(STRICT_CONFIGURATION, CLOCK);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(TENANT_INDEX_1, TENANT_NAME_1)));
        context.setBackupIndices(ImmutableList.empty());
        IndexStats indexStateMock = repository.findIndexState(TENANT_INDEX_1).getIndex(TENANT_INDEX_1);
        when(indexStateMock.getHealth()).thenReturn(YELLOW);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(UNHEALTHY_INDICES_ERROR));
        assertThat(result.details(), containsString(TENANT_INDEX_1));
        assertThat(result.details(), containsString(YELLOW.name()));
    }

    @Test
    public void shouldReturnFailureResponseWhenIndexIsInRedStateInStrictConfiguration() {
        DataMigrationContext context = new DataMigrationContext(STRICT_CONFIGURATION, CLOCK);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(TENANT_INDEX_1, TENANT_NAME_1)));
        context.setBackupIndices(ImmutableList.empty());
        IndexStats indexStateMock = repository.findIndexState(TENANT_INDEX_1).getIndex(TENANT_INDEX_1);
        when(indexStateMock.getHealth()).thenReturn(RED);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(UNHEALTHY_INDICES_ERROR));
        assertThat(result.details(), containsString(TENANT_INDEX_1));
        assertThat(result.details(), containsString(RED.name()));
    }

    @Test
    public void shouldReturnFailureResponseWhenIndexIsInRedStateInLenientConfiguration() {
        DataMigrationContext context = new DataMigrationContext(LENIENT_CONFIG, CLOCK);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(TENANT_INDEX_1, TENANT_NAME_1)));
        context.setBackupIndices(ImmutableList.empty());
        IndexStats indexStateMock = repository.findIndexState(TENANT_INDEX_1).getIndex(TENANT_INDEX_1);
        when(indexStateMock.getHealth()).thenReturn(RED);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(UNHEALTHY_INDICES_ERROR));
        assertThat(result.details(), containsString(TENANT_INDEX_1));
        assertThat(result.details(), containsString(RED.name()));
    }

    @Test
    public void shouldReturnSuccessResponseWhenIndexIsInYellowStateInLenientConfiguration() {
        DataMigrationContext context = new DataMigrationContext(LENIENT_CONFIG, CLOCK);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(TENANT_INDEX_1, TENANT_NAME_1)));
        context.setBackupIndices(ImmutableList.empty());
        IndexStats indexStateMock = repository.findIndexState(TENANT_INDEX_1).getIndex(TENANT_INDEX_1);
        when(indexStateMock.getHealth()).thenReturn(YELLOW);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
    }

    @Test
    public void shouldReturnSuccessResponseWhenAllIndicesAreGreen() {
        DataMigrationContext context = new DataMigrationContext(STRICT_CONFIGURATION, CLOCK);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(TENANT_INDEX_1, TENANT_NAME_1),
            new TenantIndex(TENANT_INDEX_2, TENANT_NAME_2),
            new TenantIndex(TENANT_INDEX_3, TENANT_NAME_3)));
        context.setBackupIndices(ImmutableList.empty());
        IndicesStatsResponse responseMock = repository.findIndexState(TENANT_INDEX_1, TENANT_INDEX_2, TENANT_INDEX_3);
        when(responseMock.getIndex(TENANT_INDEX_1).getHealth()).thenReturn(GREEN);
        when(responseMock.getIndex(TENANT_INDEX_2).getHealth()).thenReturn(GREEN);
        when(responseMock.getIndex(TENANT_INDEX_3).getHealth()).thenReturn(GREEN);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
    }

    @Test
    public void shouldReturnFailureResponseWhenLastIndexIsYellow() {
        DataMigrationContext context = new DataMigrationContext(STRICT_CONFIGURATION, CLOCK);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(TENANT_INDEX_1, TENANT_NAME_1),
            new TenantIndex(TENANT_INDEX_2, TENANT_NAME_2),
            new TenantIndex(TENANT_INDEX_3, TENANT_NAME_3)));
        context.setBackupIndices(ImmutableList.empty());
        IndicesStatsResponse responseMock = repository.findIndexState(TENANT_INDEX_1, TENANT_INDEX_2, TENANT_INDEX_3);
        when(responseMock.getIndex(TENANT_INDEX_1).getHealth()).thenReturn(GREEN);
        when(responseMock.getIndex(TENANT_INDEX_2).getHealth()).thenReturn(GREEN);
        when(responseMock.getIndex(TENANT_INDEX_3).getHealth()).thenReturn(YELLOW);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(UNHEALTHY_INDICES_ERROR));
        assertThat(result.details(), containsString(TENANT_INDEX_3));
        assertThat(result.details(), containsString(YELLOW.name()));
    }

    @Test
    public void shouldReturnFailureResponseWhenTwoIndicesAreYellow() {
        DataMigrationContext context = new DataMigrationContext(STRICT_CONFIGURATION, CLOCK);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(TENANT_INDEX_1, TENANT_NAME_1),
            new TenantIndex(TENANT_INDEX_2, TENANT_NAME_2),
            new TenantIndex(TENANT_INDEX_3, TENANT_NAME_3)));
        context.setBackupIndices(ImmutableList.empty());
        IndicesStatsResponse responseMock = repository.findIndexState(TENANT_INDEX_1, TENANT_INDEX_2, TENANT_INDEX_3);
        when(responseMock.getIndex(TENANT_INDEX_1).getHealth()).thenReturn(GREEN);
        when(responseMock.getIndex(TENANT_INDEX_2).getHealth()).thenReturn(YELLOW);
        when(responseMock.getIndex(TENANT_INDEX_3).getHealth()).thenReturn(YELLOW);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(UNHEALTHY_INDICES_ERROR));
        assertThat(result.details(), containsString(TENANT_INDEX_2));
        assertThat(result.details(), containsString(TENANT_INDEX_3));
        assertThat(result.details(), containsString(YELLOW.name()));
    }

    @Test
    public void shouldReturnSuccessResponseWhenAllIndicesAreYellowInLenientMode() {
        DataMigrationContext context = new DataMigrationContext(LENIENT_CONFIG, CLOCK);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(TENANT_INDEX_1, TENANT_NAME_1),
            new TenantIndex(TENANT_INDEX_2, TENANT_NAME_2),
            new TenantIndex(TENANT_INDEX_3, TENANT_NAME_3)));
        context.setBackupIndices(ImmutableList.empty());
        IndicesStatsResponse responseMock = repository.findIndexState(TENANT_INDEX_1, TENANT_INDEX_2, TENANT_INDEX_3);
        when(responseMock.getIndex(TENANT_INDEX_1).getHealth()).thenReturn(YELLOW);
        when(responseMock.getIndex(TENANT_INDEX_2).getHealth()).thenReturn(YELLOW);
        when(responseMock.getIndex(TENANT_INDEX_3).getHealth()).thenReturn(YELLOW);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
    }

    @Test
    public void shouldReturnFailureResponseWhenIndicesContainThreeColorsInLenientMode() {
        DataMigrationContext context = new DataMigrationContext(LENIENT_CONFIG, CLOCK);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(TENANT_INDEX_1, TENANT_NAME_1),
            new TenantIndex(TENANT_INDEX_2, TENANT_NAME_2),
            new TenantIndex(TENANT_INDEX_3, TENANT_NAME_3)));
        context.setBackupIndices(ImmutableList.empty());
        IndicesStatsResponse responseMock = repository.findIndexState(TENANT_INDEX_1, TENANT_INDEX_2, TENANT_INDEX_3);
        when(responseMock.getIndex(TENANT_INDEX_1).getHealth()).thenReturn(GREEN);
        when(responseMock.getIndex(TENANT_INDEX_2).getHealth()).thenReturn(YELLOW);
        when(responseMock.getIndex(TENANT_INDEX_3).getHealth()).thenReturn(RED);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(UNHEALTHY_INDICES_ERROR));
        assertThat(result.details(), containsString(TENANT_INDEX_3));
        assertThat(result.details(), containsString(RED.name()));
    }

    @Test
    public void shouldReturnFailureResponseWhenIndicesContainThreeColorsInStrictMode() {
        DataMigrationContext context = new DataMigrationContext(STRICT_CONFIGURATION, CLOCK);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(TENANT_INDEX_1, TENANT_NAME_1),
            new TenantIndex(TENANT_INDEX_2, TENANT_NAME_2),
            new TenantIndex(TENANT_INDEX_3, TENANT_NAME_3)));
        context.setBackupIndices(ImmutableList.empty());
        IndicesStatsResponse responseMock = repository.findIndexState(TENANT_INDEX_1, TENANT_INDEX_2, TENANT_INDEX_3);
        when(responseMock.getIndex(TENANT_INDEX_1).getHealth()).thenReturn(GREEN);
        when(responseMock.getIndex(TENANT_INDEX_2).getHealth()).thenReturn(YELLOW);
        when(responseMock.getIndex(TENANT_INDEX_3).getHealth()).thenReturn(RED);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(UNHEALTHY_INDICES_ERROR));
        assertThat(result.details(), containsString(TENANT_INDEX_2));
        assertThat(result.details(), containsString(TENANT_INDEX_3));
        assertThat(result.details(), containsString(RED.name()));
        assertThat(result.details(), containsString(YELLOW.name()));
    }

    @Test
    public void shouldReturnFailureResponseWhenAllIndicesAreRedInStrictMode() {
        DataMigrationContext context = new DataMigrationContext(STRICT_CONFIGURATION, CLOCK);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(TENANT_INDEX_1, TENANT_NAME_1),
            new TenantIndex(TENANT_INDEX_2, TENANT_NAME_2),
            new TenantIndex(TENANT_INDEX_3, TENANT_NAME_3)));
        context.setBackupIndices(ImmutableList.empty());
        IndicesStatsResponse responseMock = repository.findIndexState(TENANT_INDEX_1, TENANT_INDEX_2, TENANT_INDEX_3);
        when(responseMock.getIndex(TENANT_INDEX_1).getHealth()).thenReturn(RED);
        when(responseMock.getIndex(TENANT_INDEX_2).getHealth()).thenReturn(RED);
        when(responseMock.getIndex(TENANT_INDEX_3).getHealth()).thenReturn(RED);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(UNHEALTHY_INDICES_ERROR));
        assertThat(result.details(), containsString(TENANT_INDEX_1));
        assertThat(result.details(), containsString(TENANT_INDEX_2));
        assertThat(result.details(), containsString(TENANT_INDEX_3));
        assertThat(result.details(), containsString(RED.name()));
    }

}