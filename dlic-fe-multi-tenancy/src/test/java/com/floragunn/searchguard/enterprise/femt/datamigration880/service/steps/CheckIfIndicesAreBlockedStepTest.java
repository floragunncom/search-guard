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
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationConfig;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.TenantIndex;
import com.floragunn.searchsupport.util.EsLogging;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.DATA_INDICES_LOCKED_ERROR;
import static java.time.ZoneOffset.UTC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CheckIfIndicesAreBlockedStepTest {

    static {
        IndexMetadata.builder("workaround to avoid problems related to static init of enum APIBlock");
        EsLogging.initLogging();
    }

    private static final ZonedDateTime NOW = ZonedDateTime.of(LocalDateTime.of(1990, 1, 1, 1, 1), UTC);
    private static final Clock CLOCK = Clock.fixed(NOW.toInstant(), UTC);
    public static final String INDEX_NAME_1 = "index-name-1";
    public static final String INDEX_NAME_2 = "index-name-2";

    @Mock
    private StepRepository repository;

    @Mock
    private GetSettingsResponse settingsResponse;

    private DataMigrationContext context;

    // under tests
    private CheckIfIndicesAreBlockedStep step;

    @Before
    public void before() {
        this.context = new DataMigrationContext(new MigrationConfig(false), CLOCK);
        this.step = new CheckIfIndicesAreBlockedStep(repository);
    }

    @Test
    public void shouldAccomplishSuccessfullyWhenIndicesAreNotBlocked() {
        context.setTenantIndices(ImmutableList.of(new TenantIndex(INDEX_NAME_1, "tenant name")));
        when(repository.getIndexSettings(context.getDataIndicesNames().toArray(String[]::new))).thenReturn(settingsResponse);
        Settings settings = Settings.builder().put("index.blocks.write", false).build();
        when(settingsResponse.getIndexToSettings()).thenReturn(ImmutableMap.of(INDEX_NAME_1, settings));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
    }

    @Test
    public void shouldAccomplishSuccessfullyWhenBlockSettingsAreNotPresent() {
        context.setTenantIndices(ImmutableList.of(new TenantIndex(INDEX_NAME_1, "tenant name")));
        when(repository.getIndexSettings(context.getDataIndicesNames().toArray(String[]::new))).thenReturn(settingsResponse);
        Settings settings = Settings.builder().put("index.number_of_replicas", 1).build();
        when(settingsResponse.getIndexToSettings()).thenReturn(ImmutableMap.of(INDEX_NAME_1, settings));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
    }

    @Test
    public void shouldAccomplishSuccessfullyWhenBlockSettingsAreNotPresentForTheFirstIndex() {
        TenantIndex tenantIndexOne = new TenantIndex(INDEX_NAME_1, "tenant name");
        TenantIndex tenantIndexTwo = new TenantIndex(INDEX_NAME_2, "no name");
        ImmutableList<TenantIndex> tenantIndices = ImmutableList.of(tenantIndexOne, tenantIndexTwo);
        context.setTenantIndices(tenantIndices);
        when(repository.getIndexSettings(context.getDataIndicesNames().toArray(String[]::new))).thenReturn(settingsResponse);
        Settings settingsOne = Settings.builder().put("index.number_of_replicas", 1).build();
        Settings settingsTwo = Settings.builder().put("index.blocks.write", false).build();
        when(settingsResponse.getIndexToSettings()).thenReturn(ImmutableMap.of(INDEX_NAME_1, settingsOne, INDEX_NAME_2, settingsTwo));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
    }

    @Test
    public void shouldReturnErrorWhenSettingsForIndexAreNotFound() {
        context.setTenantIndices(ImmutableList.of(new TenantIndex(INDEX_NAME_1, "tenant name")));
        when(repository.getIndexSettings(context.getDataIndicesNames().toArray(String[]::new))).thenReturn(settingsResponse);
        when(settingsResponse.getIndexToSettings()).thenReturn(ImmutableMap.empty());

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(DATA_INDICES_LOCKED_ERROR));
        assertThat(result.details(), containsString("Settings for index"));
        assertThat(result.details(), containsString(INDEX_NAME_1));
        assertThat(result.details(), containsString("are not available"));
    }

    @Test
    public void shouldReturnErrorWhenTheSecondIndexIsBlocked() {
        TenantIndex tenantIndexOne = new TenantIndex(INDEX_NAME_1, "tenant name");
        TenantIndex tenantIndexTwo = new TenantIndex(INDEX_NAME_2, "no name");
        ImmutableList<TenantIndex> tenantIndices = ImmutableList.of(tenantIndexOne, tenantIndexTwo);
        context.setTenantIndices(tenantIndices);
        when(repository.getIndexSettings(context.getDataIndicesNames().toArray(String[]::new))).thenReturn(settingsResponse);
        Settings settingsOne = Settings.builder().put("index.number_of_replicas", 1).build();
        Settings settingsTwo = Settings.builder().put("index.blocks.write", true).build();
        when(settingsResponse.getIndexToSettings()).thenReturn(ImmutableMap.of(INDEX_NAME_1, settingsOne, INDEX_NAME_2, settingsTwo));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(DATA_INDICES_LOCKED_ERROR));
    }

    @Test
    public void shouldHandleIndicesWithBlockedMetadata() {
        context.setTenantIndices(ImmutableList.of(new TenantIndex(INDEX_NAME_1, "tenant name")));
        when(repository.getIndexSettings(context.getDataIndicesNames().toArray(String[]::new))).thenThrow(ClusterBlockException.class);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(DATA_INDICES_LOCKED_ERROR));
    }
}