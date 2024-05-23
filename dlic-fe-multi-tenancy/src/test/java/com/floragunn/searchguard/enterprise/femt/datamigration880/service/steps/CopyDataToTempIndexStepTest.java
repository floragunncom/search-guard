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
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationConfig;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.TenantIndex;
import org.elasticsearch.search.SearchHit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.function.Consumer;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.BACKUP_FROM_PREVIOUS_MIGRATION_NOT_AVAILABLE_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.BACKUP_INDICES_CONTAIN_MIGRATION_MARKER;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.DOCUMENT_ALREADY_EXISTS_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.DOCUMENT_ALREADY_MIGRATED_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.INCORRECT_INDEX_NAME_PREFIX_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.UNKNOWN_USER_PRIVATE_TENANT_NAME_ERROR;
import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static java.time.ZoneOffset.UTC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CopyDataToTempIndexStepTest {

    private static final ZonedDateTime NOW = ZonedDateTime.of(LocalDateTime.of(1993, 1, 1, 1, 7), UTC);
    private static final Clock CLOCK = Clock.fixed(NOW.toInstant(), UTC);
    public static final TenantIndex GLOBAL_TENANT = new TenantIndex("global", Tenant.GLOBAL_TENANT_ID);
    public static final String BACKUP_INDEX_NAME_1 = "backup_index_1";
    public static final String BACKUP_INDEX_NAME_2 = "backup_index_2";
    public static final String BACKUP_INDEX_NAME_3 = "backup_index_3";
    public static final TenantIndex TENANT_INDEX_1 = new TenantIndex("frontend_data_index_1", "tenant-name-1");
    public static final TenantIndex TENANT_INDEX_2 = new TenantIndex("frontend_data_index_2", "tenant-name-2");
    public static final TenantIndex TENANT_INDEX_3 = new TenantIndex("frontend_data_index_3", "tenant-name-3");
    @Mock
    private StepRepository repository;
    @Mock
    private FeMultiTenancyConfigurationProvider configProvider;

    @Mock
    private IndexSettingsManager indexSettingsManager;

    private DataMigrationContext context;

    // under test
    private CopyDataToTempIndexStep step;

    @Before
    public void before() {
        this.context = new DataMigrationContext(new MigrationConfig(false), CLOCK);
        this.step = new CopyDataToTempIndexStep(repository, configProvider, indexSettingsManager);
    }

    @Test
    public void shouldDetectDuplicatedId() {
        context.setTenantIndices(ImmutableList.of(GLOBAL_TENANT, TENANT_INDEX_1));
        ImmutableList<SearchHit> hits = ImmutableList.of(new SearchHit(1, "space:default"), new SearchHit(2, "space:default"));
        doAnswer(new ProvideSearchHitsAnswer(hits)).when(repository).forEachDocumentInIndex(anyString(), anyInt(), any(Consumer.class));

        StepException exception = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));

        assertThat(exception.getStatus(), equalTo(DOCUMENT_ALREADY_EXISTS_ERROR));
    }

    @Test
    public void shouldDetectAlreadyMigratedDocument() {
        context.setTenantIndices(ImmutableList.of(GLOBAL_TENANT, TENANT_INDEX_1));
        SearchHit hitOne = new SearchHit(1, "space:default__sg_ten___3292183_kirk_8.7.0_001");
        SearchHit hitTwo = new SearchHit(2, "space:default");
        ImmutableList<SearchHit> hits = ImmutableList.of(hitOne, hitTwo);
        doAnswer(new ProvideSearchHitsAnswer(hits)).when(repository).forEachDocumentInIndex(anyString(), anyInt(), any(Consumer.class));

        StepException exception = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));

        assertThat(exception.getStatus(), equalTo(DOCUMENT_ALREADY_MIGRATED_ERROR));
    }

    @Test
    public void shouldDetectIncorrectIndexNamePrefix() {
        String indexName = "incorrect-index-name-without-prefix";
        context.setTenantIndices(ImmutableList.of(GLOBAL_TENANT, new TenantIndex(indexName, null)));
        when(configProvider.getKibanaIndex()).thenReturn("required-index-name-prefix");
        ImmutableMap<String, Object> searchHitMap = ImmutableMap.of("_index", indexName, "_id", "space:default");
        SearchHit searchHit = SearchHit.createFromMap(searchHitMap);
        ImmutableList<SearchHit> hits = ImmutableList.of(searchHit);
        doAnswer(new ProvideSearchHitsAnswer(hits)).when(repository).forEachDocumentInIndex(anyString(), anyInt(), any(Consumer.class));

        StepException exception = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));

        assertThat(exception.getStatus(), equalTo(INCORRECT_INDEX_NAME_PREFIX_ERROR));
    }

    @Test
    public void shouldReportErrorWhenItIsNotPossibleToGetPrivateTenantName() {
        String prefix = "required-index-name-prefix";
        String indexName = prefix + "-one";
        context.setTenantIndices(ImmutableList.of(GLOBAL_TENANT, new TenantIndex(indexName, null)));
        when(configProvider.getKibanaIndex()).thenReturn(prefix);
        ImmutableMap<String, Object> searchHitMap = ImmutableMap.of("_index", indexName, "_id", "space:default");
        SearchHit searchHit = SearchHit.createFromMap(searchHitMap);
        ImmutableList<SearchHit> hits = ImmutableList.of(searchHit);
        doAnswer(new ProvideSearchHitsAnswer(hits)).when(repository).forEachDocumentInIndex(anyString(), anyInt(), any(Consumer.class));

        StepException exception = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));

        assertThat(exception.getStatus(), equalTo(UNKNOWN_USER_PRIVATE_TENANT_NAME_ERROR));
    }

    @Test
    public void shouldFlushAndRefreshTempIndex() {
        String prefix = ".openfind";
        String indexName = prefix + "_3292183_kirk_8.7.0_003";
        context.setTenantIndices(ImmutableList.of(GLOBAL_TENANT, new TenantIndex(indexName, null)));
        when(configProvider.getKibanaIndex()).thenReturn(prefix);
        ImmutableMap<String, Object> searchHitMap = ImmutableMap.of("_index", indexName, "_id", "space:default");
        SearchHit searchHit = SearchHit.createFromMap(searchHitMap);
        ImmutableList<SearchHit> hits = ImmutableList.of(searchHit);
        doAnswer(new ProvideSearchHitsAnswer(hits)).when(repository).forEachDocumentInIndex(anyString(), anyInt(), any(Consumer.class));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        verify(repository).flushIndex("data_migration_temp_fe_1993_01_01_01_07_00");
        verify(repository).refreshIndex("data_migration_temp_fe_1993_01_01_01_07_00");
    }

    @Test
    public void shouldUseOnlyGlobalIndexAsDataSource() {
        context.setTenantIndices(ImmutableList.of(GLOBAL_TENANT));
        when(indexSettingsManager.isMigrationMarkerPresent(GLOBAL_TENANT.indexName())).thenReturn(false);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        verify(repository).forEachDocumentInIndex(eq(GLOBAL_TENANT.indexName()), anyInt(), any(Consumer.class));
        verify(repository).refreshIndex(context.getTempIndexName());
        verify(repository).flushIndex(context.getTempIndexName());
        verifyNoMoreInteractions(repository);
        verify(indexSettingsManager).isMigrationMarkerPresent(GLOBAL_TENANT.indexName());
        verifyNoMoreInteractions(indexSettingsManager);
    }

    @Test
    public void shouldUseManyTenantIndicesAsDataSource() {
        context.setTenantIndices(ImmutableList.of(GLOBAL_TENANT, TENANT_INDEX_1, TENANT_INDEX_2, TENANT_INDEX_3));
        when(indexSettingsManager.isMigrationMarkerPresent(GLOBAL_TENANT.indexName())).thenReturn(false);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        verify(repository).forEachDocumentInIndex(eq(GLOBAL_TENANT.indexName()), anyInt(), any(Consumer.class));
        verify(repository).forEachDocumentInIndex(eq(TENANT_INDEX_1.indexName()), anyInt(), any(Consumer.class));
        verify(repository).forEachDocumentInIndex(eq(TENANT_INDEX_2.indexName()), anyInt(), any(Consumer.class));
        verify(repository).forEachDocumentInIndex(eq(TENANT_INDEX_3.indexName()), anyInt(), any(Consumer.class));
        verify(repository).refreshIndex(context.getTempIndexName());
        verify(repository).flushIndex(context.getTempIndexName());
        verifyNoMoreInteractions(repository);
        verify(indexSettingsManager).isMigrationMarkerPresent(GLOBAL_TENANT.indexName());
        verifyNoMoreInteractions(indexSettingsManager);
    }

    @Test
    public void shouldReportErrorWhenGlobalTenantIsMissing() {
        context.setTenantIndices(ImmutableList.of(TENANT_INDEX_1, TENANT_INDEX_2, TENANT_INDEX_3));

        assertThatThrown(() -> step.execute(context), instanceOf(IllegalStateException.class));
    }

    @Test
    public void shouldUseGlobalTenantIndexAsDataSource() {
        context.setTenantIndices(ImmutableList.of(GLOBAL_TENANT, TENANT_INDEX_1));
        context.setBackupIndices(ImmutableList.of(BACKUP_INDEX_NAME_1, BACKUP_INDEX_NAME_2, BACKUP_INDEX_NAME_3));
        when(indexSettingsManager.isMigrationMarkerPresent(GLOBAL_TENANT.indexName())).thenReturn(false);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        verify(repository).forEachDocumentInIndex(eq(TENANT_INDEX_1.indexName()), anyInt(), any(Consumer.class));
        verify(repository).forEachDocumentInIndex(eq(GLOBAL_TENANT.indexName()), anyInt(), any(Consumer.class));
        verify(repository).refreshIndex(context.getTempIndexName());
        verify(repository).flushIndex(context.getTempIndexName());
        verifyNoMoreInteractions(repository);
        verify(indexSettingsManager).isMigrationMarkerPresent(GLOBAL_TENANT.indexName());
        verifyNoMoreInteractions(indexSettingsManager);
    }

    @Test
    public void shouldNotRequireBackupIndexListWhenGlobalIndexDoesNotContainMigrationMarker() {
        context.setTenantIndices(ImmutableList.of(GLOBAL_TENANT, TENANT_INDEX_1));
        context.setBackupIndices(null);//backup index should not be read
        when(indexSettingsManager.isMigrationMarkerPresent(GLOBAL_TENANT.indexName())).thenReturn(false);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        verify(repository).forEachDocumentInIndex(eq(TENANT_INDEX_1.indexName()), anyInt(), any(Consumer.class));
        verify(repository).forEachDocumentInIndex(eq(GLOBAL_TENANT.indexName()), anyInt(), any(Consumer.class));
        verify(repository).refreshIndex(context.getTempIndexName());
        verify(repository).flushIndex(context.getTempIndexName());
        verifyNoMoreInteractions(repository);
        verify(indexSettingsManager).isMigrationMarkerPresent(GLOBAL_TENANT.indexName());
        verifyNoMoreInteractions(indexSettingsManager);
    }

    @Test
    public void shouldUseTheOnlyBackupIndexAsDataSource() {
        context.setTenantIndices(ImmutableList.of(GLOBAL_TENANT, TENANT_INDEX_1));
        context.setBackupIndices(ImmutableList.of(BACKUP_INDEX_NAME_1));
        when(indexSettingsManager.isMigrationMarkerPresent(GLOBAL_TENANT.indexName())).thenReturn(true);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        verify(repository).forEachDocumentInIndex(eq(BACKUP_INDEX_NAME_1), anyInt(), any(Consumer.class));
        verify(repository).forEachDocumentInIndex(eq(TENANT_INDEX_1.indexName()), anyInt(), any(Consumer.class));
        verify(repository).refreshIndex(context.getTempIndexName());
        verify(repository).flushIndex(context.getTempIndexName());
        verifyNoMoreInteractions(repository);
        verify(indexSettingsManager).isMigrationMarkerPresent(GLOBAL_TENANT.indexName());
        verify(indexSettingsManager).isMigrationMarkerPresent(BACKUP_INDEX_NAME_1);
        verifyNoMoreInteractions(indexSettingsManager);
    }

    @Test
    public void shouldUseTheFirstBackupIndexAsDataSource() {
        context.setTenantIndices(ImmutableList.of(GLOBAL_TENANT, TENANT_INDEX_1));
        context.setBackupIndices(ImmutableList.of(BACKUP_INDEX_NAME_1, BACKUP_INDEX_NAME_2, BACKUP_INDEX_NAME_3));
        when(indexSettingsManager.isMigrationMarkerPresent(GLOBAL_TENANT.indexName())).thenReturn(true);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        verify(repository).forEachDocumentInIndex(eq(BACKUP_INDEX_NAME_1), anyInt(), any(Consumer.class));
        verify(repository).forEachDocumentInIndex(eq(TENANT_INDEX_1.indexName()), anyInt(), any(Consumer.class));
        verify(repository).refreshIndex(context.getTempIndexName());
        verify(repository).flushIndex(context.getTempIndexName());
        verifyNoMoreInteractions(repository);
        verify(indexSettingsManager).isMigrationMarkerPresent(GLOBAL_TENANT.indexName());
        verify(indexSettingsManager).isMigrationMarkerPresent(BACKUP_INDEX_NAME_1);
        verifyNoMoreInteractions(indexSettingsManager);
    }

    @Test
    public void shouldUseTheSecondBackupIndexAsDataSource() {
        context.setTenantIndices(ImmutableList.of(GLOBAL_TENANT, TENANT_INDEX_1));
        context.setBackupIndices(ImmutableList.of(BACKUP_INDEX_NAME_1, BACKUP_INDEX_NAME_2, BACKUP_INDEX_NAME_3));
        when(indexSettingsManager.isMigrationMarkerPresent(GLOBAL_TENANT.indexName())).thenReturn(true);
        when(indexSettingsManager.isMigrationMarkerPresent(BACKUP_INDEX_NAME_1)).thenReturn(true);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        verify(repository).forEachDocumentInIndex(eq(BACKUP_INDEX_NAME_2), anyInt(), any(Consumer.class));
        verify(repository).forEachDocumentInIndex(eq(TENANT_INDEX_1.indexName()), anyInt(), any(Consumer.class));
        verify(repository).refreshIndex(context.getTempIndexName());
        verify(repository).flushIndex(context.getTempIndexName());
        verifyNoMoreInteractions(repository);
        verify(indexSettingsManager).isMigrationMarkerPresent(GLOBAL_TENANT.indexName());
        verify(indexSettingsManager).isMigrationMarkerPresent(BACKUP_INDEX_NAME_1);
        verify(indexSettingsManager).isMigrationMarkerPresent(BACKUP_INDEX_NAME_2);
        verifyNoMoreInteractions(indexSettingsManager);
    }

    @Test
    public void shouldUseTheThirdBackupIndexAsDataSource() {
        context.setTenantIndices(ImmutableList.of(GLOBAL_TENANT, TENANT_INDEX_1));
        context.setBackupIndices(ImmutableList.of(BACKUP_INDEX_NAME_1, BACKUP_INDEX_NAME_2, BACKUP_INDEX_NAME_3));
        when(indexSettingsManager.isMigrationMarkerPresent(GLOBAL_TENANT.indexName())).thenReturn(true);
        when(indexSettingsManager.isMigrationMarkerPresent(BACKUP_INDEX_NAME_1)).thenReturn(true);
        when(indexSettingsManager.isMigrationMarkerPresent(BACKUP_INDEX_NAME_2)).thenReturn(true);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        verify(repository).forEachDocumentInIndex(eq(BACKUP_INDEX_NAME_3), anyInt(), any(Consumer.class));
        verify(repository).forEachDocumentInIndex(eq(TENANT_INDEX_1.indexName()), anyInt(), any(Consumer.class));
        verify(repository).refreshIndex(context.getTempIndexName());
        verify(repository).flushIndex(context.getTempIndexName());
        verifyNoMoreInteractions(repository);
        verify(indexSettingsManager).isMigrationMarkerPresent(GLOBAL_TENANT.indexName());
        verify(indexSettingsManager).isMigrationMarkerPresent(BACKUP_INDEX_NAME_1);
        verify(indexSettingsManager).isMigrationMarkerPresent(BACKUP_INDEX_NAME_2);
        verify(indexSettingsManager).isMigrationMarkerPresent(BACKUP_INDEX_NAME_3);
        verifyNoMoreInteractions(indexSettingsManager);
    }

    @Test
    public void shouldReportErrorWhenGlobalIndexAndItsBackupsContainMigratedData() {
        context.setTenantIndices(ImmutableList.of(GLOBAL_TENANT, TENANT_INDEX_1));
        context.setBackupIndices(ImmutableList.of(BACKUP_INDEX_NAME_1, BACKUP_INDEX_NAME_2, BACKUP_INDEX_NAME_3));
        when(indexSettingsManager.isMigrationMarkerPresent(GLOBAL_TENANT.indexName())).thenReturn(true);
        when(indexSettingsManager.isMigrationMarkerPresent(BACKUP_INDEX_NAME_1)).thenReturn(true);
        when(indexSettingsManager.isMigrationMarkerPresent(BACKUP_INDEX_NAME_2)).thenReturn(true);
        when(indexSettingsManager.isMigrationMarkerPresent(BACKUP_INDEX_NAME_3)).thenReturn(true);

        StepException ex = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));

        assertThat(ex.getStatus(), equalTo(BACKUP_INDICES_CONTAIN_MIGRATION_MARKER));
    }

    @Test
    public void shouldReportErrorWhenGlobalTenantIndexContainMigratedDataAndBackupIndicesAreMissing() {
        context.setTenantIndices(ImmutableList.of(GLOBAL_TENANT, TENANT_INDEX_1));
        context.setBackupIndices(ImmutableList.empty());
        when(indexSettingsManager.isMigrationMarkerPresent(GLOBAL_TENANT.indexName())).thenReturn(true);

        StepException ex = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));

        assertThat(ex.getStatus(), equalTo(BACKUP_FROM_PREVIOUS_MIGRATION_NOT_AVAILABLE_ERROR));
    }

    @Test
    public void shouldReportErrorWhenGlobalTenantIndexContainMigratedDataAndBackupIndicesAreEqualToNull() {
        context.setTenantIndices(ImmutableList.of(GLOBAL_TENANT, TENANT_INDEX_1));
        context.setBackupIndices(null);
        when(indexSettingsManager.isMigrationMarkerPresent(GLOBAL_TENANT.indexName())).thenReturn(true);

        StepException ex = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));

        assertThat(ex.getStatus(), equalTo(BACKUP_FROM_PREVIOUS_MIGRATION_NOT_AVAILABLE_ERROR));
    }

    private static class ProvideSearchHitsAnswer implements Answer<Void> {

        private final ImmutableList<SearchHit> hits;

        private ProvideSearchHitsAnswer(ImmutableList<SearchHit> hits) {
            this.hits = Objects.requireNonNull(hits, "Search hits are required");
        }

        @Override
        public Void answer(InvocationOnMock invocation) {
            Consumer<ImmutableList<SearchHit>> consumer = invocation.getArgument(2);
            consumer.accept(hits);
            return null;
        }
    }
}