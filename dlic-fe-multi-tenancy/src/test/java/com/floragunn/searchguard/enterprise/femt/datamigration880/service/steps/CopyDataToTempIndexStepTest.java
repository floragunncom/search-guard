package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
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

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.DOCUMENT_ALREADY_EXISTS_ERROR;
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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CopyDataToTempIndexStepTest {

    private static final ZonedDateTime NOW = ZonedDateTime.of(LocalDateTime.of(1993, 1, 1, 1, 7), UTC);
    private static final Clock CLOCK = Clock.fixed(NOW.toInstant(), UTC);
    @Mock
    private StepRepository repository;
    @Mock
    private FeMultiTenancyConfigurationProvider configProvider;

    private DataMigrationContext context;

    // under test
    private CopyDataToTempIndexStep step;


    @Before
    public void before() {
        this.context = new DataMigrationContext(new MigrationConfig(false), CLOCK);
        this.step = new CopyDataToTempIndexStep(repository, configProvider);
    }

    @Test
    public void shouldDetectDuplicatedId() {
        context.setTenantIndices(ImmutableList.of(new TenantIndex("frontend-data-index", "tenant-name")));
        ImmutableList<SearchHit> hits = ImmutableList.of(new SearchHit(1, "space:default"), new SearchHit(2, "space:default"));
        doAnswer(new ProvideSearchHitsAnswer(hits)).when(repository).forEachDocumentInIndex(anyString(), anyInt(), any(Consumer.class));

        StepException exception = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));

        assertThat(exception.getStatus(), equalTo(DOCUMENT_ALREADY_EXISTS_ERROR));
    }

    @Test
    public void shouldDetectIncorrectIndexNamePrefix() {
        String indexName = "incorrect-index-name-without-prefix";
        context.setTenantIndices(ImmutableList.of(new TenantIndex(indexName, null)));
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
        context.setTenantIndices(ImmutableList.of(new TenantIndex(indexName, null)));
        when(configProvider.getKibanaIndex()).thenReturn(prefix);
        ImmutableMap<String, Object> searchHitMap = ImmutableMap.of("_index", indexName, "_id", "space:default");
        SearchHit searchHit = SearchHit.createFromMap(searchHitMap);
        ImmutableList<SearchHit> hits = ImmutableList.of(searchHit);
        doAnswer(new ProvideSearchHitsAnswer(hits)).when(repository).forEachDocumentInIndex(anyString(), anyInt(), any(Consumer.class));

        StepException exception = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));

        assertThat(exception.getStatus(), equalTo(UNKNOWN_USER_PRIVATE_TENANT_NAME_ERROR));
    }

    @Test
    public void shouldFlushTempIndex() {
        String prefix = ".openfind";
        String indexName = prefix + "_3292183_kirk_8.7.0_003";
        context.setTenantIndices(ImmutableList.of(new TenantIndex(indexName, null)));
        when(configProvider.getKibanaIndex()).thenReturn(prefix);
        ImmutableMap<String, Object> searchHitMap = ImmutableMap.of("_index", indexName, "_id", "space:default");
        SearchHit searchHit = SearchHit.createFromMap(searchHitMap);
        ImmutableList<SearchHit> hits = ImmutableList.of(searchHit);
        doAnswer(new ProvideSearchHitsAnswer(hits)).when(repository).forEachDocumentInIndex(anyString(), anyInt(), any(Consumer.class));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        verify(repository).flushIndex("data_migration_temp_fe_1993_01_01_01_07_00");
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