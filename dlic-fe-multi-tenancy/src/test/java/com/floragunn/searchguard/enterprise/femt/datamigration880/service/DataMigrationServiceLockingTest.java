package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.persistence.IndexMigrationStateRepository;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.StepsFactory;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchsupport.action.StandardResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.UNEXPECTED_ERROR;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static org.apache.http.HttpStatus.SC_BAD_REQUEST;
import static org.apache.http.HttpStatus.SC_CONFLICT;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_PRECONDITION_FAILED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DataMigrationServiceLockingTest {

    private static final Logger log = LogManager.getLogger(DataMigrationServiceLockingTest.class);

    private static final ZonedDateTime NOW = ZonedDateTime.of(LocalDateTime.of(2010, 1, 1, 12, 1), ZoneOffset.UTC);

    private ExecutorService executor;

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
        .sslEnabled()
        .resources("multitenancy")
        .enterpriseModulesEnabled()
        .build();

    @Mock
    private StepsFactory stepFactory1;

    @Mock
    private StepsFactory stepFactory2;

    private MigrationStateRepository repository;

    @Before
    public void before() {
        PrivilegedConfigClient client = PrivilegedConfigClient.adapt(cluster.getInternalNodeClient());
        this.repository = new IndexMigrationStateRepository(client);
        this.executor = Executors.newFixedThreadPool(1);
        if(repository.isIndexCreated()) {
            client.admin().indices().delete(new DeleteIndexRequest(".sg_data_migration_state")).actionGet();
        }
    }

    @After
    public void after() {
        this.executor.shutdown();
    }

    @Test(timeout = 10_000)
    public void shouldExecuteSingleMigration() {
        when(stepFactory1.createSteps()).thenReturn(ImmutableList.of(new WaitStep().enoughWaiting()));
        DataMigrationService migrationService = new DataMigrationService(repository, stepFactory1, Clock.systemUTC());

        StandardResponse response = migrationService.migrateData();

        assertThat(response.getStatus(), equalTo(SC_OK));
    }

    @Test(timeout = 10_000)
    public void shouldNotAttemptToRerunMigrationTooEarly() throws InterruptedException, DocumentParseException {
        Clock nowClock = Clock.fixed(NOW.toInstant(), ZoneOffset.UTC);
        Clock pastClock = Clock.offset(nowClock, Duration.ofSeconds(-1));
        //two services which represents two cluster nodes
        DataMigrationService migrationService1 = new DataMigrationService(repository, stepFactory1, pastClock);
        DataMigrationService migrationService2 = new DataMigrationService(repository, stepFactory2, nowClock);
        SyncStep syncStep = new SyncStep();
        WaitStep waitStep = new WaitStep();
        when(stepFactory1.createSteps()).thenReturn(ImmutableList.of(syncStep, waitStep));
        executor.submit(migrationService1::migrateData);
        syncStep.waitUntilStepExecution();// From this point the first migration is executed until invocation of waitStep.enoughWaiting()

        //try to run another migration in parallel
        StandardResponse response = migrationService2.migrateData();

        String jsonString = response.toJsonString();
        log.debug("Status and body from the second data migration run in parallel: '{}', '{}'", response.getStatus(), jsonString);
        assertThat(response.getStatus(), equalTo(SC_BAD_REQUEST));
        DocNode responseBody = DocNode.parse(Format.JSON).from(jsonString);
        assertThat(responseBody, containsValue("$.data.status", "failure"));
        assertThat(responseBody, containsValue("$.data.stages[0].status", "migration_already_in_progress_error"));
        waitStep.enoughWaiting();//the first migration process can be finished very soon
        //verify that execution of the first data migration process is executed and accomplished correctly
        Awaitility.await().until(() -> repository.findById("migration_8_8_0").get().status(), equalTo(ExecutionStatus.SUCCESS));
    }

    @Test
    public void shouldBreakMigrationIfAnotherMigrationProcessCreatedIndexInParrrarel() throws DocumentParseException {
        // this repository simulates parallel index creation in method createIndex
        MigrationStateRepository migrationStateRepository = new RepositoryWrapper(repository) {
            @Override
            public void createIndex() throws IndexAlreadyExistsException {
                this.wrapped.createIndex();// parallel index creation by another migration process
                this.wrapped.createIndex();
            }
        };
        Clock fixed = Clock.fixed(NOW.toInstant(), ZoneOffset.UTC);
        DataMigrationService migrationService = new DataMigrationService(migrationStateRepository, stepFactory1, fixed);

        StandardResponse response = migrationService.migrateData();

        log.debug("Parallel index creation caused response '{}'.", response.toJsonString());
        assertThat(response.getStatus(), equalTo(SC_CONFLICT));
        DocNode responseBody = DocNode.parse(Format.JSON).from(response.toJsonString());
        assertThat(responseBody, containsValue("$.data.status", "failure"));
        assertThat(responseBody, containsValue("$.data.stages[0].status", "status_index_already_exists"));
    }

    @Test
    public void shouldBreakInCaseOfParallelMigrationStatusDocumentCreation() throws DocumentParseException {
        // this repository simulates parallel document creation (of course it is possible to create document only once,
        // another creation attempt fail)
        MigrationStateRepository migrationStateRepository = new RepositoryWrapper(repository) {
            @Override
            public void create(String migrationId, MigrationExecutionSummary summary) throws OptimisticLockException {
                MigrationExecutionSummary migrationSummary = createMigrationSummary();
                wrapped.create(migrationId, migrationSummary);
                wrapped.create(migrationId, summary);
            }

            private MigrationExecutionSummary createMigrationSummary() {
                LocalDateTime localDateTime = NOW.toLocalDateTime();
                StepExecutionSummary stepSummary = new StepExecutionSummary(0, localDateTime, "created by another process",
                        StepExecutionStatus.OK, "I am race condition winner!");
                ImmutableList<StepExecutionSummary> stages = ImmutableList.of(stepSummary);
                return new MigrationExecutionSummary(localDateTime, ExecutionStatus.IN_PROGRESS, null, null, stages, null);
            }
        };
        Clock fixed = Clock.fixed(NOW.toInstant(), ZoneOffset.UTC);
        DataMigrationService migrationService = new DataMigrationService(migrationStateRepository, stepFactory1, fixed);

        StandardResponse response = migrationService.migrateData();

        log.debug("Parallel index creation caused response '{}'.", response.toJsonString());
        assertThat(response.getStatus(), equalTo(SC_PRECONDITION_FAILED));
        DocNode responseBody = DocNode.parse(Format.JSON).from(response.toJsonString());
        assertThat(responseBody, containsValue("$.data.status", "failure"));
        assertThat(responseBody, containsValue("$.data.stages[0].status", "cannot_create_status_document_error"));
    }

    @Test
    public void shouldBreakMigrationIfOptimisticLockFailsDuringMigrationStatusUpdate() throws DocumentParseException {
        repository.createIndex();
        // let's force migration re-run mode. So store data related to previously failed migration process
        LocalDateTime now = NOW.toLocalDateTime();
        var stepSummary = new StepExecutionSummary(0, now, "precondition check", UNEXPECTED_ERROR, "Sth. went wrong.");
        ImmutableList<StepExecutionSummary> stages = ImmutableList.of(stepSummary);
        repository.upsert("migration_8_8_0", new MigrationExecutionSummary(now, ExecutionStatus.FAILURE, null, null, stages));
        // this repository simulates optimistic lock failure, when migration status document is updated
        MigrationStateRepository migrationStateRepository = new RepositoryWrapper(repository) {

            @Override
            public void updateWithLock(String id, MigrationExecutionSummary dataMigrationSummary, OptimisticLock lock)
                throws OptimisticLockException {
                wrapped.upsert(id, createMigrationSummary());// this brake optimistic lock and simulates other migration process
                wrapped.updateWithLock(id, dataMigrationSummary, lock);
            }

            private MigrationExecutionSummary createMigrationSummary() {
                LocalDateTime localDateTime = NOW.toLocalDateTime();
                StepExecutionSummary stepSummary = new StepExecutionSummary(0, localDateTime, "created by another process",
                    OK, "Optimistic lock broken");
                ImmutableList<StepExecutionSummary> stages = ImmutableList.of(stepSummary);
                return new MigrationExecutionSummary(localDateTime, ExecutionStatus.IN_PROGRESS, null, null, stages, null);
            }
        };
        Clock fixed = Clock.fixed(NOW.toInstant(), ZoneOffset.UTC);
        DataMigrationService migrationService = new DataMigrationService(migrationStateRepository, stepFactory1, fixed);

        StandardResponse response = migrationService.migrateData();

        log.debug("Optimistic lock failure response '{}'.", response.toJsonString());
        assertThat(response.getStatus(), equalTo(SC_CONFLICT));
        DocNode responseBody = DocNode.parse(Format.JSON).from(response.toJsonString());
        assertThat(responseBody, containsValue("$.data.status", "failure"));
        assertThat(responseBody, containsValue("$.data.stages[0].status", "cannot_update_status_document_lock_error"));
    }

    private static class SyncStep implements MigrationStep {

        private final CountDownLatch countDownLatch;

        public SyncStep() {
            this.countDownLatch = new CountDownLatch(1);
        }

        @Override
        public StepResult execute(DataMigrationContext dataMigrationContext) {
            this.countDownLatch.countDown();
            return new StepResult(OK, "Synchronized on countDownLatch");
        }

        @Override
        public String name() {
            return "external sync step";
        }

        public void waitUntilStepExecution() {
            try {
                countDownLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                String message = "Cannot wait so long for SyncStep execution";
                log.error(message);
                throw new RuntimeException(message, e);
            }
        }
    }

    /**
     * This migration step is executed until {@link #enoughWaiting()} method is invoked
     */
    private static class WaitStep implements MigrationStep {

        private final CountDownLatch countDownLatch;

        public WaitStep() {
            this.countDownLatch = new CountDownLatch(1);
        }

        @Override
        public StepResult execute(DataMigrationContext dataMigrationContext) {
            log.debug("Start waiting in migration '{}'", dataMigrationContext.getMigrationId());
            try {
                if(!countDownLatch.await(1, TimeUnit.MINUTES)) {
                    String message = "Cannot wait so long, probably invocation of method enoughWaiting is missing";
                    log.error(message);
                    throw new RuntimeException(message);
                }
                log.debug("Finish waiting in migration '{}'", dataMigrationContext.getMigrationId());
                return new StepResult(OK, "Waited so long...");
            } catch (InterruptedException e) {
                String message = "Unexpected interruption of waiting";
                log.error(message);
                throw new RuntimeException(message, e);
            }
        }

        public WaitStep enoughWaiting() {
            log.debug("Step execution will be resumed.");
            countDownLatch.countDown();
            return this;
        }

        @Override
        public String name() {
            return "just wait";
        }
    }

    private static class RepositoryWrapper implements MigrationStateRepository {

        protected final MigrationStateRepository wrapped;

        public RepositoryWrapper(MigrationStateRepository wrapped) {
            this.wrapped = Objects.requireNonNull(wrapped, "Repository is required");
        }

        @Override
        public void upsert(String id, MigrationExecutionSummary migrationExecutionSummary) {
            wrapped.upsert(id, migrationExecutionSummary);
        }

        @Override
        public void updateWithLock(String id, MigrationExecutionSummary migrationExecutionSummary, OptimisticLock lock)
            throws OptimisticLockException {
            wrapped.updateWithLock(id, migrationExecutionSummary, lock);
        }

        @Override
        public boolean isIndexCreated() {
            return wrapped.isIndexCreated();
        }

        @Override
        public void createIndex() throws IndexAlreadyExistsException {
            wrapped.createIndex();
        }

        @Override
        public Optional<MigrationExecutionSummary> findById(String id) {
            return wrapped.findById(id);
        }

        @Override
        public void create(String migrationId, MigrationExecutionSummary summary) throws OptimisticLockException {
            wrapped.create(migrationId, summary);
        }
    }
}
