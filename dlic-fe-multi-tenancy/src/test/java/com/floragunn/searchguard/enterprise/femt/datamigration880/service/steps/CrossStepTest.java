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

import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.FrontendObjectCatalog;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationConfig;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.BackupIndex;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.BACKUP_FROM_PREVIOUS_MIGRATION_NOT_AVAILABLE_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.BACKUP_INDICES_CONTAIN_MIGRATION_MARKER;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.GLOBAL_TENANT_INDEX;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.PRIVATE_USER_KIRK_INDEX;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.TENANT_MANAGEMENT;
import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static java.time.ZoneOffset.UTC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class CrossStepTest {
    private static final ZonedDateTime NOW = ZonedDateTime.of(LocalDateTime.of(2020, 1, 1, 1, 1), UTC);

    private final Clock clock = Clock.fixed(NOW.toInstant(), UTC);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
        .nodeSettings("searchguard.unsupported.single_index_mt_enabled", true)
        .singleNode()
        .sslEnabled()
        .resources("multitenancy")
        .enterpriseModulesEnabled()
        .build();

    private DataMigrationContext context;

    @Rule
    public final MigrationEnvironmentHelper environmentHelper = new MigrationEnvironmentHelper(cluster, clock);

    @Before
    public void before() {
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
    }

    @Test
    public void shouldUseTheBackupIndexAsDataSource_1() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        MigrationEnvironmentHelper.DoubleAliasIndex managementTenant = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, managementTenant, PRIVATE_USER_KIRK_INDEX);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), 100);
        environmentHelper.addDataMigrationMarkerToTheIndex(GLOBAL_TENANT_INDEX.indexName());
        catalog.insertSpace(managementTenant.indexName(), "management-tenant-space");
        catalog.insertSpace(PRIVATE_USER_KIRK_INDEX.indexName(), "kirk-tenant-space");
        catalog.insertSpace(managementTenant.indexName(), "management-tenant-space");
        BackupIndex backupIndex1 = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndex2 = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        BackupIndex backupIndex3 = new BackupIndex(NOW.toLocalDateTime().minusDays(2));
        BackupIndex backupIndex4 = new BackupIndex(NOW.toLocalDateTime().minusDays(3));
        BackupIndex backupIndex5 = new BackupIndex(NOW.toLocalDateTime().minusDays(4));
        environmentHelper.createBackupIndex(backupIndex1, backupIndex2, backupIndex3, backupIndex4, backupIndex5);
        catalog.insertSpace(backupIndex1.indexName(), "backup-1-tenant-space");
        catalog.insertSpace(backupIndex2.indexName(), "backup-2-tenant-space");
        catalog.insertSpace(backupIndex3.indexName(), "backup-3-tenant-space");
        catalog.insertSpace(backupIndex4.indexName(), "backup-4-tenant-space");
        catalog.insertSpace(backupIndex5.indexName(), "backup-5-tenant-space");
        StepRepository stepRepository = new StepRepository(client);
        populateBackupIndicesViaStepExecution(stepRepository);
        assertThat(context.getBackupIndices(), hasSize(5));
        populateDataIndicesViaStep();
        assertThat(context.getDataIndicesNames(), hasSize(3));
        CopyDataToTempIndexStep step = createCopyDataToTempIndexStep(client);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        // documents from global tenant index not included (~100 documents)
        String tempIndex = context.getTempIndexName();
        assertThat(environmentHelper.countDocumentInIndex(tempIndex), equalTo(3L));
        environmentHelper.assertThatDocumentExists(tempIndex, "space:backup-1-tenant-space");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:management-tenant-space__sg_ten__-1799980989_management");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:kirk-tenant-space__sg_ten__3292183_kirk");
    }

    @Test
    public void shouldUseTheBackupIndexAsDataSource_2() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        MigrationEnvironmentHelper.DoubleAliasIndex managementTenant = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, managementTenant, PRIVATE_USER_KIRK_INDEX);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), 100);
        environmentHelper.addDataMigrationMarkerToTheIndex(GLOBAL_TENANT_INDEX.indexName()); // added marker of migrated data
        catalog.insertSpace(managementTenant.indexName(), "management-tenant-space");
        catalog.insertSpace(PRIVATE_USER_KIRK_INDEX.indexName(), "kirk-tenant-space");
        catalog.insertSpace(managementTenant.indexName(), "management-tenant-space");
        BackupIndex backupIndex1 = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndex2 = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        BackupIndex backupIndex3 = new BackupIndex(NOW.toLocalDateTime().minusDays(2));
        BackupIndex backupIndex4 = new BackupIndex(NOW.toLocalDateTime().minusDays(3));
        BackupIndex backupIndex5 = new BackupIndex(NOW.toLocalDateTime().minusDays(4));
        environmentHelper.createBackupIndex(backupIndex1, backupIndex2, backupIndex3, backupIndex4, backupIndex5);
        catalog.insertSpace(backupIndex1.indexName(), "backup-1-tenant-space");
        catalog.insertSpace(backupIndex2.indexName(), "backup-2-tenant-space");
        catalog.insertSpace(backupIndex3.indexName(), "backup-3-tenant-space");
        catalog.insertSpace(backupIndex4.indexName(), "backup-4-tenant-space");
        catalog.insertSpace(backupIndex5.indexName(), "backup-5-tenant-space");
        environmentHelper.addDataMigrationMarkerToTheIndex(backupIndex1.indexName()); // added marker of migrated data
        StepRepository stepRepository = new StepRepository(client);
        populateBackupIndicesViaStepExecution(stepRepository);
        assertThat(context.getBackupIndices(), hasSize(5));
        populateDataIndicesViaStep();
        assertThat(context.getDataIndicesNames(), hasSize(3));
        CopyDataToTempIndexStep step = createCopyDataToTempIndexStep(client);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        // documents from global tenant index not included (~100 documents)
        String tempIndex = context.getTempIndexName();
        assertThat(environmentHelper.countDocumentInIndex(tempIndex), equalTo(3L));
        environmentHelper.assertThatDocumentExists(tempIndex, "space:backup-2-tenant-space");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:management-tenant-space__sg_ten__-1799980989_management");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:kirk-tenant-space__sg_ten__3292183_kirk");
    }

    @Test
    public void shouldUseTheBackupIndexAsDataSource_3() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        MigrationEnvironmentHelper.DoubleAliasIndex managementTenant = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, managementTenant, PRIVATE_USER_KIRK_INDEX);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), 100);
        environmentHelper.addDataMigrationMarkerToTheIndex(GLOBAL_TENANT_INDEX.indexName()); // added marker of migrated data
        catalog.insertSpace(managementTenant.indexName(), "management-tenant-space");
        catalog.insertSpace(PRIVATE_USER_KIRK_INDEX.indexName(), "kirk-tenant-space");
        catalog.insertSpace(managementTenant.indexName(), "management-tenant-space");
        BackupIndex backupIndex1 = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndex2 = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        BackupIndex backupIndex3 = new BackupIndex(NOW.toLocalDateTime().minusDays(2));
        BackupIndex backupIndex4 = new BackupIndex(NOW.toLocalDateTime().minusDays(3));
        BackupIndex backupIndex5 = new BackupIndex(NOW.toLocalDateTime().minusDays(4));
        environmentHelper.createBackupIndex(backupIndex1, backupIndex2, backupIndex3, backupIndex4, backupIndex5);
        catalog.insertSpace(backupIndex1.indexName(), "backup-1-tenant-space");
        catalog.insertSpace(backupIndex2.indexName(), "backup-2-tenant-space");
        catalog.insertSpace(backupIndex3.indexName(), "backup-3-tenant-space");
        catalog.insertSpace(backupIndex4.indexName(), "backup-4-tenant-space");
        catalog.insertSpace(backupIndex5.indexName(), "backup-5-tenant-space");
        environmentHelper.addDataMigrationMarkerToTheIndex(backupIndex1.indexName()); // added marker of migrated data
        environmentHelper.addDataMigrationMarkerToTheIndex(backupIndex2.indexName()); // added marker of migrated data
        StepRepository stepRepository = new StepRepository(client);
        populateBackupIndicesViaStepExecution(stepRepository);
        assertThat(context.getBackupIndices(), hasSize(5));
        populateDataIndicesViaStep();
        assertThat(context.getDataIndicesNames(), hasSize(3));
        CopyDataToTempIndexStep step = createCopyDataToTempIndexStep(client);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        // documents from global tenant index not included (~100 documents)
        String tempIndex = context.getTempIndexName();
        assertThat(environmentHelper.countDocumentInIndex(tempIndex), equalTo(3L));
        environmentHelper.assertThatDocumentExists(tempIndex, "space:backup-3-tenant-space");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:management-tenant-space__sg_ten__-1799980989_management");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:kirk-tenant-space__sg_ten__3292183_kirk");
    }

    @Test
    public void shouldUseTheBackupIndexAsDataSource_4() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        MigrationEnvironmentHelper.DoubleAliasIndex managementTenant = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, managementTenant, PRIVATE_USER_KIRK_INDEX);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), 100);
        environmentHelper.addDataMigrationMarkerToTheIndex(GLOBAL_TENANT_INDEX.indexName()); // added marker of migrated data
        catalog.insertSpace(managementTenant.indexName(), "management-tenant-space");
        catalog.insertSpace(PRIVATE_USER_KIRK_INDEX.indexName(), "kirk-tenant-space");
        catalog.insertSpace(managementTenant.indexName(), "management-tenant-space");
        BackupIndex backupIndex1 = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndex2 = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        BackupIndex backupIndex3 = new BackupIndex(NOW.toLocalDateTime().minusDays(2));
        BackupIndex backupIndex4 = new BackupIndex(NOW.toLocalDateTime().minusDays(3));
        BackupIndex backupIndex5 = new BackupIndex(NOW.toLocalDateTime().minusDays(4));
        environmentHelper.createBackupIndex(backupIndex1, backupIndex2, backupIndex3, backupIndex4, backupIndex5);
        catalog.insertSpace(backupIndex1.indexName(), "backup-1-tenant-space");
        catalog.insertSpace(backupIndex2.indexName(), "backup-2-tenant-space");
        catalog.insertSpace(backupIndex3.indexName(), "backup-3-tenant-space");
        catalog.insertSpace(backupIndex4.indexName(), "backup-4-tenant-space");
        catalog.insertSpace(backupIndex5.indexName(), "backup-5-tenant-space");
        environmentHelper.addDataMigrationMarkerToTheIndex(backupIndex1.indexName()); // added marker of migrated data
        environmentHelper.addDataMigrationMarkerToTheIndex(backupIndex2.indexName()); // added marker of migrated data
        environmentHelper.addDataMigrationMarkerToTheIndex(backupIndex3.indexName()); // added marker of migrated data
        StepRepository stepRepository = new StepRepository(client);
        populateBackupIndicesViaStepExecution(stepRepository);
        assertThat(context.getBackupIndices(), hasSize(5));
        populateDataIndicesViaStep();
        assertThat(context.getDataIndicesNames(), hasSize(3));
        CopyDataToTempIndexStep step = createCopyDataToTempIndexStep(client);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        // documents from global tenant index not included (~100 documents)
        String tempIndex = context.getTempIndexName();
        assertThat(environmentHelper.countDocumentInIndex(tempIndex), equalTo(3L));
        environmentHelper.assertThatDocumentExists(tempIndex, "space:backup-4-tenant-space");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:management-tenant-space__sg_ten__-1799980989_management");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:kirk-tenant-space__sg_ten__3292183_kirk");
    }

    @Test
    public void shouldUseTheBackupIndexAsDataSource_5() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        MigrationEnvironmentHelper.DoubleAliasIndex managementTenant = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, managementTenant, PRIVATE_USER_KIRK_INDEX);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), 100);
        environmentHelper.addDataMigrationMarkerToTheIndex(GLOBAL_TENANT_INDEX.indexName()); // added marker of migrated data
        catalog.insertSpace(managementTenant.indexName(), "management-tenant-space");
        catalog.insertSpace(PRIVATE_USER_KIRK_INDEX.indexName(), "kirk-tenant-space");
        catalog.insertSpace(managementTenant.indexName(), "management-tenant-space");
        BackupIndex backupIndex1 = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndex2 = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        BackupIndex backupIndex3 = new BackupIndex(NOW.toLocalDateTime().minusDays(2));
        BackupIndex backupIndex4 = new BackupIndex(NOW.toLocalDateTime().minusDays(3));
        BackupIndex backupIndex5 = new BackupIndex(NOW.toLocalDateTime().minusDays(4));
        environmentHelper.createBackupIndex(backupIndex1, backupIndex2, backupIndex3, backupIndex4, backupIndex5);
        catalog.insertSpace(backupIndex1.indexName(), "backup-1-tenant-space");
        catalog.insertSpace(backupIndex2.indexName(), "backup-2-tenant-space");
        catalog.insertSpace(backupIndex3.indexName(), "backup-3-tenant-space");
        catalog.insertSpace(backupIndex4.indexName(), "backup-4-tenant-space");
        catalog.insertSpace(backupIndex5.indexName(), "backup-5-tenant-space");
        environmentHelper.addDataMigrationMarkerToTheIndex(backupIndex1.indexName()); // added marker of migrated data
        environmentHelper.addDataMigrationMarkerToTheIndex(backupIndex2.indexName()); // added marker of migrated data
        environmentHelper.addDataMigrationMarkerToTheIndex(backupIndex3.indexName()); // added marker of migrated data
        environmentHelper.addDataMigrationMarkerToTheIndex(backupIndex4.indexName()); // added marker of migrated data
        StepRepository stepRepository = new StepRepository(client);
        populateBackupIndicesViaStepExecution(stepRepository);
        assertThat(context.getBackupIndices(), hasSize(5));
        populateDataIndicesViaStep();
        assertThat(context.getDataIndicesNames(), hasSize(3));
        CopyDataToTempIndexStep step = createCopyDataToTempIndexStep(client);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        // documents from global tenant index not included (~100 documents)
        assertThat(environmentHelper.countDocumentInIndex(context.getTempIndexName()), equalTo(3L));
        environmentHelper.assertThatDocumentExists(context.getTempIndexName(), "space:backup-5-tenant-space");
        environmentHelper.assertThatDocumentExists(context.getTempIndexName(), "space:management-tenant-space__sg_ten__-1799980989_management");
        environmentHelper.assertThatDocumentExists(context.getTempIndexName(), "space:kirk-tenant-space__sg_ten__3292183_kirk");
    }

    @Test
    public void shouldUseTheBackupIndexAsDataSource_6() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        MigrationEnvironmentHelper.DoubleAliasIndex managementTenant = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, managementTenant, PRIVATE_USER_KIRK_INDEX);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), 100);
        environmentHelper.addDataMigrationMarkerToTheIndex(GLOBAL_TENANT_INDEX.indexName()); // added marker of migrated data
        catalog.insertSpace(managementTenant.indexName(), "management-tenant-space");
        catalog.insertSpace(PRIVATE_USER_KIRK_INDEX.indexName(), "kirk-tenant-space");
        catalog.insertSpace(managementTenant.indexName(), "management-tenant-space");
        BackupIndex backupIndex1 = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndex2 = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        BackupIndex backupIndex3 = new BackupIndex(NOW.toLocalDateTime().minusDays(2));
        BackupIndex backupIndex4 = new BackupIndex(NOW.toLocalDateTime().minusDays(3));
        BackupIndex backupIndex5 = new BackupIndex(NOW.toLocalDateTime().minusDays(4));
        environmentHelper.createBackupIndex(backupIndex1, backupIndex2, backupIndex3, backupIndex4, backupIndex5);
        catalog.insertSpace(backupIndex1.indexName(), "backup-1-tenant-space");
        catalog.insertSpace(backupIndex2.indexName(), "backup-2-tenant-space");
        catalog.insertSpace(backupIndex3.indexName(), "backup-3-tenant-space");
        catalog.insertSpace(backupIndex4.indexName(), "backup-4-tenant-space");
        catalog.insertSpace(backupIndex5.indexName(), "backup-5-tenant-space");
        environmentHelper.addDataMigrationMarkerToTheIndex(backupIndex1.indexName()); // added marker of migrated data
        environmentHelper.addDataMigrationMarkerToTheIndex(backupIndex2.indexName()); // added marker of migrated data
        environmentHelper.addDataMigrationMarkerToTheIndex(backupIndex3.indexName()); // added marker of migrated data
        environmentHelper.addDataMigrationMarkerToTheIndex(backupIndex4.indexName()); // added marker of migrated data
        environmentHelper.addDataMigrationMarkerToTheIndex(backupIndex5.indexName()); // added marker of migrated data
        StepRepository stepRepository = new StepRepository(client);
        populateBackupIndicesViaStepExecution(stepRepository);
        assertThat(context.getBackupIndices(), hasSize(5));
        populateDataIndicesViaStep();
        assertThat(context.getDataIndicesNames(), hasSize(3));
        CopyDataToTempIndexStep step = createCopyDataToTempIndexStep(client);

        StepException exception = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));

        // all backup indices contain migration marker, so that cannot be used
        assertThat(exception.getStatus(), equalTo(BACKUP_INDICES_CONTAIN_MIGRATION_MARKER));
    }

    @Test
    public void shouldMarkGlobalTenantIndexWithMigratedDataMarkerByStep() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        MigrationEnvironmentHelper.DoubleAliasIndex managementTenant = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, managementTenant, PRIVATE_USER_KIRK_INDEX);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), 100);
        catalog.insertSpace(managementTenant.indexName(), "management-tenant-space");
        catalog.insertSpace(PRIVATE_USER_KIRK_INDEX.indexName(), "kirk-tenant-space");
        catalog.insertSpace(managementTenant.indexName(), "management-tenant-space");
        BackupIndex backupIndex1 = new BackupIndex(NOW.toLocalDateTime());
        environmentHelper.createBackupIndex(backupIndex1);
        catalog.insertSpace(backupIndex1.indexName(), "backup-1-tenant-space");
        StepRepository stepRepository = new StepRepository(client);
        populateBackupIndicesViaStepExecution(stepRepository);
        assertThat(context.getBackupIndices(), hasSize(1));
        populateDataIndicesViaStep();
        assertThat(context.getDataIndicesNames(), hasSize(3));
        CopyDataToTempIndexStep step = createCopyDataToTempIndexStep(client);
        IndexSettingsManager indexSettingsManager = new IndexSettingsManager(stepRepository);

        addMigrationMarkerToGlobalTenantIndexViaStep(indexSettingsManager);

        StepResult result = step.execute(context);
        assertThat(result.isSuccess(), equalTo(true));
        // documents from global tenant index not included (~100 documents)
        assertThat(environmentHelper.countDocumentInIndex(context.getTempIndexName()), equalTo(3L));
        String tempIndex = context.getTempIndexName();
        environmentHelper.assertThatDocumentExists(tempIndex, "space:backup-1-tenant-space");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:management-tenant-space__sg_ten__-1799980989_management");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:kirk-tenant-space__sg_ten__3292183_kirk");
    }

    @Test
    public void shouldUseTheBackupIndexAsDataSource_7() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        MigrationEnvironmentHelper.DoubleAliasIndex managementTenant = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, managementTenant, PRIVATE_USER_KIRK_INDEX);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), 100);
        environmentHelper.addDataMigrationMarkerToTheIndex(GLOBAL_TENANT_INDEX.indexName()); // added marker of migrated data
        catalog.insertSpace(managementTenant.indexName(), "management-tenant-space");
        catalog.insertSpace(PRIVATE_USER_KIRK_INDEX.indexName(), "kirk-tenant-space");
        catalog.insertSpace(managementTenant.indexName(), "management-tenant-space");
        StepRepository stepRepository = new StepRepository(client);
        populateBackupIndicesViaStepExecution(stepRepository);
        populateDataIndicesViaStep();
        assertThat(context.getDataIndicesNames(), hasSize(3));
        CopyDataToTempIndexStep step = createCopyDataToTempIndexStep(client);

        StepException exception = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));

        assertThat(exception.getStatus(), equalTo(BACKUP_FROM_PREVIOUS_MIGRATION_NOT_AVAILABLE_ERROR));
    }

    @Test
    public void shouldUseTheBackupDataCreatedByStep() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        MigrationEnvironmentHelper.DoubleAliasIndex managementTenant = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, managementTenant, PRIVATE_USER_KIRK_INDEX);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "should-be-moved-to-backup-space");

        catalog.insertSpace(managementTenant.indexName(), "management-tenant-space");
        catalog.insertSpace(PRIVATE_USER_KIRK_INDEX.indexName(), "kirk-tenant-space");
        catalog.insertSpace(managementTenant.indexName(), "management-tenant-space");

        populateDataIndicesViaStep();
        assertThat(context.getDataIndicesNames(), hasSize(3));

        StepRepository stepRepository = new StepRepository(client);
        createBackupByStep(stepRepository);
        environmentHelper.assertThatDocumentExists(context.getBackupIndexName(), "space:should-be-moved-to-backup-space");

        environmentHelper.addDataMigrationMarkerToTheIndex(GLOBAL_TENANT_INDEX.indexName());
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), 100);


        populateBackupIndicesViaStepExecution(stepRepository);
        assertThat(context.getBackupIndices(), hasSize(1));
        CopyDataToTempIndexStep step = createCopyDataToTempIndexStep(client);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        // documents from global tenant index not included (~100 documents)
        String tempIndex = context.getTempIndexName();
        assertThat(environmentHelper.countDocumentInIndex(tempIndex), equalTo(3L));
        environmentHelper.assertThatDocumentExists(tempIndex, "space:should-be-moved-to-backup-space");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:management-tenant-space__sg_ten__-1799980989_management");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:kirk-tenant-space__sg_ten__3292183_kirk");
    }

    private void createBackupByStep(StepRepository stepRepository) {
        IndexSettingsManager indexSettingsManager = new IndexSettingsManager(stepRepository);
        CreateBackupOfGlobalIndexStep createBackupOfGlobalIndexStep = new CreateBackupOfGlobalIndexStep(stepRepository, indexSettingsManager);
        StepResult backupResult = createBackupOfGlobalIndexStep.execute(context);
        assertThat(backupResult.isSuccess(), equalTo(true));
        assertThat(context.getBackupCreated(), equalTo(true));
    }

    private void populateDataIndicesViaStep() {
        PopulateTenantsStep step = createPopulateTenantsStep();
        StepResult result = step.execute(context);
        assertThat(result.isSuccess(), equalTo(true));
    }

    private void populateBackupIndicesViaStepExecution(StepRepository stepRepository) {
        PopulateBackupIndicesStep step = new PopulateBackupIndicesStep(stepRepository);
        StepResult result = step.execute(context);
        assertThat(result.isSuccess(), equalTo(true));
    }

    private PopulateTenantsStep createPopulateTenantsStep() {
        FeMultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(FeMultiTenancyConfigurationProvider.class);
        PrivilegedConfigClient privilegedConfigClient = environmentHelper.getPrivilegedClient();
        return new PopulateTenantsStep(configurationProvider, new StepRepository(privilegedConfigClient));
    }

    private void addMigrationMarkerToGlobalTenantIndexViaStep(IndexSettingsManager indexSettingsManager) {
        AddMigrationMarkerToGlobalTenantIndexStep step = new AddMigrationMarkerToGlobalTenantIndexStep(indexSettingsManager);
        StepResult result = step.execute(context);
        assertThat(result.isSuccess(), equalTo(true));
    }

    private static CopyDataToTempIndexStep createCopyDataToTempIndexStep(PrivilegedConfigClient client) {
        FeMultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(FeMultiTenancyConfigurationProvider.class);
        StepRepository stepRepository = new StepRepository(client);
        IndexSettingsManager indexSettingsManager = new IndexSettingsManager(stepRepository);
        return new CopyDataToTempIndexStep(stepRepository, configurationProvider, indexSettingsManager);
    }
}
