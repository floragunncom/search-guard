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

package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;

import com.floragunn.searchguard.enterprise.femt.datamigration880.service.persistence.IndexMigrationStateRepository;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.DoubleAliasIndex;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchsupport.junit.matcher.DocNodeMatchers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.internal.Client;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Optional;

import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.GLOBAL_TENANT_INDEX;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.PRIVATE_USER_KIRK_INDEX;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_1_INDEX;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_2_INDEX;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_3_INDEX;
import static org.apache.http.HttpStatus.SC_OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;

public class MigrationApiTest {

    private static final Logger log = LogManager.getLogger(MigrationApiTest.class);
    private static final String MIGRATION_STATE_DOC_ID = "migration_8_8_0";

    private IndexMigrationStateRepository indexMigrationStateRepository;

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
        .nodeSettings("searchguard.unsupported.single_index_mt_enabled", true)
        .sslEnabled()
        .resources("multitenancy")
        .enterpriseModulesEnabled()
        .build();

    @Rule
    public final MigrationEnvironmentHelper environmentHelper = new MigrationEnvironmentHelper(cluster, Clock.systemDefaultZone());



    @Before
    public void before() {
        try (Client client = cluster.getInternalNodeClient()) {
            IndexMigrationStateRepository repository = new IndexMigrationStateRepository(PrivilegedConfigClient.adapt(client));
            indexMigrationStateRepository = new IndexMigrationStateRepository(PrivilegedConfigClient.adapt(client));
            if (indexMigrationStateRepository.isIndexCreated()) {
                Awaitility.await("Data migration isn't in progress")
                    .atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(25))
                    .until(() -> {
                        Optional<MigrationExecutionSummary> executionSummary = indexMigrationStateRepository.findById(MIGRATION_STATE_DOC_ID);
                        return executionSummary.map(summary -> !summary.isMigrationInProgress(LocalDateTime.now())).orElse(true);
                    });
                if (repository.isIndexCreated()) {
                    environmentHelper.deleteIndex(".sg_data_migration_state");
                    assertThatMigrationStateIndexExists(false);
                }
            }
        }
    }

    @Test
    public void shouldStartMigrationProcess() throws Exception {
        createTenantsAndSavedObjects(mediumAmountOfData());
        try (GenericRestClient client = cluster.createGenericAdminRestClient(Collections.emptyList())) {
            DocNode body = DocNode.EMPTY;

            HttpResponse response = client.postJson("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0", body);

            log.info("Start migration response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThat(environmentHelper.countDocumentInIndex(GLOBAL_TENANT_INDEX.indexName()), equalTo(93L));
            GetIndexResponse getIndexResponse = environmentHelper.findHiddenIndexByName("backup_fe_migration_to_8_8_0_*")
                .orElseThrow();
            assertThat(getIndexResponse.getIndices(), arrayWithSize(1));
        }
    }

    @Test
    public void shouldMigrateSmallAmountOfData() throws Exception {
        createTenantsAndSavedObjects(smallAmountOfData());
        try (GenericRestClient client = cluster.createGenericAdminRestClient(Collections.emptyList())) {
            DocNode body = DocNode.EMPTY;

            HttpResponse response = client.postJson("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0", body);

            log.info("Start migration response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThatOnlySmallMigratedDatasetIsPresentInGlobalTenantIndex();
            GetIndexResponse getIndexResponse = environmentHelper.findHiddenIndexByName("backup_fe_migration_to_8_8_0_*")
                .orElseThrow();
            assertThat(getIndexResponse.getIndices(), arrayWithSize(1));
        }
    }

    @Test
    public void shouldRerunMigrationProcessUsingBackupIndex() throws Exception {
        createTenantsAndSavedObjects(smallAmountOfData());
        try (GenericRestClient client = cluster.createGenericAdminRestClient(Collections.emptyList())) {
            DocNode body = DocNode.EMPTY;
            HttpResponse response = client.postJson("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0", body);
            log.info("First migration run response '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThatOnlySmallMigratedDatasetIsPresentInGlobalTenantIndex();
            try(Client nodeClient = cluster.getInternalNodeClient()) {
                FrontendObjectCatalog catalog = new FrontendObjectCatalog(PrivilegedConfigClient.adapt(nodeClient));
                // damage global tenant index
                catalog.insertIndexPattern(GLOBAL_TENANT_INDEX.indexName(), 100);
            }

            response = client.postJson("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0", body);

            log.info("Second migration run response '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThatOnlySmallMigratedDatasetIsPresentInGlobalTenantIndex();
            GetIndexResponse getIndexResponse = environmentHelper.findHiddenIndexByName("backup_fe_migration_to_8_8_0_*")
                .orElseThrow();
            assertThat(getIndexResponse.getIndices(), arrayWithSize(1));
        }
    }

    @Test
    public void shouldRerunMigrationThreeTimes() throws Exception {
        createTenantsAndSavedObjects(smallAmountOfData());
        try (GenericRestClient client = cluster.createGenericAdminRestClient(Collections.emptyList())) {
            DocNode body = DocNode.EMPTY;
            HttpResponse response = client.postJson("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0", body);
            log.info("First migration run response '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThatOnlySmallMigratedDatasetIsPresentInGlobalTenantIndex();
            try(Client nodeClient = cluster.getInternalNodeClient()) {
                FrontendObjectCatalog catalog = new FrontendObjectCatalog(PrivilegedConfigClient.adapt(nodeClient));
                // damage global tenant index
                catalog.insertIndexPattern(GLOBAL_TENANT_INDEX.indexName(), 100);
            }
            response = client.postJson("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0", body);
            log.info("Second migration run response '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThatOnlySmallMigratedDatasetIsPresentInGlobalTenantIndex();
            try(Client nodeClient = cluster.getInternalNodeClient()) {
                FrontendObjectCatalog catalog = new FrontendObjectCatalog(PrivilegedConfigClient.adapt(nodeClient));
                // damage global tenant index
                catalog.insertIndexPattern(GLOBAL_TENANT_INDEX.indexName(), 100);
            }

            response = client.postJson("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0", body);

            log.info("Third migration run response '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThatOnlySmallMigratedDatasetIsPresentInGlobalTenantIndex();
            GetIndexResponse getIndexResponse = environmentHelper.findHiddenIndexByName("backup_fe_migration_to_8_8_0_*")
                .orElseThrow();
            assertThat(getIndexResponse.getIndices(), arrayWithSize(1));
        }
    }

    @Test
    public void shouldRerunMigrationProcessAndCreateAdditionalBackupWhenGlobalTenantIndexContainsNotMigratedData() throws Exception {
        createTenantsAndSavedObjects(smallAmountOfData());
        try (GenericRestClient client = cluster.createGenericAdminRestClient(Collections.emptyList())) {
            DocNode body = DocNode.EMPTY;
            HttpResponse response = client.postJson("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0", body);
            log.info("First migration run response '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThatOnlySmallMigratedDatasetIsPresentInGlobalTenantIndex();
            String spaceId = null;
            try(Client nodeClient = cluster.getInternalNodeClient()) {
                FrontendObjectCatalog catalog = new FrontendObjectCatalog(PrivilegedConfigClient.adapt(nodeClient));
                // remove data migration marker form the global index. First delete index and create new one and insert required data
                // this should cause backup index creation when the migration process is run 2nd time.
                environmentHelper.deleteIndex(GLOBAL_TENANT_INDEX.indexName());
                environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
                spaceId = catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "global_default").get(0);
            }

            response = client.postJson("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0", body);

            log.info("Second migration run response '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThatOnlySmallMigratedDatasetIsPresentInGlobalTenantIndex();
            GetIndexResponse getIndexResponse = environmentHelper.findHiddenIndexByName("backup_fe_migration_to_8_8_0_*")
                .orElseThrow();
            if(log.isDebugEnabled()) {
                String indices = String.join(", ", getIndexResponse.getIndices());
                log.debug("Backup indices created when migration process is run twice '{}'", indices);
            }
            assertThat(getIndexResponse.getIndices(), arrayWithSize(2));
            String migratedSpaceId = spaceId + "__sg_ten__-1216324346_sgsglobaltenant";
            String spaceSource = environmentHelper.getDocumentSource(GLOBAL_TENANT_INDEX.indexName(), migratedSpaceId).orElseThrow();
            for (String backupIndex : getIndexResponse.getIndices()) {
                environmentHelper.assertThatDocumentExists(backupIndex, spaceId);
                String backupSource = environmentHelper.getDocumentSource(backupIndex, spaceId).orElseThrow();
                assertThat(backupSource, equalTo(spaceSource));
            }
        }
    }

    @Test
    @Ignore // takes too much time
    public void shouldMigrateLargeAmountOfData() throws Exception {
        createTenantsAndSavedObjects(largeAmountOfData());
        try (GenericRestClient client = cluster.createGenericAdminRestClient(Collections.emptyList())) {
            DocNode body = DocNode.EMPTY;
            HttpResponse response = client.postJson("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0", body);

            log.info("Start migration response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThat(environmentHelper.countDocumentInIndex(GLOBAL_TENANT_INDEX.indexName()), equalTo(80_000L));
        }
    }

    @Test
    @Ignore // takes too much time
    public void shouldMigrateHugeAmountOfData() throws Exception {
        createTenantsAndSavedObjects(hugeAmountOfData());
        try (GenericRestClient client = cluster.createGenericAdminRestClient(Collections.emptyList())) {
            DocNode body = DocNode.EMPTY;
            HttpResponse response = client.postJson("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0", body);

            log.info("Start migration response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThat(environmentHelper.countDocumentInIndex(GLOBAL_TENANT_INDEX.indexName()), equalTo(800_000L));
        }
    }

    @Test
    public void getMigrationState_shouldReturnNotFound_indexContainingMigrationStateDoesNotExist() throws Exception {
        assertThatMigrationStateIndexExists(false);

        try (GenericRestClient client = cluster.createGenericAdminRestClient(Collections.emptyList())) {
            HttpResponse response = client.get("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0");

            assertThat(response.getStatusCode(), equalTo(SC_NOT_FOUND));
        }
    }

    @Test
    public void getMigrationState_shouldReturnNotFound_indexContainingMigrationStateIsEmpty() throws Exception {
        indexMigrationStateRepository.createIndex();
        assertThatMigrationStateIndexExists(true);

        try (GenericRestClient client = cluster.createGenericAdminRestClient(Collections.emptyList())) {
            HttpResponse response = client.get("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0");

            assertThat(response.getStatusCode(), equalTo(SC_NOT_FOUND));
        }
    }

    @Test
    public void getMigrationState_shouldReturnMigrationState_migrationStateDocExists() throws Exception {
        ZonedDateTime date = ZonedDateTime.of(2023, 10, 6, 10, 10, 10, 10, ZoneOffset.UTC);
        MigrationExecutionSummary migrationExecutionSummary = new MigrationExecutionSummary(
                LocalDateTime.from(date), ExecutionStatus.IN_PROGRESS, "temp-index", "backup-index",
                ImmutableList.of(
                        new StepExecutionSummary(1, LocalDateTime.from(date.plusMinutes(2)), "step-name",
                                StepExecutionStatus.OK, "msg", "details"
                        )
                ), null
        );
        saveMigrationState(migrationExecutionSummary);
        assertThatMigrationStateIndexExists(true);

        try (GenericRestClient client = cluster.createGenericAdminRestClient(Collections.emptyList())) {
            HttpResponse response = client.get("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0");

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThat(response.getBodyAsDocNode(), DocNodeMatchers.containsValue("status", 200));
            assertThat(response.getBodyAsDocNode(), DocNodeMatchers.docNodeSizeEqualTo("$.data", 5));
            assertThat(response.getBodyAsDocNode(), DocNodeMatchers.containsValue("$.data.start_time", "2023-10-06T10:10:10.00000001Z"));
            assertThat(response.getBodyAsDocNode(), DocNodeMatchers.containsValue("$.data.status", "in_progress"));
            assertThat(response.getBodyAsDocNode(), DocNodeMatchers.containsValue("$.data.temp_index_name", "temp-index"));
            assertThat(response.getBodyAsDocNode(), DocNodeMatchers.containsFieldPointedByJsonPath("$.data", "stages"));
            assertThat(response.getBodyAsDocNode(), DocNodeMatchers.docNodeSizeEqualTo("$.data.stages", 1));
            assertThat(response.getBodyAsDocNode(), DocNodeMatchers.docNodeSizeEqualTo("$.data.stages[0]", 6));
            assertThat(response.getBodyAsDocNode(), DocNodeMatchers.containsValue("$.data.stages[0].start_time", "2023-10-06T10:12:10.00000001Z"));
            assertThat(response.getBodyAsDocNode(), DocNodeMatchers.containsValue("$.data.stages[0].name", "step-name"));
            assertThat(response.getBodyAsDocNode(), DocNodeMatchers.containsValue("$.data.stages[0].status", "ok"));
            assertThat(response.getBodyAsDocNode(), DocNodeMatchers.containsValue("$.data.stages[0].message", "msg"));
            assertThat(response.getBodyAsDocNode(), DocNodeMatchers.containsValue("$.data.stages[0].number", 1));
            assertThat(response.getBodyAsDocNode(), DocNodeMatchers.containsValue("$.data.stages[0].details", "details"));
        }
    }

    private void assertThatOnlySmallMigratedDatasetIsPresentInGlobalTenantIndex() {
        assertThat(environmentHelper.countDocumentInIndex(GLOBAL_TENANT_INDEX.indexName()), equalTo(32L));
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:global_default__sg_ten__-1216324346_sgsglobaltenant");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__191795427_performancereviews");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__-738948632_performancereviews");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__-634608247_abcdef22");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__580139487_admtenant");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__-1139640511_admin1");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__-152937574_admintenant");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__-523190050_businessintelligence");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__-1242674146_commandtenant");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__1554582075_dept01");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__1554582076_dept02");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__1554582077_dept03");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__1554582078_dept04");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__1554582079_dept05");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__-1419750584_enterprisetenant");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__-853258278_finance");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__-1992298040_financemanagement");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__1592542611_humanresources");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__1482524924_kibana712aliascreationtest");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__-815674808_kibana712aliastest");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__-2014056171_kltentro");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__-2014056163_kltentrw");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__-1799980989_management");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__1593390681_performancedata");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__-1386441184_praxisro");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__-1386441176_praxisrw");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__-1754201467_testtenantro");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:configured_default__sg_ten__-1754201459_testtenantrw");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:user_private__sg_ten__-1091682490_lukasz");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:user_private__sg_ten__3292183_kirk");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:user_private__sg_ten__-1091714203_luksz");
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), "space:user_private__sg_ten__739988528_ukasz");
    }

    private List<DoubleAliasIndex> createTenantsAndSavedObjects(TenantDataProvider dataProvider) {
        List<DoubleAliasIndex> tenants = environmentHelper.findIndicesForTenantsDefinedInConfigurationWithoutGlobal();
        environmentHelper.createIndex(tenants);
        List<DoubleAliasIndex> createdIndices = new ArrayList<>(tenants);
        try(Client client = cluster.getInternalNodeClient()) {
            FrontendObjectCatalog catalog = new FrontendObjectCatalog(PrivilegedConfigClient.adapt(client));
            for (DoubleAliasIndex index : tenants) {
                dataProvider.configuredTenant(catalog, index.indexName());
            }
            // global tenant index
            environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
            createdIndices.add(GLOBAL_TENANT_INDEX);
            dataProvider.globalTenant(catalog, GLOBAL_TENANT_INDEX.indexName());
            // user tenant indices
            ImmutableList<DoubleAliasIndex> privateUserTenants = ImmutableList.of(PRIVATE_USER_KIRK_INDEX, PRIVATE_USER_LUKASZ_1_INDEX,
                PRIVATE_USER_LUKASZ_2_INDEX, PRIVATE_USER_LUKASZ_3_INDEX);
            environmentHelper.createIndex(privateUserTenants);
            createdIndices.addAll(privateUserTenants);
            for (DoubleAliasIndex index : privateUserTenants) {
                dataProvider.privateUserTenant(catalog, index.indexName());
            }
        }
        return createdIndices;
    }

    private TenantDataProvider mediumAmountOfData() {
        return new TenantDataProvider() {
            @Override
            public void globalTenant(FrontendObjectCatalog catalog, String indexName) {
                catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "default", "custom", "detailed", "superglobal");
            }

            @Override
            public void configuredTenant(FrontendObjectCatalog catalog, String indexName) {
                catalog.insertSpace(indexName, "default", "custom", "detailed");
            }

            @Override
            public void privateUserTenant(FrontendObjectCatalog catalog, String indexName) {
                catalog.insertSpace(indexName, "default", "super_private");
            }
        };
    }

    private TenantDataProvider smallAmountOfData() {
        return new TenantDataProvider() {
            @Override
            public void globalTenant(FrontendObjectCatalog catalog, String indexName) {
                catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "global_default");
            }

            @Override
            public void configuredTenant(FrontendObjectCatalog catalog, String indexName) {
                catalog.insertSpace(indexName, "configured_default");
            }

            @Override
            public void privateUserTenant(FrontendObjectCatalog catalog, String indexName) {
                catalog.insertSpace(indexName, "user_private");
            }
        };
    }

    private TenantDataProvider largeAmountOfData() {
        return new TenantDataProvider() {
            @Override
            public void globalTenant(FrontendObjectCatalog catalog, String indexName) {
                catalog.insertSpace(indexName, 1000);
                catalog.insertIndexPattern(indexName, 1000);
                catalog.insertDashboard(indexName, 500);
            }

            @Override
            public void configuredTenant(FrontendObjectCatalog catalog, String indexName) {
                catalog.insertSpace(indexName, 1000);
                catalog.insertIndexPattern(indexName, 1000);
                catalog.insertDashboard(indexName, 500);
            }

            @Override
            public void privateUserTenant(FrontendObjectCatalog catalog, String indexName) {
                catalog.insertSpace(indexName, 1000);
                catalog.insertIndexPattern(indexName, 1000);
                catalog.insertDashboard(indexName, 500);
            }
        };
    }

    private TenantDataProvider hugeAmountOfData() {
        return new TenantDataProvider() {
            @Override
            public void globalTenant(FrontendObjectCatalog catalog, String indexName) {
                catalog.insertSpace(indexName, 10_000);
                catalog.insertIndexPattern(indexName, 10_000);
                catalog.insertDashboard(indexName, 5000);
            }

            @Override
            public void configuredTenant(FrontendObjectCatalog catalog, String indexName) {
                catalog.insertSpace(indexName, 10_000);
                catalog.insertIndexPattern(indexName, 10_000);
                catalog.insertDashboard(indexName, 5000);
            }

            @Override
            public void privateUserTenant(FrontendObjectCatalog catalog, String indexName) {
                catalog.insertSpace(indexName, 10_000);
                catalog.insertIndexPattern(indexName, 10_000);
                catalog.insertDashboard(indexName, 5000);
            }
        };
    }

    private void saveMigrationState(MigrationExecutionSummary migrationExecutionSummary) {
        indexMigrationStateRepository.create(MIGRATION_STATE_DOC_ID, migrationExecutionSummary);
    }

    private void assertThatMigrationStateIndexExists(boolean shouldExist) {
        Awaitility.await("Index containing data migration state exists")
            .atMost(Duration.ofSeconds(2)).pollInterval(Duration.ofMillis(25))
            .untilAsserted(() -> assertThat(indexMigrationStateRepository.isIndexCreated(), equalTo(shouldExist)));
    }

    private interface TenantDataProvider {
        void globalTenant(FrontendObjectCatalog catalog, String indexName);
        void configuredTenant(FrontendObjectCatalog catalog, String indexName);
        void privateUserTenant(FrontendObjectCatalog catalog, String indexName);
    }
}
