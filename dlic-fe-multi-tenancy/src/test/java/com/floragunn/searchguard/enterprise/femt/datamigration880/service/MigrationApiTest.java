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
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.persistence.IndexMigrationStateRepository;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchsupport.junit.matcher.DocNodeMatchers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class MigrationApiTest {

    private static final Logger log = LogManager.getLogger(MigrationApiTest.class);
    private static final String MIGRATION_STATE_DOC_ID = "migration_8_8_0";

    private IndexMigrationStateRepository indexMigrationStateRepository;

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
        .sslEnabled()
        .resources("multitenancy")
        .enterpriseModulesEnabled()
        .build();

    @BeforeClass
    public static void beforeClass() {
        ConfigurationRepository configRepository = cluster.getInjectable(ConfigurationRepository.class);
        SgDynamicConfiguration<Tenant> configuration = configRepository.getConfiguration(CType.TENANTS);
        Set<String> aliasNames = configuration.getCEntries()
            .keySet()
            .stream()
            .filter(tenantName -> !Tenant.GLOBAL_TENANT_ID.equals(tenantName))
            .map(tenantName -> toInternalIndexName(".kibana", tenantName))
            .collect(Collectors.toSet());
        try(Client client = cluster.getInternalNodeClient()) {
            FrontendObjectCatalog catalog = new FrontendObjectCatalog(PrivilegedConfigClient.adapt(client));
            for (String alias : aliasNames) {
                String indexName = alias + "_8.7.0_001";
                String shortAlias = alias + "_8.7.0";
                CreateIndexRequest request = new CreateIndexRequest(indexName).alias(new Alias(alias)) //
                        .alias(new Alias(shortAlias)) //
                        .settings(Settings.builder().put("index.number_of_replicas", 0));
                CreateIndexResponse response = client.admin().indices().create(request).actionGet();
                assertThat(response.isAcknowledged(), equalTo(true));
                catalog.insertSpace(indexName, "default", "custom", "detailed");
            }
            // global tenant index
            String globalTenantIndexName = ".kibana_8.7.0_001";
            CreateIndexResponse response = client.admin().indices() //
                .create(new CreateIndexRequest(globalTenantIndexName) //
                    .alias(new Alias(".kibana_8.7.0")) //
                    .alias(new Alias(".kibana")) //
                    .settings(Settings.builder().put("index.number_of_replicas", 0))) //
                .actionGet();
            assertThat(response.isAcknowledged(), equalTo(true));
            catalog.insertSpace(globalTenantIndexName, "default", "custom", "detailed", "superglobal");
            // user tenant indices
            String[][] userIndicesAndAliases = new String[][] {
                //{"index name", "long alias", "short alias"}
                { ".kibana_3292183_kirk_8.7.0_001", ".kibana_3292183_kirk_8.7.0", ".kibana_3292183_kirk" },// kirk
                { ".kibana_-1091682490_lukasz_8.7.0_001", ".kibana_-1091682490_lukasz_8.7.0", ".kibana_-1091682490_lukasz" }, //lukasz
                { ".kibana_739988528_ukasz_8.7.0_001", ".kibana_739988528_ukasz_8.7.0", ".kibana_739988528_ukasz" }, //łukasz
                { ".kibana_-1091714203_luksz_8.7.0_001", ".kibana_-1091714203_luksz_8.7.0", ".kibana_-1091714203_luksz" }//luk@sz
            };
            for (String[] privateUserTenant : userIndicesAndAliases) {
                String indexName = privateUserTenant[0];
                CreateIndexResponse createIndexResponse = client.admin().indices() //
                    .create(new CreateIndexRequest(indexName) //
                        .alias(new Alias(privateUserTenant[1])) //
                        .alias(new Alias(privateUserTenant[2])) //
                        .settings(Settings.builder().put("index.number_of_replicas", 0))) //
                    .actionGet();
                assertThat(createIndexResponse.isAcknowledged(), equalTo(true));
                catalog.insertSpace(indexName, "default", "super_private");
            }
        }
    }

    @Before
    public void before() {
        Client client = cluster.getInternalNodeClient();
        indexMigrationStateRepository = new IndexMigrationStateRepository(PrivilegedConfigClient.adapt(client));
        if(indexMigrationStateRepository.isIndexCreated()) {
            Awaitility.await("Data migration isn't in progress")
                            .atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(25))
                            .until(() -> {
                                Optional<MigrationExecutionSummary> executionSummary = indexMigrationStateRepository
                                        .findById(MIGRATION_STATE_DOC_ID);
                                return executionSummary.map(summary -> ! summary.isMigrationInProgress(LocalDateTime.now()))
                                        .orElse(true);
                            });
            client.admin().indices().delete(new DeleteIndexRequest(".sg_data_migration_state"));
            assertThatMigrationStateIndexExists(false);
        }
    }

    @Test
    public void shouldStartMigrationProcess() throws Exception {
        try (GenericRestClient client = cluster.createGenericAdminRestClient(Collections.emptyList())) {
            DocNode body = DocNode.EMPTY;
            HttpResponse response = client.postJson("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0", body);

            log.info("Start migration response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
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

    private static String toInternalIndexName(String prefix, String tenant) {
        if (tenant == null) {
            throw new ElasticsearchException("tenant must not be null here");
        }
        String tenantInfoPart = "_" + tenant.hashCode() + "_" + tenant.toLowerCase().replaceAll("[^a-z0-9]+", "");
        StringBuilder result = new StringBuilder(prefix).append(tenantInfoPart);
        return result.toString();
    }

    private void saveMigrationState(MigrationExecutionSummary migrationExecutionSummary) {
        indexMigrationStateRepository.create(MIGRATION_STATE_DOC_ID, migrationExecutionSummary);
    }

    private void assertThatMigrationStateIndexExists(boolean shouldExist) {
        Awaitility.await("Index containing data migration state exists")
                .atMost(Duration.ofSeconds(2)).pollInterval(Duration.ofMillis(25))
                .untilAsserted(() -> assertThat(indexMigrationStateRepository.isIndexCreated(), equalTo(shouldExist)));
    }
}
