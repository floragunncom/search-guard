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

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfig;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.FrontendObjectCatalog;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationConfig;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.TenantIndex;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.BackupIndex;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.DeletableIndex;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.DoubleAliasIndex;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.LegacyIndex;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.BACKUP_CONTAINS_MIGRATED_DATA_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.BACKUP_DOES_NOT_EXIST_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.BACKUP_IS_EMPTY_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.BACKUP_NOT_FOUND_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.DELETE_ALL_BULK_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.DOCUMENT_ALREADY_MIGRATED_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.CANNOT_RESOLVE_INDEX_BY_ALIAS_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.DATA_INDICES_LOCKED_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.EMPTY_MAPPINGS_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.GLOBAL_AND_PRIVATE_TENANT_CONFLICT_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.GLOBAL_TENANT_NOT_FOUND_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.INDICES_NOT_FOUND_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.MULTI_TENANCY_CONFIG_NOT_AVAILABLE_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.UNHEALTHY_INDICES_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MarkerNodeRemoval.withoutMigrationMarker;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.DoubleAliasIndex.LEGACY_VERSION;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.GLOBAL_TENANT_INDEX;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.MULTITENANCY_INDEX_PREFIX;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.PRIVATE_USER_KIRK_INDEX;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_1_INDEX;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.TENANT_MANAGEMENT;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.MigrationEnvironmentHelper.doubleAliasIndexToTenantDataWithoutTenantName;
import static com.floragunn.searchguard.support.PrivilegedConfigClient.adapt;
import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static java.time.ZoneOffset.UTC;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_OK;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MigrationStepsTest {

    private static final Logger log = LogManager.getLogger(MigrationStepsTest.class);

    private static final ZonedDateTime NOW = ZonedDateTime.of(LocalDateTime.of(2000, 1, 1, 1, 1), UTC);
    public static final String TEMP_INDEX_NAME = "data_migration_temp_fe_2000_01_01_01_01_00";
    public static final String BACKUP_INDEX_NAME = "backup_fe_migration_to_8_8_0_2000_01_01_01_01_00";

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
        .nodeSettings("searchguard.unsupported.single_index_mt_enabled", true)
        .singleNode()
        .sslEnabled()
        .resources("multitenancy")
        .enterpriseModulesEnabled()
        .build();

    private final Clock clock = Clock.fixed(NOW.toInstant(), UTC);

    private DataMigrationContext context;

    @Mock
    private FeMultiTenancyConfigurationProvider multiTenancyConfigurationProvider;

    @Mock
    private FeMultiTenancyConfig feMultiTenancyConfig;

    @Rule
    public final MigrationEnvironmentHelper environmentHelper = new MigrationEnvironmentHelper(cluster, clock);

    @Before
    public void before() {
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
    }

    @Test
    public void shouldReportErrorWhenIndexForMigrationAreNotFound() {
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        StepResult result = populateTenantsStep.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isFailure(), equalTo(true));
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(INDICES_NOT_FOUND_ERROR));
    }

    @Test
    public void shouldReportErrorIfMultiTenancyConfigIsNotAvailable() {
        when(multiTenancyConfigurationProvider.getConfig()).thenReturn(Optional.empty());
        when(multiTenancyConfigurationProvider.getTenantNames()).thenReturn(ImmutableSet.empty());
        Client client = cluster.getInternalNodeClient();
        StepRepository repository = new StepRepository(adapt(client));
        PopulateTenantsStep populateTenantsStep = new PopulateTenantsStep(multiTenancyConfigurationProvider, repository);

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(MULTI_TENANCY_CONFIG_NOT_AVAILABLE_ERROR));
    }

    @Test
    public void shouldFindGlobalTenantIndexForVersion8_7_0() {
        DoubleAliasIndex
            taskManagerIndex = new DoubleAliasIndex(".kibana_task_manager_8.7.0_001", ".kibana_task_manager_8.7.0", "kibana_task_manager");
        DoubleAliasIndex
            eventLogIndex = new DoubleAliasIndex(".kibana-event-log-8.7.0-000001", ".kibana-event-log-8.7.0", ".kibana-event-log");
        DoubleAliasIndex
            dataIndex = new DoubleAliasIndex("iot-2020-09", "iot-2020", "iot");
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, taskManagerIndex, eventLogIndex);
        environmentHelper.createIndex("iot", 0, null, dataIndex);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        populateTenantsStep.execute(context);

        ImmutableList<TenantIndex> tenants = context.getTenantIndices();
        assertThat(tenants, hasSize(1));
        assertThat(tenants.get(0).belongsToGlobalTenant(), equalTo(true));
    }

    @Test
    public void shouldNotUseIndicesInLegacyVersion() {
        // indices with without up-to-date versions but with three aliases e.g.
        // .kibana_1593390681_performancedata_8.6.0_001 <- index name
        // .kibana_1593390681_performancedata_8.6.0 <- long alias
        // .kibana_1593390681_performancedata <- short alias name, this alias may cause that the index is recognized as up-to-date

        // The above alias can exist if user performs the following steps
        // 1. Add tenant performance_data to SG configuration in version 8.6.0
        // 2. Delete tenant from SG configuration
        // 3. Perform upgrade to version 8.7.0

        // Example of up-to-date index:
        // .kibana_-1992298040_financemanagement_8.7.0_001 <- index name
        // .kibana_-1992298040_financemanagement_8.7.0 <- long alias
        // .kibana_-1992298040_financemanagement <- short alias
        // Therefore only indices in version 8.7.x should be migrated
        DoubleAliasIndex legacyPerformanceData = DoubleAliasIndex
            .forTenantWithPrefix(MULTITENANCY_INDEX_PREFIX, "performance_data", "8.6.0");
        DoubleAliasIndex legacyEnterpriseTenant = DoubleAliasIndex
            .forTenantWithPrefix(MULTITENANCY_INDEX_PREFIX, "enterprise_tenant", "8.6.0");
        DoubleAliasIndex legacyUserTenant = DoubleAliasIndex
            .forTenantWithPrefix(MULTITENANCY_INDEX_PREFIX, "james_bond", "8.4.0");
        DoubleAliasIndex indexWithVersionMissMatch = DoubleAliasIndex
            .forTenantWithPrefix(MULTITENANCY_INDEX_PREFIX, "command_tenant", "8.7.159");
        DoubleAliasIndex modernFinanceManagement = environmentHelper.doubleAliasForTenant("finance_management");
        DoubleAliasIndex modernBusinessIntelligence = environmentHelper.doubleAliasForTenant("business_intelligence");
        LegacyIndex olderBusinessIntelligence = modernBusinessIntelligence.toLegacyIndex("8.6.0");
        LegacyIndex oldestBusinessIntelligence = modernBusinessIntelligence.toLegacyIndex("8.4.0");
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, legacyPerformanceData, legacyEnterpriseTenant, legacyUserTenant,
            modernFinanceManagement, modernBusinessIntelligence, indexWithVersionMissMatch, PRIVATE_USER_LUKASZ_1_INDEX);
        environmentHelper.createLegacyIndex(olderBusinessIntelligence, oldestBusinessIntelligence);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        populateTenantsStep.execute(context);

        ImmutableList<TenantIndex> tenants = context.getTenantIndices();
        assertThat(tenants, hasSize(4));
        assertThat(tenants.get(0).belongsToGlobalTenant(), equalTo(true));
        Set<String> tenantNames = tenants.stream() //
            .filter(tenantIndex -> ! tenantIndex.belongsToUserPrivateTenant()) //
            .filter(tenantIndex -> ! tenantIndex.belongsToGlobalTenant()) //
            .map(TenantIndex::tenantName).collect(Collectors.toSet());
        assertThat(tenantNames, containsInAnyOrder("finance_management", "business_intelligence"));
        List<String> privateUserTenantIndexNames = tenants.stream() //
            .filter(TenantIndex::belongsToUserPrivateTenant) //
            .map(TenantIndex::indexName) //
            .toList();
        assertThat(privateUserTenantIndexNames, hasSize(1));
        assertThat(privateUserTenantIndexNames, containsInAnyOrder(PRIVATE_USER_LUKASZ_1_INDEX.indexName()));
    }

    @Test
    public void shouldFindGlobalTenantIndexForVersion8_7_1() {
        DoubleAliasIndex
            taskManagerIndex = new DoubleAliasIndex(".kibana_task_manager_8.7.0_001", ".kibana_task_manager_8.7.0", "kibana_task_manager");
        DoubleAliasIndex
            eventLogIndex = new DoubleAliasIndex(".kibana-event-log-8.7.0-000001", ".kibana-event-log-8.7.0", ".kibana-event-log");
        DoubleAliasIndex
            dataIndex = new DoubleAliasIndex("iot-2020-09", "iot-2020", "iot");
        DoubleAliasIndex globalTenantIndex = new DoubleAliasIndex(".kibana_8.7.1_001", ".kibana_8.7.1",
            MULTITENANCY_INDEX_PREFIX);
        environmentHelper.createIndex(globalTenantIndex, taskManagerIndex, eventLogIndex);
        environmentHelper.createIndex("iot", 0, null, dataIndex);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        populateTenantsStep.execute(context);

        ImmutableList<TenantIndex> tenants = context.getTenantIndices();
        assertThat(tenants, hasSize(1));
        assertThat(tenants.get(0).belongsToGlobalTenant(), equalTo(true));
    }

    @Test
    public void shouldFindGlobalTenantIndexForVersion8_7_3() {
        DoubleAliasIndex
            taskManagerIndex = new DoubleAliasIndex(".kibana_task_manager_8.7.0_001", ".kibana_task_manager_8.7.0", "kibana_task_manager");
        DoubleAliasIndex
            eventLogIndex = new DoubleAliasIndex(".kibana-event-log-8.7.0-000001", ".kibana-event-log-8.7.0", ".kibana-event-log");
        DoubleAliasIndex
            dataIndex = new DoubleAliasIndex("iot-2020-09", "iot-2020", "iot");
        DoubleAliasIndex globalTenantIndex = new DoubleAliasIndex(".kibana_8.7.3_004", ".kibana_8.7.3",
            MULTITENANCY_INDEX_PREFIX);
        environmentHelper.createIndex(globalTenantIndex, taskManagerIndex, eventLogIndex);
        environmentHelper.createIndex("iot", 0, null, dataIndex);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        populateTenantsStep.execute(context);

        ImmutableList<TenantIndex> tenants = context.getTenantIndices();
        assertThat(tenants, hasSize(1));
        assertThat(tenants.get(0).belongsToGlobalTenant(), equalTo(true));
    }

    @Test
    public void shouldFindGlobalTenantIndexForVersion8_7_0_pointedByAlias8_7_1() {
        DoubleAliasIndex
            taskManagerIndex = new DoubleAliasIndex(".kibana_task_manager_8.7.0_001", ".kibana_task_manager_8.7.0", "kibana_task_manager");
        DoubleAliasIndex
            eventLogIndex = new DoubleAliasIndex(".kibana-event-log-8.7.0-000001", ".kibana-event-log-8.7.0", ".kibana-event-log");
        DoubleAliasIndex
            dataIndex = new DoubleAliasIndex("iot-2020-09", "iot-2020", "iot");
        DoubleAliasIndex globalTenantIndex = new DoubleAliasIndex(".kibana_8.7.0_001", ".kibana_8.7.1",
            MULTITENANCY_INDEX_PREFIX);
        environmentHelper.createIndex(globalTenantIndex, taskManagerIndex, eventLogIndex);
        environmentHelper.createIndex("iot", 0, null, dataIndex);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        populateTenantsStep.execute(context);

        ImmutableList<TenantIndex> tenants = context.getTenantIndices();
        assertThat(tenants, hasSize(1));
        assertThat(tenants.get(0).belongsToGlobalTenant(), equalTo(true));
    }

    @Test
    public void shouldFindGlobalTenantIndexForVersion8_7_1_pointedByAlias8_7_3() {
        DoubleAliasIndex
            taskManagerIndex = new DoubleAliasIndex(".kibana_task_manager_8.7.0_001", ".kibana_task_manager_8.7.0", "kibana_task_manager");
        DoubleAliasIndex
            eventLogIndex = new DoubleAliasIndex(".kibana-event-log-8.7.0-000001", ".kibana-event-log-8.7.0", ".kibana-event-log");
        DoubleAliasIndex
            dataIndex = new DoubleAliasIndex("iot-2020-09", "iot-2020", "iot");
        DoubleAliasIndex globalTenantIndex = new DoubleAliasIndex(".kibana_8.7.1_001", ".kibana_8.7.3",
            MULTITENANCY_INDEX_PREFIX);
        environmentHelper.createIndex(globalTenantIndex, taskManagerIndex, eventLogIndex);
        environmentHelper.createIndex("iot", 0, null, dataIndex);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        populateTenantsStep.execute(context);

        ImmutableList<TenantIndex> tenants = context.getTenantIndices();
        assertThat(tenants, hasSize(1));
        assertThat(tenants.get(0).belongsToGlobalTenant(), equalTo(true));
    }

    @Test
    public void shouldFindTenantsIndicesForVersion8_7_0_pointedByAlias8_7_1() {
        DoubleAliasIndex
            taskManagerIndex = new DoubleAliasIndex(".kibana_task_manager_8.7.0_001", ".kibana_task_manager_8.7.0", "kibana_task_manager");
        DoubleAliasIndex
            eventLogIndex = new DoubleAliasIndex(".kibana-event-log-8.7.0-000001", ".kibana-event-log-8.7.0", ".kibana-event-log");
        DoubleAliasIndex
            dataIndex = new DoubleAliasIndex("iot-2020-09", "iot-2020", "iot");
        DoubleAliasIndex globalTenantIndex = new DoubleAliasIndex(".kibana_8.7.0_001", ".kibana_8.7.1",
            MULTITENANCY_INDEX_PREFIX);
        DoubleAliasIndex adminTenant = new DoubleAliasIndex(".kibana_-152937574_admintenant_8.7.0_001", ".kibana_-152937574_admintenant", ".kibana_-152937574_admintenant_8.7.1");
        DoubleAliasIndex privateTenant = new DoubleAliasIndex(".kibana_92668751_admin_8.7.0_001", ".kibana_92668751_admin", ".kibana_92668751_admin_8.7.1");
        environmentHelper.createIndex(globalTenantIndex, adminTenant, privateTenant, taskManagerIndex, eventLogIndex);
        environmentHelper.createIndex("iot", 0, null, dataIndex);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        populateTenantsStep.execute(context);

        ImmutableList<TenantIndex> tenants = context.getTenantIndices();
        assertThat(tenants, hasSize(3));
        assertThat(tenants.stream().filter(TenantIndex::belongsToGlobalTenant).count() == 1, equalTo(true));
        assertThat(tenants.stream().map(TenantIndex::tenantName).filter(tenantName -> "admin_tenant".equals(tenantName)).count() == 1, equalTo(true));
        assertThat(tenants.stream().filter(TenantIndex::belongsToUserPrivateTenant).map(TenantIndex::indexName).findFirst().orElseThrow(), equalTo(".kibana_92668751_admin_8.7.0_001"));
    }

    @Test
    public void shouldFindTenantsIndicesForVersion8_7_0_pointedByAlias8_7_3() {
        DoubleAliasIndex
            taskManagerIndex = new DoubleAliasIndex(".kibana_task_manager_8.7.0_001", ".kibana_task_manager_8.7.0", "kibana_task_manager");
        DoubleAliasIndex
            eventLogIndex = new DoubleAliasIndex(".kibana-event-log-8.7.0-000001", ".kibana-event-log-8.7.0", ".kibana-event-log");
        DoubleAliasIndex
            dataIndex = new DoubleAliasIndex("iot-2020-09", "iot-2020", "iot");
        DoubleAliasIndex globalTenantIndex = new DoubleAliasIndex(".kibana_8.7.0_001", ".kibana_8.7.3",
            MULTITENANCY_INDEX_PREFIX);
        DoubleAliasIndex adminTenant = new DoubleAliasIndex(".kibana_-152937574_admintenant_8.7.0_001", ".kibana_-152937574_admintenant", ".kibana_-152937574_admintenant_8.7.3");
        DoubleAliasIndex privateTenant = new DoubleAliasIndex(".kibana_92668751_admin_8.7.0_001", ".kibana_92668751_admin", ".kibana_92668751_admin_8.7.3");
        environmentHelper.createIndex(globalTenantIndex, adminTenant, privateTenant, taskManagerIndex, eventLogIndex);
        environmentHelper.createIndex("iot", 0, null, dataIndex);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        populateTenantsStep.execute(context);

        ImmutableList<TenantIndex> tenants = context.getTenantIndices();
        assertThat(tenants, hasSize(3));
        assertThat(tenants.stream().filter(TenantIndex::belongsToGlobalTenant).count() == 1, equalTo(true));
        assertThat(tenants.stream().map(TenantIndex::tenantName).filter(tenantName -> "admin_tenant".equals(tenantName)).count() == 1, equalTo(true));
        assertThat(tenants.stream().filter(TenantIndex::belongsToUserPrivateTenant).map(TenantIndex::indexName).findFirst().orElseThrow(), equalTo(".kibana_92668751_admin_8.7.0_001"));
    }

    @Test
    public void shouldFindGlobalTenantIndexForVersion8_7_11() {
        DoubleAliasIndex
            taskManagerIndex = new DoubleAliasIndex(".kibana_task_manager_8.7.0_001", ".kibana_task_manager_8.7.0", "kibana_task_manager");
        DoubleAliasIndex
            eventLogIndex = new DoubleAliasIndex(".kibana-event-log-8.7.0-000001", ".kibana-event-log-8.7.0", ".kibana-event-log");
        DoubleAliasIndex
            dataIndex = new DoubleAliasIndex("iot-2020-09", "iot-2020", "iot");
        DoubleAliasIndex globalTenantIndex = new DoubleAliasIndex(".kibana_8.7.11_004", ".kibana_8.7.11",
            MULTITENANCY_INDEX_PREFIX);
        environmentHelper.createIndex(globalTenantIndex, taskManagerIndex, eventLogIndex);
        environmentHelper.createIndex("iot", 0, null, dataIndex);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        populateTenantsStep.execute(context);

        ImmutableList<TenantIndex> tenants = context.getTenantIndices();
        assertThat(tenants, hasSize(1));
        assertThat(tenants.get(0).belongsToGlobalTenant(), equalTo(true));
    }

    @Test
    public void shouldReportErrorWhenManyGlobalIndicesExist() {
        DoubleAliasIndex
            taskManagerIndex = new DoubleAliasIndex(".kibana_task_manager_8.7.0_001", ".kibana_task_manager_8.7.0", "kibana_task_manager");
        DoubleAliasIndex
            eventLogIndex = new DoubleAliasIndex(".kibana-event-log-8.7.0-000001", ".kibana-event-log-8.7.0", ".kibana-event-log");
        DoubleAliasIndex
            dataIndex = new DoubleAliasIndex("iot-2020-09", "iot-2020", "iot");
        DoubleAliasIndex globalTenantIndex1 = new DoubleAliasIndex(".kibana_8.7.11_004", ".kibana_8.7.11",
            MULTITENANCY_INDEX_PREFIX);
        DoubleAliasIndex globalTenantIndex2 = new DoubleAliasIndex(".kibana_8.7.10_001", ".kibana_8.7.10",
            MULTITENANCY_INDEX_PREFIX);
        environmentHelper.createIndex(globalTenantIndex1, globalTenantIndex2, taskManagerIndex, eventLogIndex);
        environmentHelper.createIndex("iot", 0, null, dataIndex);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(GLOBAL_TENANT_NOT_FOUND_ERROR));
    }

    @Test
    public void shouldReportErrorInCaseOfConflictBetweenUserPrivateTenantAndGlobalTenant() {
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, environmentHelper.doubleAliasForTenant(Tenant.GLOBAL_TENANT_ID));
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        var stepException = (StepException) assertThatThrown(() -> populateTenantsStep.execute(context), instanceOf(StepException.class));

        assertThat(stepException.getStatus(), equalTo(GLOBAL_AND_PRIVATE_TENANT_CONFLICT_ERROR));
    }

    @Test
    public void shouldBreakMigrationProcessWhenGlobalTenantIndexIsNotFound() {
        List<DoubleAliasIndex> configuredTenantIndices = environmentHelper.getIndicesForConfiguredTenantsWithoutGlobal();
        environmentHelper.createIndex(configuredTenantIndices);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(GLOBAL_TENANT_NOT_FOUND_ERROR));
    }

    @Test
    public void shouldFindAllConfiguredTenants() {
        List<DoubleAliasIndex> configuredTenantIndices = environmentHelper.getIndicesForConfiguredTenantsWithoutGlobal();
        environmentHelper.createIndex(configuredTenantIndices);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getTenantIndices(), hasSize(configuredTenantIndices.size() + 1));
        assertThat(context.getTenantIndices().size(), greaterThan(20));
        List<String> tenantsFoundByStep = context.getTenantIndices().stream().map(TenantIndex::tenantName).collect(Collectors.toList());
        FeMultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(FeMultiTenancyConfigurationProvider.class);
        String[] tenantsFromConfiguration = configurationProvider.getTenantNames().toArray(String[]::new);
        assertThat(tenantsFoundByStep, containsInAnyOrder(tenantsFromConfiguration));
    }

    @Test
    public void shouldFindUserPrivateTenants() {
        environmentHelper.createIndex(
            GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, PRIVATE_USER_LUKASZ_1_INDEX, MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_2_INDEX,
            MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_3_INDEX);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getTenantIndices(), hasSize(5));
        Set<String> privateTenantIndexNames = context.getTenantIndices() //
            .stream() //
            .filter(TenantIndex::belongsToUserPrivateTenant) //
            .map(TenantIndex::indexName) //
            .collect(Collectors.toSet());
        assertThat(privateTenantIndexNames, containsInAnyOrder(
            PRIVATE_USER_KIRK_INDEX.indexName(),
            PRIVATE_USER_LUKASZ_1_INDEX.indexName(),
            MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_2_INDEX.indexName(),
            MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_3_INDEX.indexName()));
    }

    /**
     * The test check paging related problems.
     */
    @Test
    public void shouldFindLargeNumberOfTenants() {
        List<DoubleAliasIndex> indices = new ArrayList<>();
        indices.add(GLOBAL_TENANT_INDEX);
        indices.addAll(environmentHelper.getIndicesForConfiguredTenantsWithoutGlobal());
        indices.addAll(environmentHelper.generatePrivateTenantNames(MULTITENANCY_INDEX_PREFIX, 101));
        environmentHelper.createIndex(indices);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getTenantIndices(), hasSize(indices.size()));
    }

    @Test
    public void shouldUseIndexPrefixReadFromConfiguration() {
        String indexNamePrefix = "wideopenfindashboard";
        DoubleAliasIndex privateUserTenant = environmentHelper.generatePrivateTenantNames(indexNamePrefix, 1).get(0);
        DoubleAliasIndex globalTenantIndex = new DoubleAliasIndex(indexNamePrefix + "_8.7.0_001",
            indexNamePrefix + "_8.7.0", indexNamePrefix);
        environmentHelper.createIndex(indexNamePrefix, 0, null, globalTenantIndex);
        environmentHelper.createIndex(indexNamePrefix, 0, null, privateUserTenant);
        when(feMultiTenancyConfig.getIndex()).thenReturn(indexNamePrefix);
        when(multiTenancyConfigurationProvider.getConfig()).thenReturn(Optional.of(feMultiTenancyConfig));
        when(multiTenancyConfigurationProvider.getTenantNames()).thenReturn(ImmutableSet.empty());
        Client client = cluster.getInternalNodeClient();
        StepRepository repository = new StepRepository(adapt(client));
        PopulateTenantsStep populateTenantsStep = new PopulateTenantsStep(multiTenancyConfigurationProvider, repository);

        StepResult result = populateTenantsStep.execute(context);

        log.debug("Step result: '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getTenantIndices(), hasSize(2));
        TenantIndex tenantIndex = context.getTenantIndices() //
            .stream() //
            .filter(TenantIndex::belongsToUserPrivateTenant) //
            .findAny() //
            .orElseThrow();
        assertThat(tenantIndex.indexName(), containsString(indexNamePrefix));
    }

    @Test
    public void shouldDetectNewlyCreatedTenant() throws Exception {
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();
        try(GenericRestClient client = cluster.getAdminCertRestClient()) {
            String body = DocNode.of("description", "Tenant created to test reading the newest multi-tenancy configuration").toJsonString();
            String newTenantName = "tenant-created-in-course-of-test-searching";
            HttpResponse response = client.putJson("/_searchguard/api/tenants/" + newTenantName, body);
            try {
                log.debug("Create tenant status code '{}' and response body '{}'", response.getStatusCode(), response.getBody());
                assertThat(response.getStatusCode(), equalTo(SC_CREATED));
                environmentHelper.createIndex(GLOBAL_TENANT_INDEX, environmentHelper.doubleAliasForTenant(newTenantName));

                StepResult result = populateTenantsStep.execute(context);

                log.debug("Step result: '{}'.", result);
                assertThat(result.isSuccess(), equalTo(true));
                assertThat(context.getTenantIndices(), hasSize(2));
                TenantIndex createdTenant = context.getTenantIndices().stream().filter(tenantIndex -> !tenantIndex.belongsToGlobalTenant()).findAny().orElseThrow();
                assertThat(createdTenant.tenantName(), equalTo(newTenantName));
                assertThat(createdTenant.belongsToUserPrivateTenant(), equalTo(false));
            } finally {
                response = client.delete("/_searchguard/api/tenants/" + newTenantName);
                assertThat(response.getStatusCode(), equalTo(SC_OK));
            }
        }
    }

    @Test
    public void shouldReportErrorWhenAliasIsAssociatedWithTooManyIndices() {
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();
        DoubleAliasIndex index = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        String additionalIndex = index.indexName() + "_another_index";
        Client client = cluster.getInternalNodeClient();
        client.admin().indices().create(new CreateIndexRequest(index.indexName())).actionGet();
        environmentHelper.addCreatedIndex(index);
        client.admin().indices().create(new CreateIndexRequest(additionalIndex)).actionGet();
        try {
            AliasActions aliasAction = new AliasActions(AliasActions.Type.ADD) //
                .alias(index.shortAlias()) //
                .indices(index.indexName(), additionalIndex);
            client.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(aliasAction)).actionGet();

            var ex = (StepException) assertThatThrown(() -> populateTenantsStep.execute(context), instanceOf(StepException.class));

            assertThat(ex.getStatus(), equalTo(CANNOT_RESOLVE_INDEX_BY_ALIAS_ERROR));
        } finally {
            client.admin().indices().delete(new DeleteIndexRequest(additionalIndex));
        }
    }

    @Test
    public void shouldCreateValidLegacyNames() {
        DoubleAliasIndex privateUserTenant = environmentHelper.doubleAliasForTenant("spock");

        assertThat(privateUserTenant.indexName(), equalTo(".kibana_109651354_spock_8.7.0_001"));
        assertThat(privateUserTenant.longAlias(), equalTo(".kibana_109651354_spock_8.7.0"));
        assertThat(privateUserTenant.shortAlias(), equalTo(".kibana_109651354_spock"));
        assertThat(privateUserTenant.getIndexNameInVersion(LEGACY_VERSION), equalTo(".kibana_109651354_spock_7.17.12_001"));
        assertThat(privateUserTenant.getLongAliasInVersion(LEGACY_VERSION), equalTo(".kibana_109651354_spock_7.17.12"));
    }

    @Test
    public void shouldChooseIndicesRelatedToNewestVersion() {
        ImmutableList<DoubleAliasIndex> indices = ImmutableList.of("admin_tenant", TENANT_MANAGEMENT, "kirk", "spock", "łuk@sz") //
            .map(environmentHelper::doubleAliasForTenant) //
            .with(GLOBAL_TENANT_INDEX);
        environmentHelper.createIndex(indices);
        environmentHelper.createLegacyIndex(indices.map(index -> index.toLegacyIndex(LEGACY_VERSION)).toArray(LegacyIndex[]::new));
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getTenantIndices(), hasSize(6));
        assertThat(context.getTenantIndices().map(TenantIndex::indexName), containsInAnyOrder(".kibana_8.7.0_001",
            ".kibana_-152937574_admintenant_8.7.0_001",
            ".kibana_-1799980989_management_8.7.0_001",
            ".kibana_3292183_kirk_8.7.0_001",
            ".kibana_739956815_uksz_8.7.0_001",
            ".kibana_109651354_spock_8.7.0_001"));
        // check that legacy indices was in fact created in course of the test
        assertThat(
            environmentHelper.isIndexCreated(".kibana_-152937574_admintenant_7.17.12_001",
                ".kibana_-152937574_admintenant_7.17.12", ".kibana_-1799980989_management_7.17.12_001",
                ".kibana_-1799980989_management_7.17.12", ".kibana_3292183_kirk_7.17.12_001", ".kibana_3292183_kirk_7.17.12",
                ".kibana_739956815_uksz_7.17.12_001", ".kibana_739956815_uksz_7.17.12_001",
                ".kibana_109651354_spock_7.17.12_001", ".kibana_109651354_spock_7.17.12"
        ), equalTo(true));
    }

    @Test
    public void shouldRecognizeTenantType() {
        ImmutableList<DoubleAliasIndex> indices = ImmutableList.of("admin_tenant",  "kirk") //
            .map(environmentHelper::doubleAliasForTenant) //
            .with(GLOBAL_TENANT_INDEX);
        environmentHelper.createIndex(indices);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getTenantIndices(), hasSize(3));
        String global = context.getTenantIndices() //
            .stream() //
            .filter(TenantIndex::belongsToGlobalTenant) //
            .map(TenantIndex::tenantName) //
            .findFirst() //
            .orElseThrow();
        String userPrivateIndex = context.getTenantIndices() //
            .stream() //
            .filter(TenantIndex::belongsToUserPrivateTenant) //
            .map(TenantIndex::indexName) //
            .findFirst() //
            .orElseThrow();
        String configured = context.getTenantIndices().stream() //
            .filter(tenant -> !tenant.belongsToGlobalTenant()) //
            .filter(tenant -> !tenant.belongsToUserPrivateTenant()) //
            .map(TenantIndex::tenantName) //
            .findFirst() //
            .orElseThrow();
        assertThat(global, equalTo(Tenant.GLOBAL_TENANT_ID));
        assertThat(configured, equalTo("admin_tenant"));
        assertThat(userPrivateIndex, equalTo(".kibana_3292183_kirk_8.7.0_001"));
    }

    @Test
    public void shouldDetectIndexInYellowState() {
        CheckIndicesStateStep step = createCheckIndicesStateStep();
        environmentHelper.createIndex(MULTITENANCY_INDEX_PREFIX, 25, Settings.EMPTY, GLOBAL_TENANT_INDEX);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        context.setBackupIndices(ImmutableList.empty());

        StepResult result = step.execute(context);

        log.debug("Check index step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.details(), containsString(GLOBAL_TENANT_INDEX.indexName()));
    }

    @Test
    public void shouldNotReportErrorWhenIndicesAreInGreenState() {
        CheckIndicesStateStep step = createCheckIndicesStateStep();
        var indices = ImmutableList.of(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, PRIVATE_USER_LUKASZ_1_INDEX);
        environmentHelper.createIndex(MULTITENANCY_INDEX_PREFIX, 0, Settings.EMPTY, indices.toArray(DoubleAliasIndex[]::new));
        context.setTenantIndices(doubleAliasIndexToTenantDataWithoutTenantName(indices));
        context.setBackupIndices(ImmutableList.empty());

        StepResult result = step.execute(context);

        log.debug("Check index step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
    }

    @Test
    public void shouldReportErrorWhenOnlyOneOfIndicesIsYellow() {
        CheckIndicesStateStep step = createCheckIndicesStateStep();
        var indices = ImmutableList.of(GLOBAL_TENANT_INDEX, PRIVATE_USER_LUKASZ_1_INDEX);
        environmentHelper.createIndex(MULTITENANCY_INDEX_PREFIX, 0, Settings.EMPTY, indices.toArray(DoubleAliasIndex[]::new));
        environmentHelper.createIndex(MULTITENANCY_INDEX_PREFIX, 25, Settings.EMPTY, PRIVATE_USER_KIRK_INDEX); // this index should be yellow
        indices = indices.with(PRIVATE_USER_KIRK_INDEX);
        context.setTenantIndices(doubleAliasIndexToTenantDataWithoutTenantName(indices));
        context.setBackupIndices(ImmutableList.empty());

        StepResult result = step.execute(context);

        log.debug("Check index step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.details(), containsString(PRIVATE_USER_KIRK_INDEX.indexName()));
    }

    @Test
    public void shouldBeConfiguredToAllowYellowIndices() {
        CheckIndicesStateStep step = createCheckIndicesStateStep();
        var indices = ImmutableList.of(GLOBAL_TENANT_INDEX, PRIVATE_USER_LUKASZ_1_INDEX);
        environmentHelper.createIndex(MULTITENANCY_INDEX_PREFIX, 0, Settings.EMPTY, indices.toArray(DoubleAliasIndex[]::new));
        environmentHelper.createIndex(MULTITENANCY_INDEX_PREFIX, 25, Settings.EMPTY, PRIVATE_USER_KIRK_INDEX); // this index should be yellow
        indices = indices.with(PRIVATE_USER_KIRK_INDEX);
        context = new DataMigrationContext(new MigrationConfig(true), clock);
        context.setTenantIndices(doubleAliasIndexToTenantDataWithoutTenantName(indices));
        context.setBackupIndices(ImmutableList.empty());

        StepResult result = step.execute(context);

        log.debug("Check index step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
    }

    @Test
    public void shouldNotFindBlockedIndices() {
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX);
        context.setTenantIndices(doubleAliasIndexToTenantDataWithoutTenantName(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX));
        CheckIfIndicesAreBlockedStep step = new CheckIfIndicesAreBlockedStep(new StepRepository(environmentHelper.getPrivilegedClient()));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(result.details(), containsString(PRIVATE_USER_KIRK_INDEX.indexName()));
    }

    @Test
    public void shouldAllowWriteBlockOnGlobalTenantIndex() throws Exception {
        DoubleAliasIndex managementTenantIndex = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, managementTenantIndex);
        var tenantIndices = doubleAliasIndexToTenantDataWithoutTenantName(
            GLOBAL_TENANT_INDEX,
            PRIVATE_USER_KIRK_INDEX,
            managementTenantIndex);
        context.setTenantIndices(tenantIndices);
        CheckIfIndicesAreBlockedStep step = new CheckIfIndicesAreBlockedStep(new StepRepository(environmentHelper.getPrivilegedClient()));
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient(ImmutableList.empty())){
            HttpResponse response = adminClient.put("/" + GLOBAL_TENANT_INDEX.indexName() + "/_block/write");
            assertThat(response.getStatusCode(), equalTo(SC_OK));
        }

        StepResult result = step.execute(context);
        assertThat(result.isSuccess(), equalTo(true));
    }

    @Test
    public void shouldFindWriteBlockedIndices() throws Exception {
        DoubleAliasIndex managementTenantIndex = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, managementTenantIndex);
        var tenantIndices = doubleAliasIndexToTenantDataWithoutTenantName(
            GLOBAL_TENANT_INDEX,
            PRIVATE_USER_KIRK_INDEX,
            managementTenantIndex);
        context.setTenantIndices(tenantIndices);
        CheckIfIndicesAreBlockedStep step = new CheckIfIndicesAreBlockedStep(new StepRepository(environmentHelper.getPrivilegedClient()));
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient(ImmutableList.empty())){
            HttpResponse response = adminClient.put("/" + managementTenantIndex.indexName() + "/_block/write");
            assertThat(response.getStatusCode(), equalTo(SC_OK));
        }

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(DATA_INDICES_LOCKED_ERROR));
        assertThat(result.details(), containsString("write"));
    }

    @Test
    public void shouldFindReadBlockedIndices() throws Exception {
        DoubleAliasIndex managementTenantIndex = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, managementTenantIndex);
        var tenantIndices = doubleAliasIndexToTenantDataWithoutTenantName(
            GLOBAL_TENANT_INDEX,
            PRIVATE_USER_KIRK_INDEX,
            managementTenantIndex);
        context.setTenantIndices(tenantIndices);
        CheckIfIndicesAreBlockedStep step = new CheckIfIndicesAreBlockedStep(new StepRepository(environmentHelper.getPrivilegedClient()));
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient(ImmutableList.empty())){
            HttpResponse response = adminClient.put("/" + managementTenantIndex.indexName() + "/_block/read");
            assertThat(response.getStatusCode(), equalTo(SC_OK));
        }

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(DATA_INDICES_LOCKED_ERROR));
        assertThat(result.details(), containsString("read"));
    }

    @Test
    public void shouldFindReadOnlyBlockedIndices() throws Exception {
        DoubleAliasIndex managementTenantIndex = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, managementTenantIndex);
        var tenantIndices = doubleAliasIndexToTenantDataWithoutTenantName(
            GLOBAL_TENANT_INDEX,
            PRIVATE_USER_KIRK_INDEX,
            managementTenantIndex);
        context.setTenantIndices(tenantIndices);
        StepRepository stepRepository = new StepRepository(environmentHelper.getPrivilegedClient());
        CheckIfIndicesAreBlockedStep step = new CheckIfIndicesAreBlockedStep(stepRepository);
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient(ImmutableList.empty())){
            HttpResponse response = adminClient.put("/" + managementTenantIndex.indexName() + "/_block/read_only");
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            try {

                StepResult result = step.execute(context);

                assertThat(result.isSuccess(), equalTo(false));
                assertThat(result.status(), equalTo(DATA_INDICES_LOCKED_ERROR));
                assertThat(result.details(), containsString("read_only"));
            } finally {
                DocNode body = DocNode.of("index.blocks.read_only", false);
                adminClient.putJson("/" + managementTenantIndex.indexName() + "/_settings", body.toJsonString());
            }
        }
    }

    @Test
    public void shouldFindMetadataBlockedIndices() throws Exception {
        DoubleAliasIndex managementTenantIndex = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, managementTenantIndex);
        var tenantIndices = doubleAliasIndexToTenantDataWithoutTenantName(managementTenantIndex,
            GLOBAL_TENANT_INDEX,
            PRIVATE_USER_KIRK_INDEX);
        context.setTenantIndices(tenantIndices);
        StepRepository stepRepository = new StepRepository(environmentHelper.getPrivilegedClient());
        CheckIfIndicesAreBlockedStep step = new CheckIfIndicesAreBlockedStep(stepRepository);
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient(ImmutableList.empty())) {
            HttpResponse response = adminClient.put("/" + managementTenantIndex.indexName() + "/_block/metadata");
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            try {

                StepResult result = step.execute(context);

                assertThat(result.isSuccess(), equalTo(false));
                assertThat(result.status(), equalTo(DATA_INDICES_LOCKED_ERROR));
                assertThat(result.details(), containsString("metadata"));
            } finally {
                DocNode body = DocNode.of("index.blocks.metadata", false);
                adminClient.putJson("/" + managementTenantIndex.indexName() + "/_settings", body.toJsonString());
            }
        }
    }

    @Test
    public void shouldNotFindAnyBackupIndices() {
        PopulateBackupIndicesStep step = new PopulateBackupIndicesStep(new StepRepository(environmentHelper.getPrivilegedClient()));

        StepResult result = step.execute(context);

        log.info("Step response " + result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupIndices(), empty());
    }

    @Test
    public void shouldFindSingleBackupIndex() {
        PopulateBackupIndicesStep step = new PopulateBackupIndicesStep(new StepRepository(environmentHelper.getPrivilegedClient()));
        BackupIndex backupIndex = new BackupIndex(NOW.toLocalDateTime());
        environmentHelper.createBackupIndex(backupIndex);

        StepResult result = step.execute(context);

        log.info("Find backup indices result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupIndices(), hasSize(1));
        assertThat(context.getBackupIndices().get(0), equalTo(backupIndex.indexName()));
    }

    @Test
    public void shouldFindManyBackupIndices_1() {
        PopulateBackupIndicesStep step = new PopulateBackupIndicesStep(new StepRepository(environmentHelper.getPrivilegedClient()));
        BackupIndex backupIndex1 = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndex2 = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        BackupIndex backupIndex3 = new BackupIndex(NOW.toLocalDateTime().minusDays(2));
        BackupIndex backupIndex4 = new BackupIndex(NOW.toLocalDateTime().minusDays(3));
        BackupIndex backupIndex5 = new BackupIndex(NOW.toLocalDateTime().minusDays(4));
        BackupIndex backupIndex6 = new BackupIndex(NOW.toLocalDateTime().minusDays(5));
        BackupIndex backupIndex7 = new BackupIndex(NOW.toLocalDateTime().minusDays(6));
        environmentHelper.createBackupIndex(backupIndex1,
            backupIndex2,
            backupIndex3,
            backupIndex4,
            backupIndex5,
            backupIndex6,
            backupIndex7);

        StepResult result = step.execute(context);

        log.info("Find backup indices result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupIndices(), hasSize(7));
        assertThat(context.getBackupIndices(), contains(backupIndex1.indexName(),
            backupIndex2.indexName(),
            backupIndex3.indexName(),
            backupIndex4.indexName(),
            backupIndex5.indexName(),
            backupIndex6.indexName(),
            backupIndex7.indexName()));
        assertThat(context.getNewestExistingBackupIndex().orElseThrow(), equalTo(backupIndex1.indexName()));
    }

    @Test
    public void shouldFindManyBackupIndices_2() {
        PopulateBackupIndicesStep step = new PopulateBackupIndicesStep(new StepRepository(environmentHelper.getPrivilegedClient()));
        BackupIndex backupIndex1 = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndex2 = new BackupIndex(NOW.toLocalDateTime().minusSeconds(1));
        BackupIndex backupIndex3 = new BackupIndex(NOW.toLocalDateTime().minusMinutes(1));
        BackupIndex backupIndex4 = new BackupIndex(NOW.toLocalDateTime().minusHours(1));
        BackupIndex backupIndex5 = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        BackupIndex backupIndex6 = new BackupIndex(NOW.toLocalDateTime().minusWeeks(1));
        BackupIndex backupIndex7 = new BackupIndex(NOW.toLocalDateTime().minusMonths(1));
        environmentHelper.createBackupIndex(backupIndex1,
            backupIndex2,
            backupIndex3,
            backupIndex4,
            backupIndex5,
            backupIndex6,
            backupIndex7);

        StepResult result = step.execute(context);

        log.info("Find backup indices result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupIndices(), hasSize(7));
        assertThat(context.getBackupIndices(), contains(backupIndex1.indexName(),
            backupIndex2.indexName(),
            backupIndex3.indexName(),
            backupIndex4.indexName(),
            backupIndex5.indexName(),
            backupIndex6.indexName(),
            backupIndex7.indexName()));
        assertThat(context.getNewestExistingBackupIndex().orElseThrow(), equalTo(backupIndex1.indexName()));
    }

    @Test
    public void shouldFindManyBackupIndices_3() {
        PopulateBackupIndicesStep step = new PopulateBackupIndicesStep(new StepRepository(environmentHelper.getPrivilegedClient()));
        BackupIndex backupIndex1 = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndex2 = new BackupIndex(NOW.toLocalDateTime().minusSeconds(1));
        BackupIndex backupIndex3 = new BackupIndex(NOW.toLocalDateTime().minusSeconds(2));
        BackupIndex backupIndex4 = new BackupIndex(NOW.toLocalDateTime().minusSeconds(3));
        BackupIndex backupIndex5 = new BackupIndex(NOW.toLocalDateTime().minusSeconds(4));
        BackupIndex backupIndex6 = new BackupIndex(NOW.toLocalDateTime().minusSeconds(5));
        BackupIndex backupIndex7 = new BackupIndex(NOW.toLocalDateTime().minusSeconds(6));
        environmentHelper.createBackupIndex(backupIndex1,
            backupIndex2,
            backupIndex3,
            backupIndex4,
            backupIndex5,
            backupIndex6,
            backupIndex7);

        StepResult result = step.execute(context);

        log.info("Find backup indices result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupIndices(), hasSize(7));
        assertThat(context.getBackupIndices(), contains(backupIndex1.indexName(),
            backupIndex2.indexName(),
            backupIndex3.indexName(),
            backupIndex4.indexName(),
            backupIndex5.indexName(),
            backupIndex6.indexName(),
            backupIndex7.indexName()));
        assertThat(context.getNewestExistingBackupIndex().orElseThrow(), equalTo(backupIndex1.indexName()));
    }

    @Test
    public void shouldFindManyBackupIndices_4() {
        PopulateBackupIndicesStep step = new PopulateBackupIndicesStep(new StepRepository(environmentHelper.getPrivilegedClient()));
        BackupIndex backupIndex1 = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndex2 = new BackupIndex(NOW.toLocalDateTime().minusYears(1));
        BackupIndex backupIndex3 = new BackupIndex(NOW.toLocalDateTime().minusYears(2));
        BackupIndex backupIndex4 = new BackupIndex(NOW.toLocalDateTime().minusYears(3));
        BackupIndex backupIndex5 = new BackupIndex(NOW.toLocalDateTime().minusYears(4));
        BackupIndex backupIndex6 = new BackupIndex(NOW.toLocalDateTime().minusYears(5));
        BackupIndex backupIndex7 = new BackupIndex(NOW.toLocalDateTime().minusYears(6));
        environmentHelper.createBackupIndex(backupIndex1,
            backupIndex2,
            backupIndex3,
            backupIndex4,
            backupIndex5,
            backupIndex6,
            backupIndex7);

        StepResult result = step.execute(context);

        log.info("Find backup indices result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupIndices(), hasSize(7));
        assertThat(context.getBackupIndices(), contains(backupIndex1.indexName(),
            backupIndex2.indexName(),
            backupIndex3.indexName(),
            backupIndex4.indexName(),
            backupIndex5.indexName(),
            backupIndex6.indexName(),
            backupIndex7.indexName()));
        assertThat(context.getNewestExistingBackupIndex().orElseThrow(), equalTo(backupIndex1.indexName()));
    }

    @Test
    public void shouldFindManyBackupIndices_5() {
        PopulateBackupIndicesStep step = new PopulateBackupIndicesStep(new StepRepository(environmentHelper.getPrivilegedClient()));
        BackupIndex backupIndex1 = new BackupIndex(NOW.toLocalDateTime().plusMonths(1));
        BackupIndex backupIndex2 = new BackupIndex(NOW.toLocalDateTime().minusHours(1));
        BackupIndex backupIndex3 = new BackupIndex(NOW.toLocalDateTime().minusHours(2));
        BackupIndex backupIndex4 = new BackupIndex(NOW.toLocalDateTime().minusHours(3));
        BackupIndex backupIndex5 = new BackupIndex(NOW.toLocalDateTime().minusHours(4));
        BackupIndex backupIndex6 = new BackupIndex(NOW.toLocalDateTime().minusHours(5));
        BackupIndex backupIndex7 = new BackupIndex(NOW.toLocalDateTime().minusHours(6));
        environmentHelper.createBackupIndex(backupIndex1,
            backupIndex2,
            backupIndex3,
            backupIndex4,
            backupIndex5,
            backupIndex6,
            backupIndex7);

        StepResult result = step.execute(context);

        log.info("Find backup indices result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupIndices(), hasSize(7));
        assertThat(context.getBackupIndices(), contains(backupIndex1.indexName(),
            backupIndex2.indexName(),
            backupIndex3.indexName(),
            backupIndex4.indexName(),
            backupIndex5.indexName(),
            backupIndex6.indexName(),
            backupIndex7.indexName()));
        assertThat(context.getNewestExistingBackupIndex().orElseThrow(), equalTo(backupIndex1.indexName()));
    }

    @Test
    public void shouldFindLargeAmountOfBackupIndices() {
        BackupIndex[] backupIndices = IntStream.range(0, 50) //
            .mapToObj(index -> new BackupIndex(NOW.minusHours(index).toLocalDateTime())) //
            .toArray(BackupIndex[]::new);
        environmentHelper.createBackupIndex(backupIndices);
        PopulateBackupIndicesStep step = new PopulateBackupIndicesStep(new StepRepository(environmentHelper.getPrivilegedClient()));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupIndices(), hasSize(backupIndices.length));
    }

    @Test
    public void shouldDetectBackupIndexInYellowStateAndReportErrorWhenYellowIndicesAreForbidden() {
        BackupIndex backupIndex = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        environmentHelper.createIndexInYellowState(backupIndex.indexName());
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        CheckIndicesStateStep step = createCheckIndicesStateStep();
        TenantIndex tenantIndex = new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), "Tenant name is irrelevant here");
        context.setTenantIndices(ImmutableList.of(tenantIndex));
        context.setBackupIndices(ImmutableList.of(backupIndex.indexName()));

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(UNHEALTHY_INDICES_ERROR));
        String expectedMessage = String.format("Index '%s' status is 'YELLOW'", backupIndex.indexName());
        assertThat(result.details(), containsString(expectedMessage));
    }

    @Test
    public void shouldDetectBackupIndexInYellowStateAndNotReportErrorWhenYellowIndicesAreAllowed() {
        this.context = new DataMigrationContext(new MigrationConfig(true), clock);
        BackupIndex backupIndex = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        environmentHelper.createIndexInYellowState(backupIndex.indexName());
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        CheckIndicesStateStep step = createCheckIndicesStateStep();
        TenantIndex tenantIndex = new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), "Tenant name is irrelevant here");
        context.setTenantIndices(ImmutableList.of(tenantIndex));
        context.setBackupIndices(ImmutableList.of(backupIndex.indexName()));

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
    }

    @Test
    public void shouldImposeWriteBlockOnDataIndices() {
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX);
        assertThat(environmentHelper.isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), "not important")));
        context.setBackupIndices(ImmutableList.empty());
        WriteBlockStep step = new WriteBlockStep(new StepRepository(environmentHelper.getPrivilegedClient()));

        StepResult result = step.execute(context);

        log.debug("Write lock step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        // the below index is not present is migration context, therefore write block is not imposed
        assertThat(environmentHelper.isDocumentInsertionPossible(PRIVATE_USER_KIRK_INDEX.indexName()), equalTo(true));
    }

    @Test
    public void shouldImposeWriteBlockOnDataAndBackupIndices() {
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        BackupIndex backupIndex = new BackupIndex(NOW.toLocalDateTime());
        environmentHelper.createBackupIndex(backupIndex);
        assertThat(environmentHelper.isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));
        assertThat(environmentHelper.isDocumentInsertionPossible(backupIndex.indexName()), equalTo(true));
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), "not important")));
        context.setBackupIndices(ImmutableList.of(backupIndex.indexName()));
        WriteBlockStep step = new WriteBlockStep(new StepRepository(environmentHelper.getPrivilegedClient()));

        StepResult result = step.execute(context);

        log.debug("Write lock step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        assertThat(environmentHelper.isDocumentInsertionPossible(backupIndex.indexName()), equalTo(false));
    }

    @Test
    public void shouldImposeWriteBlockOnMultipleIndices() {
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX);
        BackupIndex backupIndexOne = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndexTwo = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        environmentHelper.createBackupIndex(backupIndexOne, backupIndexTwo);
        assertThat(environmentHelper.isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));
        assertThat(environmentHelper.isDocumentInsertionPossible(PRIVATE_USER_KIRK_INDEX.indexName()), equalTo(true));
        assertThat(environmentHelper.isDocumentInsertionPossible(backupIndexOne.indexName()), equalTo(true));
        assertThat(environmentHelper.isDocumentInsertionPossible(backupIndexTwo.indexName()), equalTo(true));
        context.setTenantIndices(doubleAliasIndexToTenantDataWithoutTenantName(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX));
        context.setBackupIndices(ImmutableList.of(backupIndexOne.indexName(), backupIndexTwo.indexName()));
        WriteBlockStep step = new WriteBlockStep(new StepRepository(environmentHelper.getPrivilegedClient()));

        StepResult result = step.execute(context);

        log.debug("Write lock step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        assertThat(environmentHelper.isDocumentInsertionPossible(PRIVATE_USER_KIRK_INDEX.indexName()), equalTo(false));
        assertThat(environmentHelper.isDocumentInsertionPossible(backupIndexOne.indexName()), equalTo(false));
        assertThat(environmentHelper.isDocumentInsertionPossible(backupIndexTwo.indexName()), equalTo(false));
    }

    @Test
    public void shouldReLockBackupIndex() {
        StepRepository repository = new StepRepository(environmentHelper.getPrivilegedClient());
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        BackupIndex backupIndex = new BackupIndex(NOW.toLocalDateTime());
        environmentHelper.createBackupIndex(backupIndex);
        repository.writeBlockIndices(ImmutableList.of(backupIndex.indexName()));
        assertThat(environmentHelper.isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));
        assertThat(environmentHelper.isDocumentInsertionPossible(backupIndex.indexName()), equalTo(false));
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), "not important")));
        context.setBackupIndices(ImmutableList.of(backupIndex.indexName()));
        WriteBlockStep step = new WriteBlockStep(repository);

        StepResult result = step.execute(context);

        log.debug("Write lock step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        assertThat(environmentHelper.isDocumentInsertionPossible(backupIndex.indexName()), equalTo(false));
    }

    @Test
    public void shouldReleaseWriteBlockOnOneDataIndexWhenStepIsRollback() {
        StepRepository repository = new StepRepository(environmentHelper.getPrivilegedClient());
        WriteBlockStep step = new WriteBlockStep(repository);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        repository.writeBlockIndices(ImmutableList.of(GLOBAL_TENANT_INDEX.indexName()));

        assertThat(environmentHelper.isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        context.setTenantIndices(doubleAliasIndexToTenantDataWithoutTenantName(GLOBAL_TENANT_INDEX));


        StepResult result = step.rollback(context);

        log.debug("Write lock step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));

    }

    @Test
    public void shouldReleaseWriteBlockOnMultipleIndicesWhenStepIsRollback() {
        StepRepository repository = new StepRepository(environmentHelper.getPrivilegedClient());
        WriteBlockStep step = new WriteBlockStep(repository);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX);
        BackupIndex backupIndexOne = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndexTwo = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        environmentHelper.createBackupIndex(backupIndexOne, backupIndexTwo);
        ImmutableList<String> indicesToBlock = ImmutableList //
            .<DeletableIndex>of(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, backupIndexOne, backupIndexTwo) //
            .map(DeletableIndex::indexForDeletion);
        repository.writeBlockIndices(indicesToBlock);

        assertThat(environmentHelper.isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        assertThat(environmentHelper.isDocumentInsertionPossible(PRIVATE_USER_KIRK_INDEX.indexName()), equalTo(false));
        assertThat(environmentHelper.isDocumentInsertionPossible(backupIndexOne.indexName()), equalTo(false));
        assertThat(environmentHelper.isDocumentInsertionPossible(backupIndexTwo.indexName()), equalTo(false));
        context.setTenantIndices(doubleAliasIndexToTenantDataWithoutTenantName(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX));
        context.setBackupIndices(ImmutableList.of(backupIndexOne.indexName(), backupIndexTwo.indexName()));

        StepResult result = step.rollback(context);

        log.debug("Write lock step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));
        assertThat(environmentHelper.isDocumentInsertionPossible(PRIVATE_USER_KIRK_INDEX.indexName()), equalTo(true));
        assertThat(environmentHelper.isDocumentInsertionPossible(backupIndexOne.indexName()), equalTo(false));
        assertThat(environmentHelper.isDocumentInsertionPossible(backupIndexTwo.indexName()), equalTo(false));
    }

    @Test
    public void shouldReleaseWriteBlockOnOneDataIndex() {
        StepRepository repository = new StepRepository(environmentHelper.getPrivilegedClient());
        UnblockDataIndicesStep step = new UnblockDataIndicesStep(repository);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        repository.writeBlockIndices(ImmutableList.of(GLOBAL_TENANT_INDEX.indexName()));

        assertThat(environmentHelper.isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        context.setTenantIndices(doubleAliasIndexToTenantDataWithoutTenantName(GLOBAL_TENANT_INDEX));


        StepResult result = step.execute(context);

        log.debug("Write lock step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));

    }

    @Test
    public void shouldReleaseWriteBlockOnMultipleIndices() {
        StepRepository repository = new StepRepository(environmentHelper.getPrivilegedClient());
        UnblockDataIndicesStep step = new UnblockDataIndicesStep(repository);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX);
        BackupIndex backupIndexOne = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndexTwo = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        environmentHelper.createBackupIndex(backupIndexOne, backupIndexTwo);
        ImmutableList<String> indicesToBlock = ImmutableList //
            .<DeletableIndex>of(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, backupIndexOne, backupIndexTwo) //
                .map(DeletableIndex::indexForDeletion);
        repository.writeBlockIndices(indicesToBlock);

        assertThat(environmentHelper.isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        assertThat(environmentHelper.isDocumentInsertionPossible(PRIVATE_USER_KIRK_INDEX.indexName()), equalTo(false));
        assertThat(environmentHelper.isDocumentInsertionPossible(backupIndexOne.indexName()), equalTo(false));
        assertThat(environmentHelper.isDocumentInsertionPossible(backupIndexTwo.indexName()), equalTo(false));
        context.setTenantIndices(doubleAliasIndexToTenantDataWithoutTenantName(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX));
        context.setBackupIndices(ImmutableList.of(backupIndexOne.indexName(), backupIndexTwo.indexName()));

        StepResult result = step.execute(context);

        log.debug("Write lock step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));
        assertThat(environmentHelper.isDocumentInsertionPossible(PRIVATE_USER_KIRK_INDEX.indexName()), equalTo(true));
        assertThat(environmentHelper.isDocumentInsertionPossible(backupIndexOne.indexName()), equalTo(false));
        assertThat(environmentHelper.isDocumentInsertionPossible(backupIndexTwo.indexName()), equalTo(false));
    }

    @Test
    public void shouldGenerateTempIndexName() {
        Clock clock = Clock.fixed(NOW.toInstant(), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getTempIndexName(), equalTo(TEMP_INDEX_NAME));

        clock = Clock.fixed(NOW.toInstant().plus(2, ChronoUnit.HOURS), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getTempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_03_01_00"));

        clock = Clock.fixed(NOW.toInstant().plus(5, ChronoUnit.HOURS), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getTempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_06_01_00"));

        clock = Clock.fixed(NOW.toInstant().plus(8, ChronoUnit.DAYS), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getTempIndexName(), equalTo("data_migration_temp_fe_2000_01_09_01_01_00"));

        clock = Clock.fixed(NOW.toInstant().plus(30, ChronoUnit.SECONDS), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getTempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_01_30"));

        clock = Clock.fixed(NOW.toInstant().plus(57, ChronoUnit.SECONDS), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getTempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_01_57"));

        clock = Clock.fixed(NOW.toInstant().plus(40, ChronoUnit.MINUTES), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getTempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_41_00"));

        clock = Clock.fixed(NOW.toInstant().plus(47, ChronoUnit.MINUTES), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getTempIndexName(), equalTo("data_migration_temp_fe_2000_01_01_01_48_00"));

        clock = Clock.fixed(NOW.plusYears(9).toInstant(), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getTempIndexName(), equalTo("data_migration_temp_fe_2009_01_01_01_01_00"));

        clock = Clock.fixed(NOW.plusYears(23).plusMonths(8).plusDays(24).plusHours(11).plusSeconds(15).toInstant(), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getTempIndexName(), equalTo("data_migration_temp_fe_2023_09_25_12_01_15"));
    }

    @Test
    public void shouldCreateTempIndexWithoutSourceMappings() {
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        assertThat(environmentHelper.isIndexCreated(context.getTempIndexName()), equalTo(false));
        StepRepository stepRepository = new StepRepository(environmentHelper.getPrivilegedClient());
        CreateTempIndexStep step = new CreateTempIndexStep(new IndexSettingsManager(stepRepository));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(context.getTempIndexName()), equalTo(true));
    }

    @Test
    public void shouldCreateTempIndexWithoutMappingsWithGivenNumberOfShardsAndReplicas() {
        StepRepository stepRepository = new StepRepository(environmentHelper.getPrivilegedClient());
        environmentHelper.addCreatedIndex(GLOBAL_TENANT_INDEX);
        final int primaryShards = 2;
        final int replicas = 3;
        final long fieldLimit = 104;
        stepRepository.createIndex(GLOBAL_TENANT_INDEX.indexName(), primaryShards, replicas, fieldLimit, Collections.emptyMap());
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        assertThat(environmentHelper.isIndexCreated(context.getTempIndexName()), equalTo(false));
        CreateTempIndexStep step = new CreateTempIndexStep(new IndexSettingsManager(stepRepository));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(context.getTempIndexName()), equalTo(true));
        Settings tempIndexSettings = environmentHelper.getIndexSettings(context.getTempIndexName());
        log.debug("Temp index settings '{}'", tempIndexSettings);
        assertThat(tempIndexSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, -1), equalTo(primaryShards));
        assertThat(tempIndexSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, -1), equalTo(replicas));
        assertThat(tempIndexSettings.getAsLong(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), -1L), equalTo(fieldLimit));
    }

    @Test
    public void shouldCreateTempIndexWithoutMappingsWithGivenNumberOfShardsAndReplicas_case2() {
        StepRepository stepRepository = new StepRepository(environmentHelper.getPrivilegedClient());
        environmentHelper.addCreatedIndex(GLOBAL_TENANT_INDEX);
        final int primaryShards = 1;
        final int replicas = 2;
        final long fieldLimit = 1024;
        stepRepository.createIndex(GLOBAL_TENANT_INDEX.indexName(), primaryShards, replicas, fieldLimit, Collections.emptyMap());
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        assertThat(environmentHelper.isIndexCreated(context.getTempIndexName()), equalTo(false));
        CreateTempIndexStep step = new CreateTempIndexStep(new IndexSettingsManager(stepRepository));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(context.getTempIndexName()), equalTo(true));
        Settings tempIndexSettings = environmentHelper.getIndexSettings(context.getTempIndexName());
        log.debug("Temp index settings '{}'", tempIndexSettings);
        assertThat(tempIndexSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, -1), equalTo(primaryShards));
        assertThat(tempIndexSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, -1), equalTo(replicas));
        assertThat(tempIndexSettings.getAsLong(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), -1L), equalTo(fieldLimit));
    }

    @Test
    public void shouldCreateTempIndexWithoutMappingsWithGivenNumberOfShardsAndReplicas_case3() {
        StepRepository stepRepository = new StepRepository(environmentHelper.getPrivilegedClient());
        environmentHelper.addCreatedIndex(GLOBAL_TENANT_INDEX);
        final int primaryShards = 1;
        final int replicas = 3;
        final long fieldLimit = 1500;
        stepRepository.createIndex(GLOBAL_TENANT_INDEX.indexName(), primaryShards, replicas, fieldLimit, Collections.emptyMap());
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        assertThat(environmentHelper.isIndexCreated(context.getTempIndexName()), equalTo(false));
        CreateTempIndexStep step = new CreateTempIndexStep(new IndexSettingsManager(stepRepository));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(context.getTempIndexName()), equalTo(true));
        Settings tempIndexSettings = environmentHelper.getIndexSettings(context.getTempIndexName());
        log.debug("Temp index settings '{}'", tempIndexSettings);
        assertThat(tempIndexSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, -1), equalTo(primaryShards));
        assertThat(tempIndexSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, -1), equalTo(replicas));
        assertThat(tempIndexSettings.getAsLong(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), -1L), equalTo(fieldLimit));
    }

    @Test
    public void shouldCreateTempIndexWithExtendedMappings() {
        StepRepository stepRepository = new StepRepository(environmentHelper.getPrivilegedClient());
        environmentHelper.addCreatedIndex(GLOBAL_TENANT_INDEX);
        stepRepository.createIndex(GLOBAL_TENANT_INDEX.indexName(), 1, 1, 500, DocNode.EMPTY);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        assertThat(environmentHelper.isIndexCreated(context.getTempIndexName()), equalTo(false));
        CreateTempIndexStep step = new CreateTempIndexStep(new IndexSettingsManager(stepRepository));

        StepResult result = step.execute(context);

        log.debug("Create temp index step result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(context.getTempIndexName()), equalTo(true));
        DocNode actualMappings = environmentHelper.getIndexMappingsAsDocNode(context.getTempIndexName());
        log.debug("Temp index mappings '{}'", actualMappings.toJsonString());
        assertThat(actualMappings, containsValue("$.properties.sg_data_migrated_to_8_8_0.type", "boolean"));
    }

    @Test
    public void shouldCreateTempIndexWithMappings_simple() throws DocumentParseException {
        StepRepository stepRepository = new StepRepository(environmentHelper.getPrivilegedClient());
        environmentHelper.addCreatedIndex(GLOBAL_TENANT_INDEX);
        DocNode desiredMappings = DocNode.parse(Format.JSON).from(TestMappings.SIMPLE);
        stepRepository.createIndex(GLOBAL_TENANT_INDEX.indexName(), 1, 1, 500, desiredMappings);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        assertThat(environmentHelper.isIndexCreated(context.getTempIndexName()), equalTo(false));
        CreateTempIndexStep step = new CreateTempIndexStep(new IndexSettingsManager(stepRepository));

        StepResult result = step.execute(context);

        log.debug("Create temp index step result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(context.getTempIndexName()), equalTo(true));
        DocNode actualMappings = environmentHelper.getIndexMappingsAsDocNode(context.getTempIndexName());
        log.debug("Temp index mappings '{}'", actualMappings.toJsonString());
        assertThat(actualMappings, containsValue("$.properties.sg_data_migrated_to_8_8_0.type", "boolean"));
        actualMappings = withoutMigrationMarker(actualMappings);
        assertThat(actualMappings.equals(desiredMappings), equalTo(true));
    }

    @Test
    public void shouldCreateTempIndexWithMappings_medium() throws DocumentParseException {
        StepRepository stepRepository = new StepRepository(environmentHelper.getPrivilegedClient());
        environmentHelper.addCreatedIndex(GLOBAL_TENANT_INDEX);
        DocNode desiredMappings = DocNode.parse(Format.JSON).from(TestMappings.MEDIUM);
        stepRepository.createIndex(GLOBAL_TENANT_INDEX.indexName(), 1, 1, 500, desiredMappings);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        assertThat(environmentHelper.isIndexCreated(context.getTempIndexName()), equalTo(false));
        CreateTempIndexStep step = new CreateTempIndexStep(new IndexSettingsManager(stepRepository));

        StepResult result = step.execute(context);

        log.debug("Create temp index step result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(context.getTempIndexName()), equalTo(true));
        DocNode actualMappings = environmentHelper.getIndexMappingsAsDocNode(context.getTempIndexName());
        log.debug("Temp index mappings '{}'", actualMappings.toJsonString());
        assertThat(actualMappings, containsValue("$.properties.sg_data_migrated_to_8_8_0.type", "boolean"));
        actualMappings = withoutMigrationMarker(actualMappings);
        assertThat(actualMappings.equals(desiredMappings), equalTo(true));
    }

    @Test
    public void shouldCreateTempIndexWithMappings_hard() throws DocumentParseException {
        StepRepository stepRepository = new StepRepository(environmentHelper.getPrivilegedClient());
        environmentHelper.addCreatedIndex(GLOBAL_TENANT_INDEX);
        DocNode desiredMappings = DocNode.parse(Format.JSON).from(TestMappings.HARD);
        stepRepository.createIndex(GLOBAL_TENANT_INDEX.indexName(), 1, 1, 1500, desiredMappings);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        assertThat(environmentHelper.isIndexCreated(context.getTempIndexName()), equalTo(false));
        CreateTempIndexStep step = new CreateTempIndexStep(new IndexSettingsManager(stepRepository));

        StepResult result = step.execute(context);

        log.debug("Create temp index step result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(context.getTempIndexName()), equalTo(true));
        DocNode actualMappings = environmentHelper.getIndexMappingsAsDocNode(context.getTempIndexName());
        log.debug("Temp index mappings '{}'", actualMappings.toJsonString());
        assertThat(actualMappings, containsValue("$.properties.sg_data_migrated_to_8_8_0.type", "boolean"));
        actualMappings = withoutMigrationMarker(actualMappings);
        assertThat(actualMappings.equals(desiredMappings), equalTo(true));
    }

    @Test
    public void shouldTransferLargeNumberOfDocuments_FromSingleDataIndex() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        final int countOfDocuments = 1005;
        String[] spaceNames = IntStream.range(0, countOfDocuments).mapToObj(index -> "space_no_" + index).toArray(String[]::new);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), spaceNames);
        CopyDataToTempIndexStep step =  createCopyDataToTempIndexStep(client);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.countDocumentInIndex(context.getTempIndexName()), equalTo((long)countOfDocuments));
    }

    @Test
    public void shouldTransferLargeNumberOfDocuments_FromFewDataIndex() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        DoubleAliasIndex managementTenant = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        ImmutableList<DoubleAliasIndex> indices = ImmutableList.of(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, managementTenant);
        environmentHelper.createIndex(indices);
        final int countOfDocuments = 1004;
        indices.map(DoubleAliasIndex::indexName).forEach(indexName -> {
            String[] spaceNames = IntStream.range(0, countOfDocuments) //
                .mapToObj(index -> indexName + "space_no_" + index) //
                .toArray(String[]::new);
            catalog.insertSpace(indexName, spaceNames);
        });
        CopyDataToTempIndexStep step =  createCopyDataToTempIndexStep(client);
        context.setTenantIndices(doubleAliasIndexToTenantDataWithoutTenantName(indices));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        String tempIndex = context.getTempIndexName();
        assertThat(environmentHelper.countDocumentInIndex(tempIndex), equalTo((long)countOfDocuments * indices.size()));
    }

    @Test
    public void shouldStoreSameDocumentsInTempIndex() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        DoubleAliasIndex managementTenant = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        ImmutableList<DoubleAliasIndex> indices = ImmutableList.of(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, managementTenant);
        environmentHelper.createIndex(indices);
        indices.map(DoubleAliasIndex::indexName).forEach(indexName -> catalog.insertSpace(indexName, indexName));
        catalog.insertIndexPattern(GLOBAL_TENANT_INDEX.indexName(), "iot-1");
        catalog.insertIndexPattern(PRIVATE_USER_KIRK_INDEX.indexName(), "iot-2");
        catalog.insertIndexPattern(managementTenant.indexName(), "iot-3");
        CopyDataToTempIndexStep step = createCopyDataToTempIndexStep(client);
        ImmutableList<TenantIndex> tenantIndices = ImmutableList.of(
            new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID), //
            new TenantIndex(PRIVATE_USER_KIRK_INDEX.indexName(), null), //
            new TenantIndex(managementTenant.indexName(), TENANT_MANAGEMENT)
        );
        context.setTenantIndices(tenantIndices);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.countDocumentInIndex(TEMP_INDEX_NAME), equalTo(6L));
        String id = "space:.kibana_8.7.0_001";
        String globalDocument = environmentHelper.getDocumentSource(GLOBAL_TENANT_INDEX.indexName(), id).orElseThrow();
        id = "space:.kibana_3292183_kirk_8.7.0_001";
        String privateDocument = environmentHelper.getDocumentSource(PRIVATE_USER_KIRK_INDEX.indexName(), id).orElseThrow();
        id = "space:.kibana_-1799980989_management_8.7.0_001";
        String managementDocument = environmentHelper.getDocumentSource(managementTenant.indexName(),id ).orElseThrow();
        id = "space:.kibana_8.7.0_001";
        String migratedGlobal = environmentHelper.getDocumentSource(TEMP_INDEX_NAME, id).orElseThrow();
        id = "space:.kibana_3292183_kirk_8.7.0_001__sg_ten__3292183_kirk";
        String migratedPrivate = environmentHelper.getDocumentSource(TEMP_INDEX_NAME, id).orElseThrow();
        id = "space:.kibana_-1799980989_management_8.7.0_001__sg_ten__-1799980989_management";
        String migratedManagement = environmentHelper.getDocumentSource(TEMP_INDEX_NAME, id).orElseThrow();
        assertThat(migratedGlobal, equalTo(globalDocument));
        assertThat(migratedPrivate, equalTo(privateDocument));
        assertThat(migratedManagement, equalTo(managementDocument));
        id = "index-pattern::iot-1";
        globalDocument = environmentHelper.getDocumentSource(GLOBAL_TENANT_INDEX.indexName(), id).orElseThrow();
        id = "index-pattern::iot-2";
        privateDocument = environmentHelper.getDocumentSource(PRIVATE_USER_KIRK_INDEX.indexName(), id).orElseThrow();
        id = "index-pattern::iot-3";
        managementDocument = environmentHelper.getDocumentSource(managementTenant.indexName(), id).orElseThrow();
        id = "index-pattern::iot-1";
        migratedGlobal = environmentHelper.getDocumentSource(TEMP_INDEX_NAME, id).orElseThrow();
        id = "index-pattern::iot-2__sg_ten__3292183_kirk";
        migratedPrivate = environmentHelper.getDocumentSource(TEMP_INDEX_NAME, id).orElseThrow();
        id = "index-pattern::iot-3__sg_ten__-1799980989_management";
        migratedManagement = environmentHelper.getDocumentSource(TEMP_INDEX_NAME, id).orElseThrow();
        assertThat(migratedGlobal, equalTo(globalDocument));
        assertThat(migratedPrivate, equalTo(privateDocument));
        assertThat(migratedManagement, equalTo(managementDocument));
    }

    @Test
    public void shouldDetectThatIndexContainsAlreadyMigratedDocumentsAndReportError() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        String scopedId = RequestResponseTenantData.scopedId("alreadyMigratedSpace", Tenant.GLOBAL_TENANT_ID);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "alreadyMigratedSpace", scopedId);
        CopyDataToTempIndexStep step = createCopyDataToTempIndexStep(client);

        StepException stepException = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));

        assertThat(stepException.getStatus(), equalTo(DOCUMENT_ALREADY_MIGRATED_ERROR));
    }

    @Test
    public void shouldAssignTenantScopeToSavedObjectId() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        DoubleAliasIndex managementTenant = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        environmentHelper.createIndex(
            GLOBAL_TENANT_INDEX, managementTenant, PRIVATE_USER_KIRK_INDEX, PRIVATE_USER_LUKASZ_1_INDEX,
            MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_2_INDEX, MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_3_INDEX);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "global_tenant_space");
        catalog.insertSpace(managementTenant.indexName(), "management_tenant_space");
        catalog.insertSpace(PRIVATE_USER_KIRK_INDEX.indexName(), "kirk_private_tenant_space");
        catalog.insertSpace(PRIVATE_USER_LUKASZ_1_INDEX.indexName(), "lukasz_1_private_tenant_space");
        catalog.insertSpace(MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_2_INDEX.indexName(), "lukasz_2_private_tenant_space");
        catalog.insertSpace(MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_3_INDEX.indexName(), "lukasz_3_private_tenant_space");
        ImmutableList<TenantIndex> tenantIndices = ImmutableList.of(
            new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID), //
            new TenantIndex(managementTenant.indexName(), TENANT_MANAGEMENT), //
            new TenantIndex(PRIVATE_USER_KIRK_INDEX.indexName(), null), //
            new TenantIndex(PRIVATE_USER_LUKASZ_1_INDEX.indexName(), null), //
            new TenantIndex(MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_2_INDEX.indexName(), null), //
            new TenantIndex(MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_3_INDEX.indexName(), null)
        );
        context.setTenantIndices(tenantIndices);
        CopyDataToTempIndexStep step = createCopyDataToTempIndexStep(client);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        String tempIndex = context.getTempIndexName();
        environmentHelper.assertThatDocumentExists(tempIndex, "space:global_tenant_space");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:management_tenant_space__sg_ten__-1799980989_management");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:kirk_private_tenant_space__sg_ten__3292183_kirk");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:lukasz_1_private_tenant_space__sg_ten__-1091682490_lukasz");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:lukasz_2_private_tenant_space__sg_ten__739988528_ukasz");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:lukasz_3_private_tenant_space__sg_ten__-1091714203_luksz");
    }

    @Test
    public void shouldPreventIdColisions() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        DoubleAliasIndex managementTenant = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        environmentHelper.createIndex(
            GLOBAL_TENANT_INDEX, managementTenant, PRIVATE_USER_KIRK_INDEX, PRIVATE_USER_LUKASZ_1_INDEX,
            MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_2_INDEX, MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_3_INDEX);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "colliding_space_id");
        catalog.insertSpace(managementTenant.indexName(), "colliding_space_id");
        catalog.insertSpace(PRIVATE_USER_KIRK_INDEX.indexName(), "colliding_space_id");
        catalog.insertSpace(PRIVATE_USER_LUKASZ_1_INDEX.indexName(), "colliding_space_id");
        catalog.insertSpace(MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_2_INDEX.indexName(), "colliding_space_id");
        catalog.insertSpace(MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_3_INDEX.indexName(), "colliding_space_id");
        ImmutableList<TenantIndex> tenantIndices = ImmutableList.of(
            new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID), //
            new TenantIndex(managementTenant.indexName(), TENANT_MANAGEMENT), //
            new TenantIndex(PRIVATE_USER_KIRK_INDEX.indexName(), null), //
            new TenantIndex(PRIVATE_USER_LUKASZ_1_INDEX.indexName(), null), //
            new TenantIndex(MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_2_INDEX.indexName(), null), //
            new TenantIndex(MigrationEnvironmentHelper.PRIVATE_USER_LUKASZ_3_INDEX.indexName(), null)
        );
        context.setTenantIndices(tenantIndices);
        CopyDataToTempIndexStep step = createCopyDataToTempIndexStep(client);

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        String tempIndex = context.getTempIndexName();
        environmentHelper.assertThatDocumentExists(tempIndex, "space:colliding_space_id");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:colliding_space_id__sg_ten__-1799980989_management");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:colliding_space_id__sg_ten__3292183_kirk");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:colliding_space_id__sg_ten__-1091682490_lukasz");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:colliding_space_id__sg_ten__739988528_ukasz");
        environmentHelper.assertThatDocumentExists(tempIndex, "space:colliding_space_id__sg_ten__-1091714203_luksz");
    }

    @Test
    public void shouldDeleteTempIndex() {
        DoubleAliasIndex managementTenant = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, PRIVATE_USER_LUKASZ_1_INDEX, managementTenant);
        BackupIndex backupIndex1 = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndex2 = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        BackupIndex backupIndex3 = new BackupIndex(NOW.toLocalDateTime().minusDays(2));
        BackupIndex backupIndex4 = new BackupIndex(NOW.toLocalDateTime().minusDays(3));
        BackupIndex backupIndex5 = new BackupIndex(NOW.toLocalDateTime().minusDays(4));
        environmentHelper.createBackupIndex(backupIndex1, backupIndex2, backupIndex3, backupIndex4, backupIndex5);
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        client.admin().indices().create(new CreateIndexRequest(TEMP_INDEX_NAME)).actionGet();
        DeleteTempIndexStep step = new DeleteTempIndexStep(new StepRepository(client));
        assertThat(environmentHelper.isIndexCreated(TEMP_INDEX_NAME), equalTo(true));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(TEMP_INDEX_NAME), equalTo(false));
        assertThat(environmentHelper.isIndexCreated(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(PRIVATE_USER_KIRK_INDEX.indexName()), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(PRIVATE_USER_LUKASZ_1_INDEX.indexName()), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(managementTenant.indexName()), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(backupIndex1.indexName()), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(backupIndex2.indexName()), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(backupIndex3.indexName()), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(backupIndex4.indexName()), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(backupIndex5.indexName()), equalTo(true));
    }

    @Test
    public void shouldNotCreateBackupOfIndexWithoutMappingsAndReportError() {
        StepRepository repository = new StepRepository(environmentHelper.getPrivilegedClient());
        IndexSettingsManager settingsManager = new IndexSettingsManager(repository);
        CreateBackupOfGlobalIndexStep step = new CreateBackupOfGlobalIndexStep(repository, settingsManager);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));

        StepException exception = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));

        assertThat(exception.getStatus(), equalTo(EMPTY_MAPPINGS_ERROR));
    }

    @Test
    public void shouldCreateBackupOfSingleDocument() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        StepRepository repository = new StepRepository(client);
        IndexSettingsManager settingsManager = new IndexSettingsManager(repository);
        CreateBackupOfGlobalIndexStep step = new CreateBackupOfGlobalIndexStep(repository, settingsManager);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "default");
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupCreated(), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(BACKUP_INDEX_NAME), equalTo(true));
        environmentHelper.assertThatDocumentExists(BACKUP_INDEX_NAME, "space:default");
    }

    @Test
    public void shouldCreateBackupOfWriteBlockIndex() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        StepRepository repository = new StepRepository(client);
        IndexSettingsManager settingsManager = new IndexSettingsManager(repository);
        CreateBackupOfGlobalIndexStep step = new CreateBackupOfGlobalIndexStep(repository, settingsManager);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "default");
        repository.writeBlockIndices(ImmutableList.of(GLOBAL_TENANT_INDEX.indexName()));
        assertThat(environmentHelper.isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupCreated(), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(BACKUP_INDEX_NAME), equalTo(true));
        environmentHelper.assertThatDocumentExists(BACKUP_INDEX_NAME, "space:default");
    }


    @Test
    public void shouldNotCreateBackupWhenGlobalTenantIndexContainsMigrationMarkerInMappings() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        StepRepository repository = new StepRepository(client);
        IndexSettingsManager settingsManager = new IndexSettingsManager(repository);
        CreateBackupOfGlobalIndexStep step = new CreateBackupOfGlobalIndexStep(repository, settingsManager);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "default");
        environmentHelper.addDataMigrationMarkerToTheIndex(GLOBAL_TENANT_INDEX.indexName());
        repository.writeBlockIndices(ImmutableList.of(GLOBAL_TENANT_INDEX.indexName()));
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupCreated(), equalTo(false));
        assertThat(environmentHelper.isIndexCreated(BACKUP_INDEX_NAME), equalTo(false));
    }

    @Test
    public void shouldGenerateBackupIndexNameBasedOnCurrentTime() {
        Clock clock = Clock.fixed(NOW.toInstant(), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getBackupIndexName(), equalTo(BACKUP_INDEX_NAME));

        clock = Clock.fixed(NOW.toInstant().plus(2, ChronoUnit.HOURS), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getBackupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_03_01_00"));

        clock = Clock.fixed(NOW.toInstant().plus(5, ChronoUnit.HOURS), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getBackupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_06_01_00"));

        clock = Clock.fixed(NOW.toInstant().plus(8, ChronoUnit.DAYS), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getBackupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_09_01_01_00"));

        clock = Clock.fixed(NOW.toInstant().plus(30, ChronoUnit.SECONDS), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getBackupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_01_30"));

        clock = Clock.fixed(NOW.toInstant().plus(57, ChronoUnit.SECONDS), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getBackupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_01_57"));

        clock = Clock.fixed(NOW.toInstant().plus(40, ChronoUnit.MINUTES), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getBackupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_41_00"));

        clock = Clock.fixed(NOW.toInstant().plus(47, ChronoUnit.MINUTES), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getBackupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2000_01_01_01_48_00"));

        clock = Clock.fixed(NOW.plusYears(9).toInstant(), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getBackupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2009_01_01_01_01_00"));

        clock = Clock.fixed(NOW.plusYears(23).plusMonths(8).plusDays(24).plusHours(11).plusSeconds(16).toInstant(), UTC);
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
        assertThat(context.getBackupIndexName(), equalTo("backup_fe_migration_to_8_8_0_2023_09_25_12_01_16"));
    }

    @Test
    public void shouldBackupLargeNumberOfDocuments() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        StepRepository repository = new StepRepository(client);
        IndexSettingsManager settingsManager = new IndexSettingsManager(repository);
        CreateBackupOfGlobalIndexStep step = new CreateBackupOfGlobalIndexStep(repository, settingsManager);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        final int documentNumber = 12_101;
        String[] spaceNames = IntStream.range(0, documentNumber).mapToObj(i -> "space_no_" + i).toArray(String[]::new);
        ImmutableList<String> spacesIds = catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), spaceNames);
        String[] patternNames = IntStream.range(0, documentNumber).mapToObj(i -> "index_pattern_no_" + i).toArray(String[]::new);
        ImmutableList<String> patternsIds = catalog.insertIndexPattern(GLOBAL_TENANT_INDEX.indexName(), patternNames);
        assertThat(environmentHelper.countDocumentInIndex(GLOBAL_TENANT_INDEX.indexName()), equalTo(2L * documentNumber));
        repository.writeBlockIndices(ImmutableList.of(GLOBAL_TENANT_INDEX.indexName()));
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));

        StepResult result = step.execute(context);

        log.debug("Create backup result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(BACKUP_INDEX_NAME), equalTo(true));
        assertThat(environmentHelper.countDocumentInIndex(BACKUP_INDEX_NAME), equalTo(documentNumber * 2L));
        assertThat(spacesIds, hasSize(documentNumber));
        assertThat(patternsIds, hasSize(documentNumber));
        Stream.concat(spacesIds.stream(), patternsIds.stream()) //
            .forEach(id -> environmentHelper.assertThatDocumentExists(BACKUP_INDEX_NAME, id));
    }

    @Test
    public void shouldStoreExactCopyOfDocumentInBackupIndex() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        StepRepository repository = new StepRepository(client);
        IndexSettingsManager settingsManager = new IndexSettingsManager(repository);
        CreateBackupOfGlobalIndexStep step = new CreateBackupOfGlobalIndexStep(repository, settingsManager);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        final int documentNumber = 51;
        String[] spaceNames = IntStream.range(0, documentNumber).mapToObj(i -> "space_no_" + i).toArray(String[]::new);
        ImmutableList<String> spacesIds = catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), spaceNames);
        String[] patternNames = IntStream.range(0, documentNumber).mapToObj(i -> "index_pattern_no_" + i).toArray(String[]::new);
        ImmutableList<String> patternsIds = catalog.insertIndexPattern(GLOBAL_TENANT_INDEX.indexName(), patternNames);
        assertThat(environmentHelper.countDocumentInIndex(GLOBAL_TENANT_INDEX.indexName()), equalTo(2L * documentNumber));
        repository.writeBlockIndices(ImmutableList.of(GLOBAL_TENANT_INDEX.indexName()));
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));

        StepResult result = step.execute(context);

        log.debug("Create backup result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        Stream.concat(spacesIds.stream(), patternsIds.stream()).forEach(id -> {
            String genuineDocument =  environmentHelper.getDocumentSource(GLOBAL_TENANT_INDEX.indexName(), id).orElseThrow();
            String documentBackup = environmentHelper.getDocumentSource(BACKUP_INDEX_NAME, id).orElseThrow();
            assertThat(documentBackup, equalTo(genuineDocument));
        });
    }

    @Test
    public void shouldCreateBackupIndexWithGivenNumberOfShardsAndReplicas() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        StepRepository stepRepository = new StepRepository(client);
        environmentHelper.addCreatedIndex(GLOBAL_TENANT_INDEX);
        final int primaryShards = 2;
        final int replicas = 3;
        final long fieldLimit = 104;
        stepRepository.createIndex(GLOBAL_TENANT_INDEX.indexName(), primaryShards, replicas, fieldLimit, Collections.emptyMap());
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "backup-test-space");
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        assertThat(environmentHelper.isIndexCreated(context.getBackupIndexName()), equalTo(false));
        stepRepository.writeBlockIndices(ImmutableList.of(GLOBAL_TENANT_INDEX.indexName()));
        CreateBackupOfGlobalIndexStep step = new CreateBackupOfGlobalIndexStep(stepRepository, new IndexSettingsManager(stepRepository));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(context.getBackupIndexName()), equalTo(true));
        Settings backupIndexSettings = environmentHelper.getIndexSettings(context.getBackupIndexName());
        log.debug("Backup index settings '{}'", backupIndexSettings);
        assertThat(backupIndexSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, -1), equalTo(primaryShards));
        assertThat(backupIndexSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, -1), equalTo(replicas));
        assertThat(backupIndexSettings.getAsLong(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), -1L), equalTo(fieldLimit));
    }

    @Test
    public void shouldCreateBackupIndexWithMappings_simple() throws DocumentParseException {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        StepRepository stepRepository = new StepRepository(client);
        environmentHelper.addCreatedIndex(GLOBAL_TENANT_INDEX);
        DocNode desiredMappings = DocNode.parse(Format.JSON).from(TestMappings.SIMPLE);
        stepRepository.createIndex(GLOBAL_TENANT_INDEX.indexName(), 1, 1, 1500, desiredMappings);
        client.index(new IndexRequest(GLOBAL_TENANT_INDEX.indexName()).source(DocNode.EMPTY)).actionGet();
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        assertThat(environmentHelper.isIndexCreated(context.getBackupIndexName()), equalTo(false));
        stepRepository.writeBlockIndices(ImmutableList.of(GLOBAL_TENANT_INDEX.indexName()));
        CreateBackupOfGlobalIndexStep step = new CreateBackupOfGlobalIndexStep(stepRepository, new IndexSettingsManager(stepRepository));

        StepResult result = step.execute(context);

        log.debug("Create backup index step result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(context.getBackupIndexName()), equalTo(true));
        DocNode actualMappings = environmentHelper.getIndexMappingsAsDocNode(context.getBackupIndexName());
        log.debug("Backup index mappings '{}'", actualMappings.toJsonString());
        assertThat(actualMappings, equalTo(desiredMappings));
    }

    @Test
    public void shouldCreateBackupIndexWithMappings_medium() throws DocumentParseException {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        StepRepository stepRepository = new StepRepository(client);
        environmentHelper.addCreatedIndex(GLOBAL_TENANT_INDEX);
        DocNode desiredMappings = DocNode.parse(Format.JSON).from(TestMappings.MEDIUM);
        stepRepository.createIndex(GLOBAL_TENANT_INDEX.indexName(), 1, 1, 1500, desiredMappings);
        client.index(new IndexRequest(GLOBAL_TENANT_INDEX.indexName()).source(DocNode.EMPTY)).actionGet();
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        assertThat(environmentHelper.isIndexCreated(context.getBackupIndexName()), equalTo(false));
        stepRepository.writeBlockIndices(ImmutableList.of(GLOBAL_TENANT_INDEX.indexName()));
        CreateBackupOfGlobalIndexStep step = new CreateBackupOfGlobalIndexStep(stepRepository, new IndexSettingsManager(stepRepository));

        StepResult result = step.execute(context);

        log.debug("Create backup index step result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(context.getBackupIndexName()), equalTo(true));
        DocNode actualMappings = environmentHelper.getIndexMappingsAsDocNode(context.getBackupIndexName());
        log.debug("Backup index mappings '{}'", actualMappings.toJsonString());
        assertThat(actualMappings, equalTo(desiredMappings));
    }

    @Test
    public void shouldCreateBackupIndexWithMappings_hard() throws DocumentParseException {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        StepRepository stepRepository = new StepRepository(client);
        environmentHelper.addCreatedIndex(GLOBAL_TENANT_INDEX);
        DocNode desiredMappings = DocNode.parse(Format.JSON).from(TestMappings.HARD);
        stepRepository.createIndex(GLOBAL_TENANT_INDEX.indexName(), 1, 1, 1500, desiredMappings);
        client.index(new IndexRequest(GLOBAL_TENANT_INDEX.indexName()).source(DocNode.EMPTY)).actionGet();
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        assertThat(environmentHelper.isIndexCreated(context.getBackupIndexName()), equalTo(false));
        stepRepository.writeBlockIndices(ImmutableList.of(GLOBAL_TENANT_INDEX.indexName()));
        CreateBackupOfGlobalIndexStep step = new CreateBackupOfGlobalIndexStep(stepRepository, new IndexSettingsManager(stepRepository));

        StepResult result = step.execute(context);

        log.debug("Create backup index step result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.isIndexCreated(context.getBackupIndexName()), equalTo(true));
        DocNode actualMappings = environmentHelper.getIndexMappingsAsDocNode(context.getBackupIndexName());
        log.debug("Backup index mappings '{}'", actualMappings.toJsonString());
        assertThat(actualMappings, equalTo(desiredMappings));
    }

    @Test
    public void shouldWriteBlockBackupIndex() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        StepRepository repository = new StepRepository(client);
        IndexSettingsManager settingsManager = new IndexSettingsManager(repository);
        CreateBackupOfGlobalIndexStep step = new CreateBackupOfGlobalIndexStep(repository, settingsManager);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        String createdDocumentId = catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "default").get(0);
        repository.writeBlockIndices(ImmutableList.of(GLOBAL_TENANT_INDEX.indexName()));
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        DeleteRequest deleteRequest = new DeleteRequest(context.getBackupIndexName(), createdDocumentId).setRefreshPolicy(IMMEDIATE);
        assertThatThrown(() -> client.delete(deleteRequest).actionGet(), instanceOf(ClusterBlockException.class));
        environmentHelper.assertThatDocumentExists(context.getBackupIndexName(), createdDocumentId);
    }

    @Test
    public void shouldNotCheckPreviousBackupIfBackupWasCreatedInTheCurrentMigrationProcess() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        StepRepository repository = new StepRepository(client);
        IndexSettingsManager settingsManager = new IndexSettingsManager(repository);
        VerifyPreviousBackupStep step = new VerifyPreviousBackupStep(repository, settingsManager);
        context.setBackupCreated(true);

        StepResult result = step.execute(context);

        log.debug("Verify previous backup step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
    }

    @Test
    public void shouldReportErrorWhenIndexWithPreviousBackupWasNotFound() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        StepRepository repository = new StepRepository(client);
        IndexSettingsManager settingsManager = new IndexSettingsManager(repository);
        VerifyPreviousBackupStep step = new VerifyPreviousBackupStep(repository, settingsManager);
        context.setBackupCreated(false);

        StepException exception = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));

        assertThat(exception.getStatus(), equalTo(BACKUP_NOT_FOUND_ERROR));
    }

    @Test
    public void shouldReportErrorWhenIndexWithPreviousBackupWasFoundButNotExist() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        StepRepository repository = new StepRepository(client);
        IndexSettingsManager settingsManager = new IndexSettingsManager(repository);
        VerifyPreviousBackupStep step = new VerifyPreviousBackupStep(repository, settingsManager);
        context.setBackupCreated(false);
        context.setBackupIndices(ImmutableList.of("backup_index_which_does_not_exist"));

        StepException exception = (StepException) assertThatThrown(() -> step.execute(context), instanceOf(StepException.class));

        assertThat(exception.getStatus(), equalTo(BACKUP_DOES_NOT_EXIST_ERROR));
    }

    @Test
    public void shouldReportErrorWhenIndexWithPreviousBackupIsEmpty() {
        BackupIndex backupIndex = new BackupIndex(NOW.toLocalDateTime().minusHours(6).minusMinutes(5).minusSeconds(4));
        environmentHelper.createBackupIndex(backupIndex);
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        context.setBackupCreated(false);
        context.setBackupIndices(ImmutableList.of(backupIndex.indexName()));
        StepRepository repository = new StepRepository(client);
        IndexSettingsManager settingsManager = new IndexSettingsManager(repository);
        VerifyPreviousBackupStep step = new VerifyPreviousBackupStep(repository, settingsManager);

        StepResult result = step.execute(context);

        log.debug("Verify previous backup step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(BACKUP_IS_EMPTY_ERROR));
    }

    @Test
    public void shouldSuccessfullyVerifyBackupIndex() {
        BackupIndex
            backupIndex = new BackupIndex(NOW.toLocalDateTime().minusHours(6).minusMinutes(5).minusSeconds(4));
        environmentHelper.createBackupIndex(backupIndex);
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        catalog.insertSpace(backupIndex.indexName(), "default");
        context.setBackupCreated(false);
        context.setBackupIndices(ImmutableList.of(backupIndex.indexName()));
        StepRepository repository = new StepRepository(client);
        IndexSettingsManager settingsManager = new IndexSettingsManager(repository);
        VerifyPreviousBackupStep step = new VerifyPreviousBackupStep(repository, settingsManager);

        StepResult result = step.execute(context);

        log.debug("Verify previous backup step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
    }

    @Test
    public void shouldReportErrorWhenBackupIndexContainsDataMigrationMarker() {
        BackupIndex
            backupIndex = new BackupIndex(NOW.toLocalDateTime().minusHours(6).minusMinutes(5).minusSeconds(4));
        environmentHelper.createBackupIndex(backupIndex);
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        catalog.insertSpace(backupIndex.indexName(), "default");
        environmentHelper.addDataMigrationMarkerToTheIndex(backupIndex.indexName());
        context.setBackupCreated(false);
        context.setBackupIndices(ImmutableList.of(backupIndex.indexName()));
        StepRepository repository = new StepRepository(client);
        IndexSettingsManager settingsManager = new IndexSettingsManager(repository);
        VerifyPreviousBackupStep step = new VerifyPreviousBackupStep(repository, settingsManager);

        StepResult result = step.execute(context);

        log.debug("Verify previous backup step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(BACKUP_CONTAINS_MIGRATED_DATA_ERROR));
    }

    @Test
    public void shouldProvideTotalCountOfDocumentInIndexForLargeNumberOfDocuments() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        StepRepository repository = new StepRepository(client);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        final int documentNumber = 11_000;
        String[] spaceNames = IntStream.range(0, documentNumber).mapToObj(i -> "space_no_" + i).toArray(String[]::new);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), spaceNames);

        long numberOfDocuments = repository.countDocuments(GLOBAL_TENANT_INDEX.indexName());

        assertThat(numberOfDocuments, equalTo((long)documentNumber));
    }

    @Test
    public void shouldAddMigrationMarkerToGlobalTenantIndex() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        StepRepository repository = new StepRepository(client);
        IndexSettingsManager settingsManager = new IndexSettingsManager(repository);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "default");
        assertThat(settingsManager.isMigrationMarkerPresent(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        AddMigrationMarkerToGlobalTenantIndexStep step = new AddMigrationMarkerToGlobalTenantIndexStep(settingsManager);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(settingsManager.isMigrationMarkerPresent(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));
        GetMappingsResponse indexMappings = repository.findIndexMappings(GLOBAL_TENANT_INDEX.indexName());
        Map<String, Object> mappings = indexMappings.getMappings().get(GLOBAL_TENANT_INDEX.indexName()).getSourceAsMap();
        log.debug("Extended index mappings with migration marker '{}'", mappings);
        DocNode mappingNode = DocNode.wrap(mappings);
        assertThat(mappingNode, containsValue("$.properties.sg_data_migrated_to_8_8_0.type", "boolean"));
    }

    @Test
    public void shouldAddMigrationMarkerOnlyToGlobalIndex() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        StepRepository repository = new StepRepository(client);
        IndexSettingsManager settingsManager = new IndexSettingsManager(repository);
        DoubleAliasIndex managementTenantIndex = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        var doubleAliasIndices = ImmutableList.of(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, managementTenantIndex);
        environmentHelper.createIndex(doubleAliasIndices);
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "default");
        assertThat(settingsManager.isMigrationMarkerPresent(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        var tenantIndices = ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)) //
            .with(new TenantIndex(PRIVATE_USER_KIRK_INDEX.indexName(), null)) //
            .with(new TenantIndex(managementTenantIndex.indexName(), TENANT_MANAGEMENT));
        context.setTenantIndices(tenantIndices);
        AddMigrationMarkerToGlobalTenantIndexStep step = new AddMigrationMarkerToGlobalTenantIndexStep(settingsManager);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(settingsManager.isMigrationMarkerPresent(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));
        DocNode mappingNode = environmentHelper.getIndexMappingsAsDocNode(GLOBAL_TENANT_INDEX.indexName());
        assertThat(mappingNode, containsValue("$.properties.sg_data_migrated_to_8_8_0.type", "boolean"));
        mappingNode = environmentHelper.getIndexMappingsAsDocNode(PRIVATE_USER_KIRK_INDEX.indexName());
        assertThat(mappingNode, not(containsValue("$.properties.sg_data_migrated_to_8_8_0.type", "boolean")));
        mappingNode = environmentHelper.getIndexMappingsAsDocNode(managementTenantIndex.indexName());
        assertThat(mappingNode, not(containsValue("$.properties.sg_data_migrated_to_8_8_0.type", "boolean")));
    }

    @Test
    public void shouldNotAddMigrationMarkerToGlobalIndexWhenMarkerIsAlreadyPresent() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        StepRepository repository = new StepRepository(client);
        IndexSettingsManager settingsManager = new IndexSettingsManager(repository);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "default");
        environmentHelper.addDataMigrationMarkerToTheIndex(GLOBAL_TENANT_INDEX.indexName());
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        AddMigrationMarkerToGlobalTenantIndexStep step = new AddMigrationMarkerToGlobalTenantIndexStep(settingsManager);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(result.message(), containsString("marker already present"));
    }

    @Test
    public void shouldAddMigrationMarkerToBlockedIndex() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        StepRepository repository = new StepRepository(client);
        IndexSettingsManager settingsManager = new IndexSettingsManager(repository);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "default");
        repository.writeBlockIndices(ImmutableList.of(GLOBAL_TENANT_INDEX.indexName()));
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        AddMigrationMarkerToGlobalTenantIndexStep step = new AddMigrationMarkerToGlobalTenantIndexStep(settingsManager);

        StepResult result = step.execute(context);

        log.debug("Step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        DocNode mappingNode = environmentHelper.getIndexMappingsAsDocNode(GLOBAL_TENANT_INDEX.indexName());
        assertThat(mappingNode, containsValue("$.properties.sg_data_migrated_to_8_8_0.type", "boolean"));
    }

    @Test
    public void shouldBlockSingleDocumentDeletion() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        StepRepository repository = new StepRepository(client);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        String id = catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "default").get(0);
        repository.writeBlockIndices(ImmutableList.of(GLOBAL_TENANT_INDEX.indexName()));
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), id);
        DeleteRequest deleteRequest = new DeleteRequest(GLOBAL_TENANT_INDEX.indexName(), id);

        assertThatThrown(() -> client.delete(deleteRequest).actionGet(), instanceOf(ClusterBlockException.class));

        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), id);
    }

    @Test
    public void shouldBlockMultipleDocumentsDeletion() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        StepRepository repository = new StepRepository(client);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        String id = catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "one", "two", "three").get(0);
        repository.writeBlockIndices(ImmutableList.of(GLOBAL_TENANT_INDEX.indexName()));
        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), id);

        StepException exception = (StepException) assertThatThrown(() -> repository.deleteAllDocuments(GLOBAL_TENANT_INDEX.indexName()), //
            instanceOf(StepException.class));

        environmentHelper.assertThatDocumentExists(GLOBAL_TENANT_INDEX.indexName(), id);
        assertThat(exception.getStatus(), equalTo(DELETE_ALL_BULK_ERROR));
    }

    @Test
    public void shouldDeleteManyDocumentsFromGlobalTenantIndex() {
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        StepRepository repository = new StepRepository(client);
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        final int numberOfDocuments = 25_000;
        String[] names = IntStream.range(0, numberOfDocuments).mapToObj(i -> "space_no_" + i).toArray(String[]::new);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), names);
        repository.writeBlockIndices(ImmutableList.of(GLOBAL_TENANT_INDEX.indexName()));
        assertThat(environmentHelper.countDocumentInIndex(GLOBAL_TENANT_INDEX.indexName()), equalTo((long)numberOfDocuments));
        DeleteGlobalIndexContentStep step = new DeleteGlobalIndexContentStep(repository);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));

        StepResult result = step.execute(context);

        log.debug("Delete global index content step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.countDocumentInIndex(GLOBAL_TENANT_INDEX.indexName()), equalTo(0L));
    }

    @Test
    public void shouldDeleteOnlyDocumentsFromGlobalIndex() {
        DoubleAliasIndex managementTenantIndex = environmentHelper.doubleAliasForTenant(TENANT_MANAGEMENT);
        var doubleAliasIndices = ImmutableList.of(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, managementTenantIndex);
        environmentHelper.createIndex(doubleAliasIndices);
        var tenantIndices = ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)) //
            .with(new TenantIndex(PRIVATE_USER_KIRK_INDEX.indexName(), null)) //
            .with(new TenantIndex(managementTenantIndex.indexName(), TENANT_MANAGEMENT));
        context.setTenantIndices(tenantIndices);
        BackupIndex backupIndex1 = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndex2 = new BackupIndex(NOW.toLocalDateTime().minusYears(1));
        environmentHelper.createBackupIndex(backupIndex1, backupIndex2);
        context.setBackupIndices(ImmutableList.of(backupIndex1.indexName(), backupIndex2.indexName()));
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        catalog.insertSpace(GLOBAL_TENANT_INDEX.indexName(), "global");
        catalog.insertSpace(PRIVATE_USER_KIRK_INDEX.indexName(), "private");
        catalog.insertSpace(managementTenantIndex.indexName(), "management");
        catalog.insertSpace(backupIndex1.indexName(), "backup_1");
        catalog.insertSpace(backupIndex2.indexName(), "backup_2");
        DeleteGlobalIndexContentStep step = new DeleteGlobalIndexContentStep(new StepRepository(client));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.countDocumentInIndex(GLOBAL_TENANT_INDEX.indexName()), equalTo(0L));
        assertThat(environmentHelper.countDocumentInIndex(PRIVATE_USER_KIRK_INDEX.indexName()), equalTo(1L));
        assertThat(environmentHelper.countDocumentInIndex(managementTenantIndex.indexName()), equalTo(1L));
        assertThat(environmentHelper.countDocumentInIndex(backupIndex1.indexName()), equalTo(1L));
        assertThat(environmentHelper.countDocumentInIndex(backupIndex2.indexName()), equalTo(1L));
    }

    @Test
    public void shouldCopyAllFromTempIndexIntoGlobalTenantIndex() {
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        StepRepository repository = new StepRepository(client);
        CopyDataToGlobalIndexStep step = new CopyDataToGlobalIndexStep(repository);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        repository.createIndex(context.getTempIndexName(), 1, 0, 1500, Collections.emptyMap());
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        final int numberOfDocuments = 12_500;
        catalog.insertSpace(context.getTempIndexName(), numberOfDocuments);

        StepResult result = step.execute(context);

        log.debug("Copy documents to global tenant index result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(environmentHelper.countDocumentInIndex(context.getTempIndexName()), equalTo((long)numberOfDocuments));
    }

    @Test
    public void shouldCopyExactDocumentToGlobalTenantIndex() {
        environmentHelper.createIndex(GLOBAL_TENANT_INDEX);
        PrivilegedConfigClient client = environmentHelper.getPrivilegedClient();
        StepRepository repository = new StepRepository(client);
        CopyDataToGlobalIndexStep step = new CopyDataToGlobalIndexStep(repository);
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), Tenant.GLOBAL_TENANT_ID)));
        repository.createIndex(context.getTempIndexName(), 1, 0, 1500, Collections.emptyMap());
        FrontendObjectCatalog catalog = new FrontendObjectCatalog(client);
        final int numberOfDocuments = 3;
        ImmutableList<String> spacesIds = catalog.insertSpace(context.getTempIndexName(), numberOfDocuments);
        ImmutableList<String> patternIds = catalog.insertIndexPattern(context.getTempIndexName(), numberOfDocuments);

        StepResult result = step.execute(context);

        log.debug("Copy documents to global tenant index result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        ImmutableList<String> documentsToCompare = spacesIds.with(patternIds);
        assertThat(documentsToCompare, hasSize(numberOfDocuments * 2));
        for(String documentId : documentsToCompare) {
            String sourceDocument = environmentHelper.getDocumentSource(context.getTempIndexName(), documentId).orElseThrow();
            String documentCopy = environmentHelper.getDocumentSource(context.getGlobalTenantIndexName(), documentId).orElseThrow();
            assertThat(documentCopy, equalTo(sourceDocument));
        }
    }

    private  CheckIndicesStateStep createCheckIndicesStateStep() {
        return new CheckIndicesStateStep(new StepRepository(environmentHelper.getPrivilegedClient()));
    }

    private PopulateTenantsStep createPopulateTenantsStep() {
        FeMultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(FeMultiTenancyConfigurationProvider.class);
        PrivilegedConfigClient privilegedConfigClient = environmentHelper.getPrivilegedClient();
        return new PopulateTenantsStep(configurationProvider, new StepRepository(privilegedConfigClient));
    }

    private static CopyDataToTempIndexStep createCopyDataToTempIndexStep(PrivilegedConfigClient client) {
        FeMultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(FeMultiTenancyConfigurationProvider.class);
        StepRepository stepRepository = new StepRepository(client);
        IndexSettingsManager indexSettingsManager = new IndexSettingsManager(stepRepository);
        return new CopyDataToTempIndexStep(stepRepository, configurationProvider, indexSettingsManager);
    }
}