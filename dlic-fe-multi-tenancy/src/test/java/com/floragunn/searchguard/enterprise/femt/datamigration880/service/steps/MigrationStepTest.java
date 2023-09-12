package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfig;
import com.floragunn.searchguard.enterprise.femt.MultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.TenantData;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.internal.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.GLOBAL_TENANT_NOT_FOUND_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.INDICES_NOT_FOUND_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.MULTI_TENANCY_CONFIG_NOT_AVAILABLE_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.MULTI_TENANCY_DISABLED_ERROR;
import static com.floragunn.searchguard.support.PrivilegedConfigClient.adapt;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MigrationStepTest {

    private static final Logger log = LogManager.getLogger(MigrationStepTest.class);

    private static final ZonedDateTime NOW = ZonedDateTime.of(LocalDateTime.of(2000, 1, 1, 1, 1), ZoneOffset.UTC);

    private static final DoubleAliasIndex GLOBAL_TENANT_INDEX = new DoubleAliasIndex(".kibana_8.7.0_001", ".kibana_8.7.0", ".kibana");
    private static final DoubleAliasIndex TASK_MANAGER_INDEX = new DoubleAliasIndex(".kibana_task_manager_8.7.0_001", ".kibana_task_manager_8.7.0", "kibana_task_manager");
    private static final DoubleAliasIndex EVENT_LOG_INDEX = new DoubleAliasIndex(".kibana-event-log-8.7.0-000001", ".kibana-event-log-8.7.0", ".kibana-event-log");
    private static final DoubleAliasIndex DATA_INDEX = new DoubleAliasIndex("iot-2020-09", "iot-2020", "iot");

    private static final DoubleAliasIndex PRIVATE_USER_KIRK_INDEX = new DoubleAliasIndex(".kibana_3292183_kirk_8.7.0_001", ".kibana_3292183_kirk_8.7.0", ".kibana_3292183_kirk" );// kirk
    private static final DoubleAliasIndex PRIVATE_USER_LUKASZ_1_INDEX = new DoubleAliasIndex(".kibana_-1091682490_lukasz_8.7.0_001", ".kibana_-1091682490_lukasz_8.7.0", ".kibana_-1091682490_lukasz"); //lukasz
    private static final DoubleAliasIndex PRIVATE_USER_LUKASZ_2_INDEX = new DoubleAliasIndex(".kibana_739988528_ukasz_8.7.0_001", ".kibana_739988528_ukasz_8.7.0", ".kibana_739988528_ukasz"); //łukasz
    private static final DoubleAliasIndex PRIVATE_USER_LUKASZ_3_INDEX =new DoubleAliasIndex(".kibana_-1091714203_luksz_8.7.0_001", ".kibana_-1091714203_luksz_8.7.0", ".kibana_-1091714203_luksz");//luk@sz

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
        .sslEnabled()
        .resources("multitenancy")
        .enterpriseModulesEnabled()
        .build();

    private Clock clock = Clock.fixed(NOW.toInstant(), ZoneOffset.UTC);
    private DataMigrationContext context;

    @Mock
    private MultiTenancyConfigurationProvider multiTenancyConfigurationProvider;

    @Mock
    private FeMultiTenancyConfig feMultiTenancyConfig;

    private final List<DoubleAliasIndex> createdIndices = new ArrayList<>();

    @Before
    public void before() {
        this.context = new DataMigrationContext(clock);
    }

    @After
    public void after() {
        createdIndices.forEach(this::deleteIndex);
        createdIndices.clear();
    }

    private void createIndex(DoubleAliasIndex...indices) {
        for(DoubleAliasIndex index : indices) {
            CreateIndexRequest request = new CreateIndexRequest(index.indexName()) //
                .alias(new Alias(index.shortAlias())) //
                .alias(new Alias(index.longAlias()));
            cluster.getInternalNodeClient().admin().indices().create(request).actionGet();
            createdIndices.add(index);
        }
    }

    private void deleteIndex(DoubleAliasIndex doubleAliasIndex) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(doubleAliasIndex.indexName());
        cluster.getInternalNodeClient().admin().indices().delete(deleteIndexRequest).actionGet();
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
        PopulateTenantsStep populateTenantsStep = new PopulateTenantsStep(multiTenancyConfigurationProvider, adapt(client));

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(MULTI_TENANCY_CONFIG_NOT_AVAILABLE_ERROR));
    }

    @Test
    public void shouldReportErrorWhenMultiTenancyIsDisabled() {
        when(feMultiTenancyConfig.isEnabled()).thenReturn(false);
        when(multiTenancyConfigurationProvider.getConfig()).thenReturn(Optional.of(feMultiTenancyConfig));
        when(multiTenancyConfigurationProvider.getTenantNames()).thenReturn(ImmutableSet.empty());
        Client client = cluster.getInternalNodeClient();
        PopulateTenantsStep populateTenantsStep = new PopulateTenantsStep(multiTenancyConfigurationProvider, adapt(client));

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(MULTI_TENANCY_DISABLED_ERROR));
    }

    @Test
    public void shouldFindGlobalTenantIndex() {
        createIndex(GLOBAL_TENANT_INDEX, TASK_MANAGER_INDEX, EVENT_LOG_INDEX, DATA_INDEX);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        populateTenantsStep.execute(context);

        ImmutableList<TenantData> tenants = context.getTenants();
        assertThat(tenants, hasSize(1));
        assertThat(tenants.get(0).isGlobal(), equalTo(true));
    }

    @Test
    public void shouldBreakMigrationProcessWhenGlobalTenantIndexIsNotFound() {
        List<DoubleAliasIndex> configuredTenantIndices = getIndicesForConfiguredTenantsWithoutGlobal(".kibana");
        createIndex(configuredTenantIndices.toArray(size -> new DoubleAliasIndex[size]));
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(GLOBAL_TENANT_NOT_FOUND_ERROR));
    }

    @Test
    public void shouldFindAllConfiguredTenants() {
        List<DoubleAliasIndex> configuredTenantIndices = getIndicesForConfiguredTenantsWithoutGlobal(".kibana");
        createIndex(configuredTenantIndices.toArray(size -> new DoubleAliasIndex[size]));
        createIndex(GLOBAL_TENANT_INDEX);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getTenants(), hasSize(configuredTenantIndices.size() + 1));
        assertThat(context.getTenants().size(), greaterThan(20));
        List<String> tenantsFoundByStep = context.getTenants().stream().map(TenantData::tenantName).collect(Collectors.toList());
        MultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(MultiTenancyConfigurationProvider.class);
        String[] tenantsFromConfiguration = configurationProvider.getTenantNames().toArray(size -> new String[size]);
        assertThat(tenantsFoundByStep, containsInAnyOrder(tenantsFromConfiguration));
    }

    @Test
    public void shouldFindUserPrivateTenants() {
        createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, PRIVATE_USER_LUKASZ_1_INDEX, PRIVATE_USER_LUKASZ_2_INDEX,
            PRIVATE_USER_LUKASZ_3_INDEX);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getTenants(), hasSize(5));
        Set<String> privateTenantIndexNames = context.getTenants() //
            .stream() //
            .filter(TenantData::isUserPrivateTenant) //
            .map(TenantData::indexName) //
            .collect(Collectors.toSet());
        assertThat(privateTenantIndexNames, containsInAnyOrder(PRIVATE_USER_KIRK_INDEX.indexName(),
            PRIVATE_USER_LUKASZ_1_INDEX.indexName(),
            PRIVATE_USER_LUKASZ_2_INDEX.indexName(),
            PRIVATE_USER_LUKASZ_3_INDEX.indexName()));
    }

    /**
     * The test check paging related problems.
     */
    @Test
    @Ignore
    public void shouldFindLargeNumberOfTenants() {
        List<DoubleAliasIndex> indices = new ArrayList<>();
        indices.add(GLOBAL_TENANT_INDEX);
        indices.addAll(getIndicesForConfiguredTenantsWithoutGlobal(".kibana"));
        indices.addAll(generatePrivateTenantNames(".kibana", 101));
        createIndex(indices.toArray(size -> new DoubleAliasIndex[size]));
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getTenants(), hasSize(indices.size()));
    }

    @Test
    public void shouldUseIndexPrefixReadFromConfiguration() {
        String indexNamePrefix = "wideopenfindashboard";
        DoubleAliasIndex privateUserTenant = generatePrivateTenantNames(indexNamePrefix, 1).get(0);
        createIndex(GLOBAL_TENANT_INDEX, privateUserTenant);
        when(feMultiTenancyConfig.getIndex()).thenReturn(indexNamePrefix);
        when(feMultiTenancyConfig.isEnabled()).thenReturn(true);
        when(multiTenancyConfigurationProvider.getConfig()).thenReturn(Optional.of(feMultiTenancyConfig));
        when(multiTenancyConfigurationProvider.getTenantNames()).thenReturn(ImmutableSet.empty());
        Client client = cluster.getInternalNodeClient();
        PopulateTenantsStep populateTenantsStep = new PopulateTenantsStep(multiTenancyConfigurationProvider, adapt(client));

        StepResult result = populateTenantsStep.execute(context);

        log.debug("Step result: '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getTenants(), hasSize(2));
        TenantData tenantData = context.getTenants().stream().filter(TenantData::isUserPrivateTenant).findAny().get();
        assertThat(tenantData.indexName(), containsString(indexNamePrefix));
    }

    @Test
    public void shouldLoadTheNewestTenantList() throws Exception {
        String newTenantName = "tenant-created-in-course-of-test-updates";
        MultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(MultiTenancyConfigurationProvider.class);
        ImmutableSet<String> tenantNames = configurationProvider.getTenantNames();
        assertThat(tenantNames.contains(newTenantName), equalTo(false));
        try(GenericRestClient client = cluster.getAdminCertRestClient()) {
            String body = DocNode.of("description", "Tenant created to test reading the newest multi-tenancy configuration").toJsonString();
            HttpResponse response = client.putJson("/_searchguard/api/tenants/" + newTenantName, body);
            try {
                log.debug("Create tenant status code '{}' and response body '{}'", response.getStatusCode(), response.getBody());
                assertThat(response.getStatusCode(), equalTo(SC_CREATED));

                Set<String> updatedTenantList = new HashSet<>(configurationProvider.getTenantNames());

                assertThat(updatedTenantList.contains(newTenantName), equalTo(true));
                assertThat(updatedTenantList.size(), equalTo(tenantNames.size() + 1));
            } finally {
                response = client.delete("/_searchguard/api/tenants/" + newTenantName);
                assertThat(response.getStatusCode(), equalTo(SC_OK));
            }
        }
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
                createIndex(GLOBAL_TENANT_INDEX, DoubleAliasIndex.forTenant(newTenantName));

                StepResult result = populateTenantsStep.execute(context);

                log.debug("Step result: '{}'.", result);
                assertThat(result.isSuccess(), equalTo(true));
                assertThat(context.getTenants(), hasSize(2));
                TenantData createdTenant = context.getTenants().stream().filter(tenantData -> !tenantData.isGlobal()).findAny().get();
                assertThat(createdTenant.tenantName(), equalTo(newTenantName));
                assertThat(createdTenant.isUserPrivateTenant(), equalTo(false));
            } finally {
                response = client.delete("/_searchguard/api/tenants/" + newTenantName);
                assertThat(response.getStatusCode(), equalTo(SC_OK));
            }
        }
    }

    private List<DoubleAliasIndex> getIndicesForConfiguredTenantsWithoutGlobal(String indexNamePrefix) {
        MultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(MultiTenancyConfigurationProvider.class);
        return configurationProvider.getTenantNames() //
            .stream() //
            .filter(name -> !Tenant.GLOBAL_TENANT_ID.equals(name)) //
            .map(tenantName -> DoubleAliasIndex.forTenant(tenantName)) //
            .toList();
    }

    private List<DoubleAliasIndex> generatePrivateTenantNames(String prefix, int number) {
        return IntStream.range(0, number)
            .mapToObj(index -> "private tenant name - " + index)
            .map(tenantName -> DoubleAliasIndex.forTenantWithPrefix(prefix, tenantName))
            .collect(Collectors.toList());
    }

    private static String tenantNameToIndexName(String indexNamePrefix, String tenantName) {
        return indexNamePrefix + "_" + tenantName.hashCode() + "_" + tenantName.toLowerCase().replaceAll("[^a-z0-9]+", "");
    }

    private static PopulateTenantsStep createPopulateTenantsStep() {
        MultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(MultiTenancyConfigurationProvider.class);
        Client client = cluster.getInternalNodeClient();
        PopulateTenantsStep populateTenantsStep = new PopulateTenantsStep(configurationProvider, adapt(client));
        return populateTenantsStep;
    }

    private record DoubleAliasIndex(String indexName, String shortAlias, String longAlias) {

        public static final String DEFAULT_INDEX_PREFIX = ".kibana";

        DoubleAliasIndex {
            Objects.requireNonNull(indexName, "Index name is required");
            Objects.requireNonNull(shortAlias, "Short alias name is required");
            Objects.requireNonNull(longAlias, "Long alias name is required");
        }

        public static DoubleAliasIndex forTenantWithPrefix(String indexNamePrefix, String tenantName) {
            String baseIndexName = tenantNameToIndexName(indexNamePrefix, tenantName);
            return new DoubleAliasIndex( baseIndexName + "_8.7.0_001", baseIndexName + "_8.7.0", baseIndexName);
        }

        public static DoubleAliasIndex forTenant(String tenantName) {
            return forTenantWithPrefix(DEFAULT_INDEX_PREFIX, tenantName);
        }

    }

}