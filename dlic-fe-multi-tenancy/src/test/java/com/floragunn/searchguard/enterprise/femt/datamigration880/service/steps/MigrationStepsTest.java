package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfig;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.IndexNameDataFormatter;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationConfig;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.TenantIndex;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.CANNOT_RESOLVE_INDEX_BY_ALIAS;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.DATA_INDICES_LOCKED_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.GLOBAL_TENANT_NOT_FOUND_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.INDICES_NOT_FOUND_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.MULTI_TENANCY_CONFIG_NOT_AVAILABLE_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.MULTI_TENANCY_DISABLED_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.UNHEALTHY_INDICES_ERROR;
import static com.floragunn.searchguard.support.PrivilegedConfigClient.adapt;
import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static java.time.ZoneOffset.UTC;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_OK;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.Strings.requireNonEmpty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MigrationStepsTest {

    private static final Logger log = LogManager.getLogger(MigrationStepsTest.class);

    private static final ZonedDateTime NOW = ZonedDateTime.of(LocalDateTime.of(2000, 1, 1, 1, 1), UTC);

    public static final String MULTITENANCY_INDEX_PREFIX = ".kibana";
    private static final DoubleAliasIndex GLOBAL_TENANT_INDEX = new DoubleAliasIndex(".kibana_8.7.0_001", ".kibana_8.7.0",
        MULTITENANCY_INDEX_PREFIX);
    private static final DoubleAliasIndex PRIVATE_USER_KIRK_INDEX = new DoubleAliasIndex(".kibana_3292183_kirk_8.7.0_001", ".kibana_3292183_kirk_8.7.0", ".kibana_3292183_kirk" );// kirk
    private static final DoubleAliasIndex PRIVATE_USER_LUKASZ_1_INDEX = new DoubleAliasIndex(".kibana_-1091682490_lukasz_8.7.0_001", ".kibana_-1091682490_lukasz_8.7.0", ".kibana_-1091682490_lukasz"); //lukasz
    private static final DoubleAliasIndex PRIVATE_USER_LUKASZ_2_INDEX = new DoubleAliasIndex(".kibana_739988528_ukasz_8.7.0_001", ".kibana_739988528_ukasz_8.7.0", ".kibana_739988528_ukasz"); //łukasz
    private static final DoubleAliasIndex PRIVATE_USER_LUKASZ_3_INDEX =new DoubleAliasIndex(".kibana_-1091714203_luksz_8.7.0_001", ".kibana_-1091714203_luksz_8.7.0", ".kibana_-1091714203_luksz");//luk@sz
    public static final String INDEX_TEMPLATE_NAME = "kibana_test_indices_template";

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
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

    private final List<DeletableIndex> createdIndices = new ArrayList<>();

    @Before
    public void before() {
        this.context = new DataMigrationContext(new MigrationConfig(false), clock);
    }

    @After
    public void after() {
        deleteIndex(createdIndices.toArray(DeletableIndex[]::new));
        createdIndices.clear();
        deleteIndexTemplateIfExists();
    }

    private static void deleteIndexTemplateIfExists() {
        try(Client client = cluster.getInternalNodeClient()) {
            GetIndexTemplatesResponse response = client.admin().indices().prepareGetTemplates(INDEX_TEMPLATE_NAME).execute().actionGet();
            if(!response.getIndexTemplates().isEmpty()) {
                var acknowledgedResponse = client.admin().indices().prepareDeleteTemplate(INDEX_TEMPLATE_NAME).execute().actionGet();
                assertThat(acknowledgedResponse.isAcknowledged(), equalTo(true));
            }
        }
    }

    private void createIndex(String indexNamesPrefix, int numberOfReplicas, Settings additionalSettings, DoubleAliasIndex...indices) {
        BulkRequest bulkRequest = new BulkRequest();
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        Settings.Builder settings = Settings.builder()
            .put("index.hidden", true)
            .put("index.number_of_replicas", numberOfReplicas);
        if(additionalSettings != null) {
            settings.put(additionalSettings);
        }
        PutIndexTemplateRequest templateRequest = new PutIndexTemplateRequest(INDEX_TEMPLATE_NAME) //
            .patterns(Collections.singletonList(indexNamesPrefix + "*"))
            .settings(settings);
        try(Client client = cluster.getInternalNodeClient()) {
            client.admin() //
                .indices() //
                .putTemplate(templateRequest) //
                .actionGet();
            for (DoubleAliasIndex index : indices) {
                String currentIndex = index.indexName();
                if (!currentIndex.startsWith(indexNamesPrefix)) {
                    String message = String.join("", "Incorrect name of index ",
                        currentIndex, ". All created indices must have a common prefix '" + indexNamesPrefix, "'.");
                    throw new IllegalStateException(message);
                }
                bulkRequest.add(new IndexRequest(currentIndex).source(DocNode.EMPTY));
                createdIndices.add(index);
                indicesAliasesRequest.addAliasAction(AliasActions.add().index(index.indexName).aliases(index.longAlias, index.shortAlias));
            }
            BulkResponse response = client.bulk(bulkRequest.setRefreshPolicy(IMMEDIATE)).actionGet();
            if (response.hasFailures()) {
                log.error("Create index failure response {}", response.buildFailureMessage());
            }
            assertThat(response.hasFailures(), equalTo(false));
            var acknowledgedResponse = client.admin().indices().aliases(indicesAliasesRequest).actionGet();
            assertThat(acknowledgedResponse.isAcknowledged(), equalTo(true));
        }
    }

    private void createBackupIndex(BackupIndex...indices) {
        BulkRequest bulkRequest = new BulkRequest();
        for(BackupIndex index : indices) {
            bulkRequest.add(new IndexRequest(index.indexName()).source(DocNode.EMPTY));
            createdIndices.add(index);
        }

        try(Client client = cluster.getInternalNodeClient()) {
            BulkResponse response = client.bulk(bulkRequest.setRefreshPolicy(IMMEDIATE)).actionGet();
            if (response.hasFailures()) {
                log.error("Create backup index failure response {}", response.buildFailureMessage());
            }
            assertThat(response.hasFailures(), equalTo(false));
        }
    }

    private void createIndex(DoubleAliasIndex...indices) {
        createIndex(MULTITENANCY_INDEX_PREFIX, 0, null, indices);
    }

    private void createLegacyIndex(LegacyIndex...indices) {
        String configuredIndexNamePrefix = getConfiguredIndexPrefix();

        BulkRequest bulkRequest = new BulkRequest();
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        PutIndexTemplateRequest templateRequest = new PutIndexTemplateRequest(INDEX_TEMPLATE_NAME) //
            .patterns(Collections.singletonList(configuredIndexNamePrefix + "*")) //
            .settings(Settings.builder().put("index.hidden", true));
        try(Client client = cluster.getInternalNodeClient()) {
            client.admin() //
                .indices() //
                .putTemplate(templateRequest) //
                .actionGet();
            for (LegacyIndex index : indices) {
                String currentIndexName = index.indexName();
                if (!currentIndexName.startsWith(configuredIndexNamePrefix)) {
                    throw new IllegalStateException("All legacy indices names should start with " + configuredIndexNamePrefix);
                }
                bulkRequest.add(new IndexRequest(currentIndexName).source(DocNode.EMPTY));
                createdIndices.add(index);
                indicesAliasesRequest.addAliasAction(AliasActions.add().index(index.indexName).aliases(index.longAlias));
            }
            BulkResponse response = client.bulk(bulkRequest.setRefreshPolicy(IMMEDIATE)).actionGet();
            if(response.hasFailures()) {
                log.error("Cannot create legacy indices due to '{}'", response.buildFailureMessage());
            }
            assertThat(response.hasFailures(), equalTo(false));
            AcknowledgedResponse acknowledgedResponse = client.admin().indices().aliases(indicesAliasesRequest).actionGet();
            if(!acknowledgedResponse.isAcknowledged()) {
                log.error("Cannot create aliases for legacy indices.");
            }
            assertThat(acknowledgedResponse.isAcknowledged(), equalTo(true));
        }
    }

    private void deleteIndex(DeletableIndex...deletableIndices) {
        if(deletableIndices.length == 0) {
            return;
        }
        String[] indicesForDeletion = Arrays.stream(deletableIndices).map(DeletableIndex::indexForDeletion).toArray(String[]::new);
        DeleteIndexRequest request = new DeleteIndexRequest(indicesForDeletion);
        cluster.getInternalNodeClient().admin().indices().delete(request).actionGet();
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
    public void shouldReportErrorWhenMultiTenancyIsDisabled() {
        when(feMultiTenancyConfig.isEnabled()).thenReturn(false);
        when(multiTenancyConfigurationProvider.getConfig()).thenReturn(Optional.of(feMultiTenancyConfig));
        when(multiTenancyConfigurationProvider.getTenantNames()).thenReturn(ImmutableSet.empty());
        Client client = cluster.getInternalNodeClient();
        StepRepository repository = new StepRepository(adapt(client));
        PopulateTenantsStep populateTenantsStep = new PopulateTenantsStep(multiTenancyConfigurationProvider, repository);

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(MULTI_TENANCY_DISABLED_ERROR));
    }

    @Test
    public void shouldFindGlobalTenantIndex() {
        DoubleAliasIndex taskManagerIndex = new DoubleAliasIndex(".kibana_task_manager_8.7.0_001", ".kibana_task_manager_8.7.0", "kibana_task_manager");
        DoubleAliasIndex eventLogIndex = new DoubleAliasIndex(".kibana-event-log-8.7.0-000001", ".kibana-event-log-8.7.0", ".kibana-event-log");
        DoubleAliasIndex dataIndex = new DoubleAliasIndex("iot-2020-09", "iot-2020", "iot");
        createIndex(GLOBAL_TENANT_INDEX, taskManagerIndex, eventLogIndex);
        createIndex("iot", 0, null, dataIndex);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        populateTenantsStep.execute(context);

        ImmutableList<TenantIndex> tenants = context.getTenantIndices();
        assertThat(tenants, hasSize(1));
        assertThat(tenants.get(0).belongsToGlobalTenant(), equalTo(true));
    }

    @Test
    public void shouldBreakMigrationProcessWhenGlobalTenantIndexIsNotFound() {
        List<DoubleAliasIndex> configuredTenantIndices = getIndicesForConfiguredTenantsWithoutGlobal();
        createIndex(configuredTenantIndices.toArray(DoubleAliasIndex[]::new));
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(false));
        assertThat(result.status(), equalTo(GLOBAL_TENANT_NOT_FOUND_ERROR));
    }

    @Test
    public void shouldFindAllConfiguredTenants() {
        List<DoubleAliasIndex> configuredTenantIndices = getIndicesForConfiguredTenantsWithoutGlobal();
        createIndex(configuredTenantIndices.toArray(DoubleAliasIndex[]::new));
        createIndex(GLOBAL_TENANT_INDEX);
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
        createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, PRIVATE_USER_LUKASZ_1_INDEX, PRIVATE_USER_LUKASZ_2_INDEX,
            PRIVATE_USER_LUKASZ_3_INDEX);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getTenantIndices(), hasSize(5));
        Set<String> privateTenantIndexNames = context.getTenantIndices() //
            .stream() //
            .filter(TenantIndex::belongsToUserPrivateTenant) //
            .map(TenantIndex::indexName) //
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
    public void shouldFindLargeNumberOfTenants() {
        List<DoubleAliasIndex> indices = new ArrayList<>();
        indices.add(GLOBAL_TENANT_INDEX);
        indices.addAll(getIndicesForConfiguredTenantsWithoutGlobal());
        indices.addAll(generatePrivateTenantNames(MULTITENANCY_INDEX_PREFIX, 101));
        createIndex(indices.toArray(DoubleAliasIndex[]::new));
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getTenantIndices(), hasSize(indices.size()));
    }

    @Test
    public void shouldUseIndexPrefixReadFromConfiguration() {
        String indexNamePrefix = "wideopenfindashboard";
        DoubleAliasIndex privateUserTenant = generatePrivateTenantNames(indexNamePrefix, 1).get(0);
        DoubleAliasIndex globalTenantIndex = new DoubleAliasIndex(indexNamePrefix + "_8.7.0_001",
            indexNamePrefix + "_8.7.0", indexNamePrefix);
        createIndex(indexNamePrefix, 0, null, globalTenantIndex);
        createIndex(indexNamePrefix, 0, null, privateUserTenant);
        when(feMultiTenancyConfig.getIndex()).thenReturn(indexNamePrefix);
        when(feMultiTenancyConfig.isEnabled()).thenReturn(true);
        when(multiTenancyConfigurationProvider.getConfig()).thenReturn(Optional.of(feMultiTenancyConfig));
        when(multiTenancyConfigurationProvider.getTenantNames()).thenReturn(ImmutableSet.empty());
        Client client = cluster.getInternalNodeClient();
        StepRepository repository = new StepRepository(adapt(client));
        PopulateTenantsStep populateTenantsStep = new PopulateTenantsStep(multiTenancyConfigurationProvider, repository);

        StepResult result = populateTenantsStep.execute(context);

        log.debug("Step result: '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getTenantIndices(), hasSize(2));
        TenantIndex tenantIndex = context.getTenantIndices().stream().filter(TenantIndex::belongsToUserPrivateTenant).findAny().orElseThrow();
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
                createIndex(GLOBAL_TENANT_INDEX, DoubleAliasIndex.forTenant(newTenantName));

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
        createIndex(GLOBAL_TENANT_INDEX);
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();
        DoubleAliasIndex index = DoubleAliasIndex.forTenant("management");
        String additionalIndex = index.indexName() + "_another_index";
        try(Client client = cluster.getInternalNodeClient()) {
            client.admin().indices().create(new CreateIndexRequest(index.indexName())).actionGet();
            this.createdIndices.add(index);
            client.admin().indices().create(new CreateIndexRequest(additionalIndex)).actionGet();
            try {
                AliasActions aliasAction = new AliasActions(AliasActions.Type.ADD) //
                    .alias(index.shortAlias()) //
                    .indices(index.indexName(), additionalIndex);
                client.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(aliasAction)).actionGet();

                var ex = (StepException) assertThatThrown(() -> populateTenantsStep.execute(context), instanceOf(StepException.class));

                assertThat(ex.getStatus(), equalTo(CANNOT_RESOLVE_INDEX_BY_ALIAS));
            } finally {
                client.admin().indices().delete(new DeleteIndexRequest(additionalIndex));
            }
        }
    }

    @Test
    public void shouldCreateValidLegacyNames() {
        DoubleAliasIndex privateUserTenant = DoubleAliasIndex.forTenant("spock");

        assertThat(privateUserTenant.indexName(), equalTo(".kibana_109651354_spock_8.7.0_001"));
        assertThat(privateUserTenant.longAlias(), equalTo(".kibana_109651354_spock_8.7.0"));
        assertThat(privateUserTenant.shortAlias(), equalTo(".kibana_109651354_spock"));
        assertThat(privateUserTenant.getIndexNameInVersion(DoubleAliasIndex.LEGACY_VERSION), equalTo(".kibana_109651354_spock_7.17.12_001"));
        assertThat(privateUserTenant.getLongAliasInVersion(DoubleAliasIndex.LEGACY_VERSION), equalTo(".kibana_109651354_spock_7.17.12"));
    }

    @Test
    public void shouldChooseIndicesRelatedToNewestVersion() {
        ImmutableList<DoubleAliasIndex> indices = ImmutableList.of("admin_tenant", "management", "kirk", "spock", "łuk@sz") //
            .map(DoubleAliasIndex::forTenant) //
            .with(GLOBAL_TENANT_INDEX);
        createIndex(indices.toArray(DoubleAliasIndex[]::new));
        createLegacyIndex(indices.map(index -> index.toLegacyIndex(DoubleAliasIndex.LEGACY_VERSION)).toArray(LegacyIndex[]::new));
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
            isIndexCreated(".kibana_-152937574_admintenant_7.17.12_001", ".kibana_-152937574_admintenant_7.17.12",
                ".kibana_-1799980989_management_7.17.12_001", ".kibana_-1799980989_management_7.17.12",
                ".kibana_3292183_kirk_7.17.12_001", ".kibana_3292183_kirk_7.17.12",
                ".kibana_739956815_uksz_7.17.12_001", ".kibana_739956815_uksz_7.17.12_001",
                ".kibana_109651354_spock_7.17.12_001", ".kibana_109651354_spock_7.17.12"
        ), equalTo(true));
    }

    @Test
    public void shouldRecognizeTenantType() {
        ImmutableList<DoubleAliasIndex> indices = ImmutableList.of("admin_tenant",  "kirk") //
            .map(DoubleAliasIndex::forTenant) //
            .with(GLOBAL_TENANT_INDEX);
        createIndex(indices.toArray(DoubleAliasIndex[]::new));
        PopulateTenantsStep populateTenantsStep = createPopulateTenantsStep();

        StepResult result = populateTenantsStep.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getTenantIndices(), hasSize(3));
        String global = context.getTenantIndices().stream().filter(TenantIndex::belongsToGlobalTenant).map(TenantIndex::tenantName).findFirst().orElseThrow();
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
        createIndex(MULTITENANCY_INDEX_PREFIX, 25, Settings.EMPTY, GLOBAL_TENANT_INDEX);
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
        createIndex(MULTITENANCY_INDEX_PREFIX, 0, Settings.EMPTY, indices.toArray(DoubleAliasIndex[]::new));
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
        createIndex(MULTITENANCY_INDEX_PREFIX, 0, Settings.EMPTY, indices.toArray(DoubleAliasIndex[]::new));
        createIndex(MULTITENANCY_INDEX_PREFIX, 25, Settings.EMPTY, PRIVATE_USER_KIRK_INDEX); // this index should be yellow
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
        createIndex(MULTITENANCY_INDEX_PREFIX, 0, Settings.EMPTY, indices.toArray(DoubleAliasIndex[]::new));
        createIndex(MULTITENANCY_INDEX_PREFIX, 25, Settings.EMPTY, PRIVATE_USER_KIRK_INDEX); // this index should be yellow
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
        createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX);
        context.setTenantIndices(doubleAliasIndexToTenantDataWithoutTenantName(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX));
        CheckIfIndicesAreBlockedStep step = new CheckIfIndicesAreBlockedStep(new StepRepository(getPrivilegedClient()));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(result.details(), containsString(GLOBAL_TENANT_INDEX.indexName()));
        assertThat(result.details(), containsString(PRIVATE_USER_KIRK_INDEX.indexName()));
    }

    @Test
    public void shouldFindWriteBlockedIndices() throws Exception {
        DoubleAliasIndex managementTenantIndex = DoubleAliasIndex.forTenant("management");
        createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, managementTenantIndex);
        var tenantIndices = doubleAliasIndexToTenantDataWithoutTenantName(GLOBAL_TENANT_INDEX,
            PRIVATE_USER_KIRK_INDEX,
            managementTenantIndex);
        context.setTenantIndices(tenantIndices);
        CheckIfIndicesAreBlockedStep step = new CheckIfIndicesAreBlockedStep(new StepRepository(getPrivilegedClient()));
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
        DoubleAliasIndex managementTenantIndex = DoubleAliasIndex.forTenant("management");
        createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, managementTenantIndex);
        var tenantIndices = doubleAliasIndexToTenantDataWithoutTenantName(GLOBAL_TENANT_INDEX,
            PRIVATE_USER_KIRK_INDEX,
            managementTenantIndex);
        context.setTenantIndices(tenantIndices);
        CheckIfIndicesAreBlockedStep step = new CheckIfIndicesAreBlockedStep(new StepRepository(getPrivilegedClient()));
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
        DoubleAliasIndex managementTenantIndex = DoubleAliasIndex.forTenant("management");
        createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, managementTenantIndex);
        var tenantIndices = doubleAliasIndexToTenantDataWithoutTenantName(GLOBAL_TENANT_INDEX,
            PRIVATE_USER_KIRK_INDEX,
            managementTenantIndex);
        context.setTenantIndices(tenantIndices);
        CheckIfIndicesAreBlockedStep step = new CheckIfIndicesAreBlockedStep(new StepRepository(getPrivilegedClient()));
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
        DoubleAliasIndex managementTenantIndex = DoubleAliasIndex.forTenant("management");
        createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, managementTenantIndex);
        var tenantIndices = doubleAliasIndexToTenantDataWithoutTenantName(managementTenantIndex,
            GLOBAL_TENANT_INDEX,
            PRIVATE_USER_KIRK_INDEX);
        context.setTenantIndices(tenantIndices);
        CheckIfIndicesAreBlockedStep step = new CheckIfIndicesAreBlockedStep(new StepRepository(getPrivilegedClient()));
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
        PopulateBackupIndicesStep step = new PopulateBackupIndicesStep(new StepRepository(getPrivilegedClient()));

        StepResult result = step.execute(context);

        log.info("Step response " + result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupIndices(), empty());
    }

    @Test
    public void shouldFindSingleBackupIndex() {
        PopulateBackupIndicesStep step = new PopulateBackupIndicesStep(new StepRepository(getPrivilegedClient()));
        BackupIndex backupIndex = new BackupIndex(NOW.toLocalDateTime());
        createBackupIndex(backupIndex);

        StepResult result = step.execute(context);

        log.info("Find backup indices result '{}'.", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupIndices(), hasSize(1));
        assertThat(context.getBackupIndices().get(0), equalTo(backupIndex.indexName()));
    }

    @Test
    public void shouldFindManyBackupIndices() {
        PopulateBackupIndicesStep step = new PopulateBackupIndicesStep(new StepRepository(getPrivilegedClient()));
        BackupIndex backupIndex1 = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndex2 = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        BackupIndex backupIndex3 = new BackupIndex(NOW.toLocalDateTime().minusDays(2));
        BackupIndex backupIndex4 = new BackupIndex(NOW.toLocalDateTime().minusDays(3));
        BackupIndex backupIndex5 = new BackupIndex(NOW.toLocalDateTime().minusDays(4));
        BackupIndex backupIndex6 = new BackupIndex(NOW.toLocalDateTime().minusDays(5));
        BackupIndex backupIndex7 = new BackupIndex(NOW.toLocalDateTime().minusDays(6));
        createBackupIndex(backupIndex1, backupIndex2, backupIndex3, backupIndex4, backupIndex5, backupIndex6, backupIndex7);

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
        createBackupIndex(backupIndices);
        PopulateBackupIndicesStep step = new PopulateBackupIndicesStep(new StepRepository(getPrivilegedClient()));

        StepResult result = step.execute(context);

        assertThat(result.isSuccess(), equalTo(true));
        assertThat(context.getBackupIndices(), hasSize(backupIndices.length));
    }

    @Test
    public void shouldDetectBackupIndexInYellowStateAndReportErrorWhenYellowIndicesAreForbidden() {
        BackupIndex backupIndex = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        createIndexInYellowState(backupIndex.indexName());
        createIndex(GLOBAL_TENANT_INDEX);
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
        createIndexInYellowState(backupIndex.indexName());
        createIndex(GLOBAL_TENANT_INDEX);
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
        createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX);
        assertThat(isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), "not important")));
        context.setBackupIndices(ImmutableList.empty());
        WriteBlockStep step = new WriteBlockStep(new StepRepository(getPrivilegedClient()));

        StepResult result = step.execute(context);

        log.debug("Write lock step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        // the below index is not present is migration context, therefore write block is not imposed
        assertThat(isDocumentInsertionPossible(PRIVATE_USER_KIRK_INDEX.indexName()), equalTo(true));
    }

    @Test
    public void shouldImposeWriteBlockOnDataAndBackupIndices() {
        createIndex(GLOBAL_TENANT_INDEX);
        BackupIndex backupIndex = new BackupIndex(NOW.toLocalDateTime());
        createBackupIndex(backupIndex);
        assertThat(isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));
        assertThat(isDocumentInsertionPossible(backupIndex.indexName()), equalTo(true));
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), "not important")));
        context.setBackupIndices(ImmutableList.of(backupIndex.indexName()));
        WriteBlockStep step = new WriteBlockStep(new StepRepository(getPrivilegedClient()));

        StepResult result = step.execute(context);

        log.debug("Write lock step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        assertThat(isDocumentInsertionPossible(backupIndex.indexName()), equalTo(false));
    }

    @Test
    public void shouldImposeWriteBlockOnMultipleIndices() {
        createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX);
        BackupIndex backupIndexOne = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndexTwo = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        createBackupIndex(backupIndexOne, backupIndexTwo);
        assertThat(isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));
        assertThat(isDocumentInsertionPossible(PRIVATE_USER_KIRK_INDEX.indexName()), equalTo(true));
        assertThat(isDocumentInsertionPossible(backupIndexOne.indexName()), equalTo(true));
        assertThat(isDocumentInsertionPossible(backupIndexTwo.indexName()), equalTo(true));
        context.setTenantIndices(doubleAliasIndexToTenantDataWithoutTenantName(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX));
        context.setBackupIndices(ImmutableList.of(backupIndexOne.indexName(), backupIndexTwo.indexName()));
        WriteBlockStep step = new WriteBlockStep(new StepRepository(getPrivilegedClient()));

        StepResult result = step.execute(context);

        log.debug("Write lock step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        assertThat(isDocumentInsertionPossible(PRIVATE_USER_KIRK_INDEX.indexName()), equalTo(false));
        assertThat(isDocumentInsertionPossible(backupIndexOne.indexName()), equalTo(false));
        assertThat(isDocumentInsertionPossible(backupIndexTwo.indexName()), equalTo(false));
    }

    @Test
    public void shouldReLockBackupIndex() {
        StepRepository repository = new StepRepository(getPrivilegedClient());
        createIndex(GLOBAL_TENANT_INDEX);
        BackupIndex backupIndex = new BackupIndex(NOW.toLocalDateTime());
        createBackupIndex(backupIndex);
        repository.writeBlockIndices(ImmutableList.of(backupIndex.indexName()));
        assertThat(isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));
        assertThat(isDocumentInsertionPossible(backupIndex.indexName()), equalTo(false));
        context.setTenantIndices(ImmutableList.of(new TenantIndex(GLOBAL_TENANT_INDEX.indexName(), "not important")));
        context.setBackupIndices(ImmutableList.of(backupIndex.indexName()));
        WriteBlockStep step = new WriteBlockStep(repository);

        StepResult result = step.execute(context);

        log.debug("Write lock step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        assertThat(isDocumentInsertionPossible(backupIndex.indexName()), equalTo(false));
    }

    @Test
    public void shouldReleaseWriteBlockOnOneDataIndexWhenStepIsRollback() {
        StepRepository repository = new StepRepository(getPrivilegedClient());
        WriteBlockStep step = new WriteBlockStep(repository);
        createIndex(GLOBAL_TENANT_INDEX);
        repository.writeBlockIndices(ImmutableList.of(GLOBAL_TENANT_INDEX.indexName()));

        assertThat(isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        context.setTenantIndices(doubleAliasIndexToTenantDataWithoutTenantName(GLOBAL_TENANT_INDEX));


        StepResult result = step.rollback(context);

        log.debug("Write lock step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));

    }

    @Test
    public void shouldReleaseWriteBlockOnMultipleIndicesWhenStepIsRollback() {
        StepRepository repository = new StepRepository(getPrivilegedClient());
        WriteBlockStep step = new WriteBlockStep(repository);
        createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX);
        BackupIndex backupIndexOne = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndexTwo = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        createBackupIndex(backupIndexOne, backupIndexTwo);
        ImmutableList<String> indicesToBlock = ImmutableList.<DeletableIndex>of(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, backupIndexOne, backupIndexTwo)
            .map(DeletableIndex::indexForDeletion);
        repository.writeBlockIndices(indicesToBlock);

        assertThat(isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        assertThat(isDocumentInsertionPossible(PRIVATE_USER_KIRK_INDEX.indexName()), equalTo(false));
        assertThat(isDocumentInsertionPossible(backupIndexOne.indexName()), equalTo(false));
        assertThat(isDocumentInsertionPossible(backupIndexTwo.indexName()), equalTo(false));
        context.setTenantIndices(doubleAliasIndexToTenantDataWithoutTenantName(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX));
        context.setBackupIndices(ImmutableList.of(backupIndexOne.indexName(), backupIndexTwo.indexName()));

        StepResult result = step.rollback(context);

        log.debug("Write lock step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));
        assertThat(isDocumentInsertionPossible(PRIVATE_USER_KIRK_INDEX.indexName()), equalTo(true));
        assertThat(isDocumentInsertionPossible(backupIndexOne.indexName()), equalTo(false));
        assertThat(isDocumentInsertionPossible(backupIndexTwo.indexName()), equalTo(false));
    }

    @Test
    public void shouldReleaseWriteBlockOnOneDataIndex() {
        StepRepository repository = new StepRepository(getPrivilegedClient());
        UnblockDataIndicesStep step = new UnblockDataIndicesStep(repository);
        createIndex(GLOBAL_TENANT_INDEX);
        repository.writeBlockIndices(ImmutableList.of(GLOBAL_TENANT_INDEX.indexName()));

        assertThat(isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        context.setTenantIndices(doubleAliasIndexToTenantDataWithoutTenantName(GLOBAL_TENANT_INDEX));


        StepResult result = step.execute(context);

        log.debug("Write lock step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));

    }

    @Test
    public void shouldReleaseWriteBlockOnMultipleIndices() {
        StepRepository repository = new StepRepository(getPrivilegedClient());
        UnblockDataIndicesStep step = new UnblockDataIndicesStep(repository);
        createIndex(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX);
        BackupIndex backupIndexOne = new BackupIndex(NOW.toLocalDateTime());
        BackupIndex backupIndexTwo = new BackupIndex(NOW.toLocalDateTime().minusDays(1));
        createBackupIndex(backupIndexOne, backupIndexTwo);
        ImmutableList<String> indicesToBlock = ImmutableList.<DeletableIndex>of(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX, backupIndexOne, backupIndexTwo)
                .map(DeletableIndex::indexForDeletion);
        repository.writeBlockIndices(indicesToBlock);

        assertThat(isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(false));
        assertThat(isDocumentInsertionPossible(PRIVATE_USER_KIRK_INDEX.indexName()), equalTo(false));
        assertThat(isDocumentInsertionPossible(backupIndexOne.indexName()), equalTo(false));
        assertThat(isDocumentInsertionPossible(backupIndexTwo.indexName()), equalTo(false));
        context.setTenantIndices(doubleAliasIndexToTenantDataWithoutTenantName(GLOBAL_TENANT_INDEX, PRIVATE_USER_KIRK_INDEX));
        context.setBackupIndices(ImmutableList.of(backupIndexOne.indexName(), backupIndexTwo.indexName()));

        StepResult result = step.execute(context);

        log.debug("Write lock step result '{}'", result);
        assertThat(result.isSuccess(), equalTo(true));
        assertThat(isDocumentInsertionPossible(GLOBAL_TENANT_INDEX.indexName()), equalTo(true));
        assertThat(isDocumentInsertionPossible(PRIVATE_USER_KIRK_INDEX.indexName()), equalTo(true));
        assertThat(isDocumentInsertionPossible(backupIndexOne.indexName()), equalTo(false));
        assertThat(isDocumentInsertionPossible(backupIndexTwo.indexName()), equalTo(false));
    }

    private boolean isDocumentInsertionPossible(String indexName) {
        try(Client client = cluster.getInternalNodeClient()) {
            IndexRequest request = new IndexRequest(indexName).source(ImmutableMap.of("new", "document"));
            client.index(request).actionGet();
            return true;
        } catch (ClusterBlockException clusterBlockException) {
            return false;
        }
    }

    private void createIndexInYellowState(String index) {
        try(Client client = cluster.getInternalNodeClient()) {
            CreateIndexRequest request = new CreateIndexRequest(index);
            Settings.Builder settings = Settings.builder() //
                .put("index.number_of_replicas", 100); // force index yellow state
            request.settings(settings);
            CreateIndexResponse createIndexResponse = client.admin().indices().create(request).actionGet();
            assertThat(createIndexResponse.isAcknowledged(), equalTo(true));
            this.createdIndices.add(() -> index);
        }
    }

    private static CheckIndicesStateStep createCheckIndicesStateStep() {
        PrivilegedConfigClient client = getPrivilegedClient();
        return new CheckIndicesStateStep(new StepRepository(client));
    }

    public boolean isIndexCreated(String...indexOrAlias) {
        try {
            try (Client client = cluster.getInternalNodeClient()) {
                for(String index : indexOrAlias) {
                    client.admin().indices().getIndex(new GetIndexRequest().indices(index)).actionGet();
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private List<DoubleAliasIndex> getIndicesForConfiguredTenantsWithoutGlobal() {
        FeMultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(FeMultiTenancyConfigurationProvider.class);
        return configurationProvider.getTenantNames() //
            .stream() //
            .filter(name -> !Tenant.GLOBAL_TENANT_ID.equals(name)) //
            .map(DoubleAliasIndex::forTenant) //
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
        FeMultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(FeMultiTenancyConfigurationProvider.class);
        PrivilegedConfigClient privilegedConfigClient = getPrivilegedClient();
        return new PopulateTenantsStep(configurationProvider, new StepRepository(privilegedConfigClient));
    }

    private static PrivilegedConfigClient getPrivilegedClient() {
        Client client = cluster.getInternalNodeClient();
        return adapt(client);
    }

    private interface DeletableIndex {
        String indexForDeletion();
    }

    private record DoubleAliasIndex(String indexName, String shortAlias, String longAlias) implements DeletableIndex {
        public static final String LAST_VERSION_BEFORE_MIGRATION = "8.7.0";
        public static final String LEGACY_VERSION = "7.17.12";

        DoubleAliasIndex {
            requireNonEmpty(indexName, "Index name is required");
            requireNonEmpty(shortAlias, "Short alias name is required");
            requireNonEmpty(longAlias, "Long alias name is required");
        }

        public static DoubleAliasIndex forTenantWithPrefix(String indexNamePrefix, String tenantName) {
            String baseAndShortAlias = tenantNameToIndexName(indexNamePrefix, tenantName);
            String fullIndexName = createIndexName(baseAndShortAlias, LAST_VERSION_BEFORE_MIGRATION);
            String aliasWithVersionAndSeqNo = createLongAlias(baseAndShortAlias, LAST_VERSION_BEFORE_MIGRATION);
            return new DoubleAliasIndex(fullIndexName, baseAndShortAlias, aliasWithVersionAndSeqNo);
        }

        private static String createLongAlias(String baseIndexName, String version) {
            return baseIndexName + "_" + version;
        }

        private static String createIndexName(String baseIndexName, String version) {
            return createLongAlias(baseIndexName, version) +"_001";
        }

        public static DoubleAliasIndex forTenant(String tenantName) {
            return forTenantWithPrefix(getConfiguredIndexPrefix(), tenantName);
        }

        public String getIndexNameInVersion(String version) {
            return createIndexName(shortAlias, version);
        }

        public String getLongAliasInVersion(String version) {
            return createLongAlias(shortAlias, version);
        }

        public LegacyIndex toLegacyIndex(String version) {
            return new LegacyIndex(getIndexNameInVersion(version), getLongAliasInVersion(version));
        }

        @Override
        public String indexForDeletion() {
            return indexName;
        }
    }

    private record LegacyIndex(String indexName, String longAlias) implements DeletableIndex {
        LegacyIndex {
            requireNonEmpty(indexName, "Index name is required");
            requireNonEmpty(longAlias, "Long alias is required");
        }

        @Override
        public String indexForDeletion() {
            return indexName;
        }
    }

    private record BackupIndex(String indexName) implements DeletableIndex {

        public BackupIndex(LocalDateTime backupIndexCreationTime) {
            this("backup_fe_migration_to_8_8_0_" + IndexNameDataFormatter.format(backupIndexCreationTime));
        }

        @Override
        public String indexForDeletion() {
            return indexName;
        }
    }

    public static String getConfiguredIndexPrefix() {
        FeMultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(FeMultiTenancyConfigurationProvider.class);
        return configurationProvider.getConfig().orElseThrow().getIndex();
    }



    private static ImmutableList<TenantIndex> doubleAliasIndexToTenantDataWithoutTenantName(ImmutableList<DoubleAliasIndex> indices) {
        return indices.map(i -> new TenantIndex(i.indexName(), "tenant name id not important here"));
    }

    private static ImmutableList<TenantIndex> doubleAliasIndexToTenantDataWithoutTenantName(DoubleAliasIndex...indices) {
        return doubleAliasIndexToTenantDataWithoutTenantName(ImmutableList.ofArray(indices));
    }
}