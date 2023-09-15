package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfig;
import com.floragunn.searchguard.enterprise.femt.MultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.TenantIndex;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
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
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.CANNOT_RESOLVE_INDEX_BY_ALIAS;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.GLOBAL_TENANT_NOT_FOUND_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.INDICES_NOT_FOUND_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.MULTI_TENANCY_CONFIG_NOT_AVAILABLE_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.MULTI_TENANCY_DISABLED_ERROR;
import static com.floragunn.searchguard.support.PrivilegedConfigClient.adapt;
import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_OK;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.Strings.requireNonEmpty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MigrationStepsTest {

    private static final Logger log = LogManager.getLogger(MigrationStepsTest.class);

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
        .singleNode()
        .sslEnabled()
        .resources("multitenancy")
        .enterpriseModulesEnabled()
        .build();

    private final Clock clock = Clock.fixed(NOW.toInstant(), ZoneOffset.UTC);
    private DataMigrationContext context;

    @Mock
    private MultiTenancyConfigurationProvider multiTenancyConfigurationProvider;

    @Mock
    private FeMultiTenancyConfig feMultiTenancyConfig;

    private final List<DeletableIndex> createdIndices = new ArrayList<>();

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
        BulkRequest bulkRequest = new BulkRequest();
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        PutIndexTemplateRequest templateRequest = new PutIndexTemplateRequest("kibana_test_indices_template") //
            .patterns(Collections.singletonList(".kibana*")) //
            .settings(Settings.builder().put("index.hidden", true));
        cluster.getInternalNodeClient()//
            .admin() //
            .indices() //
            .putTemplate(templateRequest) //
            .actionGet();
        for(DoubleAliasIndex index : indices) {
            bulkRequest.add(new IndexRequest(index.indexName()).source(DocNode.EMPTY));
            createdIndices.add(index);
            indicesAliasesRequest.addAliasAction(AliasActions.add().index(index.indexName).aliases(index.longAlias, index.shortAlias));
        }
        cluster.getInternalNodeClient().bulk(bulkRequest.setRefreshPolicy(IMMEDIATE)).actionGet();
        cluster.getInternalNodeClient().admin().indices().aliases(indicesAliasesRequest).actionGet();
    }

    private void createLegacyIndex(LegacyIndex...indices) {
        BulkRequest bulkRequest = new BulkRequest();
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        PutIndexTemplateRequest templateRequest = new PutIndexTemplateRequest("kibana_test_indices_template") //
            .patterns(Collections.singletonList(".kibana*")) //
            .settings(Settings.builder().put("index.hidden", true));
        cluster.getInternalNodeClient()//
            .admin() //
            .indices() //
            .putTemplate(templateRequest) //
            .actionGet();
        for(LegacyIndex index : indices) {
            bulkRequest.add(new IndexRequest(index.indexName()).source(DocNode.EMPTY));
            createdIndices.add(index);
            indicesAliasesRequest.addAliasAction(AliasActions.add().index(index.indexName).aliases(index.longAlias));
        }
        cluster.getInternalNodeClient().bulk(bulkRequest.setRefreshPolicy(IMMEDIATE)).actionGet();
        cluster.getInternalNodeClient().admin().indices().aliases(indicesAliasesRequest).actionGet();
    }

    private void deleteIndex(DeletableIndex doubleAliasIndex) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(doubleAliasIndex.indexForDeletion());
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

        ImmutableList<TenantIndex> tenants = context.getTenants();
        assertThat(tenants, hasSize(1));
        assertThat(tenants.get(0).isGlobal(), equalTo(true));
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
        assertThat(context.getTenants(), hasSize(configuredTenantIndices.size() + 1));
        assertThat(context.getTenants().size(), greaterThan(20));
        List<String> tenantsFoundByStep = context.getTenants().stream().map(TenantIndex::tenantName).collect(Collectors.toList());
        MultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(MultiTenancyConfigurationProvider.class);
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
        assertThat(context.getTenants(), hasSize(5));
        Set<String> privateTenantIndexNames = context.getTenants() //
            .stream() //
            .filter(TenantIndex::isUserPrivateTenant) //
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
        indices.addAll(generatePrivateTenantNames(".kibana", 101));
        createIndex(indices.toArray(DoubleAliasIndex[]::new));
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
        TenantIndex tenantIndex = context.getTenants().stream().filter(TenantIndex::isUserPrivateTenant).findAny().orElseThrow();
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
                assertThat(context.getTenants(), hasSize(2));
                TenantIndex createdTenant = context.getTenants().stream().filter(tenantIndex -> !tenantIndex.isGlobal()).findAny().orElseThrow();
                assertThat(createdTenant.tenantName(), equalTo(newTenantName));
                assertThat(createdTenant.isUserPrivateTenant(), equalTo(false));
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
        assertThat(context.getTenants(), hasSize(6));
        assertThat(context.getTenants().map(TenantIndex::indexName), containsInAnyOrder(".kibana_8.7.0_001",
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
        assertThat(context.getTenants(), hasSize(3));
        String global = context.getTenants().stream().filter(TenantIndex::isGlobal).map(TenantIndex::tenantName).findFirst().orElseThrow();
        String userPrivateIndex = context.getTenants() //
            .stream() //
            .filter(TenantIndex::isUserPrivateTenant) //
            .map(TenantIndex::indexName) //
            .findFirst() //
            .orElseThrow();
        String configured = context.getTenants().stream() //
            .filter(tenant -> !tenant.isGlobal()) //
            .filter(tenant -> !tenant.isUserPrivateTenant()) //
            .map(TenantIndex::tenantName) //
            .findFirst() //
            .orElseThrow();
        assertThat(global, equalTo(Tenant.GLOBAL_TENANT_ID));
        assertThat(configured, equalTo("admin_tenant"));
        assertThat(userPrivateIndex, equalTo(".kibana_3292183_kirk_8.7.0_001"));
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
        MultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(MultiTenancyConfigurationProvider.class);
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
        MultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(MultiTenancyConfigurationProvider.class);
        Client client = cluster.getInternalNodeClient();
        return new PopulateTenantsStep(configurationProvider, adapt(client));
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

    public static String getConfiguredIndexPrefix() {
        MultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(MultiTenancyConfigurationProvider.class);
        return configurationProvider.getConfig().orElseThrow().getIndex();
    }
}