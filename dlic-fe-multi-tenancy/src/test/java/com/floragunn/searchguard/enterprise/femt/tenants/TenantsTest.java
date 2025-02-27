/*
 * Copyright 2023-2024 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.enterprise.femt.tenants;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.authz.TenantManager;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfig;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfigurationProvider;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.FrontendMultiTenancy;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.RoleMapping;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchsupport.StaticSettings;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsFieldPointedByJsonPath;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsNullValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsOnlyFields;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static org.apache.http.HttpStatus.SC_UNAUTHORIZED;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.rest.RestStatus.CREATED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class TenantsTest {

    private static final Logger log = LogManager.getLogger(TenantsTest.class);

    private static final String FRONTEND_INDEX = ".kibana";
    private static final TestSgConfig.Tenant HR_TENANT = new TestSgConfig.Tenant("hr_tenant");
    private static final TestSgConfig.Tenant FINANCE_TENANT = new TestSgConfig.Tenant("finance_tenant");
    private static final TestSgConfig.Tenant SALES_TENANT = new TestSgConfig.Tenant("sales_tenant");
    private static final TestSgConfig.Tenant OPERATIONS_TENANT = new TestSgConfig.Tenant("operations_tenant");
    private static final TestSgConfig.Tenant RD_TENANT = new TestSgConfig.Tenant("r&d_tenant");
    private static final TestSgConfig.Tenant BD_TENANT = new TestSgConfig.Tenant("business_development_tenant");
    private static final TestSgConfig.Tenant LEGAL_TENANT = new TestSgConfig.Tenant("legal_tenant");
    private static final TestSgConfig.Tenant IT_TENANT = new TestSgConfig.Tenant("information_technology_tenant");
    private static final TestSgConfig.Tenant PR_TENANT = new TestSgConfig.Tenant("public_relations_tenant");
    private static final TestSgConfig.Tenant QA_TENANT = new TestSgConfig.Tenant("quality_assurance_tenant");

    private MultitenancyActivationService multitenancyActivationService;

    private static final ImmutableList<TestSgConfig.Tenant> ALL_DEFINED_TENANTS = ImmutableList.of(HR_TENANT, FINANCE_TENANT,
        SALES_TENANT, OPERATIONS_TENANT, RD_TENANT, BD_TENANT, LEGAL_TENANT, IT_TENANT, PR_TENANT, QA_TENANT);
    private static final User USER_SINGLE_TENANT = new User("user_single_tenant") //
        .roles(new Role("single_tenant_access") //
            .withTenantPermission("*") //
            .on(HR_TENANT.getName()) //
            .indexPermissions("*") //
            .on(FRONTEND_INDEX +"*"));

    private static final User FRONTEND_SERVER_USER = new User("kibana_server");

    private static final User USER_EACH_TENANT_READ = new User("user_each_tenant_read") //
        .roles(new Role("each_tenant_read_access") //
            .withTenantPermission("SGS_KIBANA_ALL_READ") //
            .on(ALL_DEFINED_TENANTS.map(TestSgConfig.Tenant::getName).with(Tenant.GLOBAL_TENANT_ID).toArray(String[]::new)) //
            .indexPermissions("*") //
            .on(FRONTEND_INDEX +"*"));
    private static final User USER_EACH_TENANT_WRITE = new User("user_each_tenant_write") //
        .roles(new Role("each_tenant_write_access") //
            .withTenantPermission("SGS_KIBANA_ALL_WRITE") //
            .on(ALL_DEFINED_TENANTS.map(TestSgConfig.Tenant::getName).with(Tenant.GLOBAL_TENANT_ID).toArray(String[]::new)) //
            .indexPermissions("*") //
            .on(FRONTEND_INDEX +"*"));

    private static final User USER_SOME_TENANT_ACCESS = new User("user_some_tenant_access") //
        .roles(new Role("some_tenant_access") //
            .withTenantPermission("SGS_KIBANA_ALL_WRITE") //
            .on(HR_TENANT.getName(), FINANCE_TENANT.getName(), SALES_TENANT.getName()) //
            .withTenantPermission("SGS_KIBANA_ALL_READ").on(IT_TENANT.getName(), PR_TENANT.getName(), QA_TENANT.getName()) //
            .indexPermissions("*") //
            .on(FRONTEND_INDEX +"*"));

    private static final User USER_WITH_ACCESS_ONLY_TO_GLOBAL_TENANT = new User("user_with_access_only_to_global_tenant") //
            .roles(new Role("global_tenant_access") //
                    .clusterPermissions("cluster:admin:searchguard:femt:user/available_tenants/get")
                    .withTenantPermission("*")
                    .on(Tenant.GLOBAL_TENANT_ID));

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled() //
        .nodeSettings("action.destructive_requires_name", false, "searchguard.unsupported.single_index_mt_enabled", true) //
        .enterpriseModulesEnabled() //
        .roleMapping(new RoleMapping("SGS_KIBANA_USER").users(USER_SINGLE_TENANT.getName(), USER_EACH_TENANT_READ.getName(),
            USER_EACH_TENANT_WRITE.getName(), USER_SOME_TENANT_ACCESS.getName()),//
            new RoleMapping("SGS_KIBANA_SERVER").users(FRONTEND_SERVER_USER.getName())) //
        .users(FRONTEND_SERVER_USER, USER_SINGLE_TENANT, USER_EACH_TENANT_READ, USER_EACH_TENANT_WRITE, USER_SOME_TENANT_ACCESS, USER_WITH_ACCESS_ONLY_TO_GLOBAL_TENANT) //
        .frontendMultiTenancy(new FrontendMultiTenancy(true).index(FRONTEND_INDEX).serverUser(FRONTEND_SERVER_USER.getName())) //
        .tenants(HR_TENANT, FINANCE_TENANT, SALES_TENANT, OPERATIONS_TENANT, RD_TENANT, BD_TENANT, LEGAL_TENANT,
            IT_TENANT, PR_TENANT, QA_TENANT) //
            .embedded().build();

    @Before
    public void setUpIndex() {
        Client client = cluster.getInternalNodeClient();
        DocNode indexMappings = DocNode.of("_doc", DocNode.of("properties", DocNode.of("sg_tenant", DocNode.of("type", "keyword"))));
        CreateIndexRequest request = new CreateIndexRequest(FRONTEND_INDEX) //
            .settings(Settings.builder().put("index.hidden", true)) //
            .mapping(indexMappings);
        CreateIndexResponse response = client.admin().indices().create(request).actionGet();
        assertThat(response.isAcknowledged(), equalTo(true));
    }

    @Before
    public void setUpTestedDependencies() {
        TenantRepository tenantRepository = new TenantRepository(PrivilegedConfigClient.adapt(cluster.getInjectable(NodeClient.class)));
        ConfigurationRepository configurationRepository = cluster.getInjectable(ConfigurationRepository.class);
        FeMultiTenancyConfigurationProvider configurationProvider = cluster.getInjectable(FeMultiTenancyConfigurationProvider.class);
        multitenancyActivationService = new MultitenancyActivationService(tenantRepository, configurationRepository, configurationProvider);
    }

    @After
    public void clearIndices() {
        removeKibanaRelatedIndices();
    }

    @Test
    public void getAvailableTenantsAction_shouldFindAccessibleTenantsForSingleTenantUser() throws Exception {
        createTenants(FRONTEND_INDEX, ALL_DEFINED_TENANTS.map(TestSgConfig.Tenant::getName).toArray(String[]::new));
        Header tenantHeader = new BasicHeader("sg_tenant", "test_tenant");
        try(GenericRestClient client = cluster.getRestClient(USER_SINGLE_TENANT, tenantHeader)) {

            HttpResponse response = client.get("/_searchguard/current_user/tenants");

            log.debug("Response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.data.default_tenant", Tenant.GLOBAL_TENANT_ID));
            assertThat(body, containsValue("$.data.username", USER_SINGLE_TENANT.getName()));
            assertThat(body, containsValue("$.data.user_requested_tenant", tenantHeader.getValue()));
            assertThat(body, containsValue("$.data.multi_tenancy_enabled", true));
            assertThat(body, containsValue("$.data.tenants.hr_tenant.read_access", true));
            assertThat(body, containsValue("$.data.tenants.hr_tenant.write_access", true));
            assertThat(body, containsValue("$.data.tenants.hr_tenant.exists", true));
            assertThat(body, not(containsFieldPointedByJsonPath("$.data.tenants.user_single_tenant", "read_access")));
            assertThat(body, not(containsFieldPointedByJsonPath("$.data.tenants.user_single_tenant", "write_access")));
            assertThat(body, not(containsFieldPointedByJsonPath("$.data.tenants.user_single_tenant", "exists")));
            // user with role SGS_KIBANA_USER has write access to global tenant
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.read_access", true));
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.write_access", true));
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.exists", false));


            assertThat(body, containsOnlyFields("$.data.tenants", "hr_tenant", "SGS_GLOBAL_TENANT"));
        }
    }

    @Test
    public void getAvailableTenantsAction_shouldFindAccessibleTenantsForUserAllowedToReadEachTenantData() throws Exception {
        String[] tenantsToBeCreated = ALL_DEFINED_TENANTS.map(TestSgConfig.Tenant::getName) //
            .with(USER_EACH_TENANT_READ.getName()) //
            .toArray(String[]::new);
        createTenants(FRONTEND_INDEX, tenantsToBeCreated);
        try(GenericRestClient client = cluster.getRestClient(USER_EACH_TENANT_READ)) {

            HttpResponse response = client.get("/_searchguard/current_user/tenants");

            log.debug("Response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsOnlyFields("$.data.tenants","business_development_tenant",
                "sales_tenant", "quality_assurance_tenant", "finance_tenant", "SGS_GLOBAL_TENANT", "operations_tenant",
                "public_relations_tenant", "hr_tenant", "legal_tenant", "r&d_tenant", "information_technology_tenant"));
            assertThat(body, containsValue("$.data.default_tenant", Tenant.GLOBAL_TENANT_ID));
            assertThat(body, containsValue("$.data.username", USER_EACH_TENANT_READ.getName()));
            assertThat(body, containsNullValue("$.data.user_requested_tenant"));
            for(String tenantName : ALL_DEFINED_TENANTS.map(TestSgConfig.Tenant::getName)) {
                String readAccessPath = "$.data.tenants." + tenantName + ".read_access";
                String writeAccessPath = "$.data.tenants." + tenantName + ".write_access";
                String existPath = "$.data.tenants." + tenantName + ".exists";
                assertThat(body, containsValue(readAccessPath, true));
                assertThat(body, containsValue(writeAccessPath, false));
                assertThat(body, containsValue(existPath, true));
            }
            //user should not have access to its private tenant (private tenants are disabled)
            assertThat(body, not(containsFieldPointedByJsonPath("$.data.tenants.user_each_tenant_read", "write_access")));
            assertThat(body, not(containsFieldPointedByJsonPath("$.data.tenants.user_each_tenant_read", "exists")));
            // user with role SGS_KIBANA_USER has write access to global tenant
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.read_access", true));
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.write_access", true));
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.exists", false));
        }
    }

    @Test
    public void getAvailableTenantsAction_shouldFindAccessibleTenantsForUserAllowedToWriteEachTenantData() throws Exception {
        String[] tenantsToBeCreated = ALL_DEFINED_TENANTS.map(TestSgConfig.Tenant::getName) //
            .with(USER_EACH_TENANT_WRITE.getName()) //
            .toArray(String[]::new);
        createTenants(FRONTEND_INDEX, tenantsToBeCreated);
        List<String> tenantsWithoutPrivate = Arrays.stream(tenantsToBeCreated)
                .filter(tenant -> !tenant.equals(USER_EACH_TENANT_WRITE.getName()))
                .toList();
        try(GenericRestClient client = cluster.getRestClient(USER_EACH_TENANT_WRITE)) {

            HttpResponse response = client.get("/_searchguard/current_user/tenants");

            log.debug("Response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsOnlyFields("$.data.tenants", "business_development_tenant", "information_technology_tenant", "sales_tenant", "quality_assurance_tenant", "finance_tenant", "SGS_GLOBAL_TENANT", "operations_tenant", "public_relations_tenant", "hr_tenant", "legal_tenant", "r&d_tenant"));
            assertThat(body, containsValue("$.data.default_tenant", Tenant.GLOBAL_TENANT_ID));
            assertThat(body, containsValue("$.data.username", USER_EACH_TENANT_WRITE.getName()));
            assertThat(body, containsNullValue("$.data.user_requested_tenant"));
            for(String tenantName : tenantsWithoutPrivate) {
                String readAccessPath = "$.data.tenants." + tenantName + ".read_access";
                String writeAccessPath = "$.data.tenants." + tenantName + ".write_access";
                String existPath = "$.data.tenants." + tenantName + ".exists";
                assertThat(body, containsValue(readAccessPath, true));
                assertThat(body, containsValue(writeAccessPath, true));
                assertThat(body, containsValue(existPath, true));
            }
        }
    }

    @Test
    public void getAvailableTenantsAction_shouldFindAccessibleTenantsWhichDoesNotExist() throws Exception {
        String[] accessibleTenantsNames = ALL_DEFINED_TENANTS.map(TestSgConfig.Tenant::getName) //
            .toArray(String[]::new);
        try(GenericRestClient client = cluster.getRestClient(USER_EACH_TENANT_WRITE)) {

            HttpResponse response = client.get("/_searchguard/current_user/tenants");

            log.debug("Response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsOnlyFields("$.data.tenants", "business_development_tenant",
                "information_technology_tenant", "sales_tenant", "quality_assurance_tenant", "finance_tenant", "SGS_GLOBAL_TENANT",
                "operations_tenant", "public_relations_tenant", "hr_tenant", "legal_tenant", "r&d_tenant"));
            assertThat(body, containsValue("$.data.default_tenant", Tenant.GLOBAL_TENANT_ID));
            assertThat(body, containsValue("$.data.username", USER_EACH_TENANT_WRITE.getName()));
            assertThat(body, containsNullValue("$.data.user_requested_tenant"));
            assertThat(body, not(containsFieldPointedByJsonPath("$.data.tenants.user_each_tenant_write", "read_access")));
            assertThat(body, not(containsFieldPointedByJsonPath("$.data.tenants.user_each_tenant_write", "write_access")));
            assertThat(body, not(containsFieldPointedByJsonPath("$.data.tenants.user_each_tenant_write", "exists")));
            for(String tenantName : accessibleTenantsNames) {
                String readAccessPath = "$.data.tenants." + tenantName + ".read_access";
                String writeAccessPath = "$.data.tenants." + tenantName + ".write_access";
                String existPath = "$.data.tenants." + tenantName + ".exists";
                assertThat(body, containsValue(readAccessPath, true));
                assertThat(body, containsValue(writeAccessPath, true));
                assertThat(body, containsValue(existPath, false));
            }
        }
    }

    @Test
    public void getAvailableTenantsAction_shouldReturnInformationIfTenantIsAccessibleAndExist() throws Exception {
        createTenants(FRONTEND_INDEX, HR_TENANT.getName(), FINANCE_TENANT.getName(), PR_TENANT.getName(), QA_TENANT.getName());
        try(GenericRestClient client = cluster.getRestClient(USER_SOME_TENANT_ACCESS)) {

            HttpResponse response = client.get("/_searchguard/current_user/tenants");

            log.debug("Response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsOnlyFields("$.data.tenants", "sales_tenant", "quality_assurance_tenant",
                "finance_tenant", "SGS_GLOBAL_TENANT", "public_relations_tenant", "hr_tenant"));
            assertThat(body, containsValue("$.data.default_tenant", Tenant.GLOBAL_TENANT_ID));
            assertThat(body, containsValue("$.data.username", USER_SOME_TENANT_ACCESS.getName()));
            assertThat(body, containsNullValue("$.data.user_requested_tenant"));
            // read only, existing tenants PR_TENANT, QA_TENANT
            assertThat(body, containsValue("$.data.tenants.public_relations_tenant.write_access", false));
            assertThat(body, containsValue("$.data.tenants.public_relations_tenant.exists", true));
            assertThat(body, containsValue("$.data.tenants.quality_assurance_tenant.write_access", false));
            assertThat(body, containsValue("$.data.tenants.quality_assurance_tenant.exists", true));
            //tenants with write access HR_TENANT, FINANCE_TENANT, SALES_TENANT and user private tenant
            assertThat(body, containsValue("$.data.tenants.hr_tenant.write_access", true));
            assertThat(body, containsValue("$.data.tenants.hr_tenant.exists", true));
            assertThat(body, containsValue("$.data.tenants.finance_tenant.write_access", true));
            assertThat(body, containsValue("$.data.tenants.finance_tenant.exists", true));
            assertThat(body, containsValue("$.data.tenants.sales_tenant.write_access", true));
            assertThat(body, containsValue("$.data.tenants.sales_tenant.exists", false));
            assertThat(body, not(containsFieldPointedByJsonPath("$.data.tenants.user_some_tenant_access", "write_access")));
            assertThat(body, not(containsFieldPointedByJsonPath("$.data.tenants.user_some_tenant_access", "exists")));
            // user with role SGS_KIBANA_USER has write access to global tenant
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.read_access", true));
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.write_access", true));
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.exists", false));
        }
    }

    @Test
    public void getAvailableTenantsAction_shouldNotBeAccessibleForFrontendServerUser() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(FRONTEND_SERVER_USER)) {

            HttpResponse response = client.get("/_searchguard/current_user/tenants");

            log.debug("Response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response, isForbidden());
        }
    }

    @Test
    public void getAvailableTenantsAction_shouldReturnUnauthorized_whenUserDoesNotHaveAccessToAnyTenantAndDefaultTenantCannotBeDetermined() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_WITH_ACCESS_ONLY_TO_GLOBAL_TENANT); GenericRestClient adminClient = cluster.getAdminCertRestClient()) {

            HttpResponse response = client.get("/_searchguard/current_user/tenants");
            assertThat(response, isOk());
            assertThat(response.getBodyAsDocNode(), containsValue("$.data.default_tenant", Tenant.GLOBAL_TENANT_ID));

            cluster.callAndRestoreConfig(FeMultiTenancyConfig.TYPE, () -> {

                HttpResponse getConfigResponse = adminClient.get("/_searchguard/config/fe_multi_tenancy");
                assertThat(getConfigResponse.getBody(), getConfigResponse, isOk());
                DocNode config = getConfigResponse.getBodyAsDocNode().getAsNode("content");
                config = config.with("global_tenant_enabled", false);
                HttpResponse putConfigResponse = adminClient.putJson("/_searchguard/config/fe_multi_tenancy", config);
                assertThat(putConfigResponse.getBody(), putConfigResponse, isOk());

                HttpResponse getTenantsResponse = client.get("/_searchguard/current_user/tenants");
                log.debug("Response status '{}' and body '{}'.", getTenantsResponse.getStatusCode(), getTenantsResponse.getBody());
                assertThat(getTenantsResponse.getBody(), getTenantsResponse.getStatusCode(), equalTo(SC_UNAUTHORIZED));
                assertThat(getTenantsResponse.getBodyAsDocNode(), containsValue("$.message", "Cannot determine default tenant for current user"));


                return null;
            });
        }
    }

    @Test
    public void shouldDetectThatGlobalTenantExist() throws Exception {
        createTenants(FRONTEND_INDEX, Tenant.GLOBAL_TENANT_ID);
        Header tenantHeader = new BasicHeader("sg_tenant", "test_tenant");
        try (GenericRestClient client = cluster.getRestClient(USER_SINGLE_TENANT, tenantHeader)) {

            HttpResponse response = client.get("/_searchguard/current_user/tenants");

            log.debug("Response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
            DocNode body = response.getBodyAsDocNode();
            // user with role SGS_KIBANA_USER has write access to global tenant
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.read_access", true));
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.write_access", true));
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.exists", true));
        }
    }

    @Test
    public void tenantIndexMappingsExtender_shouldAddSgTenantFieldToMappings() {
        //there are no kibana indices
        removeKibanaRelatedIndices();

        multitenancyActivationService.activate();

        GetMappingsResponse getMappingsResponse = getKibanaIndicesMappings();
        assertThat(getMappingsResponse.getMappings(), anEmptyMap());

        //there are kibana indices
        createKibanaIndicesAndAliases();

        multitenancyActivationService.activate();
        getMappingsResponse = getKibanaIndicesMappings();
        assertThat(
                getMappingsResponse.getMappings().keySet(),
                containsInAnyOrder(".kibana_1.2.3", ".kibana_analytics_1.2.3", ".kibana_ingest_1.2.3",
                        ".kibana_security_solution_1.2.3", ".kibana_alerting_cases_1.2.3"
                )
        );
        for (MappingMetadata indexMapping : getMappingsResponse.getMappings().values()) {
            DocNode mappings = DocNode.wrap(indexMapping.sourceAsMap());
            assertThat(mappings, hasKey("properties"));
            assertThat(mappings.getAsNode("properties"), hasKey("sg_tenant"));
            assertThat(mappings.getAsNode("properties").getAsNode("sg_tenant"), equalTo(DocNode.of("type", "keyword")));
        }

        //mappings already extended
        multitenancyActivationService.activate();

        getMappingsResponse = getKibanaIndicesMappings();
        assertThat(
                getMappingsResponse.getMappings().keySet(),
                containsInAnyOrder(".kibana_1.2.3", ".kibana_analytics_1.2.3", ".kibana_ingest_1.2.3",
                        ".kibana_security_solution_1.2.3", ".kibana_alerting_cases_1.2.3"
                )
        );
        for (MappingMetadata indexMapping : getMappingsResponse.getMappings().values()) {
            DocNode mappings = DocNode.wrap(indexMapping.sourceAsMap());
            assertThat(mappings, hasKey("properties"));
            assertThat(mappings.getAsNode("properties"), hasKey("sg_tenant"));
            assertThat(mappings.getAsNode("properties").getAsNode("sg_tenant"), equalTo(DocNode.of("type", "keyword")));
        }
    }

    public void createTenants(String indexName, String...tenantNames) {
        Client client = cluster.getInternalNodeClient();
        for(String currentTenant : tenantNames) {
            ImmutableMap<String, String> source = ImmutableMap.of( "type", "space");
            String spaceId = "space:default";
            if(!Tenant.GLOBAL_TENANT_ID.equals(currentTenant)) {
                String internal = TenantManager.toInternalTenantName(currentTenant);
                source = source.with("sg_tenant", internal);
                spaceId = spaceId + "__sg_ten__" + internal;
            }
            IndexRequest indexRequest = new IndexRequest(indexName).id(spaceId).source(source).setRefreshPolicy(IMMEDIATE);
            DocWriteResponse response = client.index(indexRequest).actionGet();
            assertThat(response.status(), equalTo(CREATED));
        }
    }

    private void removeKibanaRelatedIndices() {
        Client client = cluster.getInternalNodeClient();
        AcknowledgedResponse deleteResponse = client.admin().indices().delete(new DeleteIndexRequest(".kibana*")).actionGet();
        assertThat(deleteResponse.isAcknowledged(), equalTo(true));
    }

    private GetMappingsResponse getKibanaIndicesMappings() {
        Client client = cluster.getInternalNodeClient();
        return client.admin().indices().getMappings(new GetMappingsRequest(StaticSettings.DEFAULT_MASTER_TIMEOUT).indices(".kibana*")).actionGet();
    }

    private void createKibanaIndicesAndAliases() {
        Client client = cluster.getInternalNodeClient();
        IndicesAliasesRequest addAliasesRequest = new IndicesAliasesRequest();
        for (String alias : TenantRepository.FRONTEND_MULTI_TENANCY_ALIASES) {
            String indexName = alias + "_1.2.3";
            CreateIndexResponse createIndexResponse = client.admin().indices().create(new CreateIndexRequest(indexName)).actionGet();
            assertThat(createIndexResponse.isAcknowledged(), equalTo(true));
            addAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add().alias(alias).index(indexName));
        }
        AcknowledgedResponse addAliasesResponse = client.admin().indices().aliases(addAliasesRequest).actionGet();
        assertThat(addAliasesResponse.isAcknowledged(), equalTo(true));
    }

}