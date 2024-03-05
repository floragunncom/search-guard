package com.floragunn.searchguard.enterprise.femt.tenants;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.authz.TenantManager;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfig;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.FrontendMultiTenancy;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.RoleMapping;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsNullValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.docNodeSizeEqualTo;
import static org.apache.http.HttpStatus.SC_FORBIDDEN;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_UNAUTHORIZED;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.rest.RestStatus.CREATED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class GetAvailableTenantsActionTest {

    private static final Logger log = LogManager.getLogger(GetAvailableTenantsActionTest.class);

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

    private static final ImmutableList<TestSgConfig.Tenant> ALL_DEFINED_TENANTS = ImmutableList.of(HR_TENANT, FINANCE_TENANT,
        SALES_TENANT, OPERATIONS_TENANT, RD_TENANT, BD_TENANT, LEGAL_TENANT, IT_TENANT, PR_TENANT, QA_TENANT);
    private static final User USER_SINGLE_TENANT = new User("user_single_tenant") //
        .roles(new Role("single_tenant_access") //
            .tenantPermission("*") //
            .on(HR_TENANT.getName()) //
            .indexPermissions("*") //
            .on(FRONTEND_INDEX +"*"));

    private static final User FRONTEND_SERVER_USER = new User("kibana_server");

    private static final User USER_EACH_TENANT_READ = new User("user_each_tenant_read") //
        .roles(new Role("each_tenant_read_access") //
            .tenantPermission("SGS_KIBANA_ALL_READ") //
            .on(ALL_DEFINED_TENANTS.map(TestSgConfig.Tenant::getName).with(Tenant.GLOBAL_TENANT_ID).toArray(String[]::new)) //
            .indexPermissions("*") //
            .on(FRONTEND_INDEX +"*"));
    private static final User USER_EACH_TENANT_WRITE = new User("user_each_tenant_write") //
        .roles(new Role("each_tenant_write_access") //
            .tenantPermission("SGS_KIBANA_ALL_WRITE") //
            .on(ALL_DEFINED_TENANTS.map(TestSgConfig.Tenant::getName).with(Tenant.GLOBAL_TENANT_ID).toArray(String[]::new)) //
            .indexPermissions("*") //
            .on(FRONTEND_INDEX +"*"));

    private static final User USER_SOME_TENANT_ACCESS = new User("user_some_tenant_access") //
        .roles(new Role("some_tenant_access") //
            .tenantPermission("SGS_KIBANA_ALL_WRITE") //
            .on(HR_TENANT.getName(), FINANCE_TENANT.getName(), SALES_TENANT.getName()) //
            .tenantPermission("SGS_KIBANA_ALL_READ").on(IT_TENANT.getName(), PR_TENANT.getName(), QA_TENANT.getName()) //
            .indexPermissions("*") //
            .on(FRONTEND_INDEX +"*"));

    private static final User USER_WITHOUT_ACCESS_TO_ANY_TENANT = new User("user_without_access_to_any_tenant") //
            .roles(new Role("no_tenant_access") //
                    .clusterPermissions("cluster:admin:searchguard:femt:user/available_tenants/get"));


    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled() //
        .nodeSettings("action.destructive_requires_name", false, "searchguard.unsupported.single_index_mt_enabled", true) //
        .enterpriseModulesEnabled() //
        .roleMapping(new RoleMapping("SGS_KIBANA_MT_USER").users(USER_SINGLE_TENANT.getName(), USER_EACH_TENANT_READ.getName(),
            USER_EACH_TENANT_WRITE.getName(), USER_SOME_TENANT_ACCESS.getName()),//
            new RoleMapping("SGS_KIBANA_SERVER").users(FRONTEND_SERVER_USER.getName())) //
        .users(FRONTEND_SERVER_USER, USER_SINGLE_TENANT, USER_EACH_TENANT_READ, USER_EACH_TENANT_WRITE, USER_SOME_TENANT_ACCESS, USER_WITHOUT_ACCESS_TO_ANY_TENANT) //
        .frontendMultiTenancy(new FrontendMultiTenancy(true).index(FRONTEND_INDEX).serverUser(FRONTEND_SERVER_USER.getName())) //
        .tenants(HR_TENANT, FINANCE_TENANT, SALES_TENANT, OPERATIONS_TENANT, RD_TENANT, BD_TENANT, LEGAL_TENANT,
            IT_TENANT, PR_TENANT, QA_TENANT) //
        .build();

    @BeforeClass
    public static void createIndex() {
        Client client = cluster.getInternalNodeClient();
        DocNode indexMappings = DocNode.of("_doc", DocNode.of("properties", DocNode.of("sg_tenant", DocNode.of("type", "keyword"))));
        CreateIndexRequest request = new CreateIndexRequest(FRONTEND_INDEX) //
            .settings(Settings.builder().put("index.hidden", true)) //
            .mapping(indexMappings);
        CreateIndexResponse response = client.admin().indices().create(request).actionGet();
        assertThat(response.isAcknowledged(), equalTo(true));
    }

    @After
    public void clearIndices() {
        Client client = cluster.getInternalNodeClient();
        DeleteByQueryRequest request = new DeleteByQueryRequest(FRONTEND_INDEX);
        request.setQuery(QueryBuilders.matchAllQuery());
        request.setRefresh(true);
        request.setBatchSize(100);
        request.setScroll(TimeValue.timeValueMinutes(1));
        BulkByScrollResponse response = client.execute(DeleteByQueryAction.INSTANCE, request).actionGet();
        assertThat(response.getSearchFailures(), empty());
        assertThat(response.getBulkFailures(), empty());
    }

    @Test
    public void shouldFindAccessibleTenantsForSingleTenantUser() throws Exception {
        createTenants(FRONTEND_INDEX, ALL_DEFINED_TENANTS.map(TestSgConfig.Tenant::getName).toArray(String[]::new));
        Header tenantHeader = new BasicHeader("sg_tenant", "test_tenant");
        try(GenericRestClient client = cluster.getRestClient(USER_SINGLE_TENANT, tenantHeader)) {

            HttpResponse response = client.get("/_searchguard/current_user/tenants");

            log.debug("Response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.data.default_tenant", Tenant.GLOBAL_TENANT_ID));
            assertThat(body, containsValue("$.data.username", USER_SINGLE_TENANT.getName()));
            assertThat(body, containsValue("$.data.user_requested_tenant", tenantHeader.getValue()));
            assertThat(body, containsValue("$.data.multi_tenancy_enabled", true));
            assertThat(body, containsValue("$.data.tenants.hr_tenant.read_access", true));
            assertThat(body, containsValue("$.data.tenants.hr_tenant.write_access", true));
            assertThat(body, containsValue("$.data.tenants.hr_tenant.exists", true));
            assertThat(body, containsValue("$.data.tenants.user_single_tenant.read_access", true));
            assertThat(body, containsValue("$.data.tenants.user_single_tenant.write_access", true));
            assertThat(body, containsValue("$.data.tenants.user_single_tenant.exists", false));
            // user with role SGS_KIBANA_MT_USER has write access to global tenant
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.read_access", true));
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.write_access", true));
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.exists", false));

            // only accessible tenants should be present in the response: hr_tenant,  private user tenant
            assertThat(body, docNodeSizeEqualTo("$.data.tenants", 3));
        }
    }

    @Test
    public void shouldFindAccessibleTenantsForUserAllowedToReadEachTenantData() throws Exception {
        String[] tenantsToBeCreated = ALL_DEFINED_TENANTS.map(TestSgConfig.Tenant::getName) //
            .with(USER_EACH_TENANT_READ.getName()) //
            .toArray(String[]::new);
        createTenants(FRONTEND_INDEX, tenantsToBeCreated);
        try(GenericRestClient client = cluster.getRestClient(USER_EACH_TENANT_READ)) {

            HttpResponse response = client.get("/_searchguard/current_user/tenants");

            log.debug("Response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("$.data.tenants", 12));
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
            //user should always have access to its private tenant
            assertThat(body, containsValue("$.data.tenants.user_each_tenant_read.write_access", true));
            assertThat(body, containsValue("$.data.tenants.user_each_tenant_read.exists", true));
            // user with role SGS_KIBANA_MT_USER has write access to global tenant
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.read_access", true));
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.write_access", true));
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.exists", false));
        }
    }

    @Test
    public void shouldFindAccessibleTenantsForUserAllowedToWriteEachTenantData() throws Exception {
        String[] tenantsToBeCreated = ALL_DEFINED_TENANTS.map(TestSgConfig.Tenant::getName) //
            .with(USER_EACH_TENANT_WRITE.getName()) //
            .toArray(String[]::new);
        createTenants(FRONTEND_INDEX, tenantsToBeCreated);
        try(GenericRestClient client = cluster.getRestClient(USER_EACH_TENANT_WRITE)) {

            HttpResponse response = client.get("/_searchguard/current_user/tenants");

            log.debug("Response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("$.data.tenants", 12));
            assertThat(body, containsValue("$.data.default_tenant", Tenant.GLOBAL_TENANT_ID));
            assertThat(body, containsValue("$.data.username", USER_EACH_TENANT_WRITE.getName()));
            assertThat(body, containsNullValue("$.data.user_requested_tenant"));
            for(String tenantName : tenantsToBeCreated) {
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
    public void shouldFindAccessibleTenantsWhichDoesNotExist() throws Exception {
        String[] accessibleTenantsNames = ALL_DEFINED_TENANTS.map(TestSgConfig.Tenant::getName) //
            .with(USER_EACH_TENANT_WRITE.getName()) //
            .toArray(String[]::new);
        try(GenericRestClient client = cluster.getRestClient(USER_EACH_TENANT_WRITE)) {

            HttpResponse response = client.get("/_searchguard/current_user/tenants");

            log.debug("Response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("$.data.tenants", 12));
            assertThat(body, containsValue("$.data.default_tenant", Tenant.GLOBAL_TENANT_ID));
            assertThat(body, containsValue("$.data.username", USER_EACH_TENANT_WRITE.getName()));
            assertThat(body, containsNullValue("$.data.user_requested_tenant"));
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
    public void shouldReturnInformationIfTenantIsAccessibleAndExist() throws Exception {
        createTenants(FRONTEND_INDEX, HR_TENANT.getName(), FINANCE_TENANT.getName(), PR_TENANT.getName(), QA_TENANT.getName());
        try(GenericRestClient client = cluster.getRestClient(USER_SOME_TENANT_ACCESS)) {

            HttpResponse response = client.get("/_searchguard/current_user/tenants");

            log.debug("Response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("$.data.tenants", 7));
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
            assertThat(body, containsValue("$.data.tenants.user_some_tenant_access.write_access", true));
            assertThat(body, containsValue("$.data.tenants.user_some_tenant_access.exists", false));
            // user with role SGS_KIBANA_MT_USER has write access to global tenant
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.read_access", true));
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.write_access", true));
            assertThat(body, containsValue("$.data.tenants.SGS_GLOBAL_TENANT.exists", false));
        }
    }

    @Test
    public void shouldNotBeAccessibleForFrontendServerUser() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(FRONTEND_SERVER_USER)) {

            HttpResponse response = client.get("/_searchguard/current_user/tenants");

            log.debug("Response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_FORBIDDEN));
        }
    }

    @Test
    public void shouldReturnUnauthorized_whenUserDoesNotHaveAccessToAnyTenantAndDefaultTenantCannotBeDetermined() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(USER_WITHOUT_ACCESS_TO_ANY_TENANT); GenericRestClient adminClient = cluster.getAdminCertRestClient()) {

            HttpResponse response = client.get("/_searchguard/current_user/tenants");
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThat(response.getBodyAsDocNode(), containsValue("$.data.default_tenant", USER_WITHOUT_ACCESS_TO_ANY_TENANT.getName()));

            cluster.callAndRestoreConfig(FeMultiTenancyConfig.TYPE, () -> {

                HttpResponse getConfigResponse = adminClient.get("/_searchguard/config/fe_multi_tenancy");
                assertThat(getConfigResponse.getStatusCode(), equalTo(SC_OK));
                DocNode config = getConfigResponse.getBodyAsDocNode().getAsNode("content");
                config = config.with("private_tenant_enabled", false);
                HttpResponse putConfigResponse = adminClient.putJson("/_searchguard/config/fe_multi_tenancy", config);
                assertThat(putConfigResponse.getStatusCode(), equalTo(SC_OK));

                HttpResponse getTenantsResponse = client.get("/_searchguard/current_user/tenants");
                log.debug("Response status '{}' and body '{}'.", getTenantsResponse.getStatusCode(), getTenantsResponse.getBody());
                assertThat(getTenantsResponse.getStatusCode(), equalTo(SC_UNAUTHORIZED));
                assertThat(getTenantsResponse.getBodyAsDocNode(), containsValue("$.message", "Cannot determine default tenant for current user"));


                return null;
            });
        }
    }

    public void createTenants(String indexName, String...tenantNames) {
        Client client = cluster.getInternalNodeClient();
        for(String currentTenant : tenantNames) {
            ImmutableMap<String, ?> source = ImmutableMap.of("sg_tenant", TenantManager.toInternalTenantName(currentTenant));
            DocWriteResponse response = client.index(new IndexRequest(indexName).source(source).setRefreshPolicy(IMMEDIATE)).actionGet();
            assertThat(response.status(), equalTo(CREATED));
        }
    }

}