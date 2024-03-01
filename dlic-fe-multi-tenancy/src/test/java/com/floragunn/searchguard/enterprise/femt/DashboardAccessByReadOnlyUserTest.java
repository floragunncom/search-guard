package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.apache.http.HttpStatus.SC_OK;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.rest.RestStatus.CREATED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class DashboardAccessByReadOnlyUserTest {
    private static final Logger log = LogManager.getLogger(DashboardAccessByReadOnlyUserTest.class);

    private static final String FRONTEND_INDEX = ".kibana";

    private static final TestSgConfig.User FRONTEND_SERVER_USER = new TestSgConfig.User("kibana_server");
    private static final TestSgConfig.Tenant HR_TENANT = new TestSgConfig.Tenant("hr_tenant");

    private static final TestSgConfig.User USER_READ_ONLY_TENANT = new TestSgConfig.User("user_read_only_tenant")
            .roles(new TestSgConfig.Role("hr_tenant_read_only")
                    .tenantPermission("SGS_KIBANA_ALL_READ")
                    .on(HR_TENANT.getName()));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled()
            .enterpriseModulesEnabled()
            .roleMapping(new TestSgConfig.RoleMapping("SGS_KIBANA_USER").users(USER_READ_ONLY_TENANT.getName()))
            .users(USER_READ_ONLY_TENANT)
            .frontendMultiTenancy(new TestSgConfig.FrontendMultiTenancy(true).index(FRONTEND_INDEX).serverUser(FRONTEND_SERVER_USER.getName()))
            .tenants(HR_TENANT)
            .build();

    @BeforeClass
    public static void createIndex() {
        Client client = cluster.getInternalNodeClient();
        DocNode indexMappings = DocNode.of("_doc", DocNode.of("properties", DocNode.of("sg_tenant", DocNode.of("type", "keyword"))));
        CreateIndexRequest request = new CreateIndexRequest(FRONTEND_INDEX + "_8.6.2_001") //
                .settings(Settings.builder().put("index.hidden", true)) //
                .alias(new Alias(FRONTEND_INDEX + "_8.6.2"))
                .alias(new Alias(FRONTEND_INDEX))
                .mapping(indexMappings);
        String tenantWithoutUnwantedChars = HR_TENANT.getName().toLowerCase().replaceAll("[^a-z0-9]+", "");


        CreateIndexResponse response = client.admin().indices().create(request).actionGet();
        assertThat(response.isAcknowledged(), equalTo(true));
        ImmutableMap<String, ?> source = ImmutableMap.of("sg_tenant", String.format("%d_%s", HR_TENANT.getName().hashCode(), tenantWithoutUnwantedChars));
        DocWriteResponse createTenantResponse = client.index(new IndexRequest(FRONTEND_INDEX).source(source).setRefreshPolicy(IMMEDIATE)).actionGet();
        assertThat(createTenantResponse.status(), equalTo(CREATED));
    }

    @Test
    public void shouldHavePermissionToLegacyUriAliasUpdate() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_READ_ONLY_TENANT, new BasicHeader("sg_tenant", HR_TENANT.getName()))) {
            String body = """
                    {"update":{"_id":"legacy-url-alias:default:dashboard:8aee3c30-732f-11ee-9463-735e937661a5","_index":".kibana_8.6.2","_source":true}}
                    {"script":{"source":"\\n            if (ctx._source[params.type].disabled != true) {\\n              if (ctx._source[params.type].resolveCounter == null) {\\n                ctx._source[params.type].resolveCounter = 1;\\n              }\\n              else {\\n                ctx._source[params.type].resolveCounter += 1;\\n              }\\n              ctx._source[params.type].lastResolved = params.time;\\n              ctx._source.updated_at = params.time;\\n            }\\n          ","lang":"painless","params":{"type":"legacy-url-alias","time":"2024-02-27T15:46:14.031Z"}}}
                    """;
            GenericRestClient.HttpResponse response = client.postJson("/_bulk", body);

            log.debug("Response status '{}', and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode responseBody = response.getBodyAsDocNode();
            assertThat(responseBody, containsValue("$.errors", true));
            assertThat(responseBody, containsValue("$.items[0].update.status", 403));
            assertThat(responseBody, containsValue("$.items[0].update.error.type", "security_exception"));
            assertThat(responseBody, containsValue("$.items[0].update._id", "legacy-url-alias:default:dashboard:8aee3c30-732f-11ee-9463-735e937661a5"));
        }
    }

}
