package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.documents.patch.DocPatch;
import com.floragunn.codova.documents.patch.JsonPathPatch;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.TenantManager;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.user.User;
import com.jayway.jsonpath.JsonPath;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class MultiTenancyRequestMappingTest {

    private static final String KIBANA_INDEX = ".kibana";
    private static final String KIBANA_SERVER_USER = "kibana_server";
    private static final TestSgConfig.Tenant HR_TENANT = new TestSgConfig.Tenant("hr_tenant");
    private static final TestSgConfig.User USER = new TestSgConfig.User("user")
            .roles(new TestSgConfig.Role("tenant_access").tenantPermission("*").on(HR_TENANT.getName()).indexPermissions("*").on(KIBANA_INDEX));

    private final TenantManager tenantManager = new TenantManager(ImmutableSet.of(HR_TENANT.getName()));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled()
            .enterpriseModulesEnabled()
            .user(USER)
            .frontendMultiTenancy(new TestSgConfig.FrontendMultiTenancy(true).index(KIBANA_INDEX).serverUser(KIBANA_SERVER_USER))
            .tenants(HR_TENANT)
            .build();

    @Before
    public void createTestIndex() {
        try (Client client = cluster.getInternalNodeClient()) {
            AcknowledgedResponse createIndexResponse = client.admin().indices().create(new CreateIndexRequest(KIBANA_INDEX)).actionGet();
            assertThat(createIndexResponse.isAcknowledged(), equalTo(true));
        }
    }

    @After
    public void deleteTestIndex() {
        try (Client client = cluster.getInternalNodeClient()) {
            AcknowledgedResponse deleteIndexResponse = client.admin().indices().delete(new DeleteIndexRequest(KIBANA_INDEX)).actionGet();
            assertThat(deleteIndexResponse.isAcknowledged(), equalTo(true));
        }
    }

    @Test
    public void getRequest_withoutParams() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);
        addDocumentToIndex(scopedId, DocNode.of("a", "a", "b", "b"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            GenericRestClient.HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId);
            GenericRestClient.HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id, tenantHeader());
            assertThat(responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBody().replaceAll(scopedId, id), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_withSourceParam() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);
        addDocumentToIndex(scopedId, DocNode.of("a", "a", "b", "b"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //source = false
            GenericRestClient.HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?_source=false");
            GenericRestClient.HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?_source=false", tenantHeader());
            assertThat(responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBody().replaceAll(scopedId, id), equalTo(responseWithTenant.getBody()));

            //source = true
            responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?_source=true");
            responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?_source=true", tenantHeader());
            assertThat(responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBody().replaceAll(scopedId, id), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_withSourceIncludesAndSourceExcludesParams() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);
        addDocumentToIndex(scopedId, DocNode.of("aa", "a", "ab", "b", "bb", "b"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //include and exclude
            GenericRestClient.HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?_source_includes=a*&_source_excludes=ab");
            GenericRestClient.HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?_source_includes=a*&_source_excludes=ab", tenantHeader());
            assertThat(responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBody().replaceAll(scopedId, id), equalTo(responseWithTenant.getBody()));

            //only include
            responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?_source=a*");
            responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?_source=a*", tenantHeader());
            assertThat(responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBody().replaceAll(scopedId, id), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_withStoredFieldsParam() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);
        DocNode indexMappings = DocNode.parse(Format.JSON).from("""
                {
                  "properties": {
                    "aa": {
                      "type": "text",
                      "store": true
                    },
                    "ab": {
                      "type": "text",
                      "store": true
                    },
                    "bb": {
                      "type": "text"
                    }
                  }
                }
                """);
        updateIndexMappings(indexMappings);
        addDocumentToIndex(scopedId, DocNode.of("aa", "a", "ab", "b", "bb", "b"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //stored fields = false
            GenericRestClient.HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?stored_fields=false");
            GenericRestClient.HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?stored_fields=false", tenantHeader());
            assertThat(responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBody().replaceAll(scopedId, id), equalTo(responseWithTenant.getBody()));

            //stored fields = true
            responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?stored_fields=true");
            responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?stored_fields=true", tenantHeader());
            assertThat(responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBody().replaceAll(scopedId, id), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_withRoutingParam() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);
        String routing = "test-routing";
        addDocumentToIndex(scopedId, "routing", DocNode.of("aa", "a", "ab", "b", "bb", "b"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //correct routing
            GenericRestClient.HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?routing=" + routing);
            GenericRestClient.HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?routing=" + routing, tenantHeader());
            assertThat(responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBody().replaceAll(scopedId, id), equalTo(responseWithTenant.getBody()));

            //wrong routing
            routing = routing.concat("-fake");
            responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?routing=" + routing);
            responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?routing=" + routing, tenantHeader());
            assertThat(responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithoutTenant.getBody().replaceAll(scopedId, id), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_docDoesDoesNotExist() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            GenericRestClient.HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?stored_fields=false");
            GenericRestClient.HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?stored_fields=false", tenantHeader());
            assertThat(responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithoutTenant.getBody().replaceAll(scopedId, id), equalTo(responseWithTenant.getBody()));
        }
    }

    private String scopedId(String id) {
        return RequestResponseTenantData.scopedId(id, internalTenantName());
    }

    private String internalTenantName() {
        return tenantManager.toInternalTenantName(User.forUser(USER.getName()).requestedTenant(HR_TENANT.getName()).build());
    }

    private Header tenantHeader() {
        return new BasicHeader("sg_tenant", HR_TENANT.getName());
    }

    private IndexResponse addDocumentToIndex(String id, String routing, DocNode doc) {
        try (Client client = cluster.getInternalNodeClient()) {
            IndexResponse indexResponse = client.index(new IndexRequest(KIBANA_INDEX).id(id).source(doc)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).routing(routing)).actionGet();
            assertThat(indexResponse.status().getStatus(), equalTo(HttpStatus.SC_CREATED));
            return indexResponse;
        }
    }

    private IndexResponse addDocumentToIndex(String id, DocNode doc) {
        return addDocumentToIndex(id, null, doc);
    }

    private void updateIndexMappings(DocNode mapping) {
        try (Client client = cluster.getInternalNodeClient()) {
            AcknowledgedResponse response = client.admin().indices().putMapping(new PutMappingRequest(KIBANA_INDEX).source(mapping)).actionGet();
            assertThat(response.isAcknowledged(), equalTo(true));
        }
    }

    private void enableFeMultiTenancy() throws Exception {
        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            DocPatch settingsPatch = new JsonPathPatch(new JsonPathPatch.Operation(JsonPath.compile("enabled"), true));
            GenericRestClient.HttpResponse response = restClient.patch("/_searchguard/config/fe_multi_tenancy", settingsPatch);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
        }
    }


    private void disableFeMultiTenancy() throws Exception {
        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            DocPatch settingsPatch = new JsonPathPatch(new JsonPathPatch.Operation(JsonPath.compile("enabled"), false));
            GenericRestClient.HttpResponse response = restClient.patch("/_searchguard/config/fe_multi_tenancy", settingsPatch);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
        }
    }
}
