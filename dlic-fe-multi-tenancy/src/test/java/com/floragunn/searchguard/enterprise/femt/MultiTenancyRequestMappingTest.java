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
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
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
            .roles(new TestSgConfig.Role("tenant_access").tenantPermission("*").on(HR_TENANT.getName()).clusterPermissions("*").indexPermissions("*").on(KIBANA_INDEX));

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
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, id), equalTo(responseWithTenant.getBody()));
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
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, id), equalTo(responseWithTenant.getBody()));

            //source = true
            responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?_source=true");
            responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?_source=true", tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, id), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_withSourceIncludesAndSourceExcludesParams() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);
        addDocumentToIndex(scopedId, DocNode.of("aa", "a", "ab", "b", "bb", "b"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //include and exclude
            GenericRestClient.HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_doc/" + scopedId + "?_source_includes=a*&_source_excludes=ab"
            );
            GenericRestClient.HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_doc/" + id + "?_source_includes=a*&_source_excludes=ab", tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, id), equalTo(responseWithTenant.getBody()));

            //only include
            responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?_source=a*");
            responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?_source=a*", tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, id), equalTo(responseWithTenant.getBody()));
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
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, id), equalTo(responseWithTenant.getBody()));

            //stored fields = true
            responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?stored_fields=true");
            responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?stored_fields=true", tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, id), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_withRoutingParam() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);
        String routing = "test-routing";
        addDocumentToIndex(scopedId, routing, DocNode.of("aa", "a", "ab", "b", "bb", "b"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //correct routing
            GenericRestClient.HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?routing=" + routing);
            GenericRestClient.HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?routing=" + routing, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, id), equalTo(responseWithTenant.getBody()));

            //wrong routing
            routing = routing.concat("-fake");
            responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?routing=" + routing);
            responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?routing=" + routing, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(unscopeResponseBody(responseWithoutTenant, id), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_withPreferenceParam() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);

        String preference = "_local";
        addDocumentToIndex(scopedId, DocNode.of("aa", "a", "ab", "b", "bb", "b"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //correct preference
            GenericRestClient.HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?preference=" + preference);
            GenericRestClient.HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?preference=" + preference, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, id), equalTo(responseWithTenant.getBody()));

            //wrong preference
            preference = preference.concat("-fake");
            responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?preference=" + preference);
            responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?preference=" + preference, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
            assertThat(unscopeResponseBody(responseWithoutTenant, id), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_withRealtimeParam() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);

        addDocumentToIndex(scopedId, DocNode.of("aa", "a", "ab", "b", "bb", "b"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //realtime true
            String realtime = "true";
            GenericRestClient.HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?realtime=" + realtime);
            GenericRestClient.HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?realtime=" + realtime, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, id), equalTo(responseWithTenant.getBody()));

            //realtime false
            realtime = "false";
            responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?realtime=" + realtime);
            responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?realtime=" + realtime, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, id), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_withRefreshParam() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);

        addDocumentToIndex(scopedId, DocNode.of("aa", "a", "ab", "b", "bb", "b"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //refresh true
            String refresh = "true";
            GenericRestClient.HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?refresh=" + refresh);
            GenericRestClient.HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?refresh=" + refresh, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, id), equalTo(responseWithTenant.getBody()));

            //refresh false
            refresh = "false";
            responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?refresh=" + refresh);
            responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?refresh=" + refresh, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, id), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_withVersionParam() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);

        IndexResponse indexResponse = addDocumentToIndex(scopedId, DocNode.of("aa", "a", "ab", "b", "bb", "b"));
        long docVersion = indexResponse.getVersion();
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //valid version
            GenericRestClient.HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_doc/" + scopedId + "?version=" + docVersion + "&version_type=external_gte"
            );
            GenericRestClient.HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_doc/" + id + "?version=" + docVersion + "&version_type=external_gte", tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, id), equalTo(responseWithTenant.getBody()));

            //invalid version
            docVersion++;
            responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?version=" + docVersion);
            responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id + "?version=" + docVersion, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_CONFLICT));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_CONFLICT));
            assertThat(responseWithoutTenant.getBody(), equalTo(responseWithTenant.getBody())); //todo response contains scoped id since an exception is thrown and we do not modify it's message
        }
    }

    @Test
    public void getRequest_docDoesDoesNotExist() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            GenericRestClient.HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId);
            GenericRestClient.HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + id, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(unscopeResponseBody(responseWithoutTenant, id), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void updateRequest_withoutParams() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);
        DocNode doc = DocNode.of("aa", "a", "ab", "b", "bb", "b");
        addDocumentToIndex(scopedId, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            DocNode updateReqBody = DocNode.of("doc", doc.with("aa", "new value"));
            GenericRestClient.HttpResponse responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_update/" + scopedId, updateReqBody
            );

            updateReqBody = DocNode.of("doc", doc.with("aa", "another new value"));
            GenericRestClient.HttpResponse responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_update/" + id, updateReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), id),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void updateRequest_docDoesDoesNotExist() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);
        DocNode doc = DocNode.of("aa", "a", "ab", "b", "bb", "b");
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            DocNode updateReqBody = DocNode.of("doc", doc);
            GenericRestClient.HttpResponse responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_update/" + scopedId, updateReqBody
            );
            GenericRestClient.HttpResponse responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_update/" + id, updateReqBody, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithoutTenant.getBody(), equalTo(responseWithTenant.getBody())); //todo response contains scoped id since an exception is thrown and we do not modify it's message
        }
    }

    @Test
    public void updateRequest_withIfSeqNoAndIfPrimaryTermParams() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);
        DocNode doc = DocNode.of("aa", "a", "ab", "b", "bb", "b");
        IndexResponse indexResponse = addDocumentToIndex(scopedId, doc);
        long seqNo = indexResponse.getSeqNo();
        long primaryTerm = indexResponse.getPrimaryTerm();
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //valid seqNo and primary term
            DocNode updateReqBody = DocNode.of("doc", doc.with("aa", "new value"));
            GenericRestClient.HttpResponse responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_update/" + scopedId + "?if_seq_no=" + seqNo + "&if_primary_term=" + primaryTerm, updateReqBody
            );

            GetResponse getResponse = getDocById(scopedId);
            seqNo = getResponse.getSeqNo();
            primaryTerm = getResponse.getPrimaryTerm();
            updateReqBody = DocNode.of("doc", doc.with("aa", "another new value"));

            GenericRestClient.HttpResponse responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_update/" + id + "?if_seq_no=" + seqNo + "&if_primary_term=" + primaryTerm, updateReqBody, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), id),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );

            //invalid seq no and primary term
            seqNo *= 10;
            primaryTerm *= 10;
            responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_update/" + scopedId + "?if_seq_no=" + seqNo + "&if_primary_term=" + primaryTerm, updateReqBody
            );

            responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_update/" + id + "?if_seq_no=" + seqNo + "&if_primary_term=" + primaryTerm, updateReqBody, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_CONFLICT));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_CONFLICT));
            assertThat(responseWithoutTenant.getBody(), equalTo(responseWithTenant.getBody())); //todo response contains scoped id since an exception is thrown and we do not modify it's message
        }
    }

    @Test
    public void updateRequest_withRequireAliasParam() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);
        DocNode doc = DocNode.of("aa", "a", "ab", "b", "bb", "b");
        addDocumentToIndex(scopedId, doc);
        String aliasName = KIBANA_INDEX.concat("_1.1.1");
        addAliasToIndex(aliasName);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //alias required, alias name provided
            String requireAlias = "true";
            DocNode updateReqBody = DocNode.of("doc", doc.with("aa", "new value"));
            GenericRestClient.HttpResponse responseWithoutTenant = client.postJson(
                    "/" + aliasName + "/_update/" + scopedId + "?require_alias=" + requireAlias, updateReqBody
            );

            updateReqBody = DocNode.of("doc", doc.with("aa", "another new value"));
            GenericRestClient.HttpResponse responseWithTenant = client.postJson(
                    "/" + aliasName + "/_update/" + id + "?require_alias=" + requireAlias, updateReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), id),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );

            //alias required, index name provided
            responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_update/" + scopedId + "?require_alias=" + requireAlias, updateReqBody
            );

            updateReqBody = DocNode.of("doc", doc.with("aa", "another new value"));
            responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_update/" + id + "?require_alias=" + requireAlias, updateReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithoutTenant.getBody(), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void updateRequest_withRefreshParam() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);
        DocNode doc = DocNode.of("aa", "a", "ab", "b", "bb", "b");
        addDocumentToIndex(scopedId, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //refresh true
            String refresh = "true";
            DocNode updateReqBody = DocNode.of("doc", doc.with("aa", "new value"));
            GenericRestClient.HttpResponse responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_update/" + scopedId + "?refresh=" + refresh, updateReqBody
            );

            updateReqBody = DocNode.of("doc", doc.with("aa", "another new value"));
            GenericRestClient.HttpResponse responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_update/" + id + "?refresh=" + refresh, updateReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), id),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );

            //refresh false
            refresh = "false";
            updateReqBody = DocNode.of("doc", doc.with("aa", "new value"));
            responseWithoutTenant = client.postJson("/" + KIBANA_INDEX + "/_update/" + scopedId + "?refresh=" + refresh, updateReqBody);

            updateReqBody = DocNode.of("doc", doc.with("aa", "another new value"));
            responseWithTenant = client.postJson("/" + KIBANA_INDEX + "/_update/" + id + "?refresh=" + refresh, updateReqBody, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), id),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void updateRequest_withRoutingParam() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);
        DocNode doc = DocNode.of("aa", "a", "ab", "b", "bb", "b");
        String routing = "test-routing";
        addDocumentToIndex(scopedId, routing, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //correct routing
            String refresh = "true";
            DocNode updateReqBody = DocNode.of("doc", doc.with("aa", "new value"));
            GenericRestClient.HttpResponse responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_update/" + scopedId + "?routing=" + routing, updateReqBody
            );

            updateReqBody = DocNode.of("doc", doc.with("aa", "another new value"));
            GenericRestClient.HttpResponse responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_update/" + id + "?routing=" + routing, updateReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), id),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );

            //wrong routing
            routing = routing.concat("-fake");
            updateReqBody = DocNode.of("doc", doc.with("aa", "new value"));
            responseWithoutTenant = client.postJson("/" + KIBANA_INDEX + "/_update/" + scopedId + "?routing=" + routing, updateReqBody);

            updateReqBody = DocNode.of("doc", doc.with("aa", "another new value"));
            responseWithTenant = client.postJson("/" + KIBANA_INDEX + "/_update/" + id + "?routing=" + routing, updateReqBody, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithoutTenant.getBody(), equalTo(responseWithTenant.getBody())); //todo response contains scoped id since an exception is thrown and we do not modify it's message
        }
    }

    @Test
    public void updateRequest_withSourceParam() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);
        DocNode doc = DocNode.of("aa", "a", "ab", "b", "bb", "b");
        addDocumentToIndex(scopedId, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            DocNode updateReqBody = DocNode.of("doc", doc.with("aa", "new value"));
            GenericRestClient.HttpResponse responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_update/" + scopedId + "?_source=ab,b*", updateReqBody
            );

            updateReqBody = DocNode.of("doc", doc.with("aa", "another new value"));
            GenericRestClient.HttpResponse responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_update/" + id + "?_source=ab,b*", updateReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), id),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void updateRequest_withSourceIncludesAndSourceExcludesParams() throws Exception {
        String id = "123";
        String scopedId = scopedId(id);
        DocNode doc = DocNode.of("aa", "a", "ab", "b", "bb", "b");
        addDocumentToIndex(scopedId, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            DocNode updateReqBody = DocNode.of("doc", doc.with("aa", "new value"));
            GenericRestClient.HttpResponse responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_update/" + scopedId + "?_source_includes=ab&_source_excludes=bb", updateReqBody
            );

            updateReqBody = DocNode.of("doc", doc.with("aa", "another new value"));
            GenericRestClient.HttpResponse responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_update/" + id + "?_source_includes=ab&_source_excludes=bb", updateReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), id),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );
        }
    }

    private DocNode responseBodyWithoutAutoIncrementedFields(GenericRestClient.HttpResponse response) throws Exception {
        ImmutableSet<String> autoIncrementedFields = ImmutableSet.of("_version", "_seq_no");
        String regexPattern = String.format("\"(%s)\":\"?[\\w\\d]\"?,", String.join("|", autoIncrementedFields));
        String responseWithoutAutoIncrementedFields = response.getBody().replaceAll(regexPattern, "");
        return DocNode.parse(Format.JSON).from(responseWithoutAutoIncrementedFields);
    }

    private String unscopeResponseBody(GenericRestClient.HttpResponse response, String id) throws Exception {
        return unscopeResponseBody(response.getBodyAsDocNode(), id);
    }

    private String unscopeResponseBody(DocNode response, String id) {
        return response.toJsonString().replaceAll(scopedId(id), id);
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

    private GetResponse getDocById(String id) {
        try (Client client = cluster.getInternalNodeClient()) {
            GetResponse getResponse = client.get(new GetRequest(KIBANA_INDEX).id(id)).actionGet();
            assertThat(getResponse.isExists(), equalTo(true));
            return getResponse;
        }
    }

    private void addAliasToIndex(String aliasName) {
        try (Client client = cluster.getInternalNodeClient()) {
            IndicesAliasesRequest.AliasActions addAlias = IndicesAliasesRequest.AliasActions.add().index(KIBANA_INDEX).alias(aliasName);
            AcknowledgedResponse acknowledgedResponse = client.admin().indices()
                    .aliases(new IndicesAliasesRequest().addAliasAction(addAlias))
                    .actionGet();
            assertThat(acknowledgedResponse.isAcknowledged(), equalTo(true));
        }
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