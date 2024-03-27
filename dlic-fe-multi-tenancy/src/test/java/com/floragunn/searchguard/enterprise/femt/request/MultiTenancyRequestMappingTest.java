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

package com.floragunn.searchguard.enterprise.femt.request;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.TenantManager;
import com.floragunn.searchguard.authz.config.MultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.user.User;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsFieldPointedByJsonPath;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.docNodeSizeEqualTo;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static org.apache.http.HttpStatus.SC_FORBIDDEN;
import static org.apache.http.HttpStatus.SC_OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class MultiTenancyRequestMappingTest {

    private static final Logger log = LogManager.getLogger(MultiTenancyRequestMappingTest.class);

    public static final String GLOBAL_TENANT_NAME = "SGS_GLOBAL_TENANT";

    private static final String DOC_ID = "123";
    private static final String KIBANA_INDEX = ".kibana";
    private static final String KIBANA_SERVER_USER = "kibana_server";
    private static final TestSgConfig.Tenant HR_TENANT = new TestSgConfig.Tenant("hr_tenant");
    private static final TestSgConfig.Tenant IT_TENANT = new TestSgConfig.Tenant("it_tenant");
    private static final TestSgConfig.User USER = new TestSgConfig.User("user")
            .roles(new TestSgConfig.Role("tenant_access").tenantPermission("*").on(HR_TENANT.getName()).clusterPermissions("*").indexPermissions("*").on(KIBANA_INDEX+"*"));

    private static final TestSgConfig.Role LIMITED_ROLE = new TestSgConfig.Role("limited_access_to_global_tenant") //
        .tenantPermission("SGS_KIBANA_ALL_READ").on(HR_TENANT.getName(), GLOBAL_TENANT_NAME) //
        .indexPermissions("indices:data/read/search").on(KIBANA_INDEX)
        .clusterPermissions("SGS_CLUSTER_MONITOR");

    private static final TestSgConfig.User LIMITED_USER = new TestSgConfig.User("limited_user")
        .roles("SGS_KIBANA_USER_NO_GLOBAL_TENANT", LIMITED_ROLE.getName());

    private final TenantManager tenantManager = new TenantManager(ImmutableSet.of(HR_TENANT.getName()), MultiTenancyConfigurationProvider.DEFAULT);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled()
            .nodeSettings("action.destructive_requires_name", false)
            .nodeSettings("searchguard.unsupported.single_index_mt_enabled", true)
            .enterpriseModulesEnabled()
            .roles(LIMITED_ROLE)
            .users(USER, LIMITED_USER)
            .frontendMultiTenancy(new TestSgConfig.FrontendMultiTenancy(true).index(KIBANA_INDEX).serverUser(KIBANA_SERVER_USER))
            .tenants(HR_TENANT, IT_TENANT)
            .build();

    @Before
    public void createTestIndex() {
        Client client = cluster.getInternalNodeClient();
            String mapping = """
                {
                  "properties": {
                    "sg_tenant": {
                      "type": "keyword"
                    }
                  }
                }
                """;
        AcknowledgedResponse createIndexResponse = client.admin().indices()
                .create(new CreateIndexRequest(KIBANA_INDEX).mapping(mapping)).actionGet();
        assertThat(createIndexResponse.isAcknowledged(), equalTo(true));
    }

    @After
    public void deleteTestIndex() {
        deleteIndex(KIBANA_INDEX + "*");
    }

    @Test
    public void shouldCleanThreadContext() throws Exception {
        String internalTenantName = tenantManager.toInternalTenantName(User.forUser(USER.getName()).requestedTenant(IT_TENANT.getName()).build());
        addDocumentToIndex(createInternalScopedId("space_for_it_1", IT_TENANT.getName()), DocNode.of("name", "IT tenant space 1", "sg_tenant", internalTenantName));
        addDocumentToIndex(createInternalScopedId("space_for_it_2", IT_TENANT.getName()), DocNode.of("name", "IT tenant space 2", "sg_tenant", internalTenantName));
        addDocumentToIndex(createInternalScopedId("space_for_it_3", IT_TENANT.getName()), DocNode.of("name", "IT tenant space 3", "sg_tenant", internalTenantName));
        internalTenantName = tenantManager.toInternalTenantName(User.forUser(USER.getName()).requestedTenant(HR_TENANT.getName()).build());
        addDocumentToIndex(createInternalScopedId("space_for_human resources_1", HR_TENANT.getName()), DocNode.of("name", "human resources tenant space 1", "sg_tenant", internalTenantName));
        addDocumentToIndex(createInternalScopedId("space_for_human resources_2", HR_TENANT.getName()), DocNode.of("name", "human resources tenant space 2", "sg_tenant", internalTenantName));


        DocNode matchSpace = DocNode.of("query", DocNode.of("match", DocNode.of("name", "space")));
        DocNode matchTenant = DocNode.of("query", DocNode.of("match", DocNode.of("name", "tenant")));
        DocNode msearchHeader = DocNode.of("index", KIBANA_INDEX);

        String query = Stream.of(msearchHeader, matchTenant, msearchHeader, matchSpace) //
            .map(DocNode::toJsonString) //
            .collect(Collectors.joining("\n")) + "\n";
        log.info("Msearch query : {}", query);

        try (GenericRestClient client = cluster.getRestClient(USER)) {
            BasicHeader sgTenantHeader = new BasicHeader("sg_tenant", HR_TENANT.getName());
            HttpResponse response = client.postJson( "/_msearch?max_concurrent_searches=1", query, sgTenantHeader);
            log.info("Search without tenant response status '{}' body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.responses[0].hits.total.value", 2));
            assertThat(body, containsValue("$.responses[1].hits.total.value", 2));
        }

    }

    private String createInternalScopedId(String documentId, String tenantId) {
        String internalTenantName = tenantManager.toInternalTenantName(User.forUser(USER.getName()).requestedTenant(tenantId).build());
        return RequestResponseTenantData.scopedId(documentId, internalTenantName);
    }

    @Test
    public void getRequest_withoutParams() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("a", "a", "b", "b"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId);
            HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + DOC_ID, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, DOC_ID), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_withSourceParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("a", "a", "b", "b"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //source = false
            HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?_source=false");
            HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?_source=false", tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, DOC_ID), equalTo(responseWithTenant.getBody()));

            //source = true
            responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?_source=true");
            responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?_source=true", tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, DOC_ID), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_withSourceIncludesAndSourceExcludesParams() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("aa", "a", "ab", "b", "bb", "b"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //include and exclude
            HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_doc/" + scopedId + "?_source_includes=a*&_source_excludes=ab"
            );
            HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?_source_includes=a*&_source_excludes=ab", tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, DOC_ID), equalTo(responseWithTenant.getBody()));

            //only include
            responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?_source=a*");
            responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?_source=a*", tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, DOC_ID), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_withStoredFieldsParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
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

            //valid stored fields
            HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?stored_fields=aa,ab");
            HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?stored_fields=aa,ab", tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, DOC_ID), equalTo(responseWithTenant.getBody()));

            //invalid stored fields
            responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?stored_fields=bb");
            responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?stored_fields=bb", tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, DOC_ID), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_withRoutingParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        String routing = "test-routing";
        addDocumentToIndex(scopedId, routing, DocNode.of("aa", "a", "ab", "b", "bb", "b"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //correct routing
            HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?routing=" + routing);
            HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?routing=" + routing, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, DOC_ID), equalTo(responseWithTenant.getBody()));

            //wrong routing
            routing = routing.concat("-fake");
            responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?routing=" + routing);
            responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?routing=" + routing, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(unscopeResponseBody(responseWithoutTenant, DOC_ID), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_withPreferenceParam() throws Exception {
        String scopedId = scopedId(DOC_ID);

        String preference = "_local";
        addDocumentToIndex(scopedId, DocNode.of("aa", "a", "ab", "b", "bb", "b"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //correct preference
            HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?preference=" + preference);
            HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?preference=" + preference, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, DOC_ID), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_withRealtimeParam() throws Exception {
        String scopedId = scopedId(DOC_ID);

        addDocumentToIndex(scopedId, DocNode.of("aa", "a", "ab", "b", "bb", "b"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //realtime true
            String realtime = "true";
            HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?realtime=" + realtime);
            HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?realtime=" + realtime, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, DOC_ID), equalTo(responseWithTenant.getBody()));

            //realtime false
            realtime = "false";
            responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?realtime=" + realtime);
            responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?realtime=" + realtime, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, DOC_ID), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_withRefreshParam() throws Exception {
        String scopedId = scopedId(DOC_ID);

        addDocumentToIndex(scopedId, DocNode.of("aa", "a", "ab", "b", "bb", "b"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //refresh true
            String refresh = "true";
            HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?refresh=" + refresh);
            HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?refresh=" + refresh, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, DOC_ID), equalTo(responseWithTenant.getBody()));

            //refresh false
            refresh = "false";
            responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?refresh=" + refresh);
            responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?refresh=" + refresh, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, DOC_ID), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_withVersionParam() throws Exception {
        String scopedId = scopedId(DOC_ID);

        DocWriteResponse indexResponse = addDocumentToIndex(scopedId, DocNode.of("aa", "a", "ab", "b", "bb", "b"));
        long docVersion = indexResponse.getVersion();
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //valid version
            HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_doc/" + scopedId + "?version=" + docVersion + "&version_type=external_gte"
            );
            HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?version=" + docVersion + "&version_type=external_gte", tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, DOC_ID), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void getRequest_docDoesNotExist() throws Exception {
        String scopedId = scopedId(DOC_ID);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + scopedId);
            HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_doc/" + DOC_ID, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(unscopeResponseBody(responseWithoutTenant, DOC_ID), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void updateRequest_withoutParams() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("aa", "a", "ab", "b", "bb", "b");
        addDocumentToIndex(scopedId, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            DocNode updateReqBody = DocNode.of("doc", doc.with("aa", "new value"));
            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update/" + scopedId), updateReqBody
            );

            updateReqBody = DocNode.of("doc", doc.with("aa", "another new value"));
            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update/" + DOC_ID), updateReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void updateRequest_withoutParams_usingScript() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("a", 1);
        addDocumentToIndex(scopedId, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            String script = """
                    {
                      "script" : {
                        "source": "ctx._source.a += params.add_to_a",
                        "lang": "painless",
                        "params" : {
                          "add_to_a" : 4
                        }
                      }
                    }
                    """;
            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update/" + scopedId), script
            );

            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update/" + DOC_ID), script, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void updateRequest_withIfSeqNoAndIfPrimaryTermParams() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("aa", "a", "ab", "b", "bb", "b");
        DocWriteResponse indexResponse = addDocumentToIndex(scopedId, doc);
        long seqNo = indexResponse.getSeqNo();
        long primaryTerm = indexResponse.getPrimaryTerm();
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //valid seqNo and primary term
            DocNode updateReqBody = DocNode.of("doc", doc.with("aa", "new value"));
            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update/" +
                            scopedId + "?if_seq_no=" + seqNo + "&if_primary_term=" + primaryTerm),
                    updateReqBody
            );

            GetResponse getResponse = getDocById(scopedId);
            seqNo = getResponse.getSeqNo();
            primaryTerm = getResponse.getPrimaryTerm();
            updateReqBody = DocNode.of("doc", doc.with("aa", "another new value"));

            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update/" +
                            DOC_ID + "?if_seq_no=" + seqNo + "&if_primary_term=" + primaryTerm),
                    updateReqBody, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void updateRequest_withRequireAliasParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("aa", "a", "ab", "b", "bb", "b");
        addDocumentToIndex(scopedId, doc);
        String aliasName = KIBANA_INDEX.concat("_1.1.1");
        addAliasToIndex(aliasName);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //alias required, alias name provided
            String requireAlias = "true";
            DocNode updateReqBody = DocNode.of("doc", doc.with("aa", "new value"));
            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + aliasName + "/_update/" + scopedId + "?require_alias=" + requireAlias),
                    updateReqBody
            );

            updateReqBody = DocNode.of("doc", doc.with("aa", "another new value"));
            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + aliasName + "/_update/" + DOC_ID + "?require_alias=" + requireAlias),
                    updateReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void updateRequest_withRefreshParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("aa", "a", "ab", "b", "bb", "b");
        addDocumentToIndex(scopedId, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //refresh true
            String refresh = "true";
            DocNode updateReqBody = DocNode.of("doc", doc.with("aa", "new value"));
            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update/" + scopedId + "?refresh=" + refresh),
                    updateReqBody
            );

            updateReqBody = DocNode.of("doc", doc.with("aa", "another new value"));
            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update/" + DOC_ID + "?refresh=" + refresh),
                    updateReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );

            //refresh false
            refresh = "false";
            updateReqBody = DocNode.of("doc", doc.with("aa", "new value"));
            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update/" + scopedId + "?refresh=" + refresh),
                    updateReqBody
            );

            updateReqBody = DocNode.of("doc", doc.with("aa", "another new value"));
            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update/" + DOC_ID + "?refresh=" + refresh),
                    updateReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void updateRequest_withRoutingParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("aa", "a", "ab", "b", "bb", "b");
        String routing = "test-routing";
        addDocumentToIndex(scopedId, routing, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //correct routing
            DocNode updateReqBody = DocNode.of("doc", doc.with("aa", "new value"));
            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update/" + scopedId + "?routing=" + routing),
                    updateReqBody
            );

            updateReqBody = DocNode.of("doc", doc.with("aa", "another new value"));
            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update/" + DOC_ID + "?routing=" + routing),
                    updateReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void updateRequest_withSourceParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("aa", "a", "ab", "b", "bb", "b");
        addDocumentToIndex(scopedId, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            DocNode updateReqBody = DocNode.of("doc", doc.with("aa", "new value"));
            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update/" + scopedId + "?_source=ab,b*"), updateReqBody
            );

            updateReqBody = DocNode.of("doc", doc.with("aa", "another new value"));
            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update/" + DOC_ID + "?_source=ab,b*"), updateReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void updateRequest_withSourceIncludesAndSourceExcludesParams() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("aa", "a", "ab", "b", "bb", "b");
        addDocumentToIndex(scopedId, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            DocNode updateReqBody = DocNode.of("doc", doc.with("aa", "new value"));
            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update/" + scopedId + "?_source_includes=ab&_source_excludes=bb"),
                    updateReqBody
            );

            updateReqBody = DocNode.of("doc", doc.with("aa", "another new value"));
            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update/" + DOC_ID + "?_source_includes=ab&_source_excludes=bb"),
                    updateReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void indexRequest_withoutParams() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("a", "a", "b", "b");
        addDocumentToIndex(scopedId, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //with id
            //should act same as with op_type=index param
            DocNode requestBody = doc.with("a", "new value");
            HttpResponse responseWithoutTenant = client.putJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_doc/" + scopedId),
                    requestBody
            );

            requestBody = doc.with("a", "another new value");
            HttpResponse responseWithTenant = client.putJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_doc/" + DOC_ID),
                    requestBody, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );

            //without id
            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_doc/"),
                    doc
            );

            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_doc/"),
                    doc, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_CREATED));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_CREATED));
            assertThat(
                    responseBodyWithoutAutoIncrementedFields(responseWithoutTenant).without("_id").toJsonString(),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).without("_id").toJsonString())
            );
        }
    }

    @Test
    public void indexRequest_withoutParams_createEndpoint() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("a", "a", "b", "b");
        addDocumentToIndex(scopedId, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //should act same as with op_type=create param

            //doc with id does not exist
            HttpResponse responseWithoutTenant = client.putJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_create/" + "unscoped_id"),
                    doc
            );

            HttpResponse responseWithTenant = client.putJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_create/" + "scoped_id"),
                    doc, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_CREATED));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_CREATED));
            assertThat(
                    responseBodyWithoutAutoIncrementedFields(responseWithoutTenant).without("_id").toJsonString(),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).without("_id").toJsonString())
            );

            //doc with id already exists
            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_create/" + scopedId),
                    doc
            );

            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_create/" + DOC_ID),
                    doc, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_CONFLICT));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_CONFLICT));
            assertThat(responseWithoutTenant.getBody(), equalTo(responseWithTenant.getBody())); //todo response contains scoped id since an exception is thrown and we do not modify it's message
        }
    }

    @Test
    public void indexRequest_withIfSeqNoAndIfPrimaryTermParams() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("a", "a", "b", "b");
        DocWriteResponse indexResponse = addDocumentToIndex(scopedId, doc);
        long seqNo = indexResponse.getSeqNo();
        long primaryTerm = indexResponse.getPrimaryTerm();
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //valid seq no and primary term
            HttpResponse responseWithoutTenant = client.putJson(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + scopedId + "?if_seq_no=" + seqNo + "&if_primary_term=" + primaryTerm),
                    doc
            );

            GetResponse getResponse = getDocById(scopedId);
            seqNo = getResponse.getSeqNo();
            primaryTerm = getResponse.getPrimaryTerm();

            HttpResponse responseWithTenant = client.putJson(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?if_seq_no=" + seqNo + "&if_primary_term=" + primaryTerm),
                    doc, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );

            //invalid seqNo and primary term
            seqNo *= 10;
            primaryTerm *= 10;
            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_doc/" + scopedId +
                            "?if_seq_no=" + seqNo + "&if_primary_term=" + primaryTerm),
                    doc
            );

            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_doc/" + DOC_ID +
                            "?if_seq_no=" + seqNo + "&if_primary_term=" + primaryTerm),
                    doc, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_CONFLICT));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_CONFLICT));
            assertThat(responseWithoutTenant.getBody(), equalTo(responseWithTenant.getBody())); //todo response contains scoped id since an exception is thrown and we do not modify it's message
        }
    }

    @Test
    public void indexRequest_withRefreshParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("a", "a", "b", "b");
        addDocumentToIndex(scopedId, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //refresh true
            HttpResponse responseWithoutTenant = client.putJson(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + scopedId + "?refresh=" + true),
                    doc
            );

            HttpResponse responseWithTenant = client.putJson(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?refresh=" + true),
                    doc, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );

            //refresh false
            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_doc/" + scopedId +
                            "?refresh=" + false),
                    doc
            );

            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_doc/" + DOC_ID +
                            "?refresh=" + false),
                    doc, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void indexRequest_withRoutingParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        String routing = "test-routing";
        DocNode doc = DocNode.of("a", "a", "b", "b");
        addDocumentToIndex(scopedId, routing, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //existing routing
            HttpResponse responseWithoutTenant = client.putJson(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + scopedId + "?routing=" + routing),
                    doc
            );

            HttpResponse responseWithTenant = client.putJson(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?routing=" + routing),
                    doc, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );

            //not existing routing
            routing = routing.concat("-missing-1");
            responseWithoutTenant = client.putJson(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + scopedId + "?routing=" + routing),
                    doc
            );

            deleteDoc(scopedId, routing);

            responseWithTenant = client.putJson(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?routing=" + routing),
                    doc, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_CREATED));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_CREATED));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void indexRequest_withVersionParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("a", "a", "b", "b");
        DocWriteResponse indexResponse = addDocumentToIndex(scopedId, doc);
        long version = indexResponse.getVersion() + 1;
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //valid version
            HttpResponse responseWithoutTenant = client.putJson(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + scopedId + "?version=" + version + "&version_type=external"),
                    doc
            );

            version += 1;

            HttpResponse responseWithTenant = client.putJson(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?version=" + version + "&version_type=external"),
                    doc, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );

            //invalid version
            version = 1;

            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_doc/" + scopedId +
                            "?version=" + version + "&version_type=external"),
                    doc
            );

            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_doc/" + DOC_ID +
                            "?version=" + version + "&version_type=external"),
                    doc, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_CONFLICT));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_CONFLICT));
            assertThat(responseWithoutTenant.getBody(), equalTo(responseWithTenant.getBody())); //todo response contains scoped id since an exception is thrown and we do not modify it's message
        }
    }

    @Test
    public void indexRequest_withRequireAliasParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("a", "a", "b", "b");
        addDocumentToIndex(scopedId, doc);
        String aliasName = KIBANA_INDEX.concat("_1.1.1");
        addAliasToIndex(aliasName);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //alias required, alias name provided
            HttpResponse responseWithoutTenant = client.putJson(
                    appendWaitForAllActiveShardsParam(
                            "/" + aliasName + "/_doc/" + scopedId + "?require_alias=true"),
                    doc
            );

            HttpResponse responseWithTenant = client.putJson(
                    appendWaitForAllActiveShardsParam(
                            "/" + aliasName + "/_doc/" + DOC_ID + "?require_alias=true"),
                    doc, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );

            //alias required, index name provided
            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_doc/" + scopedId + "?require_alias=true"),
                    doc
            );

            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?require_alias=true"),
                    doc, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithoutTenant.getBody(), equalTo(responseWithTenant.getBody())); //todo response contains scoped id since an exception is thrown and we do not modify it's message
        }
    }

    @Test
    public void deleteRequest_withoutParams() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("a", "a", "b", "b");
        addDocumentToIndex(scopedId, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            HttpResponse responseWithoutTenant = client.delete(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_doc/" + scopedId));

            addDocumentToIndex(scopedId, doc);

            HttpResponse responseWithTenant = client.delete(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_doc/" + DOC_ID), tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void deleteRequest_withIfSeqNoAndIfPrimaryTermParams() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("a", "a", "b", "b");
        DocWriteResponse indexResponse = addDocumentToIndex(scopedId, doc);
        long seqNo = indexResponse.getSeqNo();
        long primaryTerm = indexResponse.getPrimaryTerm();
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //valid seq no and primary term
            HttpResponse responseWithoutTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + scopedId + "?if_seq_no=" + seqNo + "&if_primary_term=" + primaryTerm)
            );

            indexResponse = addDocumentToIndex(scopedId, doc);
            seqNo = indexResponse.getSeqNo();
            primaryTerm = indexResponse.getPrimaryTerm();

            HttpResponse responseWithTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?if_seq_no=" + seqNo + "&if_primary_term=" + primaryTerm),
                    tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );

            //invalid seq no and primary term
            indexResponse = addDocumentToIndex(scopedId, doc);
            seqNo = indexResponse.getSeqNo() * 10;
            primaryTerm = indexResponse.getPrimaryTerm() * 10;

            responseWithoutTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + scopedId + "?if_seq_no=" + seqNo + "&if_primary_term=" + primaryTerm)
            );

            responseWithTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?if_seq_no=" + seqNo + "&if_primary_term=" + primaryTerm),
                    tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_CONFLICT));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_CONFLICT));
            assertThat(responseWithoutTenant.getBody(), equalTo(responseWithTenant.getBody())); //todo response contains scoped id since an exception is thrown and we do not modify it's message
        }
    }

    @Test
    public void deleteRequest_withRefreshParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("a", "a", "b", "b");
        addDocumentToIndex(scopedId, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //refresh true
            HttpResponse responseWithoutTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + scopedId + "?refresh=" + true)
            );

            addDocumentToIndex(scopedId, doc);

            HttpResponse responseWithTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?refresh=" + true),
                    tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );

            //refresh false
            addDocumentToIndex(scopedId, doc);

            responseWithoutTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + scopedId +  "?refresh=" + false)
            );

            addDocumentToIndex(scopedId, doc);

            responseWithTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?refresh=" + false),
                    tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void deleteRequest_withRoutingParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("a", "a", "b", "b");
        String routing = "test-routing";
        addDocumentToIndex(scopedId, routing, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //valid routing
            HttpResponse responseWithoutTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + scopedId + "?routing=" + routing)
            );

            addDocumentToIndex(scopedId,  routing, doc);

            HttpResponse responseWithTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?routing=" + routing),
                    tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );

            //invalid routing
            addDocumentToIndex(scopedId,  routing, doc);
            routing = routing.concat("-fake");

            responseWithoutTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + scopedId +  "?routing=" + routing)
            );

            responseWithTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?routing=" + routing),
                    tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void deleteRequest_withVersionParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("a", "a", "b", "b");
        DocWriteResponse indexResponse = addDocumentToIndex(scopedId, doc);
        long version = indexResponse.getVersion();
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //valid version
            HttpResponse responseWithoutTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + scopedId + "?version_type=external_gte&version=" + version)
            );

            indexResponse = addDocumentToIndex(scopedId, doc);
            version = indexResponse.getVersion();

            HttpResponse responseWithTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?version_type=external_gte&version=" + version),
                    tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );

            //invalid version
            addDocumentToIndex(scopedId, doc);
            version = 1;

            responseWithoutTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + scopedId +  "?version_type=external_gte&version=" + version)
            );

            responseWithTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + DOC_ID + "?version_type=external_gte&version=" + version),
                    tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_CONFLICT));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_CONFLICT));
            assertThat(responseWithoutTenant.getBody(), equalTo(responseWithTenant.getBody())); //todo response contains scoped id since an exception is thrown and we do not modify it's message
        }
    }

    @Test
    public void deleteRequest_docDoesNotExist() throws Exception {
        String scopedId = scopedId(DOC_ID);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            HttpResponse responseWithoutTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + scopedId)
            );

            HttpResponse responseWithTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_doc/" + DOC_ID),
                    tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutAutoIncrementedFields(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutAutoIncrementedFields(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void deleteRequest_indexDoesNotExist() throws Exception {
        String missingIndex = KIBANA_INDEX.concat("_1.1.1");
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            HttpResponse responseWithoutTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + missingIndex + "/_doc/" + DOC_ID)
            );

            HttpResponse responseWithTenant = client.delete(
                    appendWaitForAllActiveShardsParam(
                            "/" + missingIndex + "/_doc/" + DOC_ID),
                    tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithoutTenant.getBody(), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void clusterSearchShardsRequest_withoutParams() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search_shards"
            );

            HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search_shards",
                    tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_INTERNAL_SERVER_ERROR));
            assertThat(responseWithoutTenant.getBody(), containsString(KIBANA_INDEX));
            assertThat(responseWithTenant.getBody(), containsString("security_exception"));
        }
    }

    @Test
    public void clusterSearchShardsRequest_withAllowNoIndicesParam() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            String indexPattern = KIBANA_INDEX + "_1.1.1";
            //allow no indices true
            HttpResponse responseWithoutTenant = client.get(
                    "/" + indexPattern + "/_search_shards?allow_no_indices=" + true
            );

            HttpResponse responseWithTenant = client.get(
                    "/" + indexPattern + "/_search_shards?allow_no_indices=" + true,
                    tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_INTERNAL_SERVER_ERROR));
            assertThat(responseWithoutTenant.getBody(), not(containsString(KIBANA_INDEX)));
            assertThat(responseWithTenant.getBody(), containsString("security_exception"));

            //allow no indices false
            responseWithoutTenant = client.get(
                    "/" + indexPattern + "/_search_shards?allow_no_indices=" + false
            );

            responseWithTenant = client.get(
                    "/" + indexPattern + "/_search_shards?allow_no_indices=" + false,
                    tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_INTERNAL_SERVER_ERROR));
            assertThat(responseWithTenant.getBody(), containsString("security_exception"));
        }
    }

    @Test
    public void clusterSearchShardsRequest_withIgnoreUnavailableParam() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            String indexPattern = KIBANA_INDEX + "_1.1.1";
            //ignore unavailable true
            HttpResponse responseWithoutTenant = client.get(
                    "/" + indexPattern + "/_search_shards?ignore_unavailable=" + true
            );

            HttpResponse responseWithTenant = client.get(
                    "/" + indexPattern + "/_search_shards?ignore_unavailable=" + true,
                    tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_INTERNAL_SERVER_ERROR));
            assertThat(responseWithoutTenant.getBody(), not(containsString(KIBANA_INDEX)));
            assertThat(responseWithTenant.getBody(), containsString("security_exception"));

            //ignore unavailable false
            responseWithoutTenant = client.get(
                    "/" + indexPattern + "/_search_shards?ignore_unavailable=" + false
            );

            responseWithTenant = client.get(
                    "/" + indexPattern + "/_search_shards?ignore_unavailable=" + false,
                    tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_INTERNAL_SERVER_ERROR));
            assertThat(responseWithTenant.getBody(), containsString("security_exception"));
        }
    }

    @Test
    public void clusterSearchShardsRequest_withLocalParam() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //local true
            HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search_shards?local=" + true
            );

            HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search_shards?local=" + true,
                    tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_INTERNAL_SERVER_ERROR));
            assertThat(responseWithoutTenant.getBody(), containsString(KIBANA_INDEX));
            assertThat(responseWithTenant.getBody(), containsString("security_exception"));

            //local false
            responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search_shards?local=" + false
            );

            responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search_shards?local=" + false,
                    tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_INTERNAL_SERVER_ERROR));
            assertThat(responseWithoutTenant.getBody(), containsString(KIBANA_INDEX));
            assertThat(responseWithTenant.getBody(), containsString("security_exception"));
        }
    }

    @Test
    public void clusterSearchShardsRequest_withPreferenceParam() throws Exception {
        String preference = "_local";
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //correct preference
            HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search_shards?preference=" + preference
            );
            HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search_shards?preference=" + preference, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_INTERNAL_SERVER_ERROR));
            assertThat(responseWithoutTenant.getBody(), containsString(KIBANA_INDEX));
            assertThat(responseWithTenant.getBody(), containsString("security_exception"));

            //wrong preference
            preference = preference.concat("-fake");

            responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search_shards?preference=" + preference
            );
            responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search_shards?preference=" + preference, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_INTERNAL_SERVER_ERROR));
            assertThat(responseWithTenant.getBody(), containsString("security_exception"));
        }
    }

    @Test
    public void multiGetRequest_withoutParams() throws Exception {
        String firstId = "1";
        String firstScopedId = scopedId(firstId);
        String secondId = "2";
        String secondScopedId = scopedId(secondId);
        addDocumentToIndex(firstScopedId, DocNode.of("a", "first"));
        addDocumentToIndex(secondScopedId, DocNode.of("a", "second"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            HttpResponse responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget", multiGetReqBody(firstScopedId, secondScopedId)
            );
            HttpResponse responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget", multiGetReqBody(firstId, secondId), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, firstId, secondId), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void multiGetRequest_withPreferenceParam() throws Exception {
        String firstId = "1";
        String firstScopedId = scopedId(firstId);
        String secondId = "2";
        String secondScopedId = scopedId(secondId);
        addDocumentToIndex(firstScopedId, DocNode.of("a", "first"));
        addDocumentToIndex(secondScopedId, DocNode.of("a", "second"));

        String preference = "_local";

        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //correct preference
            HttpResponse responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?preference=" + preference, multiGetReqBody(firstScopedId, secondScopedId)
            );
            HttpResponse responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?preference=" + preference, multiGetReqBody(firstId, secondId), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, firstId, secondId), equalTo(responseWithTenant.getBody()));

            //wrong preference
            preference = preference.concat("-fake");
            responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?preference=" + preference, multiGetReqBody(firstScopedId, secondScopedId)
            );
            responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?preference=" + preference, multiGetReqBody(firstId, secondId), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBody(), containsString("error"));
            assertThat(responseWithTenant.getBody(), containsString("error"));
            assertThat(unscopeResponseBody(responseWithoutTenant, firstId, secondId), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void multiGetRequest_withRealtimeParam() throws Exception {
        String firstId = "1";
        String firstScopedId = scopedId(firstId);
        String secondId = "2";
        String secondScopedId = scopedId(secondId);
        addDocumentToIndex(firstScopedId, DocNode.of("a", "first"));
        addDocumentToIndex(secondScopedId, DocNode.of("a", "second"));

        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //realtime true
            HttpResponse responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?realtime=" + true, multiGetReqBody(firstScopedId, secondScopedId)
            );
            HttpResponse responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?realtime=" + true, multiGetReqBody(firstId, secondId), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, firstId, secondId), equalTo(responseWithTenant.getBody()));

            //realtime false
            responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?realtime=" + false, multiGetReqBody(firstScopedId, secondScopedId)
            );
            responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?realtime=" + false, multiGetReqBody(firstId, secondId), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, firstId, secondId), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void multiGetRequest_withRefreshParam() throws Exception {
        String firstId = "1";
        String firstScopedId = scopedId(firstId);
        String secondId = "2";
        String secondScopedId = scopedId(secondId);
        addDocumentToIndex(firstScopedId, DocNode.of("a", "first"));
        addDocumentToIndex(secondScopedId, DocNode.of("a", "second"));

        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //refresh true
            HttpResponse responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?refresh=" + true, multiGetReqBody(firstScopedId, secondScopedId)
            );
            HttpResponse responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?refresh=" + true, multiGetReqBody(firstId, secondId), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, firstId, secondId), equalTo(responseWithTenant.getBody()));

            //refresh false
            responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?refresh=" + false, multiGetReqBody(firstScopedId, secondScopedId)
            );
            responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?refresh=" + false, multiGetReqBody(firstId, secondId), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, firstId, secondId), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void multiGetRequest_withRoutingParam() throws Exception {
        String firstId = "1";
        String firstScopedId = scopedId(firstId);
        String secondId = "2";
        String secondScopedId = scopedId(secondId);
        String routing = "test-routing";
        addDocumentToIndex(firstScopedId, routing, DocNode.of("a", "first"));
        addDocumentToIndex(secondScopedId, routing, DocNode.of("a", "second"));

        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //correct routing
            HttpResponse responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?routing=" + routing, multiGetReqBody(firstScopedId, secondScopedId)
            );
            HttpResponse responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?routing=" + routing, multiGetReqBody(firstId, secondId), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, firstId, secondId), equalTo(responseWithTenant.getBody()));

            //wrong routing
            routing = routing.concat("-fake");
            responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?routing=" + routing, multiGetReqBody(firstScopedId, secondScopedId)
            );
            responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?routing=" + routing, multiGetReqBody(firstId, secondId), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBody(), containsString("\"found\":false"));
            assertThat(unscopeResponseBody(responseWithoutTenant, firstId, secondId), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void multiGetRequest_withStoredFieldsParam() throws Exception {
        String firstId = "1";
        String firstScopedId = scopedId(firstId);
        String secondId = "2";
        String secondScopedId = scopedId(secondId);
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
        addDocumentToIndex(firstScopedId, DocNode.of("aa", "aa-first", "ab", "ab-first", "bb", "bb-first"));
        addDocumentToIndex(secondScopedId, DocNode.of("aa", "aa-second", "ab", "ab-second", "bb", "bb-second"));

        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //valid stored fields
            HttpResponse responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?stored_fields=aa,ab", multiGetReqBody(firstScopedId, secondScopedId)
            );
            HttpResponse responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?stored_fields=aa,ab", multiGetReqBody(firstId, secondId), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, firstId, secondId), equalTo(responseWithTenant.getBody()));

            //invalid stored fields
            responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?stored_fields=bb", multiGetReqBody(firstScopedId, secondScopedId)
            );
            responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?stored_fields=bb", multiGetReqBody(firstId, secondId), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, firstId, secondId), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void multiGetRequest_withSourceParam() throws Exception {
        String firstId = "1";
        String firstScopedId = scopedId(firstId);
        String secondId = "2";
        String secondScopedId = scopedId(secondId);
        addDocumentToIndex(firstScopedId, DocNode.of("a", "first"));
        addDocumentToIndex(secondScopedId, DocNode.of("a", "second"));

        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //source = false
            HttpResponse responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?_source=" + false, multiGetReqBody(firstScopedId, secondScopedId)
            );
            HttpResponse responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?_source=" + false, multiGetReqBody(firstId, secondId), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, firstId, secondId), equalTo(responseWithTenant.getBody()));

            //source = true
            responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?_source=" + true, multiGetReqBody(firstScopedId, secondScopedId)
            );
            responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?_source=" + true, multiGetReqBody(firstId, secondId), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, firstId, secondId), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void multiGequest_withSourceIncludesAndSourceExcludesParams() throws Exception {
        String firstId = "1";
        String firstScopedId = scopedId(firstId);
        String secondId = "2";
        String secondScopedId = scopedId(secondId);
        addDocumentToIndex(firstScopedId, DocNode.of("a", "a-first", "ab", "ab-first", "c", "c-first"));
        addDocumentToIndex(secondScopedId, DocNode.of("a", "a-second", "ab", "ab-second", "c", "c-second"));

        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //include and exclude
            HttpResponse responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?_source_includes=a*&_source_excludes=ab", multiGetReqBody(firstScopedId, secondScopedId)
            );
            HttpResponse responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?_source_includes=a*&_source_excludes=ab", multiGetReqBody(firstId, secondId), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, firstId, secondId), equalTo(responseWithTenant.getBody()));

            //only include
            responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?_source=a*", multiGetReqBody(firstScopedId, secondScopedId)
            );
            responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget?_source=a*", multiGetReqBody(firstId, secondId), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, firstId, secondId), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void multiGetRequest_docsDoNotExist() throws Exception {
        String firstId = "1";
        String firstScopedId = scopedId(firstId);
        String secondId = "2";
        String secondScopedId = scopedId(secondId);
        String thirdId = "3";
        String thirdScopedId = scopedId(thirdId);
        String fourthId = "4";
        String fourthScopedId = scopedId(fourthId);
        addDocumentToIndex(firstScopedId, DocNode.of("aa", "aa-first", "ab", "ab-first", "bb", "bb-first"));

        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //one of docs does not exist
            HttpResponse responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget", multiGetReqBody(firstScopedId, secondScopedId)
            );
            HttpResponse responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget", multiGetReqBody(firstId, secondId), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, firstId, secondId), equalTo(responseWithTenant.getBody()));

            //all docs do not exist
            responseWithoutTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget", multiGetReqBody(thirdScopedId, fourthScopedId)
            );
            responseWithTenant = client.postJson(
                    "/" + KIBANA_INDEX + "/_mget", multiGetReqBody(thirdId, fourthId), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, thirdId, fourthId), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void multiGetRequest_indexDoesNotExist() throws Exception {
        String firstMissingIndex = KIBANA_INDEX.concat("_1.1.1");
        String secondMissingIndex = KIBANA_INDEX.concat("_2.2.2");
        String firstId = "1";
        String firstScopedId = scopedId(firstId);
        String secondId = "2";
        String secondScopedId = scopedId(secondId);
        addDocumentToIndex(firstScopedId, DocNode.of("aa", "aa-first"));
        addDocumentToIndex(secondScopedId, DocNode.of("aa", "aa-second"));

        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //one of indices does not exist
            HttpResponse responseWithoutTenant = client.postJson(
                    "/_mget", multiGetReqBody(Tuple.tuple(KIBANA_INDEX, Arrays.asList(firstScopedId, secondScopedId)),
                            Tuple.tuple(firstMissingIndex, Arrays.asList(firstScopedId, secondScopedId)))
            );
            HttpResponse responseWithTenant = client.postJson(
                    "/_mget", multiGetReqBody(Tuple.tuple(KIBANA_INDEX, Arrays.asList(firstId, secondId)),
                            Tuple.tuple(firstMissingIndex, Arrays.asList(firstId, secondId))), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, firstId, secondId), equalTo(responseWithTenant.getBody()));

            //all indices do not exist
            responseWithoutTenant = client.postJson(
                    "/_mget", multiGetReqBody(Tuple.tuple(firstMissingIndex, Arrays.asList(firstScopedId, secondScopedId)),
                            Tuple.tuple(secondMissingIndex, Arrays.asList(firstScopedId, secondScopedId)))
            );
            responseWithTenant = client.postJson(
                    "/_mget", multiGetReqBody(Tuple.tuple(firstMissingIndex, Arrays.asList(firstId, secondId)),
                            Tuple.tuple(secondMissingIndex, Arrays.asList(firstId, secondId))), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(unscopeResponseBody(responseWithoutTenant, firstId, secondId), equalTo(responseWithTenant.getBody()));
        }
    }

    @Test
    public void searchRequest_withoutParams() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("a", "a", "sg_tenant", internalTenantName()));
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            HttpResponse responseWithoutTenant = client.get("/" + KIBANA_INDEX + "/_search/");
            HttpResponse responseWithTenant = client.get("/" + KIBANA_INDEX + "/_search/", tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void searchRequest_withPointInTimeQuery_success() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("a", "a", "sg_tenant", internalTenantName()));
        try (GenericRestClient client = cluster.getRestClient(LIMITED_USER)) {
            HttpResponse response = client.post("/" + KIBANA_INDEX + "/_pit?keep_alive=500ms", tenantHeader());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            String pitId = response.getBodyAsDocNode().getAsString("id");
            assertThat(pitId, not(emptyOrNullString()));
            DocNode searchRequestBody = DocNode.of("pit", DocNode.of("id", pitId, "keep_alive", "250ms"));

            HttpResponse responseWithoutTenant = client.postJson("/_search/", searchRequestBody);
            HttpResponse responseWithTenant = client.postJson("/_search/", searchRequestBody, tenantHeader());
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant), DOC_ID),
                equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithTenant).toJsonString())
            );
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("$", "pit_id"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("$", "pit_id"));
        }
    }

    @Test
    public void searchRequest_withPointInTimeQuery_failure() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("a", "a", "sg_tenant", internalTenantName()));
        try (GenericRestClient client = cluster.getRestClient(LIMITED_USER)) {
            //user is allowed to access HR tenant
            BasicHeader tenantHeader = new BasicHeader("sg_tenant", HR_TENANT.getName());
            HttpResponse response = client.post("/" + KIBANA_INDEX + "/_pit?keep_alive=500ms", tenantHeader);
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            String pitId = response.getBodyAsDocNode().getAsString("id");
            assertThat(pitId, not(emptyOrNullString()));
            DocNode searchRequestBody = DocNode.of("pit", DocNode.of("id", pitId, "keep_alive", "250ms"));

            //user is not allowed to access IT tenant
            tenantHeader = new BasicHeader("sg_tenant", IT_TENANT.getName());
            HttpResponse responseWithTenant = client.postJson("/_search/", searchRequestBody, tenantHeader);
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(SC_FORBIDDEN));
        }
    }

    @Test
    public void searchRequest_withAllowNoIndicesParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("a", "a", "sg_tenant", internalTenantName()));
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            //allow no indices true
            HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + ",*_1.1.1" + "/_search?allow_no_indices=" + true
            );
            HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + ",*_1.1.1" + "/_search?allow_no_indices=" + true, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void searchRequest_withDocValueFieldsParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode indexMappings = DocNode.parse(Format.JSON).from("""
                {
                  "properties": {
                    "aa": {
                      "type": "keyword"
                    },
                    "ab": {
                      "type": "keyword"
                    }
                  }
                }
                """);
        updateIndexMappings(indexMappings);
        addDocumentToIndex(scopedId, DocNode.of("aa", "a", "ab", "ab", "sg_tenant", internalTenantName()));
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?docvalue_fields=a*"
            );
            HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?docvalue_fields=a*", tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void searchRequest_withExplainParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("aa", "a", "sg_tenant", internalTenantName()));
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            //explain false
            HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?explain=" + false
            );
            HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?explain=" + false, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithTenant).toJsonString())
            );

            //explain true
            responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?explain=" + true
            );
            responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?explain=" + true, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getBody(), containsString("_explanation"));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getBody(), containsString("_explanation"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant), DOC_ID),
                    not(equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithTenant).toJsonString()))
            );
            //todo _explanation may differ since we're adding query for sg_tenant field
            assertThat(
                    unscopeResponseBody(
                            removeAttributesFromSearchHits(
                                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant), "_explanation"), DOC_ID
                    ),
                    equalTo(removeAttributesFromSearchHits(
                            responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithTenant), "_explanation")
                            .toJsonString()
                    )
            );
        }
    }

    @Test
    public void searchRequest_withIgnoreUnavailableParam() throws Exception {
        String indexName = KIBANA_INDEX.concat("_1.1.1");
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            //ignore unavailable true
            HttpResponse responseWithoutTenant = client.get(
                    "/" + indexName + "/_search?ignore_unavailable=" + true
            );
            HttpResponse responseWithTenant = client.get(
                    "/" + indexName + "/_search?ignore_unavailable=" + true, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );
        }
    }

    @Test
    public void searchRequest_withSeqNoPrimaryTermParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("aa", "a", "sg_tenant", internalTenantName()));

        try (GenericRestClient client = cluster.getRestClient(USER)) {
            //seq no primary term true
            HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?seq_no_primary_term=" + true
            );
            HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?seq_no_primary_term=" + true, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );

            //seq no primary term false
            responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?seq_no_primary_term=" + false
            );
            responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?seq_no_primary_term=" + false, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );
        }
    }

    @Test
    public void searchRequest_withSizeSortAndFromParams() throws Exception {
        IntStream.rangeClosed(1, 10).forEach( docNo -> {
            String id = String.valueOf(docNo);
            addDocumentToIndex(scopedId(id), DocNode.of("field", docNo, "sg_tenant", internalTenantName()));
        });

        try (GenericRestClient client = cluster.getRestClient(USER)) {
            //size 3, sort desc
            int size = 3;
            String sort = "field:desc";
            HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?size=" + size + "&sort=" + sort
            );
            HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?size=" + size + "&sort=" + sort, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );

            //size 4, sort desc, from 5
            size = 4;
            sort = "field:desc";
            int from = 5;
            responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?size=" + size + "&sort=" + sort + "&from=" + from
            );
            responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?size=" + size + "&sort=" + sort + "&from=" + from, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );

            //size 5, sort asc
            size = 5;
            sort = "field:asc";
            responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?size=" + size + "&sort=" + sort
            );
            responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?size=" + size + "&sort=" + sort, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );

            //size 20, sort desc
            size = 20;
            sort = "field:desc";
            responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?size=" + size + "&sort=" + sort
            );
            responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?size=" + size + "&sort=" + sort, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );

            //size 15, sort asc, from 2
            size = 15;
            sort = "field:asc";
            from = 2;
            responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?size=" + size + "&sort=" + sort + "&from=" + from
            );
            responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?size=" + size + "&sort=" + sort + "&from=" + from, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );
        }
    }

    @Test
    public void searchRequest_withSourceParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("aa", "a", "sg_tenant", internalTenantName()));

        try (GenericRestClient client = cluster.getRestClient(USER)) {
            //source true
            HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?_source=" + true
            );
            HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?_source=" + true, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );

            //source false
            responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?_source=" + false
            );
            responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?_source=" + false, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );
        }
    }

    @Test
    public void searchRequest_withSourceIncludesAndSourceExcludesParams() throws Exception {
        IntStream.rangeClosed(1, 10).forEach( docNo -> {
            String id = String.valueOf(docNo);
            addDocumentToIndex(
                    scopedId(id), DocNode.of("a", "a" + docNo, "ab", "ab" + docNo,
                            "c", "c" + docNo, "sg_tenant", internalTenantName())
            );
        });

        try (GenericRestClient client = cluster.getRestClient(USER)) {
            //include and exclude
            HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?_source_includes=a*,c&_source_excludes=ab"
            );
            HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?_source_includes=a*,c&_source_excludes=ab", tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );

            //only include
            responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?_source=a*"
            );
            responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?_source=a*", tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );
        }
    }

    @Test
    public void searchRequest_withStoredFieldsParam() throws Exception {
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
        IntStream.rangeClosed(1, 10).forEach( docNo -> {
            String id = String.valueOf(docNo);
            addDocumentToIndex(
                    scopedId(id), DocNode.of("aa", "a" + docNo, "ab", "ab" + docNo,
                            "bb", "bb" + docNo, "sg_tenant", internalTenantName())
            );
        });

        try (GenericRestClient client = cluster.getRestClient(USER)) {
            //valid stored fields
            HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?stored_fields=aa,ab"
            );
            HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?stored_fields=aa,ab", tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );

            //invalid stored fields
            responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?stored_fields=bb"
            );
            responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?stored_fields=bb", tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );
        }
    }

    @Test
    public void searchRequest_withVersionParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("aa", "a", "sg_tenant", internalTenantName()));

        try (GenericRestClient client = cluster.getRestClient(USER)) {
            //version true
            HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?version=" + true
            );
            HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?version=" + true, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );

            //version false
            responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?version=" + false
            );
            responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?version=" + false, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );
        }
    }

    @Test
    public void searchRequest_withQParam() throws Exception {
        IntStream.rangeClosed(1, 10).forEach( docNo -> {
            String id = String.valueOf(docNo);
            addDocumentToIndex(
                    scopedId(id), DocNode.of("field", docNo, "sg_tenant", internalTenantName())
            );
        });

        try (GenericRestClient client = cluster.getRestClient(USER)) {
            HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?q=" + URLEncoder.encode("field:[2 TO 5]", StandardCharsets.UTF_8)
            );
            HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?q=" + URLEncoder.encode("field:[2 TO 5]", StandardCharsets.UTF_8), tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );
        }
    }

    @Test
    public void searchRequest_withRestTotalHitsAsIntParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("a", "a"));

        try (GenericRestClient client = cluster.getRestClient(USER)) {
            //rest_total_hits_as_int true
            HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?rest_total_hits_as_int=" + true
            );
            HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?rest_total_hits_as_int=" + true, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );

            //rest_total_hits_as_int false
            responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?rest_total_hits_as_int=" + false
            );
            responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?rest_total_hits_as_int=" + false, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );
        }
    }

    @Test
    public void searchRequest_withScrollParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("a", "a"));

        try (GenericRestClient client = cluster.getRestClient(USER)) {
            HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?scroll=" + "1d"
            );

            HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?scroll=" + "1d", tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );
        }
    }

    @Test
    public void searchRequest_withSuggestFieldSuggestModeSuggestTextAndSuggestSizeParams() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("a", "value1", "b", "test", "sg_tenant", internalTenantName()));
        addDocumentToIndex(scopedId("456"), DocNode.of("a", "value2", "b", "test", "sg_tenant", internalTenantName()));

        try (GenericRestClient client = cluster.getRestClient(USER)) {
            HttpResponse responseWithoutTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?q=a:value*&suggest_field=a&suggest_mode=always&suggest_text=value&suggest_size=1"
            );

            HttpResponse responseWithTenant = client.get(
                    "/" + KIBANA_INDEX + "/_search?q=a:value*&suggest_field=a&suggest_mode=always&suggest_text=value&suggest_size=1",
                    tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString(),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForSearchRequests(responseWithoutTenant).toJsonString())
            );
        }
    }

    @Test
    public void bulkRequest_createAction_withoutParams() throws Exception {
        String scopedId = scopedId(DOC_ID);
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            //doc does not exist
            String bulkReqBody = bulkCreateReqBody(scopedId, DocNode.of("a", "a", "b", "b"));
            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), bulkReqBody
            );

            deleteDoc(scopedId);

            bulkReqBody = bulkCreateReqBody(DOC_ID, DocNode.of("a", "a", "b", "b"));
            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), bulkReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "create"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "create"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString())
            );

            //doc exists
            bulkReqBody = bulkCreateReqBody(scopedId, DocNode.of("a", "a", "b", "b"));
            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), bulkReqBody
            );

            bulkReqBody = bulkCreateReqBody(DOC_ID, DocNode.of("a", "a", "b", "b"));
            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), bulkReqBody, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "create"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "create"));

            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    not(equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString()))
            ); //todo response (item.error) contains scoped id since an exception is thrown and we do not modify it's message

            assertThat(responseWithoutTenant.getBody(), containsString("document already exists"));
            assertThat(responseWithTenant.getBody(), containsString("document already exists"));

            assertThat(
                    unscopeResponseBody(
                            removeAttributesFromBulkItems(
                                    responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant),
                                    "error"), DOC_ID
                    ),
                    equalTo(removeAttributesFromBulkItems(
                            responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant), "error")
                            .toJsonString()
                    )
            );
        }
    }

    @Test
    public void bulkRequest_indexAction_withoutParams() throws Exception {
        String scopedId = scopedId(DOC_ID);
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            //doc does not exist
            String bulkReqBody = bulkIndexReqBody(scopedId, DocNode.of("a", "a", "b", "b"));
            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), bulkReqBody
            );

            deleteDoc(scopedId);

            bulkReqBody = bulkIndexReqBody(DOC_ID, DocNode.of("a", "a", "b", "b"));
            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), bulkReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "index"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "index"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString())
            );

            //doc exists
            bulkReqBody = bulkIndexReqBody(scopedId, DocNode.of("c", "c"));
            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), bulkReqBody
            );

            bulkReqBody = bulkIndexReqBody(DOC_ID, DocNode.of("d", "d"));
            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), bulkReqBody, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "index"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "index"));

            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void bulkRequest_updateAction_withoutParams() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("a", "a"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            //doc exist
            String bulkReqBody = bulkUpdateReqBody(scopedId, DocNode.of("b", "b"));
            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), bulkReqBody
            );

            bulkReqBody = bulkUpdateReqBody(DOC_ID, DocNode.of("c", "c"));
            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), bulkReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "update"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "update"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString())
            );

            //doc does not exist
            deleteDoc(scopedId);

            bulkReqBody = bulkUpdateReqBody(scopedId, DocNode.of("c", "c"));
            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), bulkReqBody
            );

            bulkReqBody = bulkUpdateReqBody(DOC_ID, DocNode.of("d", "d"));
            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), bulkReqBody, tenantHeader()
            );

            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "update"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "update"));

            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    not(equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString()))
            ); //todo response (item.error) contains scoped id since an exception is thrown and we do not modify it's message

            assertThat(responseWithoutTenant.getBody(), containsString("document missing"));
            assertThat(responseWithTenant.getBody(), containsString("document missing"));

            assertThat(
                    unscopeResponseBody(
                            removeAttributesFromBulkItems(
                                    responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant),
                                    "error"), DOC_ID
                    ),
                    equalTo(removeAttributesFromBulkItems(
                            responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant), "error")
                            .toJsonString()
                    )
            );
        }
    }

    @Test
    public void bulkRequest_deleteAction_withoutParams() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("a", "a"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            //doc exists
            String  bulkReqBody = bulkDeleteReqBody(scopedId);
            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), bulkReqBody
            );

            addDocumentToIndex(scopedId, DocNode.of("a", "a"));

            bulkReqBody = bulkDeleteReqBody(DOC_ID);
            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), bulkReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "delete"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "delete"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString())
            );

            //doc does not exist
            bulkReqBody = bulkDeleteReqBody(scopedId);
            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), bulkReqBody
            );

            bulkReqBody = bulkDeleteReqBody(DOC_ID);
            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), bulkReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "delete"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "delete"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void bulkRequest_allActions_indexDoesNotExist() throws Exception {
        String index = KIBANA_INDEX.concat("_1.1.1");
        String scopedId = scopedId(DOC_ID);
        DocNode docContent = DocNode.of("a", "a");
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            //create
            String  bulkReqBody = bulkCreateReqBody(scopedId, docContent);
            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + index + "/_bulk/"), bulkReqBody
            );

            deleteIndex(index);

            bulkReqBody = bulkCreateReqBody(DOC_ID, docContent);
            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + index + "/_bulk/"), bulkReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "create"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "create"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString())
            );

            //index
            deleteIndex(index);

            bulkReqBody = bulkIndexReqBody(scopedId, docContent);
            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + index + "/_bulk/"), bulkReqBody
            );

            deleteIndex(index);

            bulkReqBody = bulkIndexReqBody(DOC_ID, docContent);
            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + index + "/_bulk/"), bulkReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "index"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "index"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString())
            );

            //update
            deleteIndex(index);

            bulkReqBody = bulkUpdateReqBody(scopedId, docContent);
            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + index + "/_bulk/"), bulkReqBody
            );

            deleteIndex(index);

            bulkReqBody = bulkUpdateReqBody(DOC_ID, docContent);
            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + index + "/_bulk/"), bulkReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "update"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "update"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    not(equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString()))
            ); //todo response (item.error) contains scoped id since an exception is thrown and we do not modify it's message

            assertThat(responseWithoutTenant.getBody(), containsString("document missing"));
            assertThat(responseWithTenant.getBody(), containsString("document missing"));

            assertThat(
                    unscopeResponseBody(
                            removeAttributesFromBulkItems(
                                    responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant),
                                    "error"), DOC_ID
                    ),
                    equalTo(removeAttributesFromBulkItems(
                            responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant), "error")
                            .toJsonString()
                    )
            );

            //delete
            bulkReqBody = bulkDeleteReqBody(scopedId);
            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), bulkReqBody
            );

            bulkReqBody = bulkDeleteReqBody(DOC_ID);
            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), bulkReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "delete"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "delete"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void bulkRequest_manyActionsInOneRequest() throws Exception {
        String firstId = "1";
        String firstScopedId = scopedId(firstId);
        String secondId = "2";
        String secondScopedId = scopedId(secondId);
        String thirdId = "3";
        String thirdScopedId = scopedId(thirdId);
        addDocumentToIndex(firstScopedId, DocNode.of("a", "a"));
        addDocumentToIndex(secondScopedId, DocNode.of("a", "a"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            String bulkCreateReqBody = bulkCreateReqBody(thirdScopedId, DocNode.of("a", "a"));
            String bulkIndexReqBody = bulkIndexReqBody(firstScopedId, DocNode.of("b", "b"));
            String bulkUpdateReqBody = bulkUpdateReqBody(secondScopedId, DocNode.of("b", "b"));
            String bulkDeleteReqBody = bulkDeleteReqBody(secondScopedId);
            String reqBodyWithManyActions = String.join("", bulkCreateReqBody, bulkIndexReqBody, bulkUpdateReqBody, bulkDeleteReqBody);

            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), reqBodyWithManyActions
            );

            deleteDoc(thirdScopedId);
            addDocumentToIndex(secondScopedId, DocNode.of("a", "a"));

            bulkCreateReqBody = bulkCreateReqBody(thirdId, DocNode.of("a", "a"));
            bulkIndexReqBody = bulkIndexReqBody(firstId, DocNode.of("b", "b"));
            bulkUpdateReqBody = bulkUpdateReqBody(secondId, DocNode.of("b", "b"));
            bulkDeleteReqBody = bulkDeleteReqBody(secondId);
            reqBodyWithManyActions = String.join("", bulkCreateReqBody, bulkIndexReqBody, bulkUpdateReqBody, bulkDeleteReqBody);

            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk/"), reqBodyWithManyActions, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "create"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "create"));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[1]", "index"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[1]", "index"));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[2]", "update"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[2]", "update"));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[3]", "delete"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[3]", "delete"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), firstId, secondId, thirdId),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void bulkRequest_withRequireAliasParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("a", "a"));
        String aliasName = KIBANA_INDEX.concat("_1.1.1");
        addAliasToIndex(aliasName);

        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //alias required, alias name provided
            String  bulkReqBody = bulkIndexReqBody(scopedId, DocNode.of("a", "a"));
            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + aliasName + "/_bulk?require_alias=" + true), bulkReqBody
            );

            bulkReqBody = bulkIndexReqBody(DOC_ID, DocNode.of("a", "a"));
            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + aliasName + "/_bulk?require_alias=" + true), bulkReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "index"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "index"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString())
            );

            //alias required, index name provided
            bulkReqBody = bulkIndexReqBody(scopedId, DocNode.of("a", "a"));
            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk?require_alias=" + true), bulkReqBody
            );

            bulkReqBody = bulkIndexReqBody(DOC_ID, DocNode.of("a", "a"));
            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk?require_alias=" + true), bulkReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "index"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "index"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void bulkRequest_withRoutingParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        String routing = "test-routing";

        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //correct routing
            addDocumentToIndex(scopedId, routing, DocNode.of("a", "a"));

            String  bulkReqBody = bulkDeleteReqBody(scopedId);
            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk?routing=" + routing), bulkReqBody
            );

            addDocumentToIndex(scopedId, routing, DocNode.of("a", "a"));

            bulkReqBody = bulkDeleteReqBody(DOC_ID);
            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk?routing=" + routing), bulkReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "delete"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "delete"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString())
            );

            //wrong routing
            addDocumentToIndex(scopedId, routing, DocNode.of("a", "a"));

            routing = routing.concat("-fake");
            bulkReqBody = bulkDeleteReqBody(scopedId);
            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk?routing=" + routing), bulkReqBody
            );

            bulkReqBody = bulkDeleteReqBody(DOC_ID);
            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk?routing=" + routing), bulkReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "delete"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "delete"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void bulkRequest_withSourceParam() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode docContent = DocNode.of("a", "a");
        addDocumentToIndex(scopedId, docContent);

        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //source false
            String  bulkReqBody = bulkUpdateReqBody(scopedId, docContent);
            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk?_source=" + false), bulkReqBody
            );

            bulkReqBody = bulkUpdateReqBody(DOC_ID, docContent);
            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk?_source=" + false), bulkReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "update"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "update"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString())
            );

            //source true
            bulkReqBody = bulkUpdateReqBody(scopedId, docContent);
            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk?_source=" + true), bulkReqBody
            );

            bulkReqBody = bulkUpdateReqBody(DOC_ID, docContent);
            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk?_source=" + true), bulkReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "update"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "update"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void bulkRequest_withSourceIncludesAndSourceExcludesParams() throws Exception {
        String scopedId = scopedId(DOC_ID);
        DocNode docContent = DocNode.of("a", "a", "ab", "ab", "b", "b");
        addDocumentToIndex(scopedId, docContent);

        try (GenericRestClient client = cluster.getRestClient(USER)) {

            //include and exclude
            String  bulkReqBody = bulkUpdateReqBody(scopedId, docContent);
            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk?_source_includes=a*&_source_excludes=ab"), bulkReqBody
            );

            bulkReqBody = bulkUpdateReqBody(DOC_ID, docContent);
            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk?_source_includes=a*&_source_excludes=ab"), bulkReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "update"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "update"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString())
            );

            //only include
            bulkReqBody = bulkUpdateReqBody(scopedId, docContent);
            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk?_source=" + "a*"), bulkReqBody
            );

            bulkReqBody = bulkUpdateReqBody(DOC_ID, docContent);
            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_bulk?_source=" + "a*"), bulkReqBody, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithoutTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "update"));
            assertThat(responseWithTenant.getBodyAsDocNode(), containsFieldPointedByJsonPath("items[0]", "update"));
            assertThat(
                    unscopeResponseBody(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithoutTenant), DOC_ID),
                    equalTo(responseBodyWithoutFieldsWhichMayDifferForBulkRequests(responseWithTenant).toJsonString())
            );
        }
    }

    @Test
    public void openPointInTimeRequest_success() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("point", "in", "time", "test"));
        try (GenericRestClient client = cluster.getRestClient(USER)) {
            HttpResponse response = client.post("/" + KIBANA_INDEX + "/_pit?keep_alive=500ms", tenantHeader());

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath("$", "id"));
            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("$", 1));
        }
    }

    @Test
    public void openPointInTimeRequest_failure() throws Exception {
        String scopedId = scopedId(DOC_ID);
        addDocumentToIndex(scopedId, DocNode.of("point", "in", "time", "test"));
        try (GenericRestClient client = cluster.getRestClient(LIMITED_USER)) {
            BasicHeader tenantHeader = new BasicHeader("sg_tenant", IT_TENANT.getName());
            HttpResponse response = client.post("/" + KIBANA_INDEX + "/_pit?keep_alive=500ms", tenantHeader);

            assertThat(response.getStatusCode(), equalTo(SC_FORBIDDEN));
        }
    }

    @Test
    public void shouldNotDeleteFrontendIndexWithReadOnlyPermissions() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(LIMITED_USER, new BasicHeader("sg_tenant", "SGS_GLOBAL_TENANT"))) {
            HttpResponse response = client.delete(KIBANA_INDEX);

            assertThat(response.getStatusCode(), equalTo(SC_FORBIDDEN));
        }
    }

    @Test
    public void updateByQueryRequest_withRefreshParam() throws Exception {
        // refresh set to true, otherwise from time to time the second request ends with 409 CONFLICT
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("a", 1, "b", "value", "sg_tenant", internalTenantName());
        addDocumentToIndex(scopedId, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            // one doc matches query
            String scriptAndQuery = """
                    {
                      "script": {
                        "source": "ctx._source.a += params.add_to_a",
                        "lang": "painless",
                        "params": {
                          "add_to_a": 4
                        }
                      },
                      "query": {
                        "term": {
                          "b": "value"
                        }
                      }
                    }
                    """;

            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update_by_query?refresh=true"), scriptAndQuery
            );

            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update_by_query?refresh=true"), scriptAndQuery, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseWithoutTenant.getBodyAsDocNode().without("took"),
                    equalTo(responseWithTenant.getBodyAsDocNode().without("took"))
            );

            // no doc matches query
            scriptAndQuery = """
                    {
                      "script": {
                        "source": "ctx._source.a += params.add_to_a",
                        "lang": "painless",
                        "params": {
                          "add_to_a": 4
                        }
                      },
                      "query": {
                        "term": {
                          "b": "fake-value"
                        }
                      }
                    }
                    """;

            responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update_by_query?refresh=true"), scriptAndQuery
            );

            responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam("/" + KIBANA_INDEX + "/_update_by_query?refresh=true"), scriptAndQuery, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseWithoutTenant.getBodyAsDocNode().without("took"),
                    equalTo(responseWithTenant.getBodyAsDocNode().without("took"))
            );
        }
    }

    @Test
    public void updateByQueryRequest_withAllowNoIndicesParam() throws Exception {
        // refresh set to true, otherwise from time to time the second request ends with 409 CONFLICT
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("a", 1, "b", "value", "sg_tenant", internalTenantName());
        addDocumentToIndex(scopedId, doc);
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            // allow no indices true
            String scriptAndQuery = """
                    {
                      "script": {
                        "source": "ctx._source.a += params.add_to_a",
                        "lang": "painless",
                        "params": {
                          "add_to_a": 4
                        }
                      },
                      "query": {
                        "term": {
                          "b": "value"
                        }
                      }
                    }
                    """;

            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + ",*_1.1.1" + "/_update_by_query?refresh=true&" + "allow_no_indices=" + true
                    ), scriptAndQuery
            );

            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + ",*_1.1.1" + "/_update_by_query?refresh=true&" + "allow_no_indices=" + true
                    ), scriptAndQuery, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseWithoutTenant.getBodyAsDocNode().without("took"),
                    equalTo(responseWithTenant.getBodyAsDocNode().without("took"))
            );
        }
    }

    @Test
    public void updateByQueryRequest_withPreferenceParam() throws Exception {
        // refresh set to true, otherwise from time to time the second request ends with 409 CONFLICT
        String scopedId = scopedId(DOC_ID);
        DocNode doc = DocNode.of("a", 1, "b", "value", "sg_tenant", internalTenantName());
        addDocumentToIndex(scopedId, doc);
        String preference = "_local";
        try (GenericRestClient client = cluster.getRestClient(USER)) {

            // correct preference
            String scriptAndQuery = """
                    {
                      "script": {
                        "source": "ctx._source.a += params.add_to_a",
                        "lang": "painless",
                        "params": {
                          "add_to_a": 4
                        }
                      },
                      "query": {
                        "term": {
                          "b": "value"
                        }
                      }
                    }
                    """;

            HttpResponse responseWithoutTenant = client.postJson(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_update_by_query?refresh=true&" + "preference=" + preference
                    ), scriptAndQuery
            );

            HttpResponse responseWithTenant = client.postJson(
                    appendWaitForAllActiveShardsParam(
                            "/" + KIBANA_INDEX + "/_update_by_query?refresh=true&" + "preference=" + preference
                    ), scriptAndQuery, tenantHeader()
            );
            assertThat(responseWithoutTenant.getBody(), responseWithoutTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(responseWithTenant.getBody(), responseWithTenant.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(
                    responseWithoutTenant.getBodyAsDocNode().without("took"),
                    equalTo(responseWithTenant.getBodyAsDocNode().without("took"))
            );
        }
    }

    private DocNode responseBodyWithoutAutoIncrementedFields(HttpResponse response) throws Exception {
        ImmutableSet<String> autoIncrementedFields = ImmutableSet.of("_version", "_seq_no");
        String regexPattern = String.format("\"(%s)\":\"?[\\w\\d]*\"?,", String.join("|", autoIncrementedFields));
        String responseWithoutAutoIncrementedFields = response.getBody().replaceAll(regexPattern, "");
        return DocNode.parse(Format.JSON).from(responseWithoutAutoIncrementedFields);
    }

    private DocNode responseBodyWithoutFieldsWhichMayDifferForSearchRequests(HttpResponse response) throws Exception {
        //todo _score and max_score may differ since we're adding query for sg_tenant field
        ImmutableSet<String> excludedFields = ImmutableSet.of("took", "_score", "max_score");
        String regexPattern = String.format("\"(%s)\":\"?[\\w\\d\\.]*\"?,", String.join("|", excludedFields));
        String responseWithoutExcludedFields = response.getBody().replaceAll(regexPattern, "");
        return DocNode.parse(Format.JSON).from(responseWithoutExcludedFields);
    }

    private DocNode responseBodyWithoutFieldsWhichMayDifferForBulkRequests(HttpResponse response) throws Exception {
        return responseBodyWithoutAutoIncrementedFields(response).without("took");
    }

    private DocNode removeAttributesFromSearchHits(DocNode searchResponseBody, String... attributes) {
        DocNode hits = searchResponseBody.getAsNode("hits");
        List<DocNode> hitEntries = hits.getAsListOfNodes("hits");
        List<DocNode> hitEntriesWithoutAttributes = hitEntries.stream().map(entry -> {
            for (String attribute : attributes) {
                entry = entry.without(attribute);
            }
            return entry;
        }).toList();
        hits = hits.with("hits", hitEntriesWithoutAttributes);
        return searchResponseBody.with("hits", hits);
    }

    private DocNode removeAttributesFromBulkItems(DocNode bulkResponseBody, String... attributes) {
        ImmutableList<DocNode> items = bulkResponseBody.getAsListOfNodes("items");
        List<DocNode> itemsWithoutAttributes = items.stream().map(item -> {
            for(DocWriteRequest.OpType opType : DocWriteRequest.OpType.values()) {
                String opTypeName = opType.name().toLowerCase();
                DocNode itemDetails = item.getAsNode(opTypeName);
                if (!itemDetails.isNull()) {
                    for (String attribute : attributes) {
                        itemDetails = itemDetails.without(attribute);
                    }
                    item = item.with(opTypeName, itemDetails);
                }
            }
            return item;
        }).toList();
        return bulkResponseBody.with("items", itemsWithoutAttributes);
    }

    private String unscopeResponseBody(HttpResponse response, String... ids) throws Exception {
        return unscopeResponseBody(response.getBodyAsDocNode(), ids);
    }

    private String unscopeResponseBody(DocNode response, String... ids) {
        String responseJson = response.toJsonString();
        for (String id : ids) {
            responseJson = responseJson.replaceAll(scopedId(id), id);
        }
        return responseJson;
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

    private DocWriteResponse addDocumentToIndex(String id, String routing, DocNode doc) {
        Client client = cluster.getInternalNodeClient();
        DocWriteResponse indexResponse = client.index(new IndexRequest(KIBANA_INDEX).id(id).source(doc)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).routing(routing)).actionGet();
        assertThat(indexResponse.status().getStatus(), equalTo(HttpStatus.SC_CREATED));
        return indexResponse;
    }

    private DocWriteResponse addDocumentToIndex(String id, DocNode doc) {
        return addDocumentToIndex(id, null, doc);
    }

    private GetResponse getDocById(String id) {
        Client client = cluster.getInternalNodeClient();
        GetResponse getResponse = client.get(new GetRequest(KIBANA_INDEX).id(id)).actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        return getResponse;
    }

    private void addAliasToIndex(String aliasName) {
        Client client = cluster.getInternalNodeClient();
        IndicesAliasesRequest.AliasActions addAlias = IndicesAliasesRequest.AliasActions.add().index(KIBANA_INDEX).alias(aliasName);
        AcknowledgedResponse acknowledgedResponse = client.admin().indices()
                .aliases(new IndicesAliasesRequest().addAliasAction(addAlias))
                .actionGet();
        assertThat(acknowledgedResponse.isAcknowledged(), equalTo(true));
    }

    private void updateIndexMappings(DocNode mapping) {
        Client client = cluster.getInternalNodeClient();
        AcknowledgedResponse response = client.admin().indices().putMapping(new PutMappingRequest(KIBANA_INDEX).source(mapping)).actionGet();
        assertThat(response.isAcknowledged(), equalTo(true));
    }

    private void deleteIndex(String indexNamePattern) {
        Client client = cluster.getInternalNodeClient();
        AcknowledgedResponse deleteIndexResponse = client.admin().indices().delete(new DeleteIndexRequest(indexNamePattern)).actionGet();
        assertThat(deleteIndexResponse.isAcknowledged(), equalTo(true));
    }

    private void deleteDoc(String id) {
        deleteDoc(id, null);
    }

    private void deleteDoc(String id, String routing) {
        Client client = cluster.getInternalNodeClient();
        DeleteResponse deleteResponse = client.delete(new DeleteRequest(KIBANA_INDEX).id(id).routing(routing)).actionGet();
        assertThat(deleteResponse.status().getStatus(), equalTo(HttpStatus.SC_OK));
    }

    private DocNode multiGetReqBody(String... docIds) {
        return DocNode.of("ids", docIds);
    }

    private DocNode multiGetReqBody(Tuple<String, List<String>>... indicesAndDocIds) {
        List<DocNode> docs = Stream.of(indicesAndDocIds)
                .flatMap(indexAndDocIds -> indexAndDocIds.v2().stream()
                        .map(docId -> DocNode.of("_index", indexAndDocIds.v1(), "_id", docId))
                )
                .toList();
        return DocNode.of("docs", docs);
    }

    private String bulkCreateReqBody(String docId, DocNode doc) {
        return bulkRequestBody("create", docId, doc);
    }

    private String bulkIndexReqBody(String docId, DocNode doc) {
        return bulkRequestBody("index", docId, doc);
    }

    private String bulkUpdateReqBody(String docId, DocNode doc) {
        return bulkRequestBody("update", docId, DocNode.of("doc", doc));
    }

    private String bulkDeleteReqBody(String docId) {
        return bulkRequestBody("delete", docId, DocNode.EMPTY);
    }

    private String bulkRequestBody(String action, String docId, DocNode doc) {
        DocNode create = DocNode.of(action, DocNode.of("_id", docId));
        return Stream.of(create, doc).filter(docNode -> !docNode.isEmpty()).map(DocNode::toJsonString)
                .collect(Collectors.joining("\n", "", "\n"));
    }

    private String appendWaitForAllActiveShardsParam(String requestPath) {
        final String waitForActiveShardsParam = "wait_for_active_shards";
        final String waitForAllActiveShardsParam = waitForActiveShardsParam.concat("=all");
        try {
            URI actualPath = new URI(requestPath);
            if (Strings.isNullOrEmpty(actualPath.getQuery())) {
                return String.format("%s%s%s", requestPath, "?", waitForAllActiveShardsParam);
            } else if (actualPath.getQuery().contains(waitForActiveShardsParam)) {
                return requestPath;
            } else {
                return String.format("%s%s%s", requestPath, "&", waitForAllActiveShardsParam);
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    };
}
