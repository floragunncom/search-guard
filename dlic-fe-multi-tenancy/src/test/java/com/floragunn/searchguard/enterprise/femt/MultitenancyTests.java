/*
 * Copyright 2017-2021 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.femt;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import co.elastic.clients.elasticsearch.core.MgetRequest;
import co.elastic.clients.elasticsearch.core.MgetResponse;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.client.RestHighLevelClient;
import com.floragunn.searchsupport.StaticSettings;
import com.google.common.collect.ImmutableList;
import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsFieldPointedByJsonPath;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.docNodeSizeEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class MultitenancyTests {

    private static final Logger log = LogManager.getLogger(MultitenancyTests.class);

    private final static TestSgConfig.User USER_DEPT_01 = new TestSgConfig.User("user_dept_01").attr("dept_no", "01").roles("sg_tenant_user_attrs");
    private final static TestSgConfig.User USER_DEPT_02 = new TestSgConfig.User("user_dept_02").attr("dept_no", "02").roles("sg_tenant_user_attrs");
    private final static TestSgConfig.User USER_WITH_ACCESS_TO_GLOBAL_TENANT = new TestSgConfig.User("user_with_access_to_global_tenant")
            .roles(new TestSgConfig.Role("access_to_global_tenant")
                    .clusterPermissions("cluster:admin:searchguard:femt:user/available_tenants/get")
                    .withTenantPermission("SGS_KIBANA_ALL_WRITE").on(Tenant.GLOBAL_TENANT_ID)
            );

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder()
            .nodeSettings("searchguard.unsupported.single_index_mt_enabled", true)
            .nodeSettings("action.destructive_requires_name", false)
            .sslEnabled()
            .resources("multitenancy")
            .enterpriseModulesEnabled()
            .users(USER_DEPT_01, USER_DEPT_02, USER_WITH_ACCESS_TO_GLOBAL_TENANT).embedded().build();

    @Before
    public void setUp() {
        Client client = cluster.getInternalNodeClient();
        AcknowledgedResponse response = client.admin().indices().delete(new DeleteIndexRequest("*")).actionGet();
        assertThat(response.isAcknowledged(), equalTo(true));
    }

    @Test
    public void testMt() throws Exception {

        try (GenericRestClient client = cluster.getRestClient("hr_employee", "hr_employee");
            GenericRestClient adminClient = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            String body = "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}";

            GenericRestClient.HttpResponse response = client.putJson(".kibana/_doc/5.6.0?pretty", body, new BasicHeader("sgtenant", "blafasel"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

            response = client.putJson(".kibana/_doc/5.6.0?pretty", body, new BasicHeader("sgtenant", "business_intelligence"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

            String tenantName = "human_resources";
            String internalTenantName = tenantName.hashCode() + "_" + "humanresources";
            response = client.putJson(".kibana/_doc/5.6.0?pretty&refresh", body, new BasicHeader("sgtenant", tenantName));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());
            Assert.assertEquals(response.getBody(), ".kibana", response.getBodyAsDocNode().get("_index"));
            Assert.assertEquals(response.getBody(), "5.6.0", response.getBodyAsDocNode().get("_id"));

            response = adminClient.get(".kibana/_doc/5.6.0__sg_ten__" + internalTenantName);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            
            response = client.get(".kibana/_doc/5.6.0?pretty", new BasicHeader("sgtenant", tenantName));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertEquals(response.getBody(), ".kibana", response.getBodyAsDocNode().get("_index"));
            Assert.assertEquals(response.getBody(), "5.6.0", response.getBodyAsDocNode().get("_id"));
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().hasNonNull("_primary_term"));
            
            
            
        } finally {
            try {
                Client tc = cluster.getInternalNodeClient();
                tc.admin().indices().delete(new DeleteIndexRequest(".kibana")).actionGet();
            } catch (Exception ignored) {
            }
        }
    }
    

    @Test
    public void testMt_search() throws Exception {

        try (GenericRestClient client = cluster.getRestClient("hr_employee", "hr_employee")) {
            String body = "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}";

            GenericRestClient.HttpResponse response = client.putJson(".kibana/_doc/5.6.0?pretty", body, new BasicHeader("sgtenant", "blafasel"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

            response = client.putJson(".kibana/_doc/5.6.0?pretty", body, new BasicHeader("sgtenant", "business_intelligence"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

            response = client.putJson(".kibana/_doc/5.6.0?pretty&refresh", body, new BasicHeader("sgtenant", "human_resources"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());
            Assert.assertEquals(response.getBody(), ".kibana", response.getBodyAsDocNode().get("_index"));
            Assert.assertEquals(response.getBody(), "5.6.0", response.getBodyAsDocNode().get("_id"));

            
            response = client.get(".kibana/_search", new BasicHeader("sgtenant", "human_resources"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertEquals(response.getBody(), 1, response.getBodyAsDocNode().getAsNode("hits", "hits").toList().size());
            Assert.assertEquals(response.getBody(), "5.6.0", response.getBodyAsDocNode().getAsNode("hits", "hits").toListOfNodes().get(0).get("_id"));
            
            
        } finally {
            try {
                Client tc = cluster.getInternalNodeClient();
                tc.admin().indices().delete(new DeleteIndexRequest(".kibana")).actionGet();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void shouldFilterTenantDocumentsDuringSearching() throws Exception {
        Client tc = cluster.getInternalNodeClient();
        DocNode indexMappings = DocNode.of("_doc", DocNode.of("properties", DocNode.of("sg_tenant", DocNode.of("type", "keyword"))));
        ImmutableMap<String, Integer> settings = ImmutableMap.of("number_of_shards", 1, "number_of_replicas", 0);
        tc.admin().indices().create(new CreateIndexRequest(".kibana").settings(settings).mapping(indexMappings)).actionGet();

        DocNode body = DocNode.of("type", "custom-saved-object", "updated_at", "2018-09-29T08:56:59.066Z",
                "details.title", "humanresources", "sg_tenant", "1592542611_humanresources");
        tc.index(new IndexRequest(".kibana").id("custom:hr__sg_ten__1592542611_humanresources")
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(body)).actionGet();

        body = DocNode.of("type", "custom-saved-object", "updated_at", "2018-09-29T08:56:59.066Z","details.title", "global tenant");

        tc.index(new IndexRequest(".kibana").id("custom-saved-object:global")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(body)).actionGet();

        body = DocNode.of("query.term", ImmutableMap.of("type.keyword", "custom-saved-object"));
        try (GenericRestClient client = cluster.getRestClient("admin", "admin", "SGS_GLOBAL_TENANT")) {
            GenericRestClient.HttpResponse response = client.postJson(".kibana/_search/?pretty", body);
            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(response.getBodyAsDocNode(), containsValue("$.hits.total.value", 1));
            assertThat(response.getBodyAsDocNode(), containsValue("$.hits.hits[0]._id", "custom-saved-object:global"));
            assertThat(response.getBodyAsDocNode(), containsValue("$.hits.hits[0]._source.details.title", "global tenant"));
        }

        try (GenericRestClient client = cluster.getRestClient("hr_employee", "hr_employee", "human_resources")) {
            GenericRestClient.HttpResponse response = client.postJson(".kibana/_search/?pretty", body);
            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(response.getBodyAsDocNode(), containsValue("$.hits.total.value", 1));
            assertThat(response.getBodyAsDocNode(), containsValue("$.hits.hits[0]._id", "custom:hr"));
            assertThat(response.getBodyAsDocNode(), containsValue("$.hits.hits[0]._source.details.title", "humanresources"));
        }
    }

    @Test
    public void shouldChooseCorrectTenantDuringDocumentGet() throws Exception {
        Client tc = cluster.getInternalNodeClient();
        DocNode indexMappings = DocNode.of("_doc", DocNode.of("properties", DocNode.of("sg_tenant", DocNode.of("type", "keyword"))));
        ImmutableMap<String, Integer> settings = ImmutableMap.of("number_of_shards", 1, "number_of_replicas", 0);
        tc.admin().indices().create(new CreateIndexRequest(".kibana").settings(settings).mapping(indexMappings)).actionGet();

        String body = "{" + "\"type\" : \"custom-saved-object\"," + "\"updated_at\" : \"2018-09-29T08:56:59.066Z\"," + "\"details\" : {"
            + "\"title\" : \"humanresources\"}," + "\"sg_tenant\":\"1592542611_humanresources\"" + "}";
        tc.index(new IndexRequest(".kibana").id("custom:hr__sg_ten__1592542611_humanresources")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(body, XContentType.JSON)).actionGet();

        body = "{" + "\"type\" : \"custom-saved-object\"," + "\"updated_at\" : \"2018-09-29T08:56:59.066Z\"," + "\"details\" : {"
            + "\"title\" : \"global tenant\"" + "}}";
        tc.index(new IndexRequest(".kibana").id("custom-saved-object:global")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(body, XContentType.JSON)).actionGet();

        try (GenericRestClient client = cluster.getRestClient("admin", "admin")) {
            BasicHeader header = new BasicHeader("sgtenant", "SGS_GLOBAL_TENANT");
            GenericRestClient.HttpResponse response = client.get(".kibana/_doc/custom-saved-object:global?pretty", header);
            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            assertThat(response.getBodyAsDocNode(), containsValue("$._source.details.title", "global tenant"));
        }

        try (GenericRestClient client = cluster.getRestClient("hr_employee", "hr_employee")) {
            BasicHeader header = new BasicHeader("sgtenant", "human_resources");
            GenericRestClient.HttpResponse response = client.get(".kibana/_doc/custom:hr?pretty", header);
            log.info("Search response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            assertThat(response.getBodyAsDocNode(), containsValue("$._source.details.title", "humanresources"));
        }
    }

    @Test
    public void testKibanaAlias() throws Exception {
        try {
            String tenantName = "human_resources";
            String internalTenantName = tenantName.hashCode() + "_" + "humanresources";
            Client tc = cluster.getInternalNodeClient();
            String body = "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\", \"sg_tenant\": \"human_resources\"}";

            tc.admin().indices().create(new CreateIndexRequest(".kibana_8.8.0_001").alias(new Alias(".kibana"))
                    .settings(ImmutableMap.of("number_of_shards", 1, "number_of_replicas", 0))).actionGet();

            tc.index(new IndexRequest(".kibana_8.8.0_001").id("6.2.2__sg_ten__" + internalTenantName).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(body, XContentType.JSON))
                    .actionGet();

            try (GenericRestClient client = cluster.getRestClient("hr_employee", "hr_employee")) {
                GenericRestClient.HttpResponse res;
              //  Assert.assertEquals(HttpStatus.SC_OK, (res = client.get(".kibana_8.8.0_001/_doc/6.2.2?pretty", new BasicHeader("sgtenant", "human_resources"))).getStatusCode());
                Assert.assertEquals(HttpStatus.SC_OK, (res = client.get(".kibana/_doc/6.2.2?pretty", new BasicHeader("sgtenant", tenantName))).getStatusCode());
            }
        } finally {
            try {
                Client tc = cluster.getInternalNodeClient();
                tc.admin().indices().prepareAliases().removeAlias(".kibana_8.8.0_001", ".kibana").get();
                tc.admin().indices().delete(new DeleteIndexRequest(".kibana_8.8.0_001")).actionGet();
            } catch (Exception ignored) {
                Assert.fail("Unexpected exception " + ignored);
            }
        }
    }

    @Test
    public void testKibanaAlias65() throws Exception {

        try {
            Client tc = cluster.getInternalNodeClient();
            String body = "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}";
            Map<String, Object> indexSettings = new HashMap<>();
            indexSettings.put("number_of_shards", 1);
            indexSettings.put("number_of_replicas", 0);
            tc.admin().indices().create(new CreateIndexRequest(".kibana_1").alias(new Alias(".kibana")).settings(indexSettings)).actionGet();

            tc.index(new IndexRequest(".kibana_1").id("6.2.2__sg_ten__-900636979_kibanaro").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(body, XContentType.JSON))
                    .actionGet();

            try (GenericRestClient client = cluster.getRestClient("kibanaro", "kibanaro")) {

                GenericRestClient.HttpResponse res;
                Assert.assertEquals(HttpStatus.SC_FORBIDDEN, //private tenants are not enabled
                        (res = client.get(".kibana/_doc/6.2.2?pretty", new BasicHeader("sgtenant", "__user__"))).getStatusCode());
            }
        } finally {
            try {
                Client tc = cluster.getInternalNodeClient();
                tc.admin().indices().delete(new DeleteIndexRequest(".kibana_1")).actionGet();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void testKibanaAliasKibana_7_12() throws Exception {
        try {

            String tenant = "kibana_7_12_alias_test";
            String internalTenantName = tenant.hashCode() + "_" + "kibana712aliastest";
            Client tc = cluster.getInternalNodeClient();
            String body = "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\", \"sg_tenant\": \"kibana_7_12_alias_test\"}";

            tc.admin().indices()
                    .create(new CreateIndexRequest(".kibana_7.12.0_001")
                            .alias(new Alias(".kibana_7.12.0"))
                            .settings(ImmutableMap.of("number_of_shards", 1, "number_of_replicas", 0)))
                    .actionGet();

            tc.index(new IndexRequest(".kibana_7.12.0").id("test__sg_ten__" + internalTenantName).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source(body, XContentType.JSON)).actionGet();

            try (GenericRestClient client = cluster.getRestClient("admin", "admin", tenant)) {

                GenericRestClient.HttpResponse response = client.get(".kibana_7.12.0/_doc/test");

                Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
                Assert.assertEquals(response.getBody(), ".kibana_7.12.0_001",
                        response.getBodyAsDocNode().getAsString("_index"));
            }
        } finally {
            try {
                Client tc = cluster.getInternalNodeClient();
                tc.admin().indices().delete(new DeleteIndexRequest(".kibana_7.12.0_001")).actionGet();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void testMgetWithKibanaAlias() throws Exception {
        String indexName = ".kibana";
        String testDoc = "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\", \"sg_tenant\": \"human_resources\"}";

        try (RestHighLevelClient restClient = cluster.getRestHighLevelClient("hr_employee", "hr_employee", "human_resources")) {
            Client client = cluster.getInternalNodeClient();
            Map<String, Object> indexSettings = new HashMap<>();
            indexSettings.put("number_of_shards", 3);
            indexSettings.put("number_of_replicas", 0);
            client.admin().indices().create(new CreateIndexRequest(indexName + "_2").alias(new Alias(indexName)).settings(indexSettings)).actionGet();

            MgetRequest.Builder multiGetRequest = new MgetRequest.Builder();
            multiGetRequest.index(".kibana");

            for (int i = 0; i < 100; i++) {
                String id = "d" + i;
                String idInTenantScope = id + "__sg_ten__human_resources";
                client.index(new IndexRequest(indexName).id(idInTenantScope).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(testDoc, XContentType.JSON))
                        .actionGet();
                multiGetRequest.ids(id);
            }

            MgetResponse<Map> response = restClient.getJavaClient().mget(multiGetRequest.build(), Map.class);
            Assert.assertFalse(response.docs().isEmpty());

            for (MultiGetResponseItem<Map> item : response.docs()) {
                if (item.result() == null || item.isFailure()) {
                    Assert.fail(item.failure().error().reason() + "\n" + item.failure().error().stackTrace());
                }
            }
        } finally {
            try {
                Client tc = cluster.getInternalNodeClient();
                tc.admin().indices().delete(new DeleteIndexRequest(indexName + "_2")).actionGet();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void testUserAttributesInTenantPattern() throws Exception {

        try {
            try (GenericRestClient restClient = cluster.getRestClient(USER_DEPT_01)) {
                GenericRestClient.HttpResponse response = restClient.get("_searchguard/authinfo");

                Assert.assertEquals(response.getBody(), "true", response.getBodyAsDocNode().getAsNode("sg_tenants", "dept_01").toString());
                Assert.assertNull(response.getBodyAsDocNode().get("sg_tenants", "dept_02"));

                response = restClient.putJson(".kibana/_doc/user_attr_test",
                        "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}",
                        new BasicHeader("sgtenant", "dept_01"));
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                response = restClient.putJson(".kibana/_doc/user_attr_test",
                        "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}",
                        new BasicHeader("sgtenant", "dept_02"));
                Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());
            }

            try (GenericRestClient restClient = cluster.getRestClient(USER_DEPT_02)) {
                GenericRestClient.HttpResponse response = restClient.get("_searchguard/authinfo");

                Assert.assertNull(response.getBodyAsDocNode().get("sg_tenants", "dept_01"));
                Assert.assertEquals("true", response.getBodyAsDocNode().getAsNode("sg_tenants", "dept_02").toString());

                response = restClient.putJson(".kibana/_doc/user_attr_test",
                        "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}",
                        new BasicHeader("sgtenant", "dept_01"));
                Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

                response = restClient.putJson(".kibana/_doc/user_attr_test",
                        "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}",
                        new BasicHeader("sgtenant", "dept_02"));
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            }
        } finally {
            try {
                Client tc = cluster.getInternalNodeClient();
                tc.admin().indices().delete(new DeleteIndexRequest(".kibana")).actionGet();
            } catch (Exception ignored) {
            }
        }

    }

    @Test
    public void testMultitenancyConfigApi_configShouldGetUpdated() throws Exception {
        try (GenericRestClient userClient = cluster.getRestClient(USER_WITH_ACCESS_TO_GLOBAL_TENANT);
             GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
            cluster.callAndRestoreConfig(FeMultiTenancyConfig.TYPE, () -> {
                GenericRestClient.HttpResponse response = adminCertClient.get("/_searchguard/config/fe_multi_tenancy");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                DocNode config = DocNode.of(
                        "enabled", true, "index", "kibana_index", "server_user", "kibana_user",
                        "global_tenant_enabled", true, "private_tenant_enabled", false,
                        "preferred_tenants", ImmutableList.of("tenant-1")
                );

                response = adminCertClient.putJson("/_searchguard/config/fe_multi_tenancy", config);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
                assertThat(response.getBodyAsDocNode(), containsValue("error.details.private_tenant_enabled.[0].error", "Unsupported attribute"));

                config = config.without("private_tenant_enabled");

                response = adminCertClient.putJson("/_searchguard/config/fe_multi_tenancy", config);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                response = adminCertClient.get("/_searchguard/config/frontend_multi_tenancy");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                config = response.getBodyAsDocNode();
                assertThat(config, containsValue("$.content.enabled", true));
                assertThat(config, containsValue("$.content.index", "kibana_index"));
                assertThat(config, containsValue("$.content.server_user", "kibana_user"));
                assertThat(config, containsValue("$.content.global_tenant_enabled", true));
                assertThat(config, not(containsFieldPointedByJsonPath("$.content", "private_tenant_enabled")));
                assertThat(config, docNodeSizeEqualTo("$.content.preferred_tenants", 1));
                assertThat(config, containsValue("$.content.preferred_tenants[0]", "tenant-1"));

                response = userClient.get("/_searchguard/current_user/tenants");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
                assertThat(response.getBodyAsDocNode(), containsFieldPointedByJsonPath("$.data.tenants", Tenant.GLOBAL_TENANT_ID));
                assertThat(response.getBodyAsDocNode(), not(containsFieldPointedByJsonPath("$.data.tenants", USER_WITH_ACCESS_TO_GLOBAL_TENANT.getName())));

                config = DocNode.of(
                        "enabled", true, "index", "kibana_index_v2", "server_user", "kibana_user_v2",
                        "global_tenant_enabled", false, "preferred_tenants", ImmutableList.of()
                );

                response = adminCertClient.putJson("/_searchguard/config/frontend_multi_tenancy", config);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                response = adminCertClient.get("/_searchguard/config/fe_multi_tenancy");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                config = response.getBodyAsDocNode();
                assertThat(config, containsValue("$.content.enabled", true));
                assertThat(config, containsValue("$.content.index", "kibana_index_v2"));
                assertThat(config, containsValue("$.content.server_user", "kibana_user_v2"));
                assertThat(config, containsValue("$.content.global_tenant_enabled", false));
                assertThat(config, not(containsFieldPointedByJsonPath("$.content", "private_tenant_enabled")));
                assertThat(config, docNodeSizeEqualTo("$.content.preferred_tenants", 0));

                response = userClient.get("/_searchguard/current_user/tenants");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_UNAUTHORIZED));
                assertThat(response.getBodyAsDocNode(), containsValue("$.message", "Cannot determine default tenant for current user"));

                config = DocNode.of(
                        "enabled", true, "global_tenant_enabled", true, "preferred_tenants",
                        ImmutableList.of("tenant-2")
                );
                response = adminCertClient.patchJsonMerge("/_searchguard/config/fe_multi_tenancy", config);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                response = adminCertClient.get("/_searchguard/config/fe_multi_tenancy");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                config = response.getBodyAsDocNode();
                assertThat(config, containsValue("$.content.enabled", true));
                assertThat(config, containsValue("$.content.index", "kibana_index_v2"));
                assertThat(config, containsValue("$.content.server_user", "kibana_user_v2"));
                assertThat(config, containsValue("$.content.global_tenant_enabled", true));
                assertThat(config, not(containsFieldPointedByJsonPath("$.content", "private_tenant_enabled")));
                assertThat(config, docNodeSizeEqualTo("$.content.preferred_tenants", 1));
                assertThat(config, containsValue("$.content.preferred_tenants[0]", "tenant-2"));

                return null;
            });
        }
    }

    @Test
    public void testMultitenancyConfigApi_shouldNotAllowToChangeEnabledFlag_whenThereAreKibanaIndices() throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
            cluster.callAndRestoreConfig(FeMultiTenancyConfig.TYPE, () -> {
                GenericRestClient.HttpResponse response = adminCertClient.get("/_searchguard/config/fe_multi_tenancy");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                //no kibana index exists
                DocNode config = DocNode.of("enabled", true);

                //FeMultiTenancyConfig API
                response = adminCertClient.putJson("/_searchguard/config/fe_multi_tenancy", config);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                config = DocNode.of("enabled", false);

                response = adminCertClient.patchJsonMerge("/_searchguard/config/fe_multi_tenancy", config);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                //BulkConfig API
                config = DocNode.of("enabled", true);
                DocNode bulkBody = DocNode.of("frontend_multi_tenancy.content", config);

                response = adminCertClient.putJson("/_searchguard/config", bulkBody);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                config = DocNode.of("enabled", false);
                bulkBody = DocNode.of("frontend_multi_tenancy.content", config);

                response = adminCertClient.putJson("/_searchguard/config", bulkBody);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                //create kibana index
                response = adminCertClient.put("/.kibana");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                //FeMultiTenancyConfig API
                //try to change enabled flag
                config = DocNode.of("enabled", true);

                response = adminCertClient.putJson("/_searchguard/config/fe_multi_tenancy", config);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
                assertThat(response.getBodyAsDocNode(),
                        containsValue("$.error.details.['frontend_multi_tenancy.default'].[0].error",
                                "You try to enable multitenancy. This operation cannot be undone. Please use the 'sgctl.sh special enable-mt' command if you are sure that you want to proceed."
                        )
                );

                config = DocNode.of("enabled", true);

                response = adminCertClient.patchJsonMerge("/_searchguard/config/fe_multi_tenancy", config);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
                assertThat(response.getBodyAsDocNode(),
                        containsValue("error.details._.[0].error",
                                "You try to enable multitenancy. This operation cannot be undone. Please use the 'sgctl.sh special enable-mt' command if you are sure that you want to proceed."
                        )
                );

                //send the same value that is already configured
                config = DocNode.of("enabled", false);

                response = adminCertClient.putJson("/_searchguard/config/fe_multi_tenancy", config);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                config = DocNode.of("enabled", false);

                response = adminCertClient.patchJsonMerge("/_searchguard/config/fe_multi_tenancy", config);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                //BulkConfig API
                //try to change enabled flag
                config = DocNode.of("enabled", true);

                bulkBody = DocNode.of("frontend_multi_tenancy.content", config);

                response = adminCertClient.putJson("/_searchguard/config", bulkBody);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
                assertThat(response.getBodyAsDocNode(),
                        containsValue("$.error.details.['frontend_multi_tenancy.default'].[0].error",
                                "You try to enable multitenancy. This operation cannot be undone. Please use the 'sgctl.sh special enable-mt' command if you are sure that you want to proceed."
                        )
                );

                //send the same value that is already configured
                config = DocNode.of("enabled", false);

                bulkBody = DocNode.of("frontend_multi_tenancy.content", config);

                response = adminCertClient.putJson("/_searchguard/config", bulkBody);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                return null;
            });
        }
    }

    @Test
    public void shouldExtendsMappingsWhenMultiTenancyIsEnabled() throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
            cluster.callAndRestoreConfig(FeMultiTenancyConfig.TYPE, () -> {
                GenericRestClient.HttpResponse response = adminCertClient.get("/_searchguard/config/fe_multi_tenancy");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
                Client client = cluster.getInternalNodeClient();
                List<String> indices = Arrays
                    .asList(".kibana", ".kibana_analytics", ".kibana_ingest", ".kibana_security_solution", ".kibana_alerting_cases");
                // disable MT
                DocNode config = DocNode.of("enabled", false);
                response = adminCertClient.putJson("/_searchguard/config/fe_multi_tenancy", config);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
                // create all frontend indices
                for(String indexName : indices) {
                    response = adminCertClient.put("/" + indexName);
                    assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
                }

                // enable MT
                response = adminCertClient.put("/_searchguard/config/fe_multi_tenancy/activation");

                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
                GetMappingsRequest request = new GetMappingsRequest(StaticSettings.DEFAULT_MASTER_TIMEOUT).indices(indices.toArray(String[]::new));
                GetMappingsResponse mappingsResponse = client.admin().indices().getMappings(request).actionGet();
                Map<String, MappingMetadata> mappings = mappingsResponse.getMappings();
                long numberOfIndicesWithExtendedMappings = indices.stream() //
                    .map(indexName -> mappings.get(indexName)) //
                    .filter(Objects::nonNull) //
                    .map(metadata -> metadata.sourceAsMap()) //
                    .map(mappingsMap -> (Map<String, Object>) mappingsMap.get("properties")) //
                    .filter(Objects::nonNull) //
                    .map(mappingProperties -> (Map<String, Object>) mappingProperties.get("sg_tenant")) //
                    .filter(Objects::nonNull) //
                    .map(fieldMappings -> fieldMappings.get("type")) //
                    .filter(fieldType -> "keyword".equals(fieldType)) //
                    .count();
                assertThat(numberOfIndicesWithExtendedMappings, equalTo((long)indices.size()));
                return null;
            });
        }
    }

    @Test
    public void shouldEnableMultitenancy() throws Exception {
        try (GenericRestClient adminCertClient = cluster.getAdminCertRestClient()) {
            cluster.callAndRestoreConfig(FeMultiTenancyConfig.TYPE, () -> {
                // disable MT
                DocNode config = DocNode.of("enabled", false);
                GenericRestClient.HttpResponse response = adminCertClient.putJson("/_searchguard/config/fe_multi_tenancy", config);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                response = adminCertClient.get("/_searchguard/config/fe_multi_tenancy");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
                assertThat(response.getBodyAsDocNode(), containsValue("$.content.enabled", false));
                List<String> indices = Arrays
                    .asList(".kibana", ".kibana_analytics", ".kibana_ingest", ".kibana_security_solution", ".kibana_alerting_cases");
                // create all frontend indices
                for(String indexName : indices) {
                    response = adminCertClient.put("/" + indexName);
                    assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
                }

                // enable MT
                response = adminCertClient.put("/_searchguard/config/fe_multi_tenancy/activation");

                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
                response = adminCertClient.get("/_searchguard/config/fe_multi_tenancy");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
                assertThat(response.getBodyAsDocNode(), containsValue("$.content.enabled", true));
                return null;
            });
        }
    }
}
