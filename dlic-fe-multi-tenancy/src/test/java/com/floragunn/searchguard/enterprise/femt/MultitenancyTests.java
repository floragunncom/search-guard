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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.MgetRequest;
import co.elastic.clients.elasticsearch.core.MgetResponse;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesResponse;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.client.RestHighLevelClient;
import com.google.common.collect.ImmutableList;
import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.google.common.collect.ImmutableMap;

import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class MultitenancyTests {

    private final static TestSgConfig.User USER_DEPT_01 = new TestSgConfig.User("user_dept_01").attr("dept_no", "01").roles("sg_tenant_user_attrs");
    private final static TestSgConfig.User USER_DEPT_02 = new TestSgConfig.User("user_dept_02").attr("dept_no", "02").roles("sg_tenant_user_attrs");

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
            .nodeSettings("searchguard.unsupported.single_index_mt_enabled", true)
            .sslEnabled()
            .resources("multitenancy")
            .enterpriseModulesEnabled()
            .users(USER_DEPT_01, USER_DEPT_02).build();

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

        try (GenericRestClient client = cluster.getRestClient("hr_employee", "hr_employee"); GenericRestClient adminClient = cluster.getRestClient("kibanaserver", "kibanaserver")) {
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

    @Ignore
    @Test
    public void testMtMulti() throws Exception {

        Client tc = cluster.getInternalNodeClient();
        String body = "{" + "\"type\" : \"index-pattern\"," + "\"updated_at\" : \"2018-09-29T08:56:59.066Z\"," + "\"index-pattern\" : {"
                + "\"title\" : \"humanresources\"" + "}}";

        tc.admin().indices().create(
                new CreateIndexRequest(".kibana").settings(ImmutableMap.of("number_of_shards", 1, "number_of_replicas", 0)))
                .actionGet();

        tc.index(new IndexRequest(".kibana").id("index-pattern:9fbbd1a0-c3c5-11e8-a13f-71b8ea5a4f7b__sg_ten__human_resources")
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(body, XContentType.JSON)).actionGet();

        try (GenericRestClient client = cluster.getRestClient("admin", "admin")) {

            //System.out.println("#### search");
            GenericRestClient.HttpResponse res;
            body = "{\"query\" : {\"term\" : { \"_id\" : \"index-pattern:9fbbd1a0-c3c5-11e8-a13f-71b8ea5a4f7b\"}}}";
            Assert.assertEquals(HttpStatus.SC_OK,
                    (res = client.postJson(".kibana/_search/?pretty", body, new BasicHeader("sgtenant", "__user__"))).getStatusCode());
            ////System.out.println(res.getBody());
            Assert.assertFalse(res.getBody().contains("exception"));
            Assert.assertTrue(res.getBody().contains("humanresources"));
            Assert.assertTrue(res.getBody().contains("\"value\" : 1"));
            Assert.assertTrue(res.getBody().contains(".kibana_92668751_admin"));

            //System.out.println("#### msearch");
            body = "{\"index\":\".kibana\", \"ignore_unavailable\": false}" + System.lineSeparator()
                    + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}" + System.lineSeparator();

            Assert.assertEquals(HttpStatus.SC_OK,
                    (res = client.postJson("_msearch/?pretty", body, new BasicHeader("sgtenant", "__user__"))).getStatusCode());
            ////System.out.println(res.getBody());
            Assert.assertFalse(res.getBody().contains("exception"));
            Assert.assertTrue(res.getBody().contains("humanresources"));
            Assert.assertTrue(res.getBody().contains("\"value\" : 1"));
            Assert.assertTrue(res.getBody().contains(".kibana_92668751_admin"));

            //System.out.println("#### get");
            Assert.assertEquals(HttpStatus.SC_OK, (res = client.get(".kibana/_doc/index-pattern:9fbbd1a0-c3c5-11e8-a13f-71b8ea5a4f7b?pretty",
                    new BasicHeader("sgtenant", "__user__"))).getStatusCode());
            ////System.out.println(res.getBody());
            Assert.assertFalse(res.getBody().contains("exception"));
            Assert.assertTrue(res.getBody().contains("humanresources"));
            Assert.assertTrue(res.getBody().contains("\"found\" : true"));
            Assert.assertTrue(res.getBody().contains(".kibana_92668751_admin"));

            //System.out.println("#### mget");
            body = "{\"docs\" : [{\"_index\" : \".kibana\",\"_id\" : \"index-pattern:9fbbd1a0-c3c5-11e8-a13f-71b8ea5a4f7b\"}]}";
            Assert.assertEquals(HttpStatus.SC_OK,
                    (res = client.postJson("_mget/?pretty", body, new BasicHeader("sgtenant", "__user__"))).getStatusCode());
            ////System.out.println(res.getBody());
            Assert.assertFalse(res.getBody().contains("exception"));
            Assert.assertTrue(res.getBody().contains("humanresources"));
            Assert.assertTrue(res.getBody().contains(".kibana_92668751_admin"));

            //System.out.println("#### index");
            body = "{" + "\"type\" : \"index-pattern\"," + "\"updated_at\" : \"2017-09-29T08:56:59.066Z\"," + "\"index-pattern\" : {"
                    + "\"title\" : \"xyz\"" + "}}";
            Assert.assertEquals(HttpStatus.SC_CREATED,
                    (res = client.putJson(".kibana/_doc/abc?pretty", body, new BasicHeader("sgtenant", "__user__"))).getStatusCode());
            ////System.out.println(res.getBody());
            Assert.assertFalse(res.getBody().contains("exception"));
            Assert.assertTrue(res.getBody().contains("\"result\" : \"created\""));
            Assert.assertTrue(res.getBody().contains(".kibana_92668751_admin"));

            //System.out.println("#### bulk");
            body = "{ \"index\" : { \"_index\" : \".kibana\", \"_id\" : \"b1\" } }" + System.lineSeparator() + "{ \"field1\" : \"value1\" }"
                    + System.lineSeparator() + "{ \"index\" : { \"_index\" : \".kibana\",\"_id\" : \"b2\" } }" + System.lineSeparator()
                    + "{ \"field2\" : \"value2\" }" + System.lineSeparator();

            Assert.assertEquals(HttpStatus.SC_OK,
                    (res = client.putJson("_bulk?pretty", body, new BasicHeader("sgtenant", "__user__"))).getStatusCode());
            ////System.out.println(res.getBody());
            Assert.assertFalse(res.getBody().contains("exception"));
            Assert.assertTrue(res.getBody().contains(".kibana_92668751_admin"));
            Assert.assertTrue(res.getBody().contains("\"errors\" : false"));
            Assert.assertTrue(res.getBody().contains("\"result\" : \"created\""));

            Assert.assertEquals(HttpStatus.SC_OK, (res = client.get("_cat/indices")).getStatusCode());
            Assert.assertTrue(res.getBody().contains(".kibana_92668751_admin"));            
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
                Assert.assertEquals(HttpStatus.SC_OK,
                        (res = client.get(".kibana/_doc/6.2.2?pretty", new BasicHeader("sgtenant", "__user__"))).getStatusCode());
                Assert.assertTrue(res.getBody().contains(".kibana_1"));
                Assert.assertTrue(res.getBody().contains("15460"));
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
    public void testMultitenancyConfigApi() throws Exception {
        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            cluster.callAndRestoreConfig(FeMultiTenancyConfig.TYPE, () -> {
                GenericRestClient.HttpResponse response = restClient.get("/_searchguard/config/fe_multi_tenancy");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                DocNode config = DocNode.of(
                        "enabled", true, "index", "kibana_index", "server_user", "kibana_user",
                        "global_tenant_enabled", true, "private_tenant_enabled", false,
                        "preferred_tenants", ImmutableList.of("tenant-1")
                );

                response = restClient.putJson("/_searchguard/config/fe_multi_tenancy", config);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                response = restClient.get("/_searchguard/config/frontend_multi_tenancy");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                config = response.getBodyAsDocNode();
                assertThat(config, containsValue("$.content.enabled", true));
                assertThat(config, containsValue("$.content.index", "kibana_index"));
                assertThat(config, containsValue("$.content.server_user", "kibana_user"));
                assertThat(config, containsValue("$.content.global_tenant_enabled", true));
                assertThat(config, containsValue("$.content.private_tenant_enabled", false));
                assertThat(config, docNodeSizeEqualTo("$.content.preferred_tenants", 1));
                assertThat(config, containsValue("$.content.preferred_tenants[0]", "tenant-1"));

                config = DocNode.of(
                        "enabled", false, "index", "kibana_index_v2", "server_user", "kibana_user_v2",
                        "global_tenant_enabled", false, "private_tenant_enabled", true,
                        "preferred_tenants", ImmutableList.of()
                );

                response = restClient.putJson("/_searchguard/config/frontend_multi_tenancy", config);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                response = restClient.get("/_searchguard/config/fe_multi_tenancy");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                config = response.getBodyAsDocNode();
                assertThat(config, containsValue("$.content.enabled", false));
                assertThat(config, containsValue("$.content.index", "kibana_index_v2"));
                assertThat(config, containsValue("$.content.server_user", "kibana_user_v2"));
                assertThat(config, containsValue("$.content.global_tenant_enabled", false));
                assertThat(config, containsValue("$.content.private_tenant_enabled", true));
                assertThat(config, docNodeSizeEqualTo("$.content.preferred_tenants", 0));

                config = DocNode.of(
                        "enabled", true, "global_tenant_enabled", true, "preferred_tenants",
                        ImmutableList.of("tenant-2")
                );
                response = restClient.patchJsonMerge("/_searchguard/config/fe_multi_tenancy", config);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                response = restClient.get("/_searchguard/config/fe_multi_tenancy");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                config = response.getBodyAsDocNode();
                assertThat(config, containsValue("$.content.enabled", true));
                assertThat(config, containsValue("$.content.index", "kibana_index_v2"));
                assertThat(config, containsValue("$.content.server_user", "kibana_user_v2"));
                assertThat(config, containsValue("$.content.global_tenant_enabled", true));
                assertThat(config, containsValue("$.content.private_tenant_enabled", true));
                assertThat(config, docNodeSizeEqualTo("$.content.preferred_tenants", 1));
                assertThat(config, containsValue("$.content.preferred_tenants[0]", "tenant-2"));

                return null;
            });
        }
    }
}
