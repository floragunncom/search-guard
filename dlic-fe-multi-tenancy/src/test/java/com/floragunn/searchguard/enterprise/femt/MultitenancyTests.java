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

import java.util.HashMap;
import java.util.Map;

import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.MgetRequest;
import co.elastic.clients.elasticsearch.core.MgetResponse;
import co.elastic.clients.elasticsearch.core.MsearchRequest;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
import co.elastic.clients.elasticsearch.core.msearch.RequestItem;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesResponse;
import com.floragunn.searchguard.client.RestHighLevelClient;
import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.google.common.collect.ImmutableMap;

public class MultitenancyTests {

    private final static TestSgConfig.User USER_DEPT_01 = new TestSgConfig.User("user_dept_01").attr("dept_no", "01").roles("sg_tenant_user_attrs");
    private final static TestSgConfig.User USER_DEPT_02 = new TestSgConfig.User("user_dept_02").attr("dept_no", "02").roles("sg_tenant_user_attrs");

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().resources("multitenancy").enterpriseModulesEnabled()
            .users(USER_DEPT_01, USER_DEPT_02).build();

    @Test
    public void testMt() throws Exception {

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

            response = adminClient.get(".kibana/_doc/5.6.0__sg_ten__human_resources");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            
            response = client.get(".kibana/_doc/5.6.0?pretty", new BasicHeader("sgtenant", "human_resources"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertEquals(response.getBody(), ".kibana", response.getBodyAsDocNode().get("_index"));
            Assert.assertEquals(response.getBody(), "5.6.0", response.getBodyAsDocNode().get("_id"));
            Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().hasNonNull("_primary_term"));
            
            
            
        } finally {
            try (Client tc = cluster.getInternalNodeClient()) {
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
            try (Client tc = cluster.getInternalNodeClient()) {
                tc.admin().indices().delete(new DeleteIndexRequest(".kibana")).actionGet();
            } catch (Exception ignored) {
            }
        }
    }

    @Ignore
    @Test
    public void testMtMulti() throws Exception {

        try (Client tc = cluster.getInternalNodeClient()) {
            String body = "{" + "\"type\" : \"index-pattern\"," + "\"updated_at\" : \"2018-09-29T08:56:59.066Z\"," + "\"index-pattern\" : {"
                    + "\"title\" : \"humanresources\"" + "}}";

            tc.admin().indices().create(
                    new CreateIndexRequest(".kibana").settings(ImmutableMap.of("number_of_shards", 1, "number_of_replicas", 0)))
                    .actionGet();

            tc.index(new IndexRequest(".kibana").id("index-pattern:9fbbd1a0-c3c5-11e8-a13f-71b8ea5a4f7b__sg_ten__human_resources")
                    .setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(body, XContentType.JSON)).actionGet();
        }

        try (GenericRestClient client = cluster.getRestClient("admin", "admin")) {

            //System.out.println("#### search");
            GenericRestClient.HttpResponse res;
            String body = "{\"query\" : {\"term\" : { \"_id\" : \"index-pattern:9fbbd1a0-c3c5-11e8-a13f-71b8ea5a4f7b\"}}}";
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
            try (Client tc = cluster.getInternalNodeClient()) {
                String body = "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\", \"namespaces\": [\"_sg_ten_human_resources\"]}";

                tc.admin().indices().create(new CreateIndexRequest(".kibana_8.8.0_001").alias(new Alias(".kibana"))
                        .settings(ImmutableMap.of("number_of_shards", 1, "number_of_replicas", 0))).actionGet();

                tc.index(new IndexRequest(".kibana_8.8.0_001").id("6.2.2__sg_ten__human_resources").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(body, XContentType.JSON))
                        .actionGet();
            }

            try (GenericRestClient client = cluster.getRestClient("hr_employee", "hr_employee")) {
                GenericRestClient.HttpResponse res;
              //  Assert.assertEquals(HttpStatus.SC_OK, (res = client.get(".kibana_8.8.0_001/_doc/6.2.2?pretty", new BasicHeader("sgtenant", "human_resources"))).getStatusCode());
                Assert.assertEquals(HttpStatus.SC_OK, (res = client.get(".kibana/_doc/6.2.2?pretty", new BasicHeader("sgtenant", "human_resources"))).getStatusCode());
            }
        } finally {
            try (Client tc = cluster.getInternalNodeClient()) {
                tc.admin().indices().delete(new DeleteIndexRequest(".kibana-6")).actionGet();
            } catch (Exception ignored) {
            }
        }
    }

    @Ignore
    @Test
    public void testKibanaAlias65() throws Exception {

        try {
            try (Client tc = cluster.getInternalNodeClient()) {
                String body = "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}";
                Map<String, Object> indexSettings = new HashMap<>();
                indexSettings.put("number_of_shards", 1);
                indexSettings.put("number_of_replicas", 0);
                tc.admin().indices().create(new CreateIndexRequest(".kibana_1").alias(new Alias(".kibana")).settings(indexSettings)).actionGet();

                tc.index(new IndexRequest(".kibana_1").id("6.2.2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(body, XContentType.JSON))
                        .actionGet();
                tc.index(new IndexRequest(".kibana_-900636979_kibanaro").id("6.2.2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(body,
                        XContentType.JSON)).actionGet();

            }

            try (GenericRestClient client = cluster.getRestClient("kibanaro", "kibanaro")) {

                GenericRestClient.HttpResponse res;
                Assert.assertEquals(HttpStatus.SC_OK,
                        (res = client.get(".kibana/_doc/6.2.2?pretty", new BasicHeader("sgtenant", "__user__"))).getStatusCode());
                //System.out.println(res.getBody());
                Assert.assertTrue(res.getBody().contains(".kibana_-900636979_kibanaro"));
            }
        } finally {
            try (Client tc = cluster.getInternalNodeClient()) {
                tc.admin().indices().delete(new DeleteIndexRequest(".kibana_1")).actionGet();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void testKibanaAliasKibana_7_12() throws Exception {
        try {

            try (Client tc = cluster.getInternalNodeClient()) {
                String body = "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\", \"namespaces\": [\"_sg_ten_kibana_7_12_alias_test\"]}";

                tc.admin().indices()
                        .create(new CreateIndexRequest(".kibana_7.12.0_001")
                                .alias(new Alias(".kibana_7.12.0"))
                                .settings(ImmutableMap.of("number_of_shards", 1, "number_of_replicas", 0)))
                        .actionGet();

                tc.index(new IndexRequest(".kibana_7.12.0").id("test__sg_ten__kibana_7_12_alias_test").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                        .source(body, XContentType.JSON)).actionGet();
            }

            try (GenericRestClient client = cluster.getRestClient("admin", "admin", "kibana_7_12_alias_test")) {

                GenericRestClient.HttpResponse response = client.get(".kibana_7.12.0/_doc/test");

                Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
                Assert.assertEquals(response.getBody(), ".kibana_7.12.0_001",
                        response.getBodyAsDocNode().getAsString("_index"));
            }
        } finally {
            try (Client tc = cluster.getInternalNodeClient()) {
                tc.admin().indices().delete(new DeleteIndexRequest(".kibana_7.12.0_001")).actionGet();
            } catch (Exception ignored) {
            }
        }
    }

    @Ignore
    @Test
    public void testAliasCreationKibana_7_12() throws Exception {
        try {
            try (RestHighLevelClient tenantClient = cluster.getRestHighLevelClient("admin", "admin", "kibana_7_12_alias_creation_test");
                 Client client = cluster.getInternalNodeClient()) {
                IndexResponse indexResponse = tenantClient.index(".kibana_7.12.0_001","test", Map.of("buildNum", 15460));
                Assert.assertEquals(indexResponse.toString(), indexResponse.result(), Result.Created);
                Assert.assertEquals(indexResponse.toString(), ".kibana_1482524924_kibana712aliascreationtest_7.12.0_001", indexResponse.index());

                UpdateAliasesResponse ackResponse = tenantClient.getJavaClient().indices().updateAliases(b->b.actions(
                        a->a.add(ac->ac.index(".kibana_7.12.0_001").alias(".kibana_7.12.0")))
                );

                Assert.assertTrue(ackResponse.toString(), ackResponse.acknowledged());

                GetResponse<Map> getResponse = tenantClient.get(".kibana_7.12.0", "test");

                Assert.assertEquals(getResponse.toString(), ".kibana_1482524924_kibana712aliascreationtest_7.12.0_001", getResponse.index());

                GetAliasesResponse getAliasesResponse = client.admin().indices()
                        .getAliases(new GetAliasesRequest(".kibana_1482524924_kibana712aliascreationtest_7.12.0")).actionGet();

                Assert.assertNotNull(getAliasesResponse.getAliases().toString(),
                        getAliasesResponse.getAliases().get(".kibana_1482524924_kibana712aliascreationtest_7.12.0_001"));
                Assert.assertEquals(getAliasesResponse.getAliases().toString(), ".kibana_1482524924_kibana712aliascreationtest_7.12.0",
                        getAliasesResponse.getAliases().get(".kibana_1482524924_kibana712aliascreationtest_7.12.0_001").get(0).alias());

            }
        } finally {
            try (Client tc = cluster.getInternalNodeClient()) {
                tc.admin().indices().delete(new DeleteIndexRequest(".kibana_1482524924_kibana712aliascreationtest_7.12.0_001")).actionGet();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void testMgetWithKibanaAlias() throws Exception {
        String indexName = ".kibana";
        String testDoc = "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\", \"namespaces\": [\"_sg_ten_human_resources\"]}";

        try (Client client = cluster.getInternalNodeClient();
                RestHighLevelClient restClient = cluster.getRestHighLevelClient("hr_employee", "hr_employee", "human_resources")) {
            Map<String, Object> indexSettings = new HashMap<>();
            indexSettings.put("number_of_shards", 3);
            indexSettings.put("number_of_replicas", 0);
            client.admin().indices().create(new CreateIndexRequest(indexName + "_2").alias(new Alias(indexName)).settings(indexSettings)).actionGet();

            MgetRequest.Builder multiGetRequest = new MgetRequest.Builder();
            multiGetRequest.index(".kibana");

            for (int i = 0; i < 100; i++) {
                client.index(new IndexRequest(indexName).id("d" + i).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(testDoc, XContentType.JSON))
                        .actionGet();
                multiGetRequest.ids("d" + i);
            }

            MgetResponse<Map> response = restClient.getJavaClient().mget(multiGetRequest.build(), Map.class);

            for (MultiGetResponseItem<Map> item : response.docs()) {
                if (item.result() == null || item.isFailure()) {
                    Assert.fail(item.failure().error().reason() + "\n" + item.failure().error().stackTrace());
                }
            }
        } finally {
            try (Client tc = cluster.getInternalNodeClient()) {
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
            try (Client tc = cluster.getInternalNodeClient()) {
                tc.admin().indices().delete(new DeleteIndexRequest(".kibana")).actionGet();
            } catch (Exception ignored) {
            }
        }

    }
}
