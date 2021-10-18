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

package com.floragunn.searchguard.multitenancy.test;

import java.util.HashMap;
import java.util.Map;

import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient.HttpResponse;
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
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.google.common.collect.ImmutableMap;

public class MultitenancyTests {

    private final static TestSgConfig.User USER_DEPT_01 = new TestSgConfig.User("user_dept_01").attr("dept_no", "01").roles("sg_tenant_user_attrs");
    private final static TestSgConfig.User USER_DEPT_02 = new TestSgConfig.User("user_dept_02").attr("dept_no", "02").roles("sg_tenant_user_attrs");

    public static TestCertificates certificatesContext = TestCertificates.builder()
            .ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard")
            .addNodes("CN=node-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addAdminClients("CN=admin-0.example.com,OU=SearchGuard,O=SearchGuard")
            .build();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled(certificatesContext).resources("multitenancy").users(USER_DEPT_01, USER_DEPT_02).build();

    @Test
    public void testMt() throws Exception {

        try (GenericRestClient client = cluster.getRestClient("hr_employee", "hr_employee")) {
            String body = "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}";

            HttpResponse response = client.putJson(".kibana/_doc/5.6.0?pretty", body, new BasicHeader("sgtenant", "blafasel"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

            response = client.putJson(".kibana/_doc/5.6.0?pretty", body, new BasicHeader("sgtenant", "business_intelligence"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

            response = client.putJson(".kibana/_doc/5.6.0?pretty", body, new BasicHeader("sgtenant", "human_resources"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());
            Assert.assertTrue(response.getBody(), WildcardMatcher.match("*.kibana_*_humanresources*", response.getBody()));

            response = client.get(".kibana/_doc/5.6.0?pretty", new BasicHeader("sgtenant", "human_resources"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertTrue(response.getBody(), WildcardMatcher.match("*human_resources*", response.getBody()));
        } finally {
            try (Client tc = cluster.getInternalNodeClient()) {
                tc.admin().indices().delete(new DeleteIndexRequest(".kibana_1592542611_humanresources")).actionGet();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void testMtMulti() throws Exception {

        try (Client tc = cluster.getInternalNodeClient()) {
            String body = "{" + "\"type\" : \"index-pattern\"," + "\"updated_at\" : \"2018-09-29T08:56:59.066Z\"," + "\"index-pattern\" : {"
                    + "\"title\" : \"humanresources\"" + "}}";

            tc.admin().indices().create(
                    new CreateIndexRequest(".kibana_92668751_admin").settings(ImmutableMap.of("number_of_shards", 1, "number_of_replicas", 0)))
                    .actionGet();

            tc.index(new IndexRequest(".kibana_92668751_admin").id("index-pattern:9fbbd1a0-c3c5-11e8-a13f-71b8ea5a4f7b")
                    .setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(body, XContentType.JSON)).actionGet();
        }

        try (GenericRestClient client = cluster.getRestClient("admin", "admin")) {

            System.out.println("#### search");
            HttpResponse res;
            String body = "{\"query\" : {\"term\" : { \"_id\" : \"index-pattern:9fbbd1a0-c3c5-11e8-a13f-71b8ea5a4f7b\"}}}";
            Assert.assertEquals(HttpStatus.SC_OK,
                    (res = client.postJson(".kibana/_search/?pretty", body, new BasicHeader("sgtenant", "__user__"))).getStatusCode());
            //System.out.println(res.getBody());
            Assert.assertFalse(res.getBody().contains("exception"));
            Assert.assertTrue(res.getBody().contains("humanresources"));
            Assert.assertTrue(res.getBody().contains("\"value\" : 1"));
            Assert.assertTrue(res.getBody().contains(".kibana_92668751_admin"));

            System.out.println("#### msearch");
            body = "{\"index\":\".kibana\", \"ignore_unavailable\": false}" + System.lineSeparator()
                    + "{\"size\":10, \"query\":{\"bool\":{\"must\":{\"match_all\":{}}}}}" + System.lineSeparator();

            Assert.assertEquals(HttpStatus.SC_OK,
                    (res = client.postJson("_msearch/?pretty", body, new BasicHeader("sgtenant", "__user__"))).getStatusCode());
            //System.out.println(res.getBody());
            Assert.assertFalse(res.getBody().contains("exception"));
            Assert.assertTrue(res.getBody().contains("humanresources"));
            Assert.assertTrue(res.getBody().contains("\"value\" : 1"));
            Assert.assertTrue(res.getBody().contains(".kibana_92668751_admin"));

            System.out.println("#### get");
            Assert.assertEquals(HttpStatus.SC_OK, (res = client.get(".kibana/_doc/index-pattern:9fbbd1a0-c3c5-11e8-a13f-71b8ea5a4f7b?pretty",
                    new BasicHeader("sgtenant", "__user__"))).getStatusCode());
            //System.out.println(res.getBody());
            Assert.assertFalse(res.getBody().contains("exception"));
            Assert.assertTrue(res.getBody().contains("humanresources"));
            Assert.assertTrue(res.getBody().contains("\"found\" : true"));
            Assert.assertTrue(res.getBody().contains(".kibana_92668751_admin"));

            System.out.println("#### mget");
            body = "{\"docs\" : [{\"_index\" : \".kibana\",\"_id\" : \"index-pattern:9fbbd1a0-c3c5-11e8-a13f-71b8ea5a4f7b\"}]}";
            Assert.assertEquals(HttpStatus.SC_OK,
                    (res = client.postJson("_mget/?pretty", body, new BasicHeader("sgtenant", "__user__"))).getStatusCode());
            //System.out.println(res.getBody());
            Assert.assertFalse(res.getBody().contains("exception"));
            Assert.assertTrue(res.getBody().contains("humanresources"));
            Assert.assertTrue(res.getBody().contains(".kibana_92668751_admin"));

            System.out.println("#### index");
            body = "{" + "\"type\" : \"index-pattern\"," + "\"updated_at\" : \"2017-09-29T08:56:59.066Z\"," + "\"index-pattern\" : {"
                    + "\"title\" : \"xyz\"" + "}}";
            Assert.assertEquals(HttpStatus.SC_CREATED,
                    (res = client.putJson(".kibana/_doc/abc?pretty", body, new BasicHeader("sgtenant", "__user__"))).getStatusCode());
            //System.out.println(res.getBody());
            Assert.assertFalse(res.getBody().contains("exception"));
            Assert.assertTrue(res.getBody().contains("\"result\" : \"created\""));
            Assert.assertTrue(res.getBody().contains(".kibana_92668751_admin"));

            System.out.println("#### bulk");
            body = "{ \"index\" : { \"_index\" : \".kibana\", \"_id\" : \"b1\" } }" + System.lineSeparator() + "{ \"field1\" : \"value1\" }"
                    + System.lineSeparator() + "{ \"index\" : { \"_index\" : \".kibana\",\"_id\" : \"b2\" } }" + System.lineSeparator()
                    + "{ \"field2\" : \"value2\" }" + System.lineSeparator();

            Assert.assertEquals(HttpStatus.SC_OK,
                    (res = client.putJson("_bulk?pretty", body, new BasicHeader("sgtenant", "__user__"))).getStatusCode());
            //System.out.println(res.getBody());
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
                String body = "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}";

                tc.admin().indices().create(new CreateIndexRequest(".kibana-6").alias(new Alias(".kibana"))
                        .settings(ImmutableMap.of("number_of_shards", 1, "number_of_replicas", 0))).actionGet();

                tc.index(new IndexRequest(".kibana-6").id("6.2.2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(body, XContentType.JSON))
                        .actionGet();
            }

            try (GenericRestClient client = cluster.getRestClient("kibanaro", "kibanaro")) {
                HttpResponse res;
                Assert.assertEquals(HttpStatus.SC_OK, (res = client.get(".kibana-6/_doc/6.2.2?pretty")).getStatusCode());
                Assert.assertEquals(HttpStatus.SC_OK, (res = client.get(".kibana/_doc/6.2.2?pretty")).getStatusCode());

                System.out.println(res.getBody());
            }
        } finally {
            try (Client tc = cluster.getInternalNodeClient()) {
                tc.admin().indices().delete(new DeleteIndexRequest(".kibana-6")).actionGet();
            } catch (Exception ignored) {
            }
        }
    }

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

                HttpResponse res;
                Assert.assertEquals(HttpStatus.SC_OK,
                        (res = client.get(".kibana/_doc/6.2.2?pretty", new BasicHeader("sgtenant", "__user__"))).getStatusCode());
                System.out.println(res.getBody());
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
                String body = "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}";

                tc.admin().indices()
                        .create(new CreateIndexRequest(".kibana_-815674808_kibana712aliastest_7.12.0_001")
                                .alias(new Alias(".kibana_-815674808_kibana712aliastest_7.12.0"))
                                .settings(ImmutableMap.of("number_of_shards", 1, "number_of_replicas", 0)))
                        .actionGet();

                tc.index(new IndexRequest(".kibana_-815674808_kibana712aliastest_7.12.0").id("test").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                        .source(body, XContentType.JSON)).actionGet();
            }

            try (GenericRestClient client = cluster.getRestClient("admin", "admin", "kibana_7_12_alias_test")) {

                HttpResponse response = client.get(".kibana_7.12.0/_doc/test");

                Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
                Assert.assertEquals(response.getBody(), ".kibana_-815674808_kibana712aliastest_7.12.0_001",
                        response.toJsonNode().path("_index").textValue());
            }
        } finally {
            try (Client tc = cluster.getInternalNodeClient()) {
                tc.admin().indices().delete(new DeleteIndexRequest(".kibana_-815674808_kibana712aliastest_7.12.0_001")).actionGet();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void testAliasCreationKibana_7_12() throws Exception {
        try {
            try (RestHighLevelClient tenantClient = cluster.getRestHighLevelClient("admin", "admin", "kibana_7_12_alias_creation_test");
                    Client client = cluster.getInternalNodeClient()) {
                IndexResponse indexResponse = tenantClient.index(new IndexRequest(".kibana_7.12.0_001").id("test")
                        .source(ImmutableMap.of("buildNum", 15460)).setRefreshPolicy(RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
                Assert.assertEquals(indexResponse.toString(), indexResponse.getResult(), DocWriteResponse.Result.CREATED);
                Assert.assertEquals(indexResponse.toString(), ".kibana_1482524924_kibana712aliascreationtest_7.12.0_001", indexResponse.getIndex());

                AcknowledgedResponse ackResponse = tenantClient.indices().updateAliases(
                        new IndicesAliasesRequest().addAliasAction(AliasActions.add().index(".kibana_7.12.0_001").alias(".kibana_7.12.0")),
                        RequestOptions.DEFAULT);

                Assert.assertTrue(ackResponse.toString(), ackResponse.isAcknowledged());

                GetResponse getResponse = tenantClient.get(new GetRequest(".kibana_7.12.0", "test"), RequestOptions.DEFAULT);

                Assert.assertEquals(getResponse.toString(), ".kibana_1482524924_kibana712aliascreationtest_7.12.0_001", getResponse.getIndex());

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
        String indexName = ".kibana_1592542611_humanresources";
        String testDoc = "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}";

        try (Client client = cluster.getInternalNodeClient();
                RestHighLevelClient restClient = cluster.getRestHighLevelClient("hr_employee", "hr_employee", "human_resources")) {
            Map<String, Object> indexSettings = new HashMap<>();
            indexSettings.put("number_of_shards", 3);
            indexSettings.put("number_of_replicas", 0);
            client.admin().indices().create(new CreateIndexRequest(indexName + "_2").alias(new Alias(indexName)).settings(indexSettings)).actionGet();

            MultiGetRequest multiGetRequest = new MultiGetRequest();

            for (int i = 0; i < 100; i++) {
                client.index(new IndexRequest(indexName).id("d" + i).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(testDoc, XContentType.JSON))
                        .actionGet();
                multiGetRequest.add(new Item(".kibana", "d" + i));
            }

            MultiGetResponse response = restClient.mget(multiGetRequest, RequestOptions.DEFAULT);

            for (MultiGetItemResponse item : response.getResponses()) {
                if (item.getFailure() != null) {
                    Assert.fail(item.getFailure().getMessage() + "\n" + item.getFailure());
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
                HttpResponse response = restClient.get("_searchguard/authinfo");

                Assert.assertEquals("true", response.toJsonNode().path("sg_tenants").path("dept_01").asText());
                Assert.assertEquals("", response.toJsonNode().path("sg_tenants").path("dept_02").asText());

                response = restClient.putJson(".kibana/config/user_attr_test",
                        "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}",
                        new BasicHeader("sgtenant", "dept_01"));
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

                response = restClient.putJson(".kibana/config/user_attr_test",
                        "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}",
                        new BasicHeader("sgtenant", "dept_02"));
                Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());
            }

            try (GenericRestClient restClient = cluster.getRestClient(USER_DEPT_02)) {
                HttpResponse response = restClient.get("_searchguard/authinfo");

                Assert.assertEquals("", response.toJsonNode().path("sg_tenants").path("dept_01").asText());
                Assert.assertEquals("true", response.toJsonNode().path("sg_tenants").path("dept_02").asText());

                response = restClient.putJson(".kibana/config/user_attr_test",
                        "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}",
                        new BasicHeader("sgtenant", "dept_01"));
                Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

                response = restClient.putJson(".kibana/config/user_attr_test",
                        "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}",
                        new BasicHeader("sgtenant", "dept_02"));
                Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            }
        } finally {
            try (Client tc = cluster.getInternalNodeClient()) {
                tc.admin().indices().delete(new DeleteIndexRequest(".kibana_1592542611_humanresources")).actionGet();
            } catch (Exception ignored) {
            }
        }

    }
}
