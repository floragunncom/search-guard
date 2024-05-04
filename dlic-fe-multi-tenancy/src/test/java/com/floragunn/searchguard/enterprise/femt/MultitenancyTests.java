/*
 * Based on https://github.com/opensearch-project/opensearch-dashboards-functional-test/pull/608
 * from Apache 2 licensed OpenSearch
 *
 * Original license header:
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 *
 * Modifications:
 *
 * Copyright 2023 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.floragunn.searchguard.enterprise.femt;

import static com.floragunn.searchguard.enterprise.femt.TenantAccessMatcher.Action.CREATE_DOCUMENT;
import static com.floragunn.searchguard.enterprise.femt.TenantAccessMatcher.Action.DELETE_INDEX;
import static com.floragunn.searchguard.enterprise.femt.TenantAccessMatcher.Action.UPDATE_DOCUMENT;
import static com.floragunn.searchguard.enterprise.femt.TenantAccessMatcher.Action.UPDATE_INDEX;
import static com.floragunn.searchguard.enterprise.femt.TenantAccessMatcher.canPerformFollowingActions;
import static com.floragunn.searchguard.legacy.test.AbstractSGUnitTest.encodeBasicHeader;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static org.apache.http.HttpStatus.SC_OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.MgetRequest;
import co.elastic.clients.elasticsearch.core.MgetResponse;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesResponse;
import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.client.RestHighLevelClient;
import com.floragunn.searchguard.enterprise.femt.TenantAccessMatcher.Action;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.google.common.collect.ImmutableMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class MultitenancyTests {

    private final static TestSgConfig.User USER_DEPT_01 = new TestSgConfig.User("user_dept_01").attr("dept_no", "01").roles("sg_tenant_user_attrs");
    private final static TestSgConfig.User USER_DEPT_02 = new TestSgConfig.User("user_dept_02").attr("dept_no", "02").roles("sg_tenant_user_attrs");
    private final static TestSgConfig.User HR_USER_READ_ONLY = new TestSgConfig.User("hr_user_read_only").roles("hr_tenant_read_only_access");
    private final static TestSgConfig.User HR_USER_READ_WRITE = new TestSgConfig.User("hr_user_read_write").roles("hr_tenant_read_write_access");
    public static final String TENANT_WRITABLE = Boolean.toString(true);
    public static final String TENANT_NOT_WRITABLE = Boolean.toString(false);

    public static final TestSgConfig.Tenant TEST_TENANT = new TestSgConfig.Tenant("test_tenant");
    public static final TestSgConfig.Role TEST_ROLE =new TestSgConfig.Role("test_role")
            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS")
            .indexPermissions("SGS_READ").on("*")
            .tenantPermission("SGS_KIBANA_ALL_READ").on(TEST_TENANT.getName());
    public final static TestSgConfig.User TEST_READ_ONLY_TENANT_USER = new TestSgConfig
            .User("test_user").roles(TEST_ROLE);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().resources("multitenancy").enterpriseModulesEnabled()
            .tenants(TEST_TENANT)
            .users(USER_DEPT_01, USER_DEPT_02, HR_USER_READ_ONLY, HR_USER_READ_WRITE, TEST_READ_ONLY_TENANT_USER).build();

    @Test
    public void testMt() throws Exception {

        try (GenericRestClient client = cluster.getRestClient("hr_employee", "hr_employee")) {
            String body = "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}";

            GenericRestClient.HttpResponse response = client.putJson(".kibana/_doc/5.6.0?pretty", body, new BasicHeader("sgtenant", "blafasel"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

            response = client.delete(".kibana", new BasicHeader("sgtenant", "business_intelligence"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

            response = client.putJson(".kibana/_doc/5.6.0?pretty", body, new BasicHeader("sgtenant", "business_intelligence"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());

            response = client.putJson(".kibana/_doc/5.6.0?pretty", body, new BasicHeader("sgtenant", "human_resources"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());
            Assert.assertTrue(response.getBody(), Pattern.create("*.kibana_*_humanresources*").matches(response.getBody()));

            response = client.get(".kibana/_doc/5.6.0?pretty", new BasicHeader("sgtenant", "human_resources"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertTrue(response.getBody(), Pattern.create("*human_resources*").matches(response.getBody()));
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
                String body = "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}";

                tc.admin().indices().create(new CreateIndexRequest(".kibana-6").alias(new Alias(".kibana"))
                        .settings(ImmutableMap.of("number_of_shards", 1, "number_of_replicas", 0))).actionGet();

                tc.index(new IndexRequest(".kibana-6").id("6.2.2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(body, XContentType.JSON))
                        .actionGet();
            }

            try (GenericRestClient client = cluster.getRestClient("kibanaro", "kibanaro")) {
                GenericRestClient.HttpResponse res;
                Assert.assertEquals(HttpStatus.SC_OK, (res = client.get(".kibana-6/_doc/6.2.2?pretty")).getStatusCode());
                Assert.assertEquals(HttpStatus.SC_OK, (res = client.get(".kibana/_doc/6.2.2?pretty")).getStatusCode());

                //System.out.println(res.getBody());
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

                GenericRestClient.HttpResponse response = client.get(".kibana_7.12.0/_doc/test");

                Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
                Assert.assertEquals(response.getBody(), ".kibana_-815674808_kibana712aliastest_7.12.0_001",
                        response.getBodyAsDocNode().getAsString("_index"));
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
        String indexName = ".kibana_1592542611_humanresources";
        String testDoc = "{\"buildNum\": 15460, \"defaultIndex\": \"humanresources\", \"tenant\": \"human_resources\"}";

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
                tc.admin().indices().delete(new DeleteIndexRequest(".kibana_1592542611_humanresources")).actionGet();
            } catch (Exception ignored) {
            }
        }

    }

    @Test
    public void checksActionsOfReadOnlyUserAgainstMultitenancy() throws Exception {
        final String tenant = "test_tenant_ro";
        final BasicHeader header = new BasicHeader("sgtenant", tenant);

        try (GenericRestClient restClient = cluster.getRestClient(HR_USER_READ_ONLY, header)) {
            assertAdminCanCreateTenantIndex(restClient, tenant);
            assertTenantWriteable(restClient, tenant, TENANT_NOT_WRITABLE);
            assertThat(restClient, canPerformFollowingActions(EnumSet.noneOf(Action.class)));
        }
    }

    @Test
    public void checksActionsOfReadWriteUserAgainstMultitenancy() throws Exception {
        final String tenant = "test_tenant_rw";
        final BasicHeader header = new BasicHeader("sgtenant", tenant);

        try (GenericRestClient restClient = cluster.getRestClient(HR_USER_READ_WRITE, header)) {
            assertAdminCanCreateTenantIndex(restClient, tenant);
            assertTenantWriteable(restClient, tenant, TENANT_WRITABLE);
            assertThat(restClient, canPerformFollowingActions(EnumSet.of(CREATE_DOCUMENT, UPDATE_DOCUMENT, UPDATE_INDEX, DELETE_INDEX)));
        }
    }

    @Test
    public void checksActionsOfReadOnlyAnonymousAgainstMultitenancy() throws Exception {
        final String tenant = "sg_anonymous";
        final BasicHeader header = new BasicHeader("sgtenant", tenant);

        try (GenericRestClient restClient = cluster.getRestClient(header)) {
            assertAdminCanCreateTenantIndex(restClient, tenant);
            assertTenantWriteable(restClient, tenant, TENANT_NOT_WRITABLE);
            assertThat(restClient, canPerformFollowingActions(EnumSet.noneOf(Action.class)));
        }
    }

    @Test
    public void checksActionsOfReadWriteAnonymousAgainstMultitenancy() throws Exception {
        final String tenant = "anonymous_rw";
        final BasicHeader header = new BasicHeader("sgtenant", tenant);

        try (GenericRestClient restClient = cluster.getRestClient(header)) {
            assertAdminCanCreateTenantIndex(restClient, tenant);
            assertTenantWriteable(restClient, tenant, TENANT_WRITABLE);
            assertThat(restClient, canPerformFollowingActions(EnumSet.of(CREATE_DOCUMENT, UPDATE_DOCUMENT, UPDATE_INDEX, DELETE_INDEX)));
        }
    }

    @Test
    public void dashboardAccessByReadOnlyUser_shouldHavePermissionToLegacyUriAliasUpdate() throws Exception {
        try(GenericRestClient client = cluster.getRestClient(TEST_READ_ONLY_TENANT_USER, new BasicHeader("sg_tenant", TEST_TENANT.getName()));
        Client internalClient = cluster.getInternalNodeClient()) {

            CreateIndexRequest request = new CreateIndexRequest(".kibana_-226117385_testtenant_8.7.1");
            CreateIndexResponse createIndexResponse = internalClient.admin().indices().create(request).actionGet();
            assertThat(createIndexResponse.isAcknowledged(), equalTo(true));

            String body = """
                    {"update":{"_id":"legacy-url-alias:default:dashboard:d90bca50-0910-11ef-ab4b-bf505c6701d0","_index":".kibana_8.7.1","_source":true}}
                    {"script":{"source":"\\n            if (ctx._source[params.type].disabled != true) {\\n              if (ctx._source[params.type].resolveCounter == null) {\\n                ctx._source[params.type].resolveCounter = 1;\\n              }\\n              else {\\n                ctx._source[params.type].resolveCounter += 1;\\n              }\\n              ctx._source[params.type].lastResolved = params.time;\\n              ctx._source.updated_at = params.time;\\n            }\\n          ","lang":"painless","params":{"type":"legacy-url-alias","time":"2024-05-04T12:27:03.048Z"}}}
                    {"update":{"_id":"legacy-url-alias:default:index-pattern:e4275435-dba8-4dbb-882f-509295543b89","_index":".kibana_8.7.1","_source":true}}
                    {"script":{"source":"\\n            if (ctx._source[params.type].disabled != true) {\\n              if (ctx._source[params.type].resolveCounter == null) {\\n                ctx._source[params.type].resolveCounter = 1;\\n              }\\n              else {\\n                ctx._source[params.type].resolveCounter += 1;\\n              }\\n              ctx._source[params.type].lastResolved = params.time;\\n              ctx._source.updated_at = params.time;\\n            }\\n          ","lang":"painless","params":{"type":"legacy-url-alias","time":"2024-05-04T12:27:03.048Z"}}}
                    """;
            HttpResponse response = client.postJson("/_bulk", body);

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode responseBody = response.getBodyAsDocNode();
            assertThat(responseBody, containsValue("$.errors", true));
            assertThat(responseBody, containsValue("$.items[0].update.status", 404));
            assertThat(responseBody, containsValue("$.items[0].update.error.type", "document_missing_exception"));
            assertThat(responseBody, containsValue("$.items[0].update._id", "legacy-url-alias:default:dashboard:d90bca50-0910-11ef-ab4b-bf505c6701d0"));
            assertThat(responseBody, containsValue("$.items[1].update.status", 404));
            assertThat(responseBody, containsValue("$.items[1].update.error.type", "document_missing_exception"));
            assertThat(responseBody, containsValue("$.items[1].update._id", "legacy-url-alias:default:index-pattern:e4275435-dba8-4dbb-882f-509295543b89"));
        }
    }

    private static void assertTenantWriteable(GenericRestClient restClient, String tenant, String isTenantWritable)
        throws Exception {
        final HttpResponse authInfo = restClient.get("/_searchguard/authinfo?pretty");
        assertThat(authInfo.getBody(), authInfo.getBodyAsDocNode().findByJsonPath("sg_tenants." + tenant).get(0).toString(),
            equalTo(isTenantWritable));
    }

    private static void assertAdminCanCreateTenantIndex(GenericRestClient restClient, String tenant) throws Exception {
        final HttpResponse adminIndexDocToCreateTenant = restClient.putJson(
            ".kibana/_doc/5.6.0",
            "{\"buildNum\": 15460, \"defaultIndex\": \"anon\", \"tenant\": \"" + tenant + "\"}",
            encodeBasicHeader("admin", "admin")
        );
        assertThat(adminIndexDocToCreateTenant.getBody(), adminIndexDocToCreateTenant.getStatusCode(), equalTo(HttpStatus.SC_CREATED));
    }
}
