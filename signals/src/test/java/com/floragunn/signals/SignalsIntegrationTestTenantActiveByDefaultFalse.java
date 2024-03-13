/*
 * Copyright 2021 floragunn GmbH
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
 *
 */

package com.floragunn.signals;

import java.util.concurrent.ExecutionException;

import com.floragunn.signals.proxy.service.HttpProxyHostRegistry;
import com.floragunn.signals.watch.common.throttle.ThrottlePeriodParser;
import com.floragunn.signals.watch.common.throttle.ValidatingThrottlePeriodParser;
import com.floragunn.signals.truststore.service.TrustManagerRegistry;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;
import com.floragunn.signals.watch.init.WatchInitializationService;

import net.jcip.annotations.NotThreadSafe;

import static com.floragunn.signals.watch.common.ValidationLevel.STRICT;

/**
 * Integration tests for signals.all_tenants_active_by_default: false
 */
@NotThreadSafe
public class SignalsIntegrationTestTenantActiveByDefaultFalse {
    private static final Logger log = LogManager.getLogger(SignalsIntegrationTestTenantActiveByDefaultFalse.class);

    private static ScriptService scriptService;
    private static ThrottlePeriodParser throttlePeriodParser;
    private static TrustManagerRegistry trustManagerRegistry;
    private static HttpProxyHostRegistry httpProxyHostRegistry;

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().resources("sg_config/signals").nodeSettings("signals.enabled", true,
            "signals.index_names.log", "signals_main_log", "signals.enterprise.enabled", false, "signals.all_tenants_active_by_default", false)
            .enableModule(SignalsModule.class).waitForComponents("signals").embedded().build();

    @BeforeClass
    public static void setupTestData() {

        try (Client client = cluster.getInternalNodeClient()) {
            client.index(new IndexRequest("testsource").source(XContentType.JSON, "key1", "val1", "key2", "val2")).actionGet();

            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "x", "b", "y"))
                    .actionGet();
            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "xx", "b", "yy"))
                    .actionGet();
        }
    }

    @BeforeClass
    public static void setupDependencies() {
        scriptService = cluster.getInjectable(ScriptService.class);
        throttlePeriodParser = new ValidatingThrottlePeriodParser(cluster.getInjectable(Signals.class).getSignalsSettings());
        trustManagerRegistry = cluster.getInjectable(Signals.class).getTruststoreRegistry();
        httpProxyHostRegistry = cluster.getInjectable(Signals.class).getHttpProxyHostRegistry();
    }

    @Test
    public void testPutWatch() throws Exception {
        String tenant = "_main";
        String watchId = "put_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        try (Client client = cluster.getInternalNodeClient();
                GenericRestClient restClient = cluster.getRestClient("uhura", "uhura").trackResources()) {
            client.admin().indices().create(new CreateIndexRequest("testsink_put_watch")).actionGet();

            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch").name("testsink").build();
            HttpResponse response = restClient.putJson(watchPath, watch.toJson());

            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = restClient.get(watchPath);

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            WatchInitializationService initService = new WatchInitializationService(null, scriptService,
                trustManagerRegistry, httpProxyHostRegistry, throttlePeriodParser, STRICT);
            watch = Watch.parseFromElasticDocument(initService, "test", "put_test", response.getBody(), -1);

            Thread.sleep(2000);

            Assert.assertEquals(0, getCountOfDocuments(client, "testsink_put_watch"));

            response = restClient.putJson("/_signals/tenant/" + tenant + "/_active", "");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            awaitMinCountOfDocuments(client, "testsink_put_watch", 1);

            response = restClient.delete("/_signals/tenant/" + tenant + "/_active");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            Thread.sleep(500);
            long countNow = getCountOfDocuments(client, "testsink_put_watch");
            Thread.sleep(1000);
            Assert.assertEquals(countNow, getCountOfDocuments(client, "testsink_put_watch"));

        }
    }

    private long getCountOfDocuments(Client client, String index) throws InterruptedException, ExecutionException {
        SearchRequest request = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        request.source(searchSourceBuilder);

        SearchResponse response = client.search(request).get();

        return response.getHits().getTotalHits().value;
    }

    private long awaitMinCountOfDocuments(Client client, String index, long minCount) throws Exception {
        long start = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {
            Thread.sleep(10);
            long count = getCountOfDocuments(client, index);

            if (count >= minCount) {
                log.info("Found " + count + " documents in " + index + " after " + (System.currentTimeMillis() - start) + " ms");

                return count;
            }
        }

        Assert.fail("Did not find " + minCount + " documents in " + index + " after " + (System.currentTimeMillis() - start) + " ms");

        return 0;
    }

}
