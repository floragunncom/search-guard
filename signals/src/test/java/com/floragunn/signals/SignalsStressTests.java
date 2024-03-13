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

import java.time.Duration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchsupport.jobs.core.IndexJobStateStore;
import com.floragunn.searchsupport.junit.AsyncAssert;
import com.floragunn.searchsupport.junit.LoggingTestWatcher;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;

import net.jcip.annotations.NotThreadSafe;

/**
 * Tests which prove stressful for the engineer
 */
@NotThreadSafe
public class SignalsStressTests {
    private static final Logger log = LogManager.getLogger(SignalsStressTests.class);

    static {
        // This is necessary to test multi-node setups in single JVMs:
        IndexJobStateStore.includeNodeIdInSchedulerToJobStoreMapKeys = true;
    }

    @Rule
    public LoggingTestWatcher loggingTestWatcher = new LoggingTestWatcher();

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().resources("sg_config/no-tenants")
            .nodeSettings("signals.enabled", true, "signals.index_names.log", "signals_main_log", "searchguard.enterprise_modules_enabled", false,
                    "searchguard.diagnosis.action_stack.enabled", true)
            .clusterConfiguration(ClusterConfiguration.THREE_MASTERS) // In case we kill the master
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

    @Test
    public void failoverOnClusterChangeTest() throws Exception {
        String tenant = "_main";
        String watchId = "put_test";
        String watchPath = "/_signals/watch/" + tenant + "/" + watchId;

        final String watchRunsOnNode;
        final String ackedAt;

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {
            Watch watch = new WatchBuilder(watchId).atMsInterval(100).search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink_put_watch").name("testsink").build();

            restClient.putJson(watchPath, watch.toJson());

            AsyncAssert.awaitAssert("Watch did not get assigned a node", () -> {
                try {
                    String node = restClient.get(watchPath + "/_state").getBodyAsDocNode().getAsString("node");
                    
                    if (node != null && node.length() > 0 && !node.equals("null")) {
                        return true;
                    } else {
                        return false;
                    }

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }, Duration.ofSeconds(10));

            watchRunsOnNode = restClient.get(watchPath + "/_state").getBodyAsDocNode().getAsString("node");
            
            log.info("Watch runs on node " + watchRunsOnNode);

            Thread.sleep(500);

            Assert.assertEquals(200, restClient.put(watchPath + "/_ack").getStatusCode());

            AsyncAssert.awaitAssert("Watch state contains acked date", () -> {
                try {
                    String acked = restClient.get(watchPath + "/_state").getBodyAsDocNode().get("actions", "testsink", "acked", "on").toString();


                    if (acked != null && acked.length() > 0 && !acked.equals("null")) {
                        return true;
                    } else {
                        return false;
                    }

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }, Duration.ofSeconds(10));

            ackedAt = restClient.get(watchPath + "/_state").getBodyAsDocNode().getAsNode("actions", "testsearch", "acked").getOrDefault("on", "").toString();
        }

        Thread.sleep(2000);

        log.warn("Stopping node " + watchRunsOnNode + "; watch must now find a new home.");

        cluster.getNodeByName(watchRunsOnNode).stop();

        Thread.sleep(500);

        try (GenericRestClient restClient = cluster.getRestClient("uhura", "uhura")) {
            AsyncAssert.awaitAssert("Watch got assigned a different node", () -> {
                try {
                    String node = restClient.get(watchPath + "/_state").getBodyAsDocNode().getAsString("node");
                    
                    if (node != null && node.length() > 0 && !node.equals("null") && !node.equals(watchRunsOnNode)) {
                        return true;
                    } else {
                        return false;
                    }

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }, Duration.ofSeconds(80));

            com.floragunn.searchguard.test.GenericRestClient.HttpResponse response = restClient.get(watchPath + "/_state");

            String watchRunsOnNodeNow = response.getBodyAsDocNode().getAsString("node");
            
            String newAckedAt = response.getBodyAsDocNode().getAsNode("actions", "testsearch", "acked").getOrDefault("on", "").toString();
            
            log.info("Watch moved from " + watchRunsOnNode + " to " + watchRunsOnNodeNow);

            Assert.assertEquals(response.getBody(), ackedAt, newAckedAt);
        }

    }

}
