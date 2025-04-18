/*
 * Copyright 2025 floragunn GmbH
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

package com.floragunn.searchguard;

import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.plugins.Plugin;

import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class LeakDetectorTest {

    @Test
    public void testLeakDetected() {
        LocalCluster.Embedded localCluster = new LocalCluster.Builder().singleNode().sslEnabled().embedded().plugin(MemoryLeakPlugin.class).build();
        try {
            localCluster.before();
        } catch (Throwable e) {
            throw new RuntimeException("Unable to start test cluster", e);
        }
        ClusterHealthResponse healthResponse = localCluster.getInternalNodeClient().admin().cluster()
                .prepareHealth(TimeValue.timeValueSeconds(10))
                .setWaitForStatus(ClusterHealthStatus.GREEN)
                .setIndices("test-index")
                .setTimeout(TimeValue.timeValueSeconds(10))
                .execute().actionGet();
        assertThat(healthResponse.isTimedOut(), is(false));
        try {
            localCluster.close();
        } catch (Exception e) {
            if (e instanceof RuntimeException runtimeException) {
                assertThat(runtimeException.getMessage(), containsString("List of logged leaks is not empty"));
            } else {
                assertThat("Expected exception not thrown", false);
            }
        }
    }

    public static class MemoryLeakPlugin extends Plugin {

        @Override
        public Collection<?> createComponents(PluginServices services) {

            Client client = services.client();

            ClusterStateListener listener = new ClusterStateListener() {
                @Override
                public void clusterChanged(ClusterChangedEvent event) {
                    if (! event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
                        services.clusterService().removeListener(this);
                        services.threadPool().generic().submit(() -> {
                            client.admin().indices().create(new CreateIndexRequest("test-index").settings(ImmutableMap.of("number_of_shards", 1, "number_of_replicas", 0)));
                            client.index(new IndexRequest("test-index").source(Map.of("a", "a")).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).actionGet();

                            for (int i = 0; i < 1000; i++) {
                                //the line below should cause memory leaks
                                client.search(new SearchRequest("test-index")).actionGet();
                            }
                        });
                    }
                }
            };
            services.clusterService().addListener(listener);

            return Collections.emptyList();
        }
    }
}
