/*
 * Copyright 2020-2022 floragunn GmbH
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
import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.floragunn.searchguard.SearchGuardPlugin.ProtectedIndices;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.junit.AsyncAssert;
import com.floragunn.searchsupport.junit.LoggingTestWatcher;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.checks.Check;

public class SignalsIndicesTest {

    @Rule
    public LoggingTestWatcher loggingTestWatcher = new LoggingTestWatcher();

    /**
     * This has the Signals module disabled!
     */
    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().nodeSettings("searchguard.diagnosis.action_stack.enabled", true)
            .embedded().build();

    @Test
    public void indexMappingUpdate() throws Exception {
        ClusterService clusterService = cluster.getInjectable(ClusterService.class);
        ProtectedConfigIndexService protectedConfigIndexService = new ProtectedConfigIndexService(cluster.getInternalNodeClient(),
                cluster.getInjectable(ClusterService.class), cluster.getInjectable(ThreadPool.class), new ProtectedIndices());

        try (Client client = cluster.getInternalNodeClient()) {
            CreateIndexResponse response = client.admin().indices().create(new CreateIndexRequest(".signals_watches")
                    .mapping("_doc", getOldWatchIndexMapping()).settings(Settings.builder().put("index.hidden", true))).actionGet();

            Assert.assertTrue(response.toString(), response.isAcknowledged());
        }

        Settings.Builder settings = Settings.builder().put("searchguard.enterprise_modules_enabled", false);

        Signals signals = new Signals(settings.build(), new ComponentState(0, "signals", "signals"));
        signals.createComponents(cluster.getInternalNodeClient(), clusterService, cluster.getInjectable(ThreadPool.class), null, null, null, null,
                cluster.getInjectable(NodeEnvironment.class), null, protectedConfigIndexService, null);

        // Actually trigger the creation:
        protectedConfigIndexService.onNodeStart();

        try {
            AsyncAssert.awaitAssert("Index updated", () -> clusterService.state().getMetadata().indices().get(".signals_watches").mapping()
                    .getSourceAsMap().toString().contains("value_no_map"), Duration.ofSeconds(10));
        } finally {
            System.out
                    .println("Updated mapping: " + clusterService.state().getMetadata().indices().get(".signals_watches").mapping().getSourceAsMap());
        }
    }

    static Map<String, Object> getOldWatchIndexMapping() {
        NestedValueMap result = new NestedValueMap();

        result.put("dynamic", true);
        result.put(new NestedValueMap.Path("properties", "checks"), Check.getIndexMapping());
        result.put(new NestedValueMap.Path("properties", "_tenant", "type"), "text");
        result.put(new NestedValueMap.Path("properties", "_tenant", "analyzer"), "keyword");
        result.put(new NestedValueMap.Path("properties", "actions", "dynamic"), true);
        result.put(new NestedValueMap.Path("properties", "actions", "properties", "checks"), Check.getIndexMapping());

        return result;
    }

    static Map<String, Object> getOldCheckIndexMapping() {
        NestedValueMap result = new NestedValueMap();

        result.put("dynamic", true);

        NestedValueMap properties = new NestedValueMap();
        properties.put(new NestedValueMap.Path("request", "type"), "object");
        properties.put(new NestedValueMap.Path("request", "dynamic"), true);
        properties.put(new NestedValueMap.Path("request", "properties", "body", "type"), "object");
        properties.put(new NestedValueMap.Path("request", "properties", "body", "dynamic"), true);
        properties.put(new NestedValueMap.Path("request", "properties", "body", "enabled"), false);

        result.put("properties", properties);

        return result;
    }

}
