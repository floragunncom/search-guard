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

package com.floragunn.searchguard.configuration;

import java.time.Duration;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.SearchGuardPlugin.ProtectedIndices;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService.ConfigIndex;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchsupport.junit.AsyncAssert;
import com.google.common.collect.ImmutableMap;

public class ProtectedConfigIndexServiceTest {
    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().embedded().build();

    @Test
    public void mappingUpdate() throws Exception {
        ClusterService clusterService = cluster.getInjectable(ClusterService.class);

        ProtectedConfigIndexService service = new ProtectedConfigIndexService(cluster.getInternalNodeClient(), clusterService,
                cluster.getInjectable(ThreadPool.class), new ProtectedIndices());

        service.createIndex(new ConfigIndex(".test_mapping_update")
                .mapping(ImmutableMap.of("properties", ImmutableMap.of("x", ImmutableMap.of("type", "text")))));

        service.onNodeStart();

        AsyncAssert.awaitAssert("Index created", () -> clusterService.state().getMetadata().indices().containsKey(".test_mapping_update"),
                Duration.ofSeconds(10));

        service = new ProtectedConfigIndexService(cluster.getInternalNodeClient(), clusterService, cluster.getInjectable(ThreadPool.class),
                new ProtectedIndices());

        service.createIndex(
                new ConfigIndex(".test_mapping_update")
                        .mapping(ImmutableMap.of("properties",
                                ImmutableMap.of("x", ImmutableMap.of("type", "text"), "y", ImmutableMap.of("type", "text"))), 2)
                        .mappingUpdate(0, ImmutableMap.of("properties", ImmutableMap.of("y", ImmutableMap.of("type", "text")))));

        service.onNodeStart();

        AsyncAssert.awaitAssert(
                "Index updated", () -> clusterService.state().getMetadata().indices().get(".test_mapping_update").mapping().getSourceAsMap()
                        .get("properties").equals(ImmutableMap.of("x", ImmutableMap.of("type", "text"), "y", ImmutableMap.of("type", "text"))),
                Duration.ofSeconds(10));

    }

    @Test
    public void settingsAndMappingUpdate() throws Exception {

        final String indexName = ".test_settings_mapping_update";

        ClusterService clusterService = cluster.getInjectable(ClusterService.class);

        AcknowledgedResponse acknowledgedResponse = cluster.getInternalNodeClient().admin().indices()
                .create(new CreateIndexRequest(indexName)
                        .mapping("_doc", ImmutableMap.of("properties", ImmutableMap.of("x", ImmutableMap.of("type", "text")))).waitForActiveShards(1))
                .actionGet();

        Assert.assertTrue(acknowledgedResponse.isAcknowledged());

        GetSettingsResponse getSettingsResponse = cluster.getInternalNodeClient().admin().indices()
                .getSettings(new GetSettingsRequest().indices(indexName)).actionGet();

        Assert.assertFalse(IndexMetadata.INDEX_HIDDEN_SETTING.get(getSettingsResponse.getIndexToSettings().get(indexName)));

        ProtectedConfigIndexService service = new ProtectedConfigIndexService(cluster.getInternalNodeClient(), clusterService,
                cluster.getInjectable(ThreadPool.class), new ProtectedIndices());

        service.createIndex(
                new ConfigIndex(indexName)
                        .mapping(ImmutableMap.of("properties",
                                ImmutableMap.of("x", ImmutableMap.of("type", "text"), "y", ImmutableMap.of("type", "text"))), 2)
                        .mappingUpdate(0, ImmutableMap.of("properties", ImmutableMap.of("y", ImmutableMap.of("type", "text")))));

        service.onNodeStart();

        AsyncAssert
                .awaitAssert("Index updated",
                        () -> clusterService.state().getMetadata().indices().get(indexName).mapping().getSourceAsMap().get("properties")
                                .equals(ImmutableMap.of("x", ImmutableMap.of("type", "text"), "y", ImmutableMap.of("type", "text"))),
                        Duration.ofSeconds(30));

        AsyncAssert.awaitAssert("Index hidden", () -> clusterService.state().getMetadata().indices().get(indexName).isHidden(),
                Duration.ofSeconds(10));
    }

    @Test
    public void settingsUpdate() throws Exception {

        final String indexName = ".test_settings_update";

        ClusterService clusterService = cluster.getInjectable(ClusterService.class);

        AcknowledgedResponse acknowledgedResponse = cluster.getInternalNodeClient().admin().indices()
                .create(new CreateIndexRequest(indexName).waitForActiveShards(1)).actionGet();

        Assert.assertTrue(acknowledgedResponse.isAcknowledged());

        GetSettingsResponse getSettingsResponse = cluster.getInternalNodeClient().admin().indices()
                .getSettings(new GetSettingsRequest().indices(indexName)).actionGet();

        Assert.assertFalse(IndexMetadata.INDEX_HIDDEN_SETTING.get(getSettingsResponse.getIndexToSettings().get(indexName)));

        ProtectedConfigIndexService service = new ProtectedConfigIndexService(cluster.getInternalNodeClient(), clusterService,
                cluster.getInjectable(ThreadPool.class), new ProtectedIndices());

        service.createIndex(new ConfigIndex(indexName));

        service.onNodeStart();

        AsyncAssert.awaitAssert("Index hidden", () -> clusterService.state().getMetadata().indices().get(indexName).isHidden(),
                Duration.ofSeconds(10));
    }

}
