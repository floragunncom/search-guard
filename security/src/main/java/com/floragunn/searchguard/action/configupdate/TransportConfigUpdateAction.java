/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard.action.configupdate;

import java.io.IOException;
import java.util.List;

import com.floragunn.searchguard.GuiceDependencies;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;

public class TransportConfigUpdateAction
extends
TransportNodesAction<ConfigUpdateRequest, ConfigUpdateResponse, TransportConfigUpdateAction.NodeConfigUpdateRequest, ConfigUpdateNodeResponse, TransportConfigUpdateAction> {

    protected Logger logger = LogManager.getLogger(getClass());
    private final ConfigurationRepository configurationRepository;
    
    @Inject
    public TransportConfigUpdateAction(final Settings settings,
                                       final ThreadPool threadPool, final ClusterService clusterService, final TransportService transportService,
                                       final ConfigurationRepository configurationRepository, final ActionFilters actionFilters, GuiceDependencies guiceDependencies,
                                       final IndicesService indicesService, final RepositoriesService repositoriesService) {
        super(ConfigUpdateAction.NAME, clusterService, transportService, actionFilters,
                TransportConfigUpdateAction.NodeConfigUpdateRequest::new,
                threadPool.executor(ThreadPool.Names.MANAGEMENT));

        guiceDependencies.setTransportService(transportService);
        guiceDependencies.setIndicesService(indicesService);
        guiceDependencies.setRepositoriesService(repositoriesService);

        this.configurationRepository = configurationRepository;
    }

    public static class NodeConfigUpdateRequest extends AbstractTransportRequest {

        private String[] configTypes;

        public NodeConfigUpdateRequest(StreamInput in) throws IOException {
            super(in);
            configTypes = in.readStringArray();
        }

        public NodeConfigUpdateRequest(final ConfigUpdateRequest request) {
            super();
            this.configTypes = request.getConfigTypes();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(configTypes);
        }
    }

    @Override
    protected ConfigUpdateNodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new ConfigUpdateNodeResponse(in);
    }
    
    @Override
    protected ConfigUpdateResponse newResponse(ConfigUpdateRequest request, List<ConfigUpdateNodeResponse> responses,
            List<FailedNodeException> failures) {
        return new ConfigUpdateResponse(this.clusterService.getClusterName(), responses, failures);

    }

    /**
     * The reload is performed asynchronously on a dedicated thread inside {@link ConfigurationRepository}. This method
     * therefore does not block the MANAGEMENT thread it is invoked on while waiting for the reload to complete. This
     * avoids a deadlock on nodes with only a single MANAGEMENT thread, where the reload would otherwise occupy that
     * thread while, at the same time, waiting for an index/mapping action that Elasticsearch dispatches to the very same
     * MANAGEMENT pool.
     * <p>
     * The approach is adapted from the OpenSearch Security project (Apache-2.0):
     * <a href="https://github.com/opensearch-project/security/pull/5479">opensearch-project/security#5479</a>. Unlike
     * that fix, this uses the {@code nodeOperationAsync} hook already provided by Elasticsearch's
     * {@code TransportNodesAction}, so no custom async transport base class is required.
     */
    @Override
    protected void nodeOperationAsync(final NodeConfigUpdateRequest request, Task task, ActionListener<ConfigUpdateNodeResponse> listener) {
        configurationRepository.reloadConfiguration(CType.fromStringValues(request.configTypes), "Config Update " + request,
                new ActionListener<ConfigurationRepository.ConfigReloadResponse>() {
                    @Override
                    public void onResponse(ConfigurationRepository.ConfigReloadResponse configReloadResponse) {
                        listener.onResponse(new ConfigUpdateNodeResponse(clusterService.localNode(), request.configTypes, null));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("Error in TransportConfigUpdateAction nodeOperation for " + request, e);
                        listener.onFailure(e);
                    }
                });
    }

    @Override
    protected ConfigUpdateNodeResponse nodeOperation(final NodeConfigUpdateRequest request, Task task) {
        // Not used: nodeOperationAsync(...) is overridden and does not delegate to this method.
        throw new UnsupportedOperationException("nodeOperationAsync is used instead of nodeOperation");
    }

    @Override
    protected NodeConfigUpdateRequest newNodeRequest(ConfigUpdateRequest request) {
        return new NodeConfigUpdateRequest(request);
    }
}
