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
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;

public class TransportConfigUpdateAction
extends
TransportNodesAction<ConfigUpdateRequest, ConfigUpdateResponse, TransportConfigUpdateAction.NodeConfigUpdateRequest, ConfigUpdateNodeResponse> {

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

    public static class NodeConfigUpdateRequest extends TransportRequest {

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

    @Override
    protected ConfigUpdateNodeResponse nodeOperation(final NodeConfigUpdateRequest request, Task task) {
        try {
            configurationRepository.reloadConfiguration(CType.fromStringValues(request.configTypes), "Config Update " + request);
           
            return new ConfigUpdateNodeResponse(clusterService.localNode(), request.configTypes, null);
        } catch (Exception e) {
            logger.error("Error in TransportConfigUpdateAction nodeOperation for " + request, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected NodeConfigUpdateRequest newNodeRequest(ConfigUpdateRequest request) {
        return new NodeConfigUpdateRequest(request);
    }
}
