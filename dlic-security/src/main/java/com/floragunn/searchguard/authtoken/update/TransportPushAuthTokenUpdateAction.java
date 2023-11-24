/*
 * Copyright 2020 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.authtoken.update;

import java.io.IOException;
import java.util.List;

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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.authtoken.AuthTokenService;

public class TransportPushAuthTokenUpdateAction extends
        TransportNodesAction<PushAuthTokenUpdateRequest, PushAuthTokenUpdateResponse, TransportPushAuthTokenUpdateAction.NodeRequest, PushAuthTokenUpdateNodeResponse> {

    private final AuthTokenService authTokenService;

    @Inject
    public TransportPushAuthTokenUpdateAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
            TransportService transportService, ActionFilters actionFilters, AuthTokenService authTokenService) {
        super(PushAuthTokenUpdateAction.NAME, threadPool, clusterService, transportService, actionFilters, PushAuthTokenUpdateRequest::new,
                TransportPushAuthTokenUpdateAction.NodeRequest::new, threadPool.executor(ThreadPool.Names.MANAGEMENT));

        this.authTokenService = authTokenService;
    }

    public static class NodeRequest extends BaseNodesRequest {

        PushAuthTokenUpdateRequest request;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new PushAuthTokenUpdateRequest(in);
        }

        public NodeRequest(PushAuthTokenUpdateRequest request) {
            super((String[]) null);
            this.request = request;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    @Override
    protected PushAuthTokenUpdateNodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new PushAuthTokenUpdateNodeResponse(in);
    }

    @Override
    protected PushAuthTokenUpdateResponse newResponse(PushAuthTokenUpdateRequest request, List<PushAuthTokenUpdateNodeResponse> responses,
            List<FailedNodeException> failures) {
        return new PushAuthTokenUpdateResponse(this.clusterService.getClusterName(), responses, failures);

    }

    @Override
    protected PushAuthTokenUpdateNodeResponse nodeOperation(NodeRequest request, Task task) {
        String status = authTokenService.pushAuthTokenUpdate(request.request);

        return new PushAuthTokenUpdateNodeResponse(clusterService.localNode(), status);
    }

    @Override
    protected NodeRequest newNodeRequest(PushAuthTokenUpdateRequest request) {
        return new NodeRequest(request);
    }
}
