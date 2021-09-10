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

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.floragunn.searchguard.authtoken.AuthTokenService;

public class TransportPushAuthTokenUpdateAction extends
        TransportNodesAction<PushAuthTokenUpdateRequest, PushAuthTokenUpdateResponse, TransportPushAuthTokenUpdateAction.NodeRequest, PushAuthTokenUpdateNodeResponse> {

    private final AuthTokenService authTokenService;

    @Inject
    public TransportPushAuthTokenUpdateAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
            TransportService transportService, ActionFilters actionFilters, AuthTokenService authTokenService) {
        super(PushAuthTokenUpdateAction.NAME, threadPool, clusterService, transportService, actionFilters, PushAuthTokenUpdateRequest::new,
                TransportPushAuthTokenUpdateAction.NodeRequest::new, ThreadPool.Names.MANAGEMENT, PushAuthTokenUpdateNodeResponse.class);

        this.authTokenService = authTokenService;
    }

    public static class NodeRequest extends BaseNodeRequest {

        PushAuthTokenUpdateRequest request;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new PushAuthTokenUpdateRequest(in);
        }

        public NodeRequest(PushAuthTokenUpdateRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    @Override
    protected PushAuthTokenUpdateNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new PushAuthTokenUpdateNodeResponse(in);
    }

    @Override
    protected PushAuthTokenUpdateResponse newResponse(PushAuthTokenUpdateRequest request, List<PushAuthTokenUpdateNodeResponse> responses,
            List<FailedNodeException> failures) {
        return new PushAuthTokenUpdateResponse(this.clusterService.getClusterName(), responses, failures);

    }

    @Override
    protected PushAuthTokenUpdateNodeResponse nodeOperation(NodeRequest request) {
        String status = authTokenService.pushAuthTokenUpdate(request.request);

        return new PushAuthTokenUpdateNodeResponse(clusterService.localNode(), status);
    }

    @Override
    protected NodeRequest newNodeRequest(PushAuthTokenUpdateRequest request) {
        return new NodeRequest(request);
    }
}
