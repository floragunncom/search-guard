/*
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
 *
 */
package com.floragunn.signals.actions.account.config_update;

import com.floragunn.signals.Signals;
import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportDestinationConfigUpdateAction extends
        TransportNodesAction<DestinationConfigUpdateRequest, DestinationConfigUpdateResponse, TransportDestinationConfigUpdateAction.NodeRequest, TransportDestinationConfigUpdateAction.NodeResponse> {

    private final static Logger log = LogManager.getLogger(TransportDestinationConfigUpdateAction.class);
    final Client client;
    private final Signals signals;

    @Inject
    public TransportDestinationConfigUpdateAction(Signals signals, final Settings settings, final ThreadPool threadPool,
            final ClusterService clusterService, final TransportService transportService, final ActionFilters actionFilters, final Client client) {
        super(DestinationConfigUpdateAction.NAME, threadPool, clusterService, transportService, actionFilters, DestinationConfigUpdateRequest::new,
                TransportDestinationConfigUpdateAction.NodeRequest::new, ThreadPool.Names.MANAGEMENT,
                TransportDestinationConfigUpdateAction.NodeResponse.class);

        this.client = client;
        this.signals = signals;

    }

    @Override
    protected DestinationConfigUpdateResponse newResponse(DestinationConfigUpdateRequest request, List<NodeResponse> responses,
            List<FailedNodeException> failures) {
        return new DestinationConfigUpdateResponse(this.clusterService.getClusterName(), responses, failures);

    }

    @Override
    protected NodeResponse nodeOperation(final NodeRequest request) {
        DiscoveryNode localNode = clusterService.localNode();

        try {
            signals.getAccountRegistry().updateAtomic(client);
            return new NodeResponse(localNode, NodeResponse.Status.SUCCESS, "");
        } catch (Exception e) {
            log.error("Error while updating destinations", e);
            return new NodeResponse(localNode, NodeResponse.Status.EXCEPTION, e.toString());
        }
    }

    public static class NodeRequest extends BaseNodeRequest {

        DestinationConfigUpdateRequest request;

        public NodeRequest(final DestinationConfigUpdateRequest request) {
            this.request = request;
        }

        public NodeRequest(final StreamInput in) throws IOException {
            super(in);
            request = new DestinationConfigUpdateRequest(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    public static class NodeResponse extends BaseNodeResponse {

        private Status status;
        private String message;

        public NodeResponse(final DiscoveryNode node, Status status, String message) {
            super(node);
            this.status = status;
            this.message = message;
        }

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            status = in.readEnum(Status.class);
            message = in.readOptionalString();
        }

        public static TransportDestinationConfigUpdateAction.NodeResponse readNodeResponse(StreamInput in) throws IOException {
            TransportDestinationConfigUpdateAction.NodeResponse result = new TransportDestinationConfigUpdateAction.NodeResponse(in);
            return result;
        }

        public String getMessage() {
            return message;
        }

        public Status getStatus() {
            return status;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeEnum(status);
            out.writeOptionalString(message);
        }

        @Override
        public String toString() {
            return "NodeResponse [status=" + status + ", message=" + message + "]";
        }

        public static enum Status {
            SUCCESS, EXCEPTION
        }
    }

    @Override
    protected NodeRequest newNodeRequest(DestinationConfigUpdateRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeResponse(in);
    }

}
