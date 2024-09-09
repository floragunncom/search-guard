/*
 * Copyright 2020-2023 floragunn GmbH
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
package com.floragunn.signals.truststore.rest;

import static com.floragunn.signals.truststore.rest.TransportTruststoreUpdatedAction.TruststoreUpdatedActionType.NAME;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import com.floragunn.signals.Signals;
import com.floragunn.signals.truststore.service.TrustManagerRegistry;

public class TransportTruststoreUpdatedAction extends
    TransportNodesAction<TransportTruststoreUpdatedAction.TruststoreUpdatedRequest, TransportTruststoreUpdatedAction.TruststoreUpdatedResponse, TransportTruststoreUpdatedAction.NodeRequest,
        TransportTruststoreUpdatedAction.NodeResponse> {

    private static final Logger log = LogManager.getLogger(TransportTruststoreUpdatedAction.class);

    private final TrustManagerRegistry trustManagerRegistry;

    @Inject
    public TransportTruststoreUpdatedAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
        ActionFilters actionFilters, Signals signals) {
        super(NAME,clusterService, transportService, actionFilters, NodeRequest::new,
                threadPool.executor(ThreadPool.Names.MANAGEMENT));
        this.trustManagerRegistry = Objects.requireNonNull(signals.getTruststoreRegistry(), "Truststore registry is required");
    }

    @Override
    protected TruststoreUpdatedResponse newResponse(
        TruststoreUpdatedRequest request, List<NodeResponse> nodeResponses, List<FailedNodeException> failures) {
        return new TruststoreUpdatedResponse(this.clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(TruststoreUpdatedRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeResponse(in);
    }

    @Override
    protected NodeResponse nodeOperation(NodeRequest request, Task task) {
        DiscoveryNode localNode = clusterService.localNode();
        log.info("Local node '{}' received notification about truststore update '{}'.", localNode.getName(), request);
        String truststoreId = request.getTruststoreId();
        try {
            trustManagerRegistry.onTruststoreUpdate(truststoreId, request.getOperationType());
            return new NodeResponse(localNode);
        } catch (Exception ex) {
            String message = "Cannot update trust managers for trust store '" + truststoreId + "'. ";
            log.error(message, ex);
            return new NodeResponse(localNode, message + ex.getMessage());
        }
    }

    public static class NodeRequest extends TransportRequest {

        private final String truststoreId;
        private final String operationType;

        public NodeRequest(StreamInput streamInput) throws IOException {
            super(streamInput);
            this.truststoreId = streamInput.readString();
            this.operationType = streamInput.readString();
        }

        public NodeRequest(TruststoreUpdatedRequest request) {
            super();
            this.truststoreId = request.getTruststoreId();
            this.operationType = request.getOperationType();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(truststoreId);
            out.writeString(operationType);
        }

        public String getTruststoreId() {
            return truststoreId;
        }

        public String getOperationType() {
            return operationType;
        }

        @Override
        public String toString() {
            return "NodeRequest{" + "truststoreId=" + truststoreId + ", operationType=" + operationType + '}';
        }
    }

    public static class NodeResponse extends BaseNodeResponse {

        private final boolean success;
        private final String message;

        protected NodeResponse(StreamInput in) throws IOException {
            super(in);
            this.success = in.readBoolean();
            this.message = in.readOptionalString();
        }

        public NodeResponse(DiscoveryNode node) {
            super(node);
            this.success = true;
            this.message = null;
        }

        public NodeResponse(DiscoveryNode node, String errorMessage) {
            super(node);
            this.success = false;
            this.message = errorMessage;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(success);
            out.writeOptionalString(message);
        }
    }

    public static class TruststoreUpdatedActionType extends ActionType<TruststoreUpdatedResponse> {

        public static final String NAME = "cluster:admin:searchguard:signals:truststores/update";
        public static final TruststoreUpdatedActionType INSTANCE = new TruststoreUpdatedActionType();
        protected TruststoreUpdatedActionType() {
            super(NAME);
        }

        public static ActionFuture<TruststoreUpdatedResponse> send(Client client, String truststoreId, String operationType) {
            Objects.requireNonNull(client, "Client is required to send action " + NAME);
            return client.execute(TruststoreUpdatedActionType.INSTANCE, new TruststoreUpdatedRequest(truststoreId, operationType));
        }
    }

    public static class TruststoreUpdatedRequest extends BaseNodesRequest<TruststoreUpdatedRequest> {

        private final String truststoreId;
        private final String operationType;

        public TruststoreUpdatedRequest(String truststoreId, String operationType) {
            super(new String[0]);
            this.truststoreId = Objects.requireNonNull(truststoreId, "Truststore id is required");
            this.operationType = Objects.requireNonNull(operationType, "Operation type is required");
        }

        public String getTruststoreId() {
            return truststoreId;
        }

        public String getOperationType() {
            return operationType;
        }

        @Override
        public String toString() {
            return "TruststoreUpdatedRequest{" + "truststoreId='" + truststoreId + '\'' + ", operationType='" + operationType + '\'' + '}';
        }
    }

    public static class TruststoreUpdatedResponse extends BaseNodesResponse<NodeResponse> {
        public TruststoreUpdatedResponse(StreamInput in) throws IOException {
            super(in);
        }

        public TruststoreUpdatedResponse(
            ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readCollectionAsList(NodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            out.writeCollection(nodes);
        }
    }
}
