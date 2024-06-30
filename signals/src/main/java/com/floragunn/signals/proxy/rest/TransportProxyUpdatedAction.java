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
package com.floragunn.signals.proxy.rest;

import com.floragunn.signals.Signals;
import com.floragunn.signals.proxy.service.HttpProxyHostRegistry;
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
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class TransportProxyUpdatedAction extends
    TransportNodesAction<TransportProxyUpdatedAction.ProxyUpdatedRequest, TransportProxyUpdatedAction.ProxyUpdatedResponse, TransportProxyUpdatedAction.NodeRequest,
        TransportProxyUpdatedAction.NodeResponse> {

    private static final Logger log = LogManager.getLogger(TransportProxyUpdatedAction.class);

    private final HttpProxyHostRegistry httpProxyHostRegistry;

    @Inject
    public TransportProxyUpdatedAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                       ActionFilters actionFilters, Signals signals) {
        super(ProxyUpdatedActionType.NAME,threadPool, clusterService, transportService, actionFilters, ProxyUpdatedRequest::new, NodeRequest::new,
                threadPool.executor(ThreadPool.Names.MANAGEMENT));
        this.httpProxyHostRegistry = Objects.requireNonNull(signals.getHttpProxyHostRegistry(), "Http proxy host registry is required");
    }

    @Override
    protected ProxyUpdatedResponse newResponse(
            ProxyUpdatedRequest request, List<NodeResponse> nodeResponses, List<FailedNodeException> failures) {
        return new ProxyUpdatedResponse(this.clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(ProxyUpdatedRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeResponse(in);
    }

    @Override
    protected NodeResponse nodeOperation(NodeRequest request, Task task) {
        DiscoveryNode localNode = clusterService.localNode();
        log.info("Local node '{}' received notification about proxy update '{}'.", localNode.getName(), request);
        String proxyId = request.getProxyId();
        try {
            httpProxyHostRegistry.onProxyUpdate(proxyId, request.getOperationType());
            return new NodeResponse(localNode);
        } catch (Exception ex) {
            String message = "Cannot update proxy host for proxy '" + proxyId + "'. ";
            log.error(message, ex);
            return new NodeResponse(localNode, message + ex.getMessage());
        }
    }

    public static class NodeRequest extends BaseNodesRequest<NodeRequest> {

        private final ProxyUpdatedRequest request;

        public NodeRequest(StreamInput streamInput) throws IOException {
            super(streamInput);
            this.request = new ProxyUpdatedRequest(streamInput);
        }

        public NodeRequest(ProxyUpdatedRequest request) {
            super((String[]) null);
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }

        public String getProxyId() {
            return request.getProxyId();
        }

        public String getOperationType() {
            return request.getOperationType();
        }

        @Override
        public String toString() {
            return "NodeRequest{" + "request=" + request + '}';
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

    public static class ProxyUpdatedActionType extends ActionType<ProxyUpdatedResponse> {

        public static final String NAME = "cluster:admin:searchguard:signals:proxies/update";
        public static final ProxyUpdatedActionType INSTANCE = new ProxyUpdatedActionType();
        protected ProxyUpdatedActionType() {
            super(NAME, ProxyUpdatedResponse::new);
        }

        public static ActionFuture<ProxyUpdatedResponse> send(Client client, String proxyId, String operationType) {
            Objects.requireNonNull(client, "Client is required to send action " + NAME);
            return client.execute(ProxyUpdatedActionType.INSTANCE, new ProxyUpdatedRequest(proxyId, operationType));
        }
    }

    public static class ProxyUpdatedRequest extends BaseNodesRequest<ProxyUpdatedRequest> {

        private final String proxyId;
        private final String operationType;

        protected ProxyUpdatedRequest(StreamInput in) throws IOException {
            super(in);
            this.proxyId = in.readString();
            this.operationType = in.readString();
        }

        public ProxyUpdatedRequest(String proxyId, String operationType) {
            super(new String[0]);
            this.proxyId = Objects.requireNonNull(proxyId, "Proxy id is required");
            this.operationType = Objects.requireNonNull(operationType, "Operation type is required");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(proxyId);
            out.writeString(operationType);
        }

        public String getProxyId() {
            return proxyId;
        }

        public String getOperationType() {
            return operationType;
        }

        @Override
        public String toString() {
            return "ProxyUpdatedRequest{" + "proxyId='" + proxyId + '\'' + ", operationType='" + operationType + '\'' + '}';
        }
    }

    public static class ProxyUpdatedResponse extends BaseNodesResponse<NodeResponse> {
        public ProxyUpdatedResponse(StreamInput in) throws IOException {
            super(in);
        }

        public ProxyUpdatedResponse(
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
