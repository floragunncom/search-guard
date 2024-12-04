/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.authc.session.backend;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

public class PushSessionTokenUpdateAction extends ActionType<PushSessionTokenUpdateAction.Response> {

    public static final PushSessionTokenUpdateAction INSTANCE = new PushSessionTokenUpdateAction();
    public static final String NAME = "cluster:admin/searchguard/session_token/update/push";

    protected PushSessionTokenUpdateAction() {
        super(NAME);
    }

    public static class Request extends BaseNodesRequest<Request> {

        private SessionToken updatedToken;
        private UpdateType updateType;
        private long newHash;

        public Request(SessionToken updatedToken, UpdateType updateType, long newHash) {
            super(new String[0]);
            this.updatedToken = updatedToken;
            this.updateType = updateType;
            this.newHash = newHash;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public static enum UpdateType {
            NEW, REVOKED
        }

        public SessionToken getUpdatedToken() {
            return updatedToken;
        }

        public UpdateType getUpdateType() {
            return updateType;
        }

        @Override
        public String toString() {
            return "PushSessionTokenUpdateAction.Request [updatedToken=" + updatedToken + ", updateType=" + updateType + "]";
        }

        public long getNewHash() {
            return newHash;
        }
    }

    public static class Response extends BaseNodesResponse<NodeResponse> {

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(final ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        public List<NodeResponse> readNodesFrom(final StreamInput in) throws IOException {
            return in.readCollectionAsList(NodeResponse::readNodeResponse);
        }

        @Override
        public void writeNodesTo(final StreamOutput out, List<NodeResponse> nodes) throws IOException {
            out.writeCollection(nodes);
        }

        @Override
        public String toString() {
            return "PushSessionTokenUpdateAction.Response [failures()=" + failures() + ", getNodes()=" + getNodes() + "]";
        }
    }

    public static class NodeRequest extends TransportRequest {

        private SessionToken updatedToken;
        private Request.UpdateType updateType;
        private long newHash;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            this.updatedToken = new SessionToken(in);
            this.updateType = in.readEnum(Request.UpdateType.class);
            this.newHash = in.readLong();
        }

        public NodeRequest(Request request) {
            super();
            this.updatedToken = request.getUpdatedToken();
            this.updateType = request.getUpdateType();
            this.newHash = request.getNewHash();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            this.updatedToken.writeTo(out);
            out.writeEnum(this.updateType);
            out.writeLong(this.newHash);
        }
    }

    public static class NodeResponse extends BaseNodeResponse {

        private String message;

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            message = in.readOptionalString();
        }

        public NodeResponse(DiscoveryNode node, String message) {
            super(node);
            this.message = message;
        }

        public static NodeResponse readNodeResponse(StreamInput in) throws IOException {
            return new NodeResponse(in);
        }

        public String getMessage() {
            return message;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(message);
        }

        @Override
        public String toString() {
            return "NodeResponse [message=" + message + "]";
        }

    }

    public static class TransportAction extends TransportNodesAction<Request, Response, NodeRequest, NodeResponse, Void> {

        private final SessionService sessionService;

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                ActionFilters actionFilters, SessionService sessionService) {
            super(PushSessionTokenUpdateAction.NAME, clusterService, transportService, actionFilters, NodeRequest::new,
                    threadPool.executor(ThreadPool.Names.MANAGEMENT));

            this.sessionService = sessionService;
        }

        @Override
        protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
            return new NodeResponse(in);
        }

        @Override
        protected Response newResponse(Request request, List<NodeResponse> responses, List<FailedNodeException> failures) {
            return new Response(this.clusterService.getClusterName(), responses, failures);

        }

        @Override
        protected NodeResponse nodeOperation(NodeRequest request, Task task) {
            String status;

            status = sessionService.pushAuthTokenUpdate(new Request(request.updatedToken, request.updateType, request.newHash));

            return new NodeResponse(clusterService.localNode(), status);
        }

        @Override
        protected NodeRequest newNodeRequest(Request request) {
            return new NodeRequest(request);
        }
    }

}
