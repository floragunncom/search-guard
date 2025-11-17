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

package com.floragunn.searchguard.configuration.variables;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

public class ConfigVarRefreshAction extends ActionType<ConfigVarRefreshAction.Response> {
    private final static Logger log = LogManager.getLogger(ConfigVarRefreshAction.class);

    public static final ConfigVarRefreshAction INSTANCE = new ConfigVarRefreshAction();
    public static final String NAME = "cluster:admin:searchguard:config_vars/refresh";

    protected ConfigVarRefreshAction() {
        super(NAME);
    }

    public static void send(Client client) {
        log.trace("Sending ConfigVarRefreshAction.Request");
        client.execute(ConfigVarRefreshAction.INSTANCE, new Request(), new ActionListener<Response>() {

            @Override
            public void onResponse(Response response) {
                log.debug("Result of settings update:\n" + response);

            }

            @Override
            public void onFailure(Exception e) {
                log.error("settings update failed", e);
            }
        });
    }

    public static void send(Client client, ActionListener<Response> actionListener) {
        log.trace("Sending SecretsRefreshAction.Request");
        client.execute(ConfigVarRefreshAction.INSTANCE, new Request(), actionListener);
    }

    public static class Request extends BaseNodesRequest {

        public Request() {
            super((String[]) null);
        }
    }

    public static class Response extends BaseNodesResponse<TransportAction.NodeResponse> {

        public Response(ClusterName clusterName, List<TransportAction.NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public List<TransportAction.NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readCollectionAsList(TransportAction.NodeResponse::readNodeResponse);
        }

        @Override
        public void writeNodesTo(StreamOutput out, List<TransportAction.NodeResponse> nodes) throws IOException {
            out.writeCollection(nodes);
        }

        @Override
        public String toString() {
            return "SecretsUpdateResponse [failures=" + failures() + ", nodes=" + getNodesMap() + "]";
        }

    }

    public static class TransportAction extends TransportNodesAction<Request, Response, TransportAction.NodeRequest, TransportAction.NodeResponse, Void> {

        private final ConfigVarService configVarService;

        @Inject
        public TransportAction(ConfigVarService configVarService, ThreadPool threadPool, ClusterService clusterService,
                TransportService transportService, ActionFilters actionFilters) {
            super(ConfigVarRefreshAction.NAME, clusterService, transportService, actionFilters,
                    TransportAction.NodeRequest::new, threadPool.executor(ThreadPool.Names.MANAGEMENT));
            this.configVarService = configVarService;
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
        protected NodeResponse nodeOperation(final NodeRequest request, Task task) {
            DiscoveryNode localNode = clusterService.localNode();

            try {
                Map<String, Object> configVars = configVarService.refresh();

                return new NodeResponse(localNode, NodeResponse.Status.SUCCESS, "");
            } catch (Exception e) {
                log.error("Error while updating config vars", e);
                throw e;
            }
        }

        public static class NodeRequest extends AbstractTransportRequest {

            public NodeRequest(StreamInput in) throws IOException {
                super(in);
            }

            public NodeRequest(Request request) {
                super();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
            }
        }

        public static class NodeResponse extends BaseNodeResponse {

            private Status status;
            private String message;

            public NodeResponse(StreamInput in) throws IOException {
                super(in);
                status = in.readEnum(Status.class);
                message = in.readOptionalString();
            }

            public NodeResponse(final DiscoveryNode node, Status status, String message) {
                super(node);
                this.status = status;
                this.message = message;
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

            public static NodeResponse readNodeResponse(StreamInput in) throws IOException {
                NodeResponse result = new NodeResponse(in);
                return result;
            }

            public static enum Status {
                SUCCESS, EXCEPTION
            }
        }

        @Override
        protected NodeRequest newNodeRequest(Request request) {
            return new NodeRequest(request);
        }

    }

}
