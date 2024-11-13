package com.floragunn.searchguard.authc.rest;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.OrderedImmutableMap;
import com.floragunn.searchsupport.action.StandardResponse;
import com.google.common.collect.ImmutableList;

public class AuthcCacheApi {
    private static final Logger LOG = LogManager.getLogger(AuthcCacheApi.class);

    public static class RestHandler extends BaseRestHandler {
        private static final Logger LOG = LogManager.getLogger(RestHandler.class);

        @Override
        public String getName() {
            return "/_searchguard/authc/cache";
        }

        @Override
        public List<Route> routes() {
            return  ImmutableList.of(new Route(DELETE, "/_searchguard/authc/cache"));
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
            switch (request.method()) {
                case DELETE:
                    return (RestChannel channel) -> {
                        try {
                            client.execute(DeleteAction.INSTANCE, new DeleteAction.Request(),
                                    new RestToXContentListener<>(channel, DeleteAction.Response::status));
                        } catch (Exception e) {
                            LOG.error(e);
                            channel.sendResponse(new StandardResponse(e).toRestResponse());
                        }
                    };
                default:
                    return (RestChannel channel) -> new StandardResponse(405, "Method not allowed: " + request.method());
            }
        }
    }

    public static class DeleteAction extends ActionType<DeleteAction.Response> {
        public static final DeleteAction INSTANCE = new DeleteAction();
        public static final String NAME = "cluster:admin:searchguard:cache/delete";

        public DeleteAction() {
            super(NAME);
        }

        public static class Request extends BaseNodesRequest<Request> {
            protected Request() {
                super(new String[0]);
            }
        }

        public static class NodeRequest extends TransportRequest {
            protected NodeRequest() {
                super();
            }

            protected NodeRequest(StreamInput in) throws IOException {
                super(in);
            }
        }

        public static class Response extends BaseNodesResponse<NodeResponse> implements ToXContentObject, Document<Response> {
            protected Response(StreamInput in) throws IOException {
                super(in);
            }

            public Response(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
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

            public RestStatus status() {
                return hasFailures() ? RestStatus.INTERNAL_SERVER_ERROR : RestStatus.OK;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
                return xContentBuilder.value(toDeepBasicObject());
            }

            @Override
            public Object toBasicObject() {
                return OrderedImmutableMap.of(
                        "successful_nodes", getNodes().stream().map(nodeResponse -> nodeResponse.getNode().getId()).collect(Collectors.toList()),
                        "failed_nodes", failures().stream().map(FailedNodeException::nodeId).collect(Collectors.toList()));
            }
        }

        public static class NodeResponse extends BaseNodeResponse {

            protected NodeResponse(DiscoveryNode node) {
                super(node);
            }

            protected NodeResponse(StreamInput in) throws IOException {
                super(in);
            }

            public NodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
                super(in, node);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
            }
        }

        public static class TransportAction extends TransportNodesAction<Request, Response, NodeRequest, NodeResponse, Void> {
            private final AuthenticatingRestFilter authenticatingRestFilter;

            @Inject
            public TransportAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService, ActionFilters actionFilters, AuthenticatingRestFilter authenticatingRestFilter) {
                super(NAME, clusterService, transportService, actionFilters, NodeRequest::new, threadPool.executor(ThreadPool.Names.MANAGEMENT));
                this.authenticatingRestFilter = authenticatingRestFilter;
            }

            @Override
            protected Response newResponse(Request request, List<NodeResponse> nodeResponses, List<FailedNodeException> failures) {
                return new Response(clusterService.getClusterName(), nodeResponses, failures);
            }

            @Override
            protected NodeRequest newNodeRequest(Request request) {
                return new NodeRequest();
            }

            @Override
            protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
                return new NodeResponse(in, node);
            }

            @Override
            protected NodeResponse nodeOperation(NodeRequest request, Task task) {
                RestAuthenticationProcessor processor = authenticatingRestFilter.getAuthenticationProcessor();
                if (processor != null) {
                    processor.clearCaches();

                    LOG.debug("Cleaned up caches on node {}", clusterService.getNodeName());
                    return new NodeResponse(clusterService.localNode());
                }
                LOG.debug("Could not clean up caches on node {}. SearchGuard might not be initialized", clusterService.getNodeName());
                throw new IllegalStateException("SearchGuard might not be initialized");
            }
        }
    }
}
