package com.floragunn.aim.api.internal;

import com.floragunn.aim.AutomatedIndexManagement;
import com.floragunn.fluent.collections.ImmutableList;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class InternalPolicyInstanceAPI {
    public static final List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> HANDLERS = ImmutableList
            .of(new ActionPlugin.ActionHandler<>(PostExecuteRetry.INSTANCE, PostExecuteRetry.Handler.class));

    public static class PostExecuteRetry extends ActionType<PostExecuteRetry.Response> {
        public static final String NAME = "cluster:admin:searchguard:aim:internal:policy:instance:execute:retry/post";
        public static final PostExecuteRetry INSTANCE = new PostExecuteRetry();

        public PostExecuteRetry() {
            super(NAME);
        }

        public static class Request extends BaseNodesRequest<Request> {
            private final String index;
            private final boolean execute;
            private final boolean retry;

            public Request(String index, boolean execute, boolean retry) {
                super((String[]) null);
                this.index = index;
                this.execute = execute;
                this.retry = retry;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Request request = (Request) o;
                return execute == request.execute && retry == request.retry && Objects.equals(index, request.index);
            }

            public String getIndex() {
                return index;
            }

            public boolean isExecute() {
                return execute;
            }

            public boolean isRetry() {
                return retry;
            }

            public static class Node extends TransportRequest {
                private final String index;
                private final boolean execute;
                private final boolean retry;

                public Node(Request request) {
                    super();
                    index = request.getIndex();
                    execute = request.isExecute();
                    retry = request.isRetry();
                }

                public Node(StreamInput input) throws IOException {
                    super(input);
                    index = input.readString();
                    execute = input.readBoolean();
                    retry = input.readBoolean();
                }

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    super.writeTo(out);
                    out.writeString(index);
                    out.writeBoolean(execute);
                    out.writeBoolean(retry);
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) {
                        return true;
                    }
                    if (o == null || getClass() != o.getClass()) {
                        return false;
                    }
                    Node node = (Node) o;
                    return Objects.equals(index, node.index) && execute == node.execute && retry == node.retry;
                }

                public String getIndex() {
                    return index;
                }

                public boolean isExecute() {
                    return execute;
                }

                public boolean isRetry() {
                    return retry;
                }
            }
        }

        public static class Response extends BaseNodesResponse<Response.Node> {
            private final boolean successful;

            public Response(ClusterName clusterName, List<Response.Node> nodeResponses, List<FailedNodeException> failed) {
                super(clusterName, nodeResponses, failed);
                boolean successful = false;
                for (Response.Node nodeResponse : nodeResponses) {
                    successful |= nodeResponse.isSuccessful();
                }
                this.successful = successful;
            }

            protected Response(StreamInput in) throws IOException {
                super(in);
                successful = in.readBoolean();
            }

            @Override
            protected List<Node> readNodesFrom(StreamInput in) throws IOException {
                return in.readCollectionAsImmutableList(Node::new);
            }

            @Override
            protected void writeNodesTo(StreamOutput out, List<Node> nodes) throws IOException {
                out.writeCollection(nodes);
                out.writeBoolean(successful);
            }

            public boolean isSuccessful() {
                return successful;
            }

            public static class Node extends BaseNodeResponse {
                private final boolean successful;

                public Node(DiscoveryNode discoveryNode, boolean successful) {
                    super(discoveryNode);
                    this.successful = successful;
                }

                protected Node(StreamInput input) throws IOException {
                    super(input);
                    successful = input.readBoolean();
                }

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    super.writeTo(out);
                    out.writeBoolean(successful);
                }

                public boolean isSuccessful() {
                    return successful;
                }
            }
        }

        public static class Handler extends TransportNodesAction<Request, Response, Request.Node, Response.Node, Void> {
            private final AutomatedIndexManagement aim;

            @Inject
            public Handler(AutomatedIndexManagement aim, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                    ActionFilters actionFilters) {
                super(NAME, clusterService, transportService, actionFilters, Request.Node::new, threadPool.executor(ThreadPool.Names.MANAGEMENT));
                this.aim = aim;
            }

            @Override
            protected Response newResponse(Request request, List<Response.Node> nodes, List<FailedNodeException> failures) {
                return new Response(clusterService.getClusterName(), nodes, failures);
            }

            @Override
            protected Request.Node newNodeRequest(Request request) {
                return new Request.Node(request);
            }

            @Override
            protected Response.Node newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
                return new Response.Node(in);
            }

            @Override
            protected Response.Node nodeOperation(Request.Node request, Task task) {
                boolean successful = aim.getPolicyInstanceManager().executeRetryPolicyInstance(request.getIndex(), request.isExecute(),
                        request.isRetry());
                return new Response.Node(clusterService.localNode(), successful);
            }
        }
    }
}
