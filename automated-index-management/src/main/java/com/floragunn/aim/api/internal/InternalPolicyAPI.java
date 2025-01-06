package com.floragunn.aim.api.internal;

import com.floragunn.aim.AutomatedIndexManagement;
import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class InternalPolicyAPI {
    public static final List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> HANDLERS = ImmutableList.of(
            new ActionPlugin.ActionHandler<>(Delete.INSTANCE, Delete.Handler.class),
            new ActionPlugin.ActionHandler<>(Put.INSTANCE, Put.Handler.class),
            new ActionPlugin.ActionHandler<>(Refresh.INSTANCE, Refresh.Handler.class));

    public static class Delete extends ActionType<StatusResponse> {
        public static final String NAME = "cluster:admin:searchguard:aim:internal:policy/delete";
        public static final Delete INSTANCE = new Delete();

        private Delete() {
            super(NAME);
        }

        public static class Request extends ActionRequest {
            private final String policyName;
            private final boolean force;

            public Request(String policyName, boolean force) {
                super();
                this.policyName = policyName;
                this.force = force;
            }

            public Request(StreamInput in) throws IOException {
                super(in);
                policyName = in.readString();
                force = in.readBoolean();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeString(policyName);
                out.writeBoolean(force);
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
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
                return force == request.force && Objects.equals(policyName, request.policyName);
            }

            public String getPolicyName() {
                return policyName;
            }

            public boolean isForce() {
                return force;
            }
        }

        public static class Handler extends HandledTransportAction<Request, StatusResponse> {
            private final Client client;

            @Inject
            public Handler(Client client, ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters) {
                super(NAME, transportService, actionFilters, Request::new, threadPool.executor(ThreadPool.Names.MANAGEMENT));
                this.client = PrivilegedConfigClient.adapt(client);
            }

            @Override
            protected void doExecute(Task task, Request request, ActionListener<StatusResponse> listener) {
                checkForActiveStates(client, request.getPolicyName(), new ActionListener<>() {

                    @Override
                    public void onResponse(Boolean activeStateExists) {
                        if (activeStateExists && !request.isForce()) {
                            listener.onResponse(new StatusResponse(RestStatus.PRECONDITION_FAILED));
                        } else {
                            client.prepareDelete().setIndex(AutomatedIndexManagementSettings.ConfigIndices.POLICIES_NAME)
                                    .setId(request.getPolicyName()).execute(new ActionListener<>() {
                                        @Override
                                        public void onResponse(DeleteResponse deleteResponse) {
                                            if (activeStateExists && deleteResponse.getResult() == DeleteResponse.Result.DELETED) {
                                                client.execute(Refresh.INSTANCE, new Refresh.Request(ImmutableList.of(request.getPolicyName()),
                                                        ImmutableList.empty(), ImmutableList.empty()), new ActionListener<>() {
                                                            @Override
                                                            public void onResponse(Refresh.Response response) {
                                                                listener.onResponse(new StatusResponse(deleteResponse.status()));
                                                            }

                                                            @Override
                                                            public void onFailure(Exception e) {
                                                                listener.onFailure(e);
                                                            }
                                                        });
                                            } else {
                                                listener.onResponse(new StatusResponse(deleteResponse.status()));
                                            }
                                        }

                                        @Override
                                        public void onFailure(Exception e) {
                                            listener.onFailure(e);
                                        }
                                    });
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
            }
        }
    }

    public static class Put extends ActionType<StatusResponse> {
        public static final String NAME = "cluster:admin:searchguard:aim:internal:policy/put";
        public static final Put INSTANCE = new Put();

        private Put() {
            super(NAME);
        }

        public static class Request extends ActionRequest {
            private final String policyName;
            private final Policy policy;
            private final boolean force;

            public Request(String policyName, Policy policy, boolean force) {
                super();
                this.policyName = policyName;
                this.policy = policy;
                this.force = force;
            }

            public Request(StreamInput in) throws IOException {
                super(in);
                policyName = in.readString();
                try {
                    policy = Policy.parse(DocNode.wrap(in.readGenericMap()), Policy.ParsingContext.lenient(AutomatedIndexManagement.SCHEDULE_FACTORY,
                            AutomatedIndexManagement.CONDITION_FACTORY, AutomatedIndexManagement.ACTION_FACTORY));
                } catch (ConfigValidationException e) {
                    throw new IOException(e);
                }
                force = in.readBoolean();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeString(policyName);
                out.writeGenericMap(policy.toDocNode());
                out.writeBoolean(force);
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
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
                return force == request.force && Objects.equals(policy, request.policy) && Objects.equals(policyName, request.policyName);
            }

            public String getPolicyName() {
                return policyName;
            }

            public Policy getPolicy() {
                return policy;
            }

            public boolean isForce() {
                return force;
            }
        }

        public static class Handler extends HandledTransportAction<Request, StatusResponse> {
            private final Client client;

            @Inject
            public Handler(Client client, ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters) {
                super(NAME, transportService, actionFilters, Request::new, threadPool.executor(ThreadPool.Names.MANAGEMENT));
                this.client = PrivilegedConfigClient.adapt(client);
            }

            @Override
            protected void doExecute(Task task, Request request, ActionListener<StatusResponse> listener) {
                checkForActiveStates(client, request.getPolicyName(), new ActionListener<>() {
                    @Override
                    public void onResponse(Boolean activeStateExists) {
                        if (activeStateExists && !request.isForce()) {
                            listener.onResponse(new StatusResponse(RestStatus.PRECONDITION_FAILED));
                        } else {
                            client.prepareIndex().setIndex(AutomatedIndexManagementSettings.ConfigIndices.POLICIES_NAME)
                                    .setId(request.getPolicyName()).setSource(request.getPolicy().toDocNode())
                                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).execute(new ActionListener<>() {
                                        @Override
                                        public void onResponse(DocWriteResponse docWriteResponse) {
                                            if (docWriteResponse.status() == RestStatus.CREATED || docWriteResponse.status() == RestStatus.OK) {
                                                Refresh.Request refreshRequest;
                                                if (activeStateExists) {
                                                    refreshRequest = new Refresh.Request(ImmutableList.empty(), ImmutableList.empty(),
                                                            ImmutableList.of(request.getPolicyName()));
                                                } else {
                                                    refreshRequest = new Refresh.Request(ImmutableList.empty(),
                                                            ImmutableList.of(request.getPolicyName()), ImmutableList.empty());
                                                }
                                                client.execute(Refresh.INSTANCE, refreshRequest, new ActionListener<>() {
                                                    @Override
                                                    public void onResponse(Refresh.Response response) {
                                                        listener.onResponse(new StatusResponse(docWriteResponse.status()));
                                                    }

                                                    @Override
                                                    public void onFailure(Exception e) {
                                                        listener.onFailure(e);
                                                    }
                                                });
                                            } else {
                                                listener.onResponse(new StatusResponse(docWriteResponse.status()));
                                            }
                                        }

                                        @Override
                                        public void onFailure(Exception e) {
                                            listener.onFailure(e);
                                        }
                                    });
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
            }
        }
    }

    public static class Refresh extends ActionType<Refresh.Response> {
        public static final String NAME = "cluster:admin:searchguard:aim:internal:policy/refresh";
        public static final Refresh INSTANCE = new Refresh();

        private Refresh() {
            super(NAME);
        }

        public static class Request extends BaseNodesRequest<Request> {
            private final List<String> deletedPolicies;
            private final List<String> createdPolicies;
            private final List<String> updatedPolicies;

            public Request(List<String> deletedPolicies, List<String> createdPolicies, List<String> updatedPolicies) {
                super(new String[] {});
                this.deletedPolicies = deletedPolicies;
                this.createdPolicies = createdPolicies;
                this.updatedPolicies = updatedPolicies;
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
                return Objects.equals(deletedPolicies, request.deletedPolicies) && Objects.equals(createdPolicies, request.createdPolicies)
                        && Objects.equals(updatedPolicies, request.updatedPolicies);
            }

            public List<String> getDeletedPolicies() {
                return deletedPolicies;
            }

            public List<String> getCreatedPolicies() {
                return createdPolicies;
            }

            public List<String> getUpdatedPolicies() {
                return updatedPolicies;
            }

            public static class Node extends TransportRequest {
                private final List<String> deletedPolicies;
                private final List<String> createdPolicies;
                private final List<String> updatedPolicies;

                public Node(Request request) {
                    deletedPolicies = request.getDeletedPolicies();
                    createdPolicies = request.getCreatedPolicies();
                    updatedPolicies = request.getUpdatedPolicies();
                }

                public Node(StreamInput in) throws IOException {
                    super(in);
                    deletedPolicies = in.readStringCollectionAsImmutableList();
                    createdPolicies = in.readStringCollectionAsImmutableList();
                    updatedPolicies = in.readStringCollectionAsImmutableList();
                }

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    super.writeTo(out);
                    out.writeStringCollection(deletedPolicies);
                    out.writeStringCollection(createdPolicies);
                    out.writeStringCollection(updatedPolicies);
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
                    return Objects.equals(deletedPolicies, node.deletedPolicies) && Objects.equals(createdPolicies, node.createdPolicies)
                            && Objects.equals(updatedPolicies, node.updatedPolicies);
                }

                public List<String> getDeletedPolicies() {
                    return deletedPolicies;
                }

                public List<String> getCreatedPolicies() {
                    return createdPolicies;
                }

                public List<String> getUpdatedPolicies() {
                    return updatedPolicies;
                }
            }
        }

        public static class Response extends BaseNodesResponse<Response.Node> {
            public Response(ClusterName clusterName, List<Node> nodeResponses, List<FailedNodeException> failed) {
                super(clusterName, nodeResponses, failed);
            }

            public Response(StreamInput in) throws IOException {
                super(in);
            }

            @Override
            public List<Node> readNodesFrom(StreamInput in) throws IOException {
                return in.readCollectionAsImmutableList(Node::new);
            }

            @Override
            public void writeNodesTo(StreamOutput out, List<Node> nodes) throws IOException {
                out.writeCollection(nodes);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                return o != null && getClass() == o.getClass();
            }

            public static class Node extends BaseNodeResponse {
                public Node(DiscoveryNode node) {
                    super(node);
                }

                public Node(StreamInput in) throws IOException {
                    super(in);
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
                aim.getPolicyInstanceManager().handlePolicyUpdates(request.getDeletedPolicies(), request.getCreatedPolicies(),
                        request.getUpdatedPolicies());
                return new Response.Node(clusterService.localNode());
            }
        }
    }

    public static class StatusResponse extends ActionResponse {
        private final RestStatus restStatus;

        public StatusResponse(RestStatus restStatus) {
            this.restStatus = restStatus;
        }

        public StatusResponse(StreamInput streamInput) throws IOException {
            restStatus = streamInput.readEnum(RestStatus.class);
        }

        public RestStatus status() {
            return restStatus;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(restStatus);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StatusResponse that = (StatusResponse) o;
            return restStatus == that.restStatus;
        }
    }

    private static void checkForActiveStates(Client client, String policyName, ActionListener<Boolean> listener) {
        client.prepareSearch(AutomatedIndexManagementSettings.ConfigIndices.POLICY_INSTANCE_STATES_NAME).setQuery(QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(PolicyInstanceState.POLICY_NAME_FIELD, policyName)).mustNot(QueryBuilders.termsQuery(
                        PolicyInstanceState.STATUS_FIELD, PolicyInstanceState.Status.DELETED.name(), PolicyInstanceState.Status.NOT_STARTED.name())))
                .execute(new ActionListener<>() {

                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        boolean res = searchResponse.getHits().getTotalHits() == null || searchResponse.getHits().getTotalHits().value > 0;
                        listener.onResponse(res);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
    }
}
