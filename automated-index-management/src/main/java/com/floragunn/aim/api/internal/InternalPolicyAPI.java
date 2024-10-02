package com.floragunn.aim.api.internal;

import com.floragunn.aim.AutomatedIndexManagement;
import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.actions.Action;
import com.floragunn.aim.policy.conditions.Condition;
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
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class InternalPolicyAPI {
    public final static List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> HANDLERS = ImmutableList.of(
            new ActionPlugin.ActionHandler<>(Delete.INSTANCE, Delete.Handler.class),
            new ActionPlugin.ActionHandler<>(Put.INSTANCE, Put.Handler.class));

    public static class Delete extends ActionType<StatusResponse> {
        public final static String NAME = "cluster:admin:searchguard:aim:internal:policy/delete";
        public final static Delete INSTANCE = new Delete();

        private Delete() {
            super(NAME);
        }

        public static class Request extends BaseRequest<Request> {

            public Request(String policyName, boolean force) {
                super(policyName, force);
            }

            public Request(StreamInput input) throws IOException {
                super(input);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
            }
        }

        public static class Handler extends TransportMasterNodeAction<Request, StatusResponse> {
            private final AutomatedIndexManagement aim;
            private final PrivilegedConfigClient client;

            @Inject
            public Handler(AutomatedIndexManagement aim, Client client, TransportService transportService, ClusterService clusterService,
                    ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
                super(NAME, transportService, clusterService, threadPool, actionFilters, Request::new, indexNameExpressionResolver,
                        StatusResponse::new, threadPool.generic());
                this.aim = aim;
                this.client = PrivilegedConfigClient.adapt(client);
            }

            @Override
            protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<StatusResponse> listener) {
                if (aim.getPolicyInstanceService().activeStateExistsForPolicy(request.getPolicyName())) {
                    if (request.isForce()) {
                        aim.getPolicyInstanceHandler().handlePoliciesDelete(ImmutableList.of(request.getPolicyName()));
                    } else {
                        listener.onResponse(new StatusResponse(RestStatus.PRECONDITION_FAILED));
                        return;
                    }
                }
                client.prepareDelete().setIndex(AutomatedIndexManagementSettings.ConfigIndices.POLICIES_NAME).setId(request.getPolicyName())
                        .execute(new ActionListener<>() {
                            @Override
                            public void onResponse(DeleteResponse deleteResponse) {
                                listener.onResponse(new StatusResponse(deleteResponse.status()));
                            }

                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }
                        });
            }

            @Override
            protected ClusterBlockException checkBlock(Request request, ClusterState state) {
                return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
            }
        }
    }

    public static class Put extends ActionType<StatusResponse> {
        public final static String NAME = "cluster:admin:searchguard:aim:internal:policy/put";
        public final static Put INSTANCE = new Put();

        private Put() {
            super(NAME);
        }

        public static class Request extends BaseRequest<Request> {
            private final Policy policy;

            public Request(String policyName, Policy policy, boolean force) {
                super(policyName, force);
                this.policy = policy;
            }

            public Request(StreamInput input, Condition.Factory conditionFactory, Action.Factory actionFactory) throws IOException {
                super(input);
                try {
                    policy = Policy.parse(DocNode.wrap(input.readGenericMap()), Policy.ParsingContext.lenient(conditionFactory, actionFactory));
                } catch (ConfigValidationException e) {
                    throw new IOException(e);
                }
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeGenericMap(policy.toDocNode());
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
                return Objects.equals(policy, request.policy);
            }

            public Policy getPolicy() {
                return policy;
            }
        }

        public static class Handler extends TransportMasterNodeAction<Request, StatusResponse> {
            private final AutomatedIndexManagement aim;
            private final PrivilegedConfigClient client;

            @Inject
            public Handler(AutomatedIndexManagement aim, Client client, TransportService transportService, ClusterService clusterService,
                    ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
                super(NAME, transportService, clusterService, threadPool, actionFilters,
                        in -> new Request(in, aim.getConditionFactory(), aim.getActionFactory()), indexNameExpressionResolver, StatusResponse::new,
                        threadPool.generic());
                this.aim = aim;
                this.client = PrivilegedConfigClient.adapt(client);
            }

            @Override
            protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<StatusResponse> listener) {
                if (aim.getPolicyInstanceService().activeStateExistsForPolicy(request.getPolicyName())) {
                    if (request.isForce()) {
                        aim.getPolicyInstanceHandler().handlePoliciesDelete(ImmutableList.of(request.getPolicyName()));
                    } else {
                        listener.onResponse(new StatusResponse(RestStatus.PRECONDITION_FAILED));
                        return;
                    }
                }
                client.prepareIndex().setIndex(AutomatedIndexManagementSettings.ConfigIndices.POLICIES_NAME).setId(request.getPolicyName())
                        .setSource(request.getPolicy().toDocNode()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .execute(new ActionListener<>() {
                            @Override
                            public void onResponse(DocWriteResponse docWriteResponse) {
                                if (docWriteResponse.status() == RestStatus.CREATED || docWriteResponse.status() == RestStatus.OK) {
                                    aim.getPolicyInstanceHandler().handlePoliciesCreate(ImmutableList.of(request.getPolicyName()));
                                }
                                listener.onResponse(new StatusResponse(docWriteResponse.status()));
                            }

                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }
                        });
            }

            @Override
            protected ClusterBlockException checkBlock(Request request, ClusterState state) {
                return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
            }
        }
    }

    private abstract static class BaseRequest<RequestType extends MasterNodeRequest<RequestType>> extends MasterNodeRequest<RequestType> {
        private final String policyName;
        private final boolean force;

        public BaseRequest(String policyName, boolean force) {
            super();
            this.policyName = policyName;
            this.force = force;
        }

        public BaseRequest(StreamInput input) throws IOException {
            super(input);
            policyName = input.readString();
            force = input.readBoolean();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(policyName);
            out.writeBoolean(force);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BaseRequest<?> that = (BaseRequest<?>) o;
            return force == that.force && Objects.equals(policyName, that.policyName);
        }

        public String getPolicyName() {
            return policyName;
        }

        public boolean isForce() {
            return force;
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
}
