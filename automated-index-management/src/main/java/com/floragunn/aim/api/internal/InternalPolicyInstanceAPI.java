package com.floragunn.aim.api.internal;

import com.floragunn.aim.AutomatedIndexManagement;
import com.floragunn.fluent.collections.ImmutableList;
import org.elasticsearch.action.*;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class InternalPolicyInstanceAPI {
    public final static List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> HANDLERS = ImmutableList
            .of(new ActionPlugin.ActionHandler<>(PostExecuteRetry.INSTANCE, PostExecuteRetry.Handler.class));

    public static class PostExecuteRetry extends ActionType<PostExecuteRetry.Response> {
        public static final String NAME = "cluster:admin:searchguard:aim:internal:policy:instance:execute:retry/post";
        public static final PostExecuteRetry INSTANCE = new PostExecuteRetry();

        public PostExecuteRetry() {
            super(NAME);
        }

        public static class Request extends MasterNodeRequest<Request> {
            private final String index;
            private final boolean execute;
            private final boolean retry;

            public Request(String index, boolean execute, boolean retry) {
                super();
                this.index = index;
                this.execute = execute;
                this.retry = retry;
            }

            public Request(StreamInput input) throws IOException {
                super(input);
                index = input.readString();
                execute = input.readBoolean();
                retry = input.readBoolean();
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
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
                if (this == o)
                    return true;
                if (o == null || getClass() != o.getClass())
                    return false;
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
        }

        public static class Response extends ActionResponse {
            private final boolean exists;

            public Response(boolean exists) {
                this.exists = exists;
            }

            public Response(StreamInput input) throws IOException {
                exists = input.readBoolean();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeBoolean(exists);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o)
                    return true;
                if (o == null || getClass() != o.getClass())
                    return false;
                Response response = (Response) o;
                return exists == response.exists;
            }

            public boolean isExists() {
                return exists;
            }
        }

        public static class Handler extends TransportMasterNodeAction<Request, Response> {
            private final AutomatedIndexManagement aim;

            @Inject
            public Handler(AutomatedIndexManagement aim, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                    ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
                super(NAME, transportService, clusterService, threadPool, actionFilters, Request::new, indexNameExpressionResolver, Response::new,
                        threadPool.generic());
                this.aim = aim;
            }

            @Override
            protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
                listener.onResponse(new Response(
                        aim.getPolicyInstanceHandler().executeRetryPolicyInstance(request.getIndex(), request.isExecute(), request.isRetry())));
            }

            @Override
            protected ClusterBlockException checkBlock(Request request, ClusterState state) {
                return null;
            }
        }
    }
}
