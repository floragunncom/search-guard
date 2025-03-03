package com.floragunn.aim.api.rest;

import com.floragunn.aim.AutomatedIndexManagement;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests;
import com.floragunn.searchsupport.action.StandardResponse;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.ActionPlugin;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class PolicyInstanceAPI {
    public static final RestApi REST = new RestApi().name("/_aim/policyinstance").handlesGet("/_aim/policyinstance/{index}/state")
            .with(GetState.INSTANCE, (requestUrlParams, requestBody) -> new StandardRequests.IdRequest(requestUrlParams.get("index")))
            .handlesPost("/_aim/policyinstance/{index}/execute")
            .with(PostExecute.INSTANCE, (requestUrlParams, requestBody) -> new PostExecute.Request(requestUrlParams.get("index"), false))
            .handlesPost("/_aim/policyinstance/{index}/execute/{retry}")
            .with(PostExecute.INSTANCE,
                    (requestUrlParams, requestBody) -> new PostExecute.Request(requestUrlParams.get("index"),
                            Boolean.getBoolean(requestUrlParams.get("retry"))))
            .handlesPost("/_aim/policyinstance/{index}/retry")
            .with(PostRetry.INSTANCE, (requestUrlParams, requestBody) -> new StandardRequests.IdRequest(requestUrlParams.get("index")));
    public static final List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> HANDLERS = ImmutableList.of(
            new ActionPlugin.ActionHandler<>(GetState.INSTANCE, GetState.Handler.class),
            new ActionPlugin.ActionHandler<>(PostExecute.INSTANCE, PostExecute.Handler.class),
            new ActionPlugin.ActionHandler<>(PostRetry.INSTANCE, PostRetry.Handler.class));

    public static class GetState extends Action<StandardRequests.IdRequest, StandardResponse> {
        public static final String NAME = "cluster:admin:searchguard:aim:policy:instance:state/get";
        public static final GetState INSTANCE = new GetState();

        public GetState() {
            super(NAME, StandardRequests.IdRequest::new, StandardResponse::new);
        }

        public static class Handler extends Action.Handler<StandardRequests.IdRequest, StandardResponse> {
            private final AutomatedIndexManagement aim;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, AutomatedIndexManagement aim) {
                super(INSTANCE, handlerDependencies);
                this.aim = aim;
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(StandardRequests.IdRequest request) {
                return aim.getPolicyInstanceService().getStateAsync(request.getId()).thenApply(response -> {
                    if (response.isExists()) {
                        return new StandardResponse(200).data(response.getSource());
                    }
                    return new StandardResponse(404).message("Not found");
                });
            }
        }
    }

    public static class PostExecute extends Action<PostExecute.Request, StandardResponse> {
        public static final String NAME = "cluster:admin:searchguard:aim:policy:instance:execute/post";
        public static final PostExecute INSTANCE = new PostExecute();

        public PostExecute() {
            super(NAME, Request::new, StandardResponse::new);
        }

        public static class Request extends StandardRequests.IdRequest {
            private final boolean retry;

            public Request(String index, boolean retry) {
                super(index);
                this.retry = retry;
            }

            public Request(UnparsedMessage message) throws ConfigValidationException {
                super(message);
                retry = message.requiredDocNode().getBoolean("retry");
            }

            @Override
            public Object toBasicObject() {
                return ImmutableMap.of("id", getId(), "retry", retry);
            }

            public boolean isRetry() {
                return retry;
            }
        }

        public static class Handler extends Action.Handler<Request, StandardResponse> {
            private final AutomatedIndexManagement aim;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, AutomatedIndexManagement aim) {
                super(INSTANCE, handlerDependencies);
                this.aim = aim;
            }

            @Override
            public CompletableFuture<StandardResponse> doExecute(Request request) {
                if (!aim.getAimSettings().getDynamic().getActive()) {
                    CompletableFuture<StandardResponse> result = new CompletableFuture<>();
                    result.complete(new StandardResponse(503).message("AIM is inactive"));
                    return result;
                }
                return aim.getPolicyInstanceService().postExecuteRetryAsync(request.getId(), true, request.isRetry()).thenApply(response -> {
                    if (response.isSuccessful()) {
                        return new StandardResponse(200);
                    }
                    return new StandardResponse(404).message("Not found");
                });
            }
        }
    }

    public static class PostRetry extends Action<StandardRequests.IdRequest, StandardResponse> {
        public static final String NAME = "cluster:admin:searchguard:aim:policy:instance:retry/post";
        public static final PostRetry INSTANCE = new PostRetry();

        public PostRetry() {
            super(NAME, StandardRequests.IdRequest::new, StandardResponse::new);
        }

        public static class Handler extends Action.Handler<StandardRequests.IdRequest, StandardResponse> {
            private final AutomatedIndexManagement aim;

            @Inject
            public Handler(HandlerDependencies handlerDependencies, AutomatedIndexManagement aim) {
                super(INSTANCE, handlerDependencies);
                this.aim = aim;
            }

            @Override
            public CompletableFuture<StandardResponse> doExecute(StandardRequests.IdRequest request) {
                if (!aim.getAimSettings().getDynamic().getActive()) {
                    CompletableFuture<StandardResponse> result = new CompletableFuture<>();
                    result.complete(new StandardResponse(503).message("AIM is inactive"));
                    return result;
                }
                return aim.getPolicyInstanceService().postExecuteRetryAsync(request.getId(), false, true).thenApply(response -> {
                    if (response.isSuccessful()) {
                        return new StandardResponse(200);
                    }
                    return new StandardResponse(404).message("Not found");
                });
            }
        }
    }
}
