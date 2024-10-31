package com.floragunn.aim.api.rest;

import com.floragunn.aim.AutomatedIndexManagement;
import com.floragunn.aim.policy.Policy;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests;
import com.floragunn.searchsupport.action.StandardResponse;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestStatus;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class PolicyAPI {
    public static final RestApi REST = new RestApi().name("/_aim/policy").handlesDelete("/_aim/policy/{policy_name}")
            .with(Delete.INSTANCE, (requestUrlParams, requestBody) -> new StandardRequests.IdRequest(requestUrlParams.get("policy_name")))
            //todo: RestApi does not support routes with path after param. This is a workaround
            .handlesGet("/_aim/policy/{policy_name}/{internal}")
            .with(Get.INSTANCE,
                    (requestUrlParams, requestBody) -> new Get.Request(requestUrlParams.get("policy_name"),
                            requestUrlParams.get("internal").equals("internal")))
            .handlesGet("/_aim/policy/{policy_name}")
            .with(Get.INSTANCE, (requestUrlParams, requestBody) -> new Get.Request(requestUrlParams.get("policy_name"), false))
            .handlesPut("/_aim/policy/{policy_name}")
            .with(Put.INSTANCE, (requestUrlParams, requestBody) -> new Put.Request(requestUrlParams.get("policy_name"), requestBody));

    public static final List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> HANDLERS = ImmutableList.of(
            new ActionPlugin.ActionHandler<>(Delete.INSTANCE, Delete.Handler.class),
            new ActionPlugin.ActionHandler<>(Get.INSTANCE, Get.Handler.class), new ActionPlugin.ActionHandler<>(Put.INSTANCE, Put.Handler.class));

    public static class Delete extends Action<StandardRequests.IdRequest, StandardResponse> {
        public static final Delete INSTANCE = new Delete();
        public static final String NAME = "cluster:admin:searchguard:aim:policy/delete";

        private Delete() {
            super(NAME, StandardRequests.IdRequest::new, StandardResponse::new);
        }

        public static class Handler extends Action.Handler<StandardRequests.IdRequest, StandardResponse> {
            private final AutomatedIndexManagement aim;

            @Inject
            public Handler(AutomatedIndexManagement aim, HandlerDependencies handlerDependencies) {
                super(Delete.INSTANCE, handlerDependencies);
                this.aim = aim;
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(StandardRequests.IdRequest request) {
                if (!aim.getAimSettings().getDynamic().getActive()) {
                    CompletableFuture<StandardResponse> result = new CompletableFuture<>();
                    result.complete(new StandardResponse(503).message("AIM is inactive"));
                    return result;
                }
                return aim.getPolicyService().deletePolicyAsync(request.getId()).thenApply(statusResponse -> {
                    switch (statusResponse.status()) {
                    case OK:
                        return new StandardResponse(200).message("Deleted");
                    case NOT_FOUND:
                        return new StandardResponse(404).message("Not found");
                    case PRECONDITION_FAILED:
                        return new StandardResponse(412).message("Could not delete policy because it is still in use");
                    default:
                        return new StandardResponse(statusResponse.status().getStatus());
                    }
                });
            }
        }
    }

    public static class Get extends Action<Get.Request, StandardResponse> {
        public static final Get INSTANCE = new Get();
        public static final String NAME = "cluster:admin:searchguard:aim:policy/get";

        private Get() {
            super(NAME, Request::new, StandardResponse::new);
        }

        public static class Request extends StandardRequests.IdRequest {
            private final boolean showInternalSteps;

            public Request(String policyName, boolean showInternalSteps) {
                super(policyName);
                this.showInternalSteps = showInternalSteps;
            }

            public Request(Action.UnparsedMessage message) throws ConfigValidationException {
                super(message);
                showInternalSteps = message.requiredDocNode().getBoolean("showInternalSteps");
            }

            @Override
            public Object toBasicObject() {
                return ImmutableMap.of("id", getPolicyName(), "showInternalSteps", showInternalSteps);
            }

            public String getPolicyName() {
                return getId();
            }

            public boolean isShowInternalSteps() {
                return showInternalSteps;
            }
        }

        public static class Handler extends Action.Handler<Request, StandardResponse> {
            private final AutomatedIndexManagement aim;

            @Inject
            public Handler(AutomatedIndexManagement aim, HandlerDependencies handlerDependencies) {
                super(Get.INSTANCE, handlerDependencies);
                this.aim = aim;
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(Request request) {
                return aim.getPolicyService().getPolicyAsync(request.getPolicyName()).thenApply(response -> {
                    if (response.isExists()) {
                        try {
                            Policy policy = Policy.parse(DocNode.parse(Format.JSON).from(response.getSourceAsBytesRef().utf8ToString()),
                                    Policy.ParsingContext.lenient(aim.getScheduleFactory(), aim.getConditionFactory(), aim.getActionFactory()));
                            return new StandardResponse(200)
                                    .data(request.isShowInternalSteps() ? policy.toBasicObject() : policy.toBasicObjectExcludeInternal());
                        } catch (ConfigValidationException e) {
                            return new StandardResponse(500).message("Policy is malformed");
                        }
                    } else {
                        return new StandardResponse(404).message("Not found");
                    }
                });
            }
        }
    }

    public static class Put extends Action<Put.Request, StandardResponse> {
        public static final Put INSTANCE = new Put();
        public static final String NAME = "cluster:admin:searchguard:aim:policy/put";

        private Put() {
            super(NAME, Request::new, StandardResponse::new);
        }

        public static class Request extends StandardRequests.IdRequest {
            private final UnparsedDocument<?> policy;

            public Request(String policyName, UnparsedDocument<?> policy) {
                super(policyName);
                this.policy = policy;
            }

            public Request(UnparsedMessage message) throws ConfigValidationException {
                super(message);
                policy = message.requiredUnparsedDoc();
            }

            @Override
            public Object toBasicObject() {
                return ImmutableMap.of("id", getPolicyName(), "policy", policy.toBasicObject());
            }

            public String getPolicyName() {
                return getId();
            }

            public UnparsedDocument<?> getPolicy() {
                return policy;
            }
        }

        public static class Handler extends Action.Handler<Request, StandardResponse> {
            private final AutomatedIndexManagement aim;

            @Inject
            public Handler(AutomatedIndexManagement aim, HandlerDependencies handlerDependencies) {
                super(Put.INSTANCE, handlerDependencies);
                this.aim = aim;
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(Request request) {
                CompletableFuture<StandardResponse> result = new CompletableFuture<>();
                if (!aim.getAimSettings().getDynamic().getActive()) {
                    result.complete(new StandardResponse(503).message("AIM is inactive"));
                    return result;
                }
                try {
                    Policy policy = Policy.parse(request.getPolicy().parseAsDocNode(),
                            Policy.ParsingContext.strict(aim.getScheduleFactory(), aim.getConditionFactory(), aim.getActionFactory()));
                    result = aim.getPolicyService().putPolicyAsync(request.getPolicyName(), policy).thenApply(statusResponse -> {
                        if (statusResponse.status() == RestStatus.PRECONDITION_FAILED) {
                            return new StandardResponse(412).message("Could not override existing policy because it is still in use");
                        }
                        return new StandardResponse(statusResponse.status().getStatus());
                    });
                } catch (ConfigValidationException e) {
                    result.complete(new StandardResponse(400).error(e));
                }
                return result;
            }
        }
    }
}
