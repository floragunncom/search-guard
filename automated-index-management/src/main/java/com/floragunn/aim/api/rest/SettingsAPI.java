package com.floragunn.aim.api.rest;

import com.floragunn.aim.AutomatedIndexManagement;
import com.floragunn.aim.AutomatedIndexManagementSettings;
import com.floragunn.aim.api.internal.InternalSettingsAPI;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests;
import com.floragunn.searchsupport.action.StandardResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.ActionPlugin;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SettingsAPI {
    public static final List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> HANDLERS = ImmutableList.of(
            new ActionPlugin.ActionHandler<>(Delete.INSTANCE, Delete.Handler.class),
            new ActionPlugin.ActionHandler<>(Get.INSTANCE, Get.Handler.class), new ActionPlugin.ActionHandler<>(Put.INSTANCE, Put.Handler.class));
    public static final RestApi REST = new RestApi().name("/_aim/settings").handlesDelete("/_aim/settings/{key}")
            .with(Delete.INSTANCE, (requestUrlParams, requestBody) -> new StandardRequests.IdRequest(requestUrlParams.get("key")))
            .handlesGet("/_aim/settings/{key}")
            .with(Get.INSTANCE, (requestUrlParams, requestBody) -> new StandardRequests.IdRequest(requestUrlParams.get("key")))
            .handlesPut("/_aim/settings/{key}")
            .with(Put.INSTANCE, (requestUrlParams, requestBody) -> new Put.Request(requestUrlParams.get("key"), requestBody));

    public static class Delete extends Action<StandardRequests.IdRequest, StandardResponse> {
        public static final Delete INSTANCE = new Delete();
        public static final String NAME = "cluster:admin:searchguard:aim:settings/delete";

        private Delete() {
            super(NAME, StandardRequests.IdRequest::new, StandardResponse::new);
        }

        public static class Handler extends Action.Handler<StandardRequests.IdRequest, StandardResponse> {
            private final PrivilegedConfigClient client;

            @Inject
            public Handler(Client client, HandlerDependencies handlerDependencies) {
                super(INSTANCE, handlerDependencies);
                this.client = PrivilegedConfigClient.adapt(client);
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(StandardRequests.IdRequest request) {
                CompletableFuture<StandardResponse> result = new CompletableFuture<>();
                AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?> attribute = AutomatedIndexManagementSettings.Dynamic
                        .findAvailableSettingByKey(request.getId());
                if (attribute == null) {
                    result.complete(new StandardResponse(404).message("Unknown setting"));
                } else {
                    client.execute(InternalSettingsAPI.Update.INSTANCE,
                            new InternalSettingsAPI.Update.Request(ImmutableMap.empty(), ImmutableList.of(attribute)), new ActionListener<>() {
                                @Override
                                public void onResponse(InternalSettingsAPI.Update.Response response) {
                                    if (response.hasRefreshFailures()) {
                                        result.complete(new StandardResponse(500).message("Failed to update settings on all nodes"));
                                    } else if (response.hasFailedAttributes()) {
                                        result.complete(new StandardResponse(500)
                                                .message("Failed to update settings " + Arrays.toString(response.getFailedAttributes().stream()
                                                        .map(AutomatedIndexManagementSettings.Dynamic.DynamicAttribute::getName).toArray())));
                                    } else {
                                        result.complete(new StandardResponse(200));
                                    }
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    result.completeExceptionally(e);
                                }
                            });
                }
                return result;
            }
        }
    }

    public static class Get extends Action<StandardRequests.IdRequest, StandardResponse> {
        public static final Get INSTANCE = new Get();
        public static final String NAME = "cluster:admin:searchguard:aim:settings/get";

        private Get() {
            super(NAME, StandardRequests.IdRequest::new, StandardResponse::new);
        }

        public static class Handler extends Action.Handler<StandardRequests.IdRequest, StandardResponse> {
            private final AutomatedIndexManagementSettings settings;

            @Inject
            public Handler(AutomatedIndexManagement aim, HandlerDependencies handlerDependencies) {
                super(INSTANCE, handlerDependencies);
                settings = aim.getAimSettings();
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(StandardRequests.IdRequest request) {
                return supplyAsync(() -> {
                    AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?> attribute = AutomatedIndexManagementSettings.Dynamic
                            .findAvailableSettingByKey(request.getId());
                    if (attribute == null) {
                        return new StandardResponse(404).message("Unknown setting");
                    } else {
                        return new StandardResponse(200).data(attribute.toBasicObject(settings.getDynamic().get(attribute)));
                    }
                });
            }
        }
    }

    public static class Put extends Action<Put.Request, StandardResponse> {
        public static final Put INSTANCE = new Put();
        public static final String NAME = "cluster:admin:searchguard:aim:settings/put";

        public Put() {
            super(NAME, Request::new, StandardResponse::new);
        }

        public static class Request extends StandardRequests.IdRequest {
            private final UnparsedDocument<?> value;

            public Request(String id, UnparsedDocument<?> value) throws DocumentParseException {
                super(id);
                this.value = value;
            }

            public Request(Action.UnparsedMessage message) throws ConfigValidationException {
                super(message);
                value = message.requiredUnparsedDoc();
            }

            @Override
            public Object toBasicObject() {
                return ImmutableMap.of("id", getId(), "value", value);
            }
        }

        public static class Handler extends Action.Handler<Request, StandardResponse> {
            private final PrivilegedConfigClient client;

            @Inject
            public Handler(Client client, HandlerDependencies handlerDependencies) {
                super(INSTANCE, handlerDependencies);
                this.client = PrivilegedConfigClient.adapt(client);
            }

            @Override
            protected CompletableFuture<StandardResponse> doExecute(Request request) {
                CompletableFuture<StandardResponse> result = new CompletableFuture<>();
                AutomatedIndexManagementSettings.Dynamic.DynamicAttribute<?> attribute = AutomatedIndexManagementSettings.Dynamic
                        .findAvailableSettingByKey(request.getId());
                if (attribute == null) {
                    result.complete(new StandardResponse(404).message("Unknown setting"));
                } else {
                    try {
                        Object value = request.value.parse();
                        attribute.validate(value);
                        client.execute(InternalSettingsAPI.Update.INSTANCE,
                                new InternalSettingsAPI.Update.Request(ImmutableMap.of(attribute, value), ImmutableList.empty()),
                                new ActionListener<>() {
                                    @Override
                                    public void onResponse(InternalSettingsAPI.Update.Response response) {
                                        if (response.hasRefreshFailures()) {
                                            result.complete(new StandardResponse(500).message("Failed to update settings on all nodes"));
                                        } else if (response.hasFailedAttributes()) {
                                            result.complete(new StandardResponse(500)
                                                    .message("Failed to update settings " + Arrays.toString(response.getFailedAttributes().stream()
                                                            .map(AutomatedIndexManagementSettings.Dynamic.DynamicAttribute::getName).toArray())));
                                        } else {
                                            result.complete(new StandardResponse(200));
                                        }
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        result.completeExceptionally(e);
                                    }
                                });
                    } catch (ConfigValidationException e) {
                        result.complete(new StandardResponse(400).error(e));
                    }
                }
                return result;
            }
        }
    }
}
